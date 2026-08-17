/*
 * OmniWatch — Entity Resolution Layer
 * Component: EntityMinIOSink
 * Phase: 3
 * Purpose: Buffers resolved entity JSON strings and periodically flushes them
 *          to MinIO as Parquet files with dt=YYYY-MM-DD-HH partitioning.
 * Inputs: Serialized UnifiedEntity JSON strings from the entity resolution pipeline
 * Outputs: Parquet files uploaded to MinIO bucket omniwatch-telemetry-archive
 */
package com.omniwatch.entity.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.omniwatch.entity.config.EntityConfig;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * RichSinkFunction that buffers resolved entity JSON strings and periodically
 * flushes them to MinIO as Parquet files. Uses the minio-java SDK directly.
 *
 * <p>Buffering strategy (mirrors Phase 2 MinIOSink):
 * <ul>
 *   <li>Flush when buffer reaches BATCH_SIZE (100 records)</li>
 *   <li>Flush when FLUSH_INTERVAL_MS has elapsed since last flush (5000ms)</li>
 *   <li>Flush on close() to ensure no data loss</li>
 * </ul>
 *
 * <p>Files are named: entities-{yyyyMMdd-HHmmss}-{uuid8}.parquet
 * and stored in: {bucket}/entity-resolution/dt={yyyy-MM-dd-HH}/
 */
public class EntityMinIOSink extends RichSinkFunction<String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EntityMinIOSink.class);

    private static final int BATCH_SIZE = 100;
    private static final long FLUSH_INTERVAL_MS = 5_000L;
    private static final DateTimeFormatter DT_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private static final Schema ENTITY_SCHEMA = Schema.createRecord(
            "EntityRecord",
            "Resolved entity record for MinIO archival",
            "com.omniwatch.entity.sink",
            false,
            Arrays.asList(
                    new Schema.Field("entity_id", Schema.create(Schema.Type.STRING)),
                    new Schema.Field("entity_type", Schema.create(Schema.Type.STRING)),
                    new Schema.Field("provider", Schema.create(Schema.Type.STRING)),
                    new Schema.Field("region", Schema.create(Schema.Type.STRING)),
                    new Schema.Field("name", Schema.create(Schema.Type.STRING)),
                    new Schema.Field("business_tags", Schema.create(Schema.Type.STRING)),
                    new Schema.Field("raw_identifiers", Schema.create(Schema.Type.STRING)),
                    new Schema.Field("first_seen", Schema.create(Schema.Type.LONG)),
                    new Schema.Field("last_seen", Schema.create(Schema.Type.LONG))
            )
    );

    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final String bucket;

    private transient MinioClient minioClient;
    private transient ObjectMapper mapper;
    private transient List<String> buffer;
    private transient long lastFlushTime;

    public EntityMinIOSink(EntityConfig config) {
        this(config.getMinioEndpoint(),
             config.getMinioAccessKey(),
             config.getMinioSecretKey(),
             config.getMinioBucket());
    }

    public EntityMinIOSink(String endpoint, String accessKey, String secretKey, String bucket) {
        this.endpoint = endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.bucket = bucket;
    }

    @Override
    public void open(Configuration parameters) {
        LOG.info("Initializing EntityMinIOSink: endpoint={}, bucket={}", endpoint, bucket);

        minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();

        mapper = new ObjectMapper();
        buffer = new ArrayList<>();
        lastFlushTime = System.currentTimeMillis();

        try {
            boolean exists = minioClient.bucketExists(
                    BucketExistsArgs.builder().bucket(bucket).build());
            if (!exists) {
                minioClient.makeBucket(
                        MakeBucketArgs.builder().bucket(bucket).build());
                LOG.info("Created MinIO bucket: {}", bucket);
            }
        } catch (Exception e) {
            LOG.warn("Failed to verify/create MinIO bucket: {}. Will retry on flush.", bucket);
        }
    }

    @Override
    public void invoke(String value, Context context) {
        if (value == null) {
            return;
        }

        synchronized (buffer) {
            buffer.add(value);
        }

        long now = System.currentTimeMillis();
        if (buffer.size() >= BATCH_SIZE || (now - lastFlushTime) >= FLUSH_INTERVAL_MS) {
            flush();
        }
    }

    private void flush() {
        List<String> batch;
        synchronized (buffer) {
            if (buffer.isEmpty()) {
                lastFlushTime = System.currentTimeMillis();
                return;
            }
            batch = new ArrayList<>(buffer);
            buffer.clear();
            lastFlushTime = System.currentTimeMillis();
        }

        try {
            byte[] parquetBytes = writeParquet(batch);

            String dtPart = LocalDateTime.now().format(DT_FORMAT);
            String tsPart = LocalDateTime.now().format(TIMESTAMP_FORMAT);
            String uuid8 = UUID.randomUUID().toString().substring(0, 8);
            String objectName = String.format(
                    "entity-resolution/dt=%s/entities-%s-%s.parquet",
                    dtPart, tsPart, uuid8);

            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucket)
                            .object(objectName)
                            .stream(new ByteArrayInputStream(parquetBytes),
                                    parquetBytes.length, -1)
                            .contentType("application/octet-stream")
                            .build());

            LOG.info("Flushed {} entities to MinIO as parquet: {}/{}",
                    batch.size(), bucket, objectName);
        } catch (Exception e) {
            LOG.error("Failed to flush {} entities to MinIO", batch.size(), e);
            synchronized (buffer) {
                buffer.addAll(batch);
            }
        }
    }

    private byte[] writeParquet(List<String> jsonBatch) throws IOException {
        InMemoryOutputFile outputFile = new InMemoryOutputFile();

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(outputFile)
                .withSchema(ENTITY_SCHEMA)
                .build()) {
            for (String json : jsonBatch) {
                GenericRecord record = jsonToRecord(json);
                if (record != null) {
                    writer.write(record);
                }
            }
        }

        return outputFile.toByteArray();
    }

    private GenericRecord jsonToRecord(String json) {
        try {
            JsonNode node = mapper.readTree(json);
            GenericRecord record = new GenericData.Record(ENTITY_SCHEMA);

            record.put("entity_id", strField(node, "entity_id"));
            record.put("entity_type", strField(node, "entity_type"));
            record.put("provider", strField(node, "provider"));
            record.put("region", strField(node, "region"));
            record.put("name", strField(node, "name"));

            JsonNode tags = node.get("business_tags");
            record.put("business_tags", tags == null || tags.isNull() ? "{}" : tags.toString());

            JsonNode ids = node.get("raw_identifiers");
            record.put("raw_identifiers", ids == null || ids.isNull() ? "[]" : ids.toString());

            record.put("first_seen", parseTimestamp(node, "first_seen"));
            record.put("last_seen", parseTimestamp(node, "last_seen"));

            return record;
        } catch (Exception e) {
            LOG.warn("Failed to convert JSON to parquet record: {}", e.getMessage());
            return null;
        }
    }

    private static String strField(JsonNode node, String fieldName) {
        JsonNode val = node.get(fieldName);
        return val == null || val.isNull() ? "" : val.asText();
    }

    private static long parseTimestamp(JsonNode node, String fieldName) {
        JsonNode val = node.get(fieldName);
        if (val == null || val.isNull()) {
            return System.currentTimeMillis();
        }
        if (val.isNumber()) {
            return val.asLong();
        }
        try {
            return Instant.parse(val.asText()).toEpochMilli();
        } catch (Exception e) {
            try {
                return Long.parseLong(val.asText());
            } catch (NumberFormatException nfe) {
                return System.currentTimeMillis();
            }
        }
    }

    @Override
    public void close() {
        LOG.info("Closing EntityMinIOSink, flushing remaining {} records", buffer.size());
        flush();
    }

    private static class InMemoryOutputFile implements OutputFile {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return new PositionOutputStream() {
                @Override
                public long getPos() {
                    return buffer.size();
                }

                @Override
                public void write(int b) {
                    buffer.write(b);
                }

                @Override
                public void write(byte[] b, int off, int len) {
                    buffer.write(b, off, len);
                }
            };
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            return create(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }

        byte[] toByteArray() {
            return buffer.toByteArray();
        }
    }
}
