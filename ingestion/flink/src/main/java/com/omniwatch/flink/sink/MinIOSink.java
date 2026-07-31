package com.omniwatch.flink.sink;

import com.omniwatch.flink.config.FlinkConfig;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * RichSinkFunction that buffers events and periodically flushes them to MinIO
 * as JSON lines (.jsonl) files. Uses the minio-java SDK directly.
 *
 * <p>Buffering strategy:
 * <ul>
 *   <li>Flush when buffer reaches BATCH_SIZE (100 events)</li>
 *   <li>Flush when FLUSH_INTERVAL_MS has elapsed since last flush (5000ms)</li>
 *   <li>Flush on close() to ensure no data loss</li>
 * </ul>
 *
 * <p>Files are named: events-{yyyyMMdd-HHmmss}-{uuid}.jsonl
 * and stored in: {bucket}/dt={yyyy-MM-dd}/
 */
public class MinIOSink extends RichSinkFunction<String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MinIOSink.class);

    private static final int BATCH_SIZE = 100;
    private static final long FLUSH_INTERVAL_MS = 5_000L;
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final String bucket;

    private transient MinioClient minioClient;
    private transient List<String> buffer;
    private transient long lastFlushTime;

    /**
     * Creates a MinIOSink from a FlinkConfig object.
     *
     * @param config the Flink configuration with MinIO settings
     */
    public MinIOSink(FlinkConfig config) {
        this(config.getMinioEndpoint(),
             config.getMinioAccessKey(),
             config.getMinioSecretKey(),
             config.getMinioBucket());
    }

    /**
     * Creates a MinIOSink with explicit MinIO connection parameters.
     *
     * @param endpoint  MinIO endpoint URL (e.g., http://minio:9010)
     * @param accessKey MinIO access key
     * @param secretKey MinIO secret key
     * @param bucket    MinIO bucket name
     */
    public MinIOSink(String endpoint, String accessKey, String secretKey, String bucket) {
        this.endpoint = endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.bucket = bucket;
    }

    @Override
    public void open(Configuration parameters) {
        LOG.info("Initializing MinIO client: endpoint={}, bucket={}", endpoint, bucket);

        minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();

        buffer = new ArrayList<>();
        lastFlushTime = System.currentTimeMillis();

        // Ensure bucket exists
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

        // Flush if buffer is full or interval has elapsed
        long now = System.currentTimeMillis();
        if (buffer.size() >= BATCH_SIZE || (now - lastFlushTime) >= FLUSH_INTERVAL_MS) {
            flush();
        }
    }

    /**
     * Flushes buffered events to MinIO as a single JSON lines file.
     */
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
            String dateStr = LocalDateTime.now().format(DATE_FORMAT);
            String tsStr = LocalDateTime.now().format(TIMESTAMP_FORMAT);
            String objectName = String.format("dt=%s/events-%s-%s.jsonl",
                    dateStr, tsStr, java.util.UUID.randomUUID().toString().substring(0, 8));

            String content = String.join("\n", batch);
            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucket)
                            .object(objectName)
                            .stream(inputStream, bytes.length, -1)
                            .contentType("application/jsonl")
                            .build());

            LOG.info("Flushed {} events to MinIO: {}/{}", batch.size(), bucket, objectName);
        } catch (Exception e) {
            LOG.error("Failed to flush {} events to MinIO", batch.size(), e);
            // Re-add failed events to buffer for retry
            synchronized (buffer) {
                buffer.addAll(batch);
            }
        }
    }

    @Override
    public void close() {
        LOG.info("Closing MinIOSink, flushing remaining {} events", buffer.size());
        flush();
    }
}
