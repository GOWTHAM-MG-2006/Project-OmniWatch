/*
 * OmniWatch — Entity Resolution Layer
 * Component: EntityResolutionJob
 * Phase: 3
 * Purpose: Flink job entry point. Consumes the five normalized Kafka topics,
 *          runs the entity resolution pipeline
 *          (ResourceIdParser -> CloudProviderMapper -> EntityEnricher ->
 *          EntityDeduplicator) producing omniwatch.entities.resolved, and a
 *          parallel trace branch (RelationshipBuilder) producing
 *          omniwatch.entities.relationships.
 * Inputs: omniwatch.{metrics,logs,traces,events,security}.normalized (Kafka)
 * Outputs: omniwatch.entities.resolved, omniwatch.entities.relationships (Kafka)
 */
package com.omniwatch.entity;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.omniwatch.entity.config.EntityConfig;
import com.omniwatch.entity.models.TelemetryEvent;
import com.omniwatch.entity.models.TraceSpanEvent;
import com.omniwatch.entity.models.UnifiedEntity;
import com.omniwatch.entity.operators.CloudProviderMapper;
import com.omniwatch.entity.operators.EntityDeduplicator;
import com.omniwatch.entity.operators.EntityEnricher;
import com.omniwatch.entity.operators.RelationshipBuilder;
import com.omniwatch.entity.operators.ResourceIdParser;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Stream topology for the Phase 3 entity resolution layer.
 */
public final class EntityResolutionJob {

    private static final Logger LOG = LoggerFactory.getLogger(EntityResolutionJob.class);

    public static final String JOB_NAME = "OmniWatch Entity Resolution";

    /** Input topics (produced by the Phase 2 ingestion normalizer). */
    static final List<String> INPUT_TOPICS = Arrays.asList(
            "omniwatch.metrics.normalized",
            "omniwatch.logs.normalized",
            "omniwatch.traces.normalized",
            "omniwatch.events.normalized",
            "omniwatch.security.normalized"
    );

    static final String OUTPUT_TOPIC_RESOLVED = "omniwatch.entities.resolved";
    static final String OUTPUT_TOPIC_RELATIONSHIPS = "omniwatch.entities.relationships";

    private EntityResolutionJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = buildEnvironment(args);
        env.execute(JOB_NAME);
    }

    /**
     * Builds the full execution environment with the entity resolution graph.
     * Exposed for tests (graph inspection without execution).
     */
    public static StreamExecutionEnvironment buildEnvironment(String[] args) {
        JobConfig config = JobConfig.fromArgs(args);
        EntityConfig entityConfig = EntityConfig.load();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000L);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(config.kafkaBrokers)
                .setTopics(INPUT_TOPICS)
                .setGroupId(config.kafkaGroupId)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();

        ObjectMapper mapper = createMapper();

        DataStream<TelemetryEvent> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "entity-source")
                .map(json -> parseEvent(mapper, json))
                .returns(Types.POJO(TelemetryEvent.class));

        // ---- Branch A: entity resolution pipeline ----
        events
                .map(new ResourceIdParser(entityConfig))
                .map(new CloudProviderMapper())
                .map(new EntityEnricher(entityConfig))
                .keyBy(UnifiedEntity::getEntityId)
                .process(new EntityDeduplicator())
                .map(entity -> serialize(mapper, entity))
                .returns(Types.STRING)
                .sinkTo(createSink(config.kafkaBrokers, OUTPUT_TOPIC_RESOLVED))
                .name("sink-entities-resolved");

        // ---- Branch B: relationship pipeline (trace spans) ----
        events
                .filter(TelemetryEvent::isTraceSpan)
                .flatMap((TelemetryEvent event, Collector<TraceSpanEvent> out) -> {
                    TraceSpanEvent span = new TraceSpanEvent();
                    span.setEntityId(event.getEntityId());
                    span.setTraceId(event.getTraceId());
                    span.setSpanId(event.getSpanId());
                    span.setParentSpanId(event.getParentSpanId());
                    span.setDurationMs(event.getDurationMs());
                    span.setStatus(event.getStatus());
                    out.collect(span);
                })
                .returns(Types.POJO(TraceSpanEvent.class))
                .keyBy(TraceSpanEvent::getTraceId)
                .process(new RelationshipBuilder(entityConfig))
                .map(rel -> serialize(mapper, rel))
                .returns(Types.STRING)
                .sinkTo(createSink(config.kafkaBrokers, OUTPUT_TOPIC_RELATIONSHIPS))
                .name("sink-entities-relationships");

        LOG.info("Entity resolution job graph built: {} -> [{}] | [{}]",
                INPUT_TOPICS, OUTPUT_TOPIC_RESOLVED, OUTPUT_TOPIC_RELATIONSHIPS);
        return env;
    }

    static ObjectMapper createMapper() {
        return new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    }

    private static TelemetryEvent parseEvent(ObjectMapper mapper, String json) {
        try {
            return mapper.readValue(json, TelemetryEvent.class);
        } catch (Exception e) {
            LOG.error("Failed to parse telemetry event, skipping: {}", e.getMessage());
            return new TelemetryEvent();
        }
    }

    private static String serialize(ObjectMapper mapper, Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception e) {
            LOG.error("Failed to serialize {}, emitting empty record", value.getClass().getSimpleName());
            return "{}";
        }
    }

    private static KafkaSink<String> createSink(String brokers, String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    /** Runtime configuration: CLI args -> env vars -> defaults (mirrors Phase 2). */
    static final class JobConfig {
        final String kafkaBrokers;
        final String kafkaGroupId;

        private JobConfig(String kafkaBrokers, String kafkaGroupId) {
            this.kafkaBrokers = kafkaBrokers;
            this.kafkaGroupId = kafkaGroupId;
        }

        static JobConfig fromArgs(String[] args) {
            ParameterTool params = ParameterTool.fromArgs(args)
                    .mergeWith(ParameterTool.fromSystemProperties());
            String brokers = params.get("kafka.brokers",
                    System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"));
            String groupId = params.get("kafka.group.id",
                    System.getenv().getOrDefault("KAFKA_GROUP_ID", "flink-entity-resolution"));
            return new JobConfig(brokers, groupId);
        }
    }
}
