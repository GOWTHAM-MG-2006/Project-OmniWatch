/*
 * OmniWatch — Entity Resolution Layer
 * Component: EntityResolutionJob
 * Phase: 3
 * Purpose: Flink job entry point. Consumes the five RAW Kafka topics from OTel Collector,
 *          parses OTLP JSON, runs the entity resolution pipeline
 *          (ResourceIdParser -> CloudProviderMapper -> EntityEnricher ->
 *          EntityDeduplicator) producing omniwatch.entities.resolved, and a
 *          parallel trace branch (RelationshipBuilder) producing
 *          omniwatch.entities.relationships.
 * Inputs: omniwatch.{metrics,logs,traces,events,security}.raw (Kafka) — OTLP JSON format
 * Outputs: omniwatch.entities.resolved, omniwatch.entities.relationships (Kafka)
 */
package com.omniwatch.entity;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
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
import com.omniwatch.entity.sink.EntityMinIOSink;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Stream topology for the Phase 3 entity resolution layer.
 * Consumes RAW OTLP JSON from OTel Collector and normalizes to TelemetryEvent.
 */
public final class EntityResolutionJob {

    private static final Logger LOG = LoggerFactory.getLogger(EntityResolutionJob.class);

    public static final String JOB_NAME = "OmniWatch Entity Resolution";

    /** Input topics (raw OTLP JSON from OTel Collector Phase 2). */
    static final List<String> INPUT_TOPICS = Arrays.asList(
            "omniwatch.metrics.raw",
            "omniwatch.logs.raw",
            "omniwatch.traces.raw",
            "omniwatch.events.raw",
            "omniwatch.security.raw"
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
        DataStream<UnifiedEntity> resolvedEntities = events
                .map(new ResourceIdParser(entityConfig))
                .map(new CloudProviderMapper())
                .map(new EntityEnricher(entityConfig))
                .keyBy(UnifiedEntity::getEntityId)
                .process(new EntityDeduplicator());

        resolvedEntities
                .map(entity -> serialize(mapper, entity))
                .returns(Types.STRING)
                .sinkTo(createSink(config.kafkaBrokers, OUTPUT_TOPIC_RESOLVED))
                .name("sink-entities-resolved");

        resolvedEntities
                .map(entity -> serialize(mapper, entity))
                .returns(Types.STRING)
                .addSink(new EntityMinIOSink(entityConfig))
                .name("sink-entities-minio");

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

    /**
     * Parses raw OTLP JSON from Kafka and converts to normalized TelemetryEvent.
     * Handles all 5 signal types: metrics, logs, traces, events, security.
     * OTLP JSON structure: { "resourceMetrics": [...], "resourceLogs": [...], "resourceSpans": [...] }
     */
    private static TelemetryEvent parseEvent(ObjectMapper mapper, String json) {
        try {
            JsonNode root = mapper.readTree(json);
            TelemetryEvent event = new TelemetryEvent();

            // Determine signal type from topic (passed via sourceTopic in real impl, infer from JSON structure here)
            String signalType = inferSignalType(root);
            event.setSourceType(signalType);

            // Extract common fields from resource attributes
            extractResourceAttributes(root, event, signalType);

            // Extract signal-specific data
            switch (signalType) {
                case "metrics":
                    extractMetricsData(root, event);
                    break;
                case "logs":
                    extractLogsData(root, event);
                    break;
                case "traces":
                    extractTracesData(root, event);
                    break;
                case "events":
                case "security":
                    extractGenericData(root, event);
                    break;
                default:
                    extractGenericData(root, event);
            }

            return event;
        } catch (Exception e) {
            LOG.error("Failed to parse telemetry event, skipping: {}", e.getMessage());
            return new TelemetryEvent();
        }
    }

    private static String inferSignalType(JsonNode root) {
        if (root.has("resourceMetrics")) return "metrics";
        if (root.has("resourceLogs")) return "logs";
        if (root.has("resourceSpans")) return "traces";
        return "events";
    }

    private static void extractResourceAttributes(JsonNode root, TelemetryEvent event, String signalType) {
        JsonNode resourceNode = null;
        if (signalType.equals("metrics") && root.has("resourceMetrics")) {
            JsonNode arr = root.get("resourceMetrics");
            if (arr.isArray() && arr.size() > 0) {
                resourceNode = arr.get(0).get("resource");
            }
        } else if (signalType.equals("logs") && root.has("resourceLogs")) {
            JsonNode arr = root.get("resourceLogs");
            if (arr.isArray() && arr.size() > 0) {
                resourceNode = arr.get(0).get("resource");
            }
        } else if (signalType.equals("traces") && root.has("resourceSpans")) {
            JsonNode arr = root.get("resourceSpans");
            if (arr.isArray() && arr.size() > 0) {
                resourceNode = arr.get(0).get("resource");
            }
        }

        if (resourceNode != null && resourceNode.has("attributes")) {
            Map<String, String> attrs = new HashMap<>();
            for (JsonNode attr : resourceNode.get("attributes")) {
                String key = attr.get("key").asText();
                String value = extractAttributeValue(attr.get("value"));
                attrs.put(key, value);
                // Set well-known entity identifiers
                if (key.equals("service.name")) {
                    event.setEntityId(value);
                    event.setEntityType("SERVICE");
                } else if (key.equals("service.instance.id") && event.getEntityId() == null) {
                    event.setEntityId(value);
                } else if (key.equals("cloud.provider")) {
                    event.getAttributes().put("cloud.provider", value);
                } else if (key.equals("cloud.region")) {
                    event.getAttributes().put("cloud.region", value);
                }
            }
            event.getAttributes().putAll(attrs);
        }
    }

    private static String extractAttributeValue(JsonNode valueNode) {
        if (valueNode.has("stringValue")) return valueNode.get("stringValue").asText();
        if (valueNode.has("intValue")) return valueNode.get("intValue").asText();
        if (valueNode.has("doubleValue")) return valueNode.get("doubleValue").asText();
        if (valueNode.has("boolValue")) return valueNode.get("boolValue").asText();
        return valueNode.toString();
    }

    private static void extractMetricsData(JsonNode root, TelemetryEvent event) {
        if (!root.has("resourceMetrics")) return;
        for (JsonNode rm : root.get("resourceMetrics")) {
            if (!rm.has("scopeMetrics")) continue;
            for (JsonNode sm : rm.get("scopeMetrics")) {
                if (!sm.has("metrics")) continue;
                for (JsonNode metric : sm.get("metrics")) {
                    String metricName = metric.get("name").asText();
                    event.getAttributes().put("metric.name", metricName);
                    if (metric.has("description")) {
                        event.getAttributes().put("metric.description", metric.get("description").asText());
                    }
                    if (metric.has("unit")) {
                        event.getAttributes().put("metric.unit", metric.get("unit").asText());
                    }

                    // Extract data points (sum, gauge, histogram)
                    if (metric.has("sum")) {
                        extractDataPoints(metric.get("sum"), event, metricName);
                    } else if (metric.has("gauge")) {
                        extractDataPoints(metric.get("gauge"), event, metricName);
                    } else if (metric.has("histogram")) {
                        extractDataPoints(metric.get("histogram"), event, metricName);
                    }
                }
            }
        }
    }

    private static void extractDataPoints(JsonNode aggregator, TelemetryEvent event, String metricName) {
        if (!aggregator.has("dataPoints")) return;
        for (JsonNode dp : aggregator.get("dataPoints")) {
            // Extract timestamp
            if (dp.has("timeUnixNano")) {
                event.setTimestamp(dp.get("timeUnixNano").asLong() / 1_000_000); // nanos to millis
            } else if (dp.has("startTimeUnixNano")) {
                event.setTimestamp(dp.get("startTimeUnixNano").asLong() / 1_000_000);
            }

            // Extract attributes
            if (dp.has("attributes")) {
                for (JsonNode attr : dp.get("attributes")) {
                    String key = attr.get("key").asText();
                    String value = extractAttributeValue(attr.get("value"));
                    event.getAttributes().put(key, value);
                }
            }

            // Extract value
            if (dp.has("asInt")) {
                event.getAttributes().put(metricName + ".value", dp.get("asInt").asText());
            } else if (dp.has("asDouble")) {
                event.getAttributes().put(metricName + ".value", dp.get("asDouble").asText());
            }
        }
    }

    private static void extractLogsData(JsonNode root, TelemetryEvent event) {
        if (!root.has("resourceLogs")) return;
        for (JsonNode rl : root.get("resourceLogs")) {
            if (!rl.has("scopeLogs")) continue;
            for (JsonNode sl : rl.get("scopeLogs")) {
                if (!sl.has("logRecords")) continue;
                for (JsonNode lr : sl.get("logRecords")) {
                    // Timestamp
                    if (lr.has("timeUnixNano")) {
                        event.setTimestamp(lr.get("timeUnixNano").asLong() / 1_000_000);
                    } else if (lr.has("observedTimeUnixNano")) {
                        event.setTimestamp(lr.get("observedTimeUnixNano").asLong() / 1_000_000);
                    }

                    // Body
                    if (lr.has("body")) {
                        String body = extractAttributeValue(lr.get("body"));
                        event.getAttributes().put("log.body", body);
                    }

                    // Severity
                    if (lr.has("severityText")) {
                        event.getAttributes().put("log.severity", lr.get("severityText").asText());
                    } else if (lr.has("severityNumber")) {
                        event.getAttributes().put("log.severity_number", lr.get("severityNumber").asText());
                    }

                    // Attributes
                    if (lr.has("attributes")) {
                        for (JsonNode attr : lr.get("attributes")) {
                            String key = attr.get("key").asText();
                            String value = extractAttributeValue(attr.get("value"));
                            event.getAttributes().put(key, value);
                        }
                    }
                }
            }
        }
    }

    private static void extractTracesData(JsonNode root, TelemetryEvent event) {
        if (!root.has("resourceSpans")) return;
        for (JsonNode rs : root.get("resourceSpans")) {
            if (!rs.has("scopeSpans")) continue;
            for (JsonNode ss : rs.get("scopeSpans")) {
                if (!ss.has("spans")) continue;
                for (JsonNode span : ss.get("spans")) {
                    // Trace context
                    if (span.has("traceId")) {
                        event.setTraceId(span.get("traceId").asText());
                    }
                    if (span.has("spanId")) {
                        event.setSpanId(span.get("spanId").asText());
                    }
                    if (span.has("parentSpanId")) {
                        event.setParentSpanId(span.get("parentSpanId").asText());
                    }
                    if (span.has("name")) {
                        event.setSpanName(span.get("name").asText());
                    }
                    if (span.has("startTimeUnixNano")) {
                        event.setStartTime(span.get("startTimeUnixNano").asLong() / 1_000_000);
                    }
                    if (span.has("endTimeUnixNano")) {
                        long endTime = span.get("endTimeUnixNano").asLong() / 1_000_000;
                        if (event.getStartTime() > 0) {
                            event.setDurationMs(endTime - event.getStartTime());
                        }
                    }
                    if (span.has("status")) {
                        JsonNode status = span.get("status");
                        if (status.has("code")) {
                            event.setStatus(status.get("code").asText());
                        }
                    }

                    // Span attributes
                    if (span.has("attributes")) {
                        for (JsonNode attr : span.get("attributes")) {
                            String key = attr.get("key").asText();
                            String value = extractAttributeValue(attr.get("value"));
                            event.getAttributes().put(key, value);
                        }
                    }
                }
            }
        }
    }

    private static void extractGenericData(JsonNode root, TelemetryEvent event) {
        // Fallback for events/security - extract whatever we can
        if (root.has("resourceMetrics")) extractMetricsData(root, event);
        else if (root.has("resourceLogs")) extractLogsData(root, event);
        else if (root.has("resourceSpans")) extractTracesData(root, event);
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
                    System.getenv().getOrDefault("KAFKA_GROUP_ID", "flink-entity-resolution-v2"));
            return new JobConfig(brokers, groupId);
        }
    }
}
