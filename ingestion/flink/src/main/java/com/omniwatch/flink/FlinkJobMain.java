package com.omniwatch.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.omniwatch.flink.config.FlinkConfig;
import com.omniwatch.flink.deserializers.LogDeserializer;
import com.omniwatch.flink.deserializers.MetricDeserializer;
import com.omniwatch.flink.deserializers.SecurityEventDeserializer;
import com.omniwatch.flink.deserializers.TraceDeserializer;
import com.omniwatch.flink.enrichment.K8sContextEnrichment;
import com.omniwatch.flink.models.LogEvent;
import com.omniwatch.flink.models.MetricEvent;
import com.omniwatch.flink.models.SecurityEvent;
import com.omniwatch.flink.models.TraceEvent;
import com.omniwatch.flink.normalizers.EventNormalizer;
import com.omniwatch.flink.normalizers.LogNormalizer;
import com.omniwatch.flink.normalizers.MetricNormalizer;
import com.omniwatch.flink.normalizers.TraceNormalizer;
import com.omniwatch.flink.producers.NormalizedEventsProducer;
import com.omniwatch.flink.producers.NormalizedLogsProducer;
import com.omniwatch.flink.producers.NormalizedMetricsProducer;
import com.omniwatch.flink.producers.NormalizedTracesProducer;
import com.omniwatch.flink.producers.SecurityEventRouter;
import com.omniwatch.flink.sink.MinIOSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for the OmniWatch Flink Ingestion Pipeline.
 *
 * <p>Assembles the full streaming pipeline:
 * <ol>
 *   <li>Reads raw telemetry from 4 Kafka topics</li>
 *   <li>Deserializes each into typed POJOs</li>
 *   <li>Normalizes each event type through type-specific normalizers</li>
 *   <li>Enriches all events with K8s context metadata</li>
 *   <li>Serializes to JSON and routes to normalized Kafka topics</li>
 *   <li>Archives all events to MinIO storage</li>
 * </ol>
 */
public class FlinkJobMain {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkJobMain.class);

    private static final String TOPIC_METRICS_RAW = "omniwatch.metrics.raw";
    private static final String TOPIC_LOGS_RAW = "omniwatch.logs.raw";
    private static final String TOPIC_TRACES_RAW = "omniwatch.traces.raw";
    private static final String TOPIC_SECURITY_RAW = "omniwatch.security.events";

    public static void main(String[] args) throws Exception {
        // Parse configuration
        FlinkConfig config = FlinkConfig.fromArgs(args);
        LOG.info("Starting OmniWatch Ingestion Pipeline with brokers: {}", config.getKafkaBrokers());

        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000L);

        ObjectMapper objectMapper = new ObjectMapper();

        // ── Source: Raw Metrics ──────────────────────────────────────────────
        KafkaSource<MetricEvent> metricsSource = KafkaSource.<MetricEvent>builder()
                .setBootstrapServers(config.getKafkaBrokers())
                .setTopics(TOPIC_METRICS_RAW)
                .setGroupId(config.getKafkaGroupId())
                .setDeserializer(new MetricDeserializer())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();

        DataStream<MetricEvent> metricStream = env.fromSource(
                metricsSource, WatermarkStrategy.noWatermarks(), "source-metrics-raw");

        // ── Source: Raw Logs ─────────────────────────────────────────────────
        KafkaSource<LogEvent> logsSource = KafkaSource.<LogEvent>builder()
                .setBootstrapServers(config.getKafkaBrokers())
                .setTopics(TOPIC_LOGS_RAW)
                .setGroupId(config.getKafkaGroupId())
                .setDeserializer(new LogDeserializer())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();

        DataStream<LogEvent> logStream = env.fromSource(
                logsSource, WatermarkStrategy.noWatermarks(), "source-logs-raw");

        // ── Source: Raw Traces ───────────────────────────────────────────────
        KafkaSource<TraceEvent> tracesSource = KafkaSource.<TraceEvent>builder()
                .setBootstrapServers(config.getKafkaBrokers())
                .setTopics(TOPIC_TRACES_RAW)
                .setGroupId(config.getKafkaGroupId())
                .setDeserializer(new TraceDeserializer())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();

        DataStream<TraceEvent> traceStream = env.fromSource(
                tracesSource, WatermarkStrategy.noWatermarks(), "source-traces-raw");

        // ── Source: Raw Security Events ──────────────────────────────────────
        KafkaSource<SecurityEvent> securitySource = KafkaSource.<SecurityEvent>builder()
                .setBootstrapServers(config.getKafkaBrokers())
                .setTopics(TOPIC_SECURITY_RAW)
                .setGroupId(config.getKafkaGroupId())
                .setDeserializer(new SecurityEventDeserializer())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();

        DataStream<SecurityEvent> securityStream = env.fromSource(
                securitySource, WatermarkStrategy.noWatermarks(), "source-security-raw");

        // ── Normalize Each Stream ────────────────────────────────────────────
        DataStream<MetricEvent> normalizedMetrics = metricStream
                .map(MetricNormalizer::normalize);

        DataStream<LogEvent> normalizedLogs = logStream
                .map(LogNormalizer::normalize);

        DataStream<TraceEvent> normalizedTraces = traceStream
                .map(TraceNormalizer::normalize);

        DataStream<SecurityEvent> normalizedSecurity = securityStream
                .map(EventNormalizer::normalize);

        // ── Enrich with K8s Context ──────────────────────────────────────────
        K8sContextEnrichment<MetricEvent> metricEnrichment = new K8sContextEnrichment<>();
        DataStream<MetricEvent> enrichedMetrics = normalizedMetrics
                .map(metricEnrichment);

        K8sContextEnrichment<LogEvent> logEnrichment = new K8sContextEnrichment<>();
        DataStream<LogEvent> enrichedLogs = normalizedLogs
                .map(logEnrichment);

        K8sContextEnrichment<TraceEvent> traceEnrichment = new K8sContextEnrichment<>();
        DataStream<TraceEvent> enrichedTraces = normalizedTraces
                .map(traceEnrichment);

        K8sContextEnrichment<SecurityEvent> securityEnrichment = new K8sContextEnrichment<>();
        DataStream<SecurityEvent> enrichedSecurity = normalizedSecurity
                .map(securityEnrichment);

        // ── Serialize to JSON ────────────────────────────────────────────────
        DataStream<String> metricsJson = enrichedMetrics
                .map(event -> serializeEvent(objectMapper, event));

        DataStream<String> logsJson = enrichedLogs
                .map(event -> serializeEvent(objectMapper, event));

        DataStream<String> tracesJson = enrichedTraces
                .map(event -> serializeEvent(objectMapper, event));

        DataStream<String> securityJson = enrichedSecurity
                .map(event -> serializeEvent(objectMapper, event));

        // ── Route to Kafka Sinks ─────────────────────────────────────────────
        metricsJson.sinkTo(NormalizedMetricsProducer.createSink(config.getKafkaBrokers()));

        logsJson.sinkTo(NormalizedLogsProducer.createSink(config.getKafkaBrokers()));

        tracesJson.sinkTo(NormalizedTracesProducer.createSink(config.getKafkaBrokers()));

        // Security: both normalized and legacy
        securityJson.sinkTo(SecurityEventRouter.createNormalizedSink(config.getKafkaBrokers()));
        securityJson.sinkTo(SecurityEventRouter.createLegacySink(config.getKafkaBrokers()));

        // General events (union of all types)
        DataStream<String> allEventsJson = metricsJson
                .union(logsJson, tracesJson, securityJson);

        allEventsJson.sinkTo(NormalizedEventsProducer.createSink(config.getKafkaBrokers()));

        // ── Archive to MinIO ─────────────────────────────────────────────────
        allEventsJson.addSink(new MinIOSink(config));

        // ── Execute Pipeline ─────────────────────────────────────────────────
        LOG.info("OmniWatch Ingestion Pipeline assembled. Starting execution...");
        env.execute("OmniWatch Ingestion Pipeline");
    }

    private static String serializeEvent(ObjectMapper objectMapper, Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            LOG.error("Failed to serialize event: {}", e.getMessage());
            return "{}";
        }
    }
}
