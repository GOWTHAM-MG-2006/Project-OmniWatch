/*
 * OmniWatch — Windowing Layer + Feature Store
 * Component: FeatureStoreJob
 * Phase: 4
 * Purpose: Flink job entry point. Consumes the omniwatch.metrics.normalized
 *          Kafka topic, keys by entity, and branches into tumbling (1m/5m/15m),
 *          sliding (5m/1m), and session (30s gap) window operators producing
 *          omniwatch.features.windowed_{1m,5m,15m}. A second pipeline reads
 *          the three windowed topics, builds FeatureVectors, and sinks to both
 *          Kafka (omniwatch.features.windowed_15m) and ClickHouse (feature_vectors).
 * Inputs: omniwatch.metrics.normalized (Kafka)
 * Outputs: omniwatch.features.windowed_{1m,5m,15m} (Kafka), feature_vectors (ClickHouse)
 */
package com.omniwatch.features;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.omniwatch.features.models.FeatureVector;
import com.omniwatch.features.models.MetricsEvent;
import com.omniwatch.features.models.WindowedFeature;
import com.omniwatch.features.operators.FeatureStoreWriter;
import com.omniwatch.features.operators.FeatureVectorBuilder;
import com.omniwatch.features.operators.SessionWindowDetector;
import com.omniwatch.features.operators.SlidingWindowAggregator;
import com.omniwatch.features.operators.TumblingWindowAggregator;
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
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stream topology for the Phase 4 windowing + feature store layer.
 */
public final class FeatureStoreJob {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureStoreJob.class);

    public static final String JOB_NAME = "OmniWatch Feature Store";

    /** Input topic (produced by the Phase 2 ingestion normalizer). */
    static final String INPUT_TOPIC = "omniwatch.metrics.normalized";

    /** Output windowed topics (consumed by FeatureVectorBuilder + Phase 6). */
    static final String OUTPUT_TOPIC_WINDOWED_1M = "omniwatch.features.windowed_1m";
    static final String OUTPUT_TOPIC_WINDOWED_5M = "omniwatch.features.windowed_5m";
    static final String OUTPUT_TOPIC_WINDOWED_15M = "omniwatch.features.windowed_15m";

    private FeatureStoreJob() {
    }

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);
        waitForKafka(config.kafkaBrokers);
        StreamExecutionEnvironment env = buildEnvironment(args);
        env.execute(JOB_NAME);
    }

    /**
     * Builds the full execution environment with the feature store graph.
     * Exposed for tests (graph inspection without execution).
     */
    public static StreamExecutionEnvironment buildEnvironment(String[] args) {
        JobConfig config = JobConfig.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000L);

        ObjectMapper mapper = createMapper();

        // ---- Source: normalized metrics from Kafka ----
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(config.kafkaBrokers)
                .setTopics(INPUT_TOPIC)
                .setGroupId(config.kafkaGroupId)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();

        DataStream<MetricsEvent> metrics = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "metrics-source")
                .map(json -> parseEvent(mapper, json))
                .filter(e -> e.getEntityId() != null)
                .returns(Types.POJO(MetricsEvent.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<MetricsEvent>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(5))
                                .withTimestampAssigner(
                                        (event, ts) -> event.getTimestamp()));

        KeyedStream<MetricsEvent, String> keyedMetrics = metrics
                .keyBy(MetricsEvent::getEntityId);

        // ---- Branch A: Tumbling 1-minute window ----
        keyedMetrics
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new TumblingWindowAggregator("1m"), new WindowBoundsStamper())
                .map(feat -> serialize(mapper, feat))
                .returns(Types.STRING)
                .sinkTo(createSink(config.kafkaBrokers, OUTPUT_TOPIC_WINDOWED_1M))
                .name("sink-windowed-1m");

        // ---- Branch B: Tumbling 5-minute window ----
        keyedMetrics
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(new TumblingWindowAggregator("5m"), new WindowBoundsStamper())
                .map(feat -> serialize(mapper, feat))
                .returns(Types.STRING)
                .sinkTo(createSink(config.kafkaBrokers, OUTPUT_TOPIC_WINDOWED_5M))
                .name("sink-windowed-5m");

        // ---- Branch C: Tumbling 15-minute window ----
        keyedMetrics
                .window(TumblingEventTimeWindows.of(Time.minutes(15)))
                .aggregate(new TumblingWindowAggregator("15m"), new WindowBoundsStamper())
                .map(feat -> serialize(mapper, feat))
                .returns(Types.STRING)
                .sinkTo(createSink(config.kafkaBrokers, OUTPUT_TOPIC_WINDOWED_15M))
                .name("sink-windowed-15m");

        // ---- Branch D: Sliding 5-minute window, 1-minute slide ----
        keyedMetrics
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .process(new SlidingWindowAggregator("5m"))
                .map(feat -> serialize(mapper, feat))
                .returns(Types.STRING)
                .sinkTo(createSink(config.kafkaBrokers, OUTPUT_TOPIC_WINDOWED_5M))
                .name("sink-sliding-5m");

        // ---- Branch E: Session window (30s gap, burst threshold 3) ----
        keyedMetrics
                .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
                .process(new SessionWindowDetector(3))
                .map(feat -> serialize(mapper, feat))
                .returns(Types.STRING)
                .sinkTo(createSink(config.kafkaBrokers, OUTPUT_TOPIC_WINDOWED_15M))
                .name("sink-session-15m");

        // ---- FeatureVectorBuilder: reads windowed topics, builds vectors ----
        KafkaSource<String> windowedSource = KafkaSource.<String>builder()
                .setBootstrapServers(config.kafkaBrokers)
                .setTopics(OUTPUT_TOPIC_WINDOWED_1M, OUTPUT_TOPIC_WINDOWED_5M,
                        OUTPUT_TOPIC_WINDOWED_15M)
                .setGroupId(config.kafkaGroupId + "-vector-builder")
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(
                        new SimpleStringSchema()))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(
                        OffsetResetStrategy.EARLIEST))
                .build();

        DataStream<WindowedFeature> windowedFeatures = env
                .fromSource(windowedSource, WatermarkStrategy.noWatermarks(),
                        "windowed-source")
                .map(json -> parseWindowedFeature(mapper, json))
                .filter(wf -> wf.getEntityId() != null)
                .returns(Types.POJO(WindowedFeature.class));

        DataStream<FeatureVector> featureVectors = windowedFeatures
                .keyBy(WindowedFeature::getEntityId)
                .process(new FeatureVectorBuilder());

        // Sink FeatureVectors to Kafka
        featureVectors
                .map(vec -> serialize(mapper, vec))
                .returns(Types.STRING)
                .sinkTo(createSink(config.kafkaBrokers, OUTPUT_TOPIC_WINDOWED_15M))
                .name("sink-feature-vectors-kafka");

        // Sink FeatureVectors to ClickHouse
        featureVectors
                .addSink(new FeatureStoreWriter(
                        config.clickhouseHost, config.clickhousePort, config.clickhouseDb))
                .name("sink-feature-vectors-clickhouse");

        LOG.info("Feature store job graph built: {} -> windowed sinks | windowed sources -> "
                + "FeatureVectorBuilder -> {} + ClickHouse", INPUT_TOPIC,
                OUTPUT_TOPIC_WINDOWED_15M);
        return env;
    }

    static ObjectMapper createMapper() {
        return new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    }

    /** Pre-flight Kafka reachability check: 5 attempts, 5s apart, then fail. */
    static void waitForKafka(String brokers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        for (int attempt = 1; attempt <= 5; attempt++) {
            try (AdminClient admin = AdminClient.create(props)) {
                admin.describeCluster().nodes().get(3, TimeUnit.SECONDS);
                LOG.info("Kafka reachable at {} (attempt {}/5)", brokers, attempt);
                return;
            } catch (Exception e) {
                LOG.warn("Kafka at {} not reachable (attempt {}/5): {}",
                        brokers, attempt, e.getMessage());
                if (attempt < 5) {
                    try {
                        Thread.sleep(5000L);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while waiting for Kafka", ie);
                    }
                }
            }
        }
        throw new RuntimeException("Kafka not reachable at " + brokers + " after 5 attempts");
    }

    private static MetricsEvent parseEvent(ObjectMapper mapper, String json) {
        try {
            return mapper.readValue(json, MetricsEvent.class);
        } catch (Exception e) {
            LOG.error("Failed to parse metrics event, skipping: {}", e.getMessage());
            return new MetricsEvent();
        }
    }

    private static WindowedFeature parseWindowedFeature(ObjectMapper mapper, String json) {
        try {
            return mapper.readValue(json, WindowedFeature.class);
        } catch (Exception e) {
            LOG.error("Failed to parse windowed feature, skipping: {}", e.getMessage());
            return new WindowedFeature();
        }
    }

    private static String serialize(ObjectMapper mapper, Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception e) {
            LOG.error("Failed to serialize {}, emitting empty record",
                    value.getClass().getSimpleName());
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

    /** Stamps the actual window [start, end) onto tumbling aggregation results. */
    private static final class WindowBoundsStamper
            extends ProcessWindowFunction<WindowedFeature, WindowedFeature, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void process(String key, Context ctx,
                            Iterable<WindowedFeature> elements,
                            Collector<WindowedFeature> out) {
            for (WindowedFeature wf : elements) {
                wf.setWindowStart(ctx.window().getStart());
                wf.setWindowEnd(ctx.window().getEnd());
                out.collect(wf);
            }
        }
    }

    /** Runtime configuration: CLI args -> env vars -> defaults (mirrors Phase 3). */
    static final class JobConfig {
        final String kafkaBrokers;
        final String kafkaGroupId;
        final String clickhouseHost;
        final int clickhousePort;
        final String clickhouseDb;

        private JobConfig(String kafkaBrokers, String kafkaGroupId,
                          String clickhouseHost, int clickhousePort,
                          String clickhouseDb) {
            this.kafkaBrokers = kafkaBrokers;
            this.kafkaGroupId = kafkaGroupId;
            this.clickhouseHost = clickhouseHost;
            this.clickhousePort = clickhousePort;
            this.clickhouseDb = clickhouseDb;
        }

        static JobConfig fromArgs(String[] args) {
            ParameterTool params = ParameterTool.fromArgs(args)
                    .mergeWith(ParameterTool.fromSystemProperties());
            String brokers = params.get("kafka.brokers",
                    System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"));
            String groupId = params.get("kafka.group.id",
                    System.getenv().getOrDefault("KAFKA_GROUP_ID", "flink-feature-store"));
            String chHost = params.get("clickhouse.host",
                    System.getenv().getOrDefault("CLICKHOUSE_HOST", "clickhouse-server"));
            int chPort = Integer.parseInt(params.get("clickhouse.port",
                    System.getenv().getOrDefault("CLICKHOUSE_PORT", "8123")));
            String chDb = params.get("clickhouse.db",
                    System.getenv().getOrDefault("CLICKHOUSE_DB", "omniwatch"));
            return new JobConfig(brokers, groupId, chHost, chPort, chDb);
        }
    }
}
