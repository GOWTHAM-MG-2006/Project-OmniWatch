/*
 * OmniWatch — Windowing Layer + Feature Store
 * Component: FeatureStoreJobTest
 * Phase: 4
 * Purpose: Unit tests for job topology, constants, config loading and JSON parsing
 * Inputs: args array + raw JSON strings
 * Outputs: job/config/parse assertions
 */
package com.omniwatch.features;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.omniwatch.features.models.MetricsEvent;
import com.omniwatch.features.models.WindowedFeature;
import com.omniwatch.features.models.FeatureVector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

class FeatureStoreJobTest {

    @Test
    void jobNameIsOmniWatchFeatureStore() {
        assertEquals("OmniWatch Feature Store", FeatureStoreJob.JOB_NAME);
    }

    @Test
    void inputTopicIsRawOtlpMetrics() {
        // Prod truth: FeatureStoreJob.java:73 — INPUT_TOPIC = "omniwatch.metrics.raw"
        // (raw OTLP JSON from OTel Collector; parseEvent reads the resourceMetrics shape).
        assertEquals("omniwatch.metrics.raw", FeatureStoreJob.INPUT_TOPIC);
    }

    @Test
    void outputTopicConstantsDefined() {
        assertEquals("omniwatch.features.windowed_1m",
                FeatureStoreJob.OUTPUT_TOPIC_WINDOWED_1M);
        assertEquals("omniwatch.features.windowed_5m",
                FeatureStoreJob.OUTPUT_TOPIC_WINDOWED_5M);
        assertEquals("omniwatch.features.windowed_15m",
                FeatureStoreJob.OUTPUT_TOPIC_WINDOWED_15M);
    }

    @Test
    void outputTopicsAreNamespaced() {
        String[] topics = {
                FeatureStoreJob.OUTPUT_TOPIC_WINDOWED_1M,
                FeatureStoreJob.OUTPUT_TOPIC_WINDOWED_5M,
                FeatureStoreJob.OUTPUT_TOPIC_WINDOWED_15M
        };
        for (String t : topics) {
            assertTrue(t.startsWith("omniwatch."), "topic must be namespaced: " + t);
            assertTrue(t.startsWith("omniwatch.features."),
                    "topic must be under features: " + t);
        }
    }

    @Test
    void jobConfigParsesCliArguments() {
        FeatureStoreJob.JobConfig cfg = FeatureStoreJob.JobConfig.fromArgs(
                new String[]{
                        "--kafka.brokers", "localhost:9092",
                        "--kafka.group.id", "test-group",
                        "--clickhouse.host", "ch-local",
                        "--clickhouse.port", "9000",
                        "--clickhouse.db", "testdb",
                        "--clickhouse.user", "ch-user",
                        "--clickhouse.password", "ch-pass"
                });
        assertEquals("localhost:9092", cfg.kafkaBrokers);
        assertEquals("test-group", cfg.kafkaGroupId);
        assertEquals("ch-local", cfg.clickhouseHost);
        assertEquals(9000, cfg.clickhousePort);
        assertEquals("testdb", cfg.clickhouseDb);
        assertEquals("ch-user", cfg.clickhouseUser);
        assertEquals("ch-pass", cfg.clickhousePassword);
    }

    @Test
    void jobConfigAppliesDefaults() {
        FeatureStoreJob.JobConfig cfg = FeatureStoreJob.JobConfig.fromArgs(new String[]{});
        assertNotNull(cfg.kafkaBrokers);
        assertNotNull(cfg.kafkaGroupId);
        assertEquals("flink-feature-store", cfg.kafkaGroupId);
        assertNotNull(cfg.clickhouseHost);
        assertEquals(8123, cfg.clickhousePort);
        assertNotNull(cfg.clickhouseDb);
        assertEquals("omniwatch", cfg.clickhouseDb);
        assertNotNull(cfg.clickhouseUser);
        assertEquals("omniwatch", cfg.clickhouseUser);
        assertNotNull(cfg.clickhousePassword);
        assertEquals("omniwatch", cfg.clickhousePassword);
    }

    @Test
    void buildEnvironmentConstructsStreamGraph() {
        StreamExecutionEnvironment env = FeatureStoreJob.buildEnvironment(
                new String[]{"--kafka.brokers", "localhost:9092"});
        assertNotNull(env);
    }

    @Test
    void parseEventParsesValidJson() throws Exception {
        // Prod truth: FeatureStoreJob.parseEvent (FeatureStoreJob.java:251) reads raw
        // OTLP JSON — entity id from resource.attributes service.name (:280-282),
        // metric name/value into the attributes map from scopeMetrics dataPoints
        // (:309-355), timestamp from timeUnixNano nanos->millis (:335-339).
        // Scalar MetricsEvent fields (metricName/value/sourceType) are NOT populated
        // by the OTLP path, so their defaults are asserted as-is.
        String json = "{\"resourceMetrics\":[{\"resource\":{\"attributes\":["
                + "{\"key\":\"service.name\",\"value\":{\"stringValue\":\"svc-web-1\"}}]},"
                + "\"scopeMetrics\":[{\"metrics\":[{\"name\":\"latency_ms\","
                + "\"sum\":{\"dataPoints\":[{\"timeUnixNano\":1700000000000000000,"
                + "\"asDouble\":42.5}]}}]}]}]}";
        Method m = FeatureStoreJob.class.getDeclaredMethod(
                "parseEvent", ObjectMapper.class, String.class);
        m.setAccessible(true);
        MetricsEvent evt = (MetricsEvent) m.invoke(
                null, FeatureStoreJob.createMapper(), json);
        assertNotNull(evt);
        assertEquals("svc-web-1", evt.getEntityId());
        assertEquals("SERVICE", evt.getEntityType());
        assertEquals(1700000000000L, evt.getTimestamp());
        assertEquals("latency_ms", evt.getAttributes().get("metric.name"));
        assertEquals("42.5", evt.getAttributes().get("latency_ms.value"));
        assertNull(evt.getMetricName());
        assertFalse(evt.isError());
        assertNull(evt.getSourceType());
    }

    @Test
    void parseEventToleratesUnknownFields() throws Exception {
        // Prod truth: parseEvent only reads the resourceMetrics subtree
        // (FeatureStoreJob.java:251-267) and createMapper disables
        // FAIL_ON_UNKNOWN_PROPERTIES (:215-219) — unknown fields are ignored.
        String json = "{\"resourceMetrics\":[{\"resource\":{\"attributes\":["
                + "{\"key\":\"service.name\",\"value\":{\"stringValue\":\"x\"}}]},"
                + "\"scopeMetrics\":[]}],\"future_field\":123}";
        Method m = FeatureStoreJob.class.getDeclaredMethod(
                "parseEvent", ObjectMapper.class, String.class);
        m.setAccessible(true);
        MetricsEvent evt = (MetricsEvent) m.invoke(
                null, FeatureStoreJob.createMapper(), json);
        assertNotNull(evt);
        assertEquals("x", evt.getEntityId());
    }

    @Test
    void parseEventReturnsEmptyEventForMalformedJson() throws Exception {
        Method m = FeatureStoreJob.class.getDeclaredMethod(
                "parseEvent", ObjectMapper.class, String.class);
        m.setAccessible(true);
        MetricsEvent evt = (MetricsEvent) m.invoke(
                null, new ObjectMapper(), "{not-json");
        assertNotNull(evt);
        assertNull(evt.getEntityId());
    }

    @Test
    void parseWindowedFeatureParsesValidJson() throws Exception {
        String json = "{\"entity_id\":\"svc-web-1\",\"window_start\":1700000000000,"
                + "\"window_end\":1700000060000,\"window_size\":\"1m\","
                + "\"metric_name\":\"latency_ms\",\"min\":1.0,\"max\":100.0,"
                + "\"avg\":50.0,\"count\":10,\"sum\":500.0,"
                + "\"p50\":45.0,\"p95\":90.0,\"p99\":99.0,"
                + "\"stddev\":25.0,\"rate\":0.1}";
        Method m = FeatureStoreJob.class.getDeclaredMethod(
                "parseWindowedFeature", ObjectMapper.class, String.class);
        m.setAccessible(true);
        WindowedFeature wf = (WindowedFeature) m.invoke(
                null, FeatureStoreJob.createMapper(), json);
        assertNotNull(wf);
        assertEquals("svc-web-1", wf.getEntityId());
        assertEquals(1700000000000L, wf.getWindowStart());
        assertEquals(1700000060000L, wf.getWindowEnd());
        assertEquals("1m", wf.getWindowSize());
        assertEquals(45.0, wf.getP50(), 0.001);
    }

    @Test
    void metricsEventHasNoArgCtor() {
        MetricsEvent evt = new MetricsEvent();
        assertNotNull(evt);
        assertNull(evt.getEntityId());
        assertEquals(0.0, evt.getValue(), 0.0);
    }

    @Test
    void windowedFeatureHasNoArgCtor() {
        WindowedFeature wf = new WindowedFeature();
        assertNotNull(wf);
        assertNull(wf.getEntityId());
        assertEquals(0L, wf.getCount());
    }

    @Test
    void featureVectorDefaultTtl() {
        FeatureVector fv = new FeatureVector();
        assertNotNull(fv);
        assertEquals(90, fv.getTtl());
    }
}
