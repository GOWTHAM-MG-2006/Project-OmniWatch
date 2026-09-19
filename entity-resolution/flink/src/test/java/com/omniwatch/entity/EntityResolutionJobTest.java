/*
 * OmniWatch — Entity Resolution Layer
 * Component: EntityResolutionJobTest
 * Phase: 3
 * Purpose: Unit tests for job topology, constants, config loading and JSON parsing
 * Inputs: args array + raw JSON strings
 * Outputs: job/config/parse assertions
 */
package com.omniwatch.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.omniwatch.entity.models.TelemetryEvent;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

class EntityResolutionJobTest {

    @Test
    void jobNameIsOmniWatchEntityResolution() {
        assertEquals("OmniWatch Entity Resolution", EntityResolutionJob.JOB_NAME);
    }

    @Test
    void inputTopicsContainsFiveRawTopics() {
        // Prod truth: EntityResolutionJob.java:64-70 — INPUT_TOPICS are *.raw OTLP
        // (raw OTLP JSON from OTel Collector; parseEvent reads resourceMetrics/
        // resourceLogs/resourceSpans shapes).
        assertEquals(5, EntityResolutionJob.INPUT_TOPICS.size());
        assertTrue(!EntityResolutionJob.INPUT_TOPICS.isEmpty());
        for (String t : EntityResolutionJob.INPUT_TOPICS) {
            assertTrue(t.startsWith("omniwatch."), "topic must be namespaced: " + t);
            assertTrue(t.endsWith(".raw"), "topic must be raw OTLP: " + t);
        }
    }

    @Test
    void outputTopicConstantsDefined() {
        assertEquals("omniwatch.entities.resolved", EntityResolutionJob.OUTPUT_TOPIC_RESOLVED);
        assertEquals("omniwatch.entities.relationships", EntityResolutionJob.OUTPUT_TOPIC_RELATIONSHIPS);
    }

    @Test
    void jobConfigParsesCliArguments() {
        EntityResolutionJob.JobConfig cfg = EntityResolutionJob.JobConfig.fromArgs(
                new String[]{"--kafka.brokers", "localhost:9092", "--kafka.group.id", "test-group"});
        assertEquals("localhost:9092", cfg.kafkaBrokers);
        assertEquals("test-group", cfg.kafkaGroupId);
    }

    @Test
    void jobConfigAppliesDefaults() {
        EntityResolutionJob.JobConfig cfg = EntityResolutionJob.JobConfig.fromArgs(new String[]{});
        assertNotNull(cfg.kafkaBrokers);
        assertNotNull(cfg.kafkaGroupId);
        // Prod truth: EntityResolutionJob.java:448 — default group id moved to -v2.
        assertEquals("flink-entity-resolution-v2", cfg.kafkaGroupId);
    }

    @Test
    void buildEnvironmentConstructsStreamGraph() {
        StreamExecutionEnvironment env = EntityResolutionJob.buildEnvironment(
                new String[]{"--kafka.brokers", "localhost:9092"});
        assertNotNull(env);
    }

    @Test
    void parseEventParsesValidJson() throws Exception {
        // Prod truth: EntityResolutionJob.parseEvent (EntityResolutionJob.java:165)
        // reads raw OTLP JSON — signal type inferred from the resource* key
        // (:203-208), entity id from resource.attributes service.name (:236-238),
        // timestamp from dataPoints timeUnixNano nanos->millis (:292-296).
        String json = "{\"resourceMetrics\":[{\"resource\":{\"attributes\":["
                + "{\"key\":\"service.name\",\"value\":{\"stringValue\":\"svc-web-1\"}}]},"
                + "\"scopeMetrics\":[{\"metrics\":[{\"name\":\"latency_ms\","
                + "\"sum\":{\"dataPoints\":[{\"timeUnixNano\":1700000000000000000,"
                + "\"asDouble\":42.5}]}}]}]}]}";
        Method m = EntityResolutionJob.class.getDeclaredMethod("parseEvent", ObjectMapper.class, String.class);
        m.setAccessible(true);
        TelemetryEvent evt = (TelemetryEvent) m.invoke(null, EntityResolutionJob.createMapper(), json);
        assertNotNull(evt);
        assertEquals("svc-web-1", evt.getEntityId());
        assertEquals("SERVICE", evt.getEntityType());
        assertEquals("metrics", evt.getSourceType());
        assertEquals(1700000000000L, evt.getTimestamp());
        assertEquals("latency_ms", evt.getAttributes().get("metric.name"));
    }

    @Test
    void parseEventToleratesUnknownFields() throws Exception {
        // Prod truth: parseEvent only reads the resource* subtrees
        // (EntityResolutionJob.java:165-201) and createMapper disables
        // FAIL_ON_UNKNOWN_PROPERTIES (:154-158) — unknown fields are ignored.
        String json = "{\"resourceMetrics\":[{\"resource\":{\"attributes\":["
                + "{\"key\":\"service.name\",\"value\":{\"stringValue\":\"x\"}}]}}],"
                + "\"future_field\":123}";
        Method m = EntityResolutionJob.class.getDeclaredMethod("parseEvent", ObjectMapper.class, String.class);
        m.setAccessible(true);
        TelemetryEvent evt = (TelemetryEvent) m.invoke(null, EntityResolutionJob.createMapper(), json);
        assertNotNull(evt);
        assertEquals("x", evt.getEntityId());
    }

    @Test
    void parseEventReturnsEmptyEntityForMalformedJson() throws Exception {
        Method m = EntityResolutionJob.class.getDeclaredMethod("parseEvent", ObjectMapper.class, String.class);
        m.setAccessible(true);
        TelemetryEvent evt = (TelemetryEvent) m.invoke(null, new ObjectMapper(), "{not-json");
        assertNotNull(evt);
        assertNull(evt.getEntityId());
    }
}
