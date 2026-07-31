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
    void inputTopicsContainsFiveNormalizedTopics() {
        assertEquals(5, EntityResolutionJob.INPUT_TOPICS.size());
        assertTrue(!EntityResolutionJob.INPUT_TOPICS.isEmpty());
        for (String t : EntityResolutionJob.INPUT_TOPICS) {
            assertTrue(t.startsWith("omniwatch."), "topic must be namespaced: " + t);
            assertTrue(t.endsWith(".normalized"), "topic must be normalized: " + t);
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
        assertEquals("flink-entity-resolution", cfg.kafkaGroupId);
    }

    @Test
    void buildEnvironmentConstructsStreamGraph() {
        StreamExecutionEnvironment env = EntityResolutionJob.buildEnvironment(
                new String[]{"--kafka.brokers", "localhost:9092"});
        assertNotNull(env);
    }

    @Test
    void parseEventParsesValidJson() throws Exception {
        String json = "{\"entity_id\":\"projects/p1/zones/us-central1-a/instances/web-1\","
                + "\"entity_type\":\"API_NODE\",\"timestamp\":1700000000000,"
                + "\"source_type\":\"metrics\",\"source_topic\":\"omniwatch.metrics.normalized\"}";
        Method m = EntityResolutionJob.class.getDeclaredMethod("parseEvent", ObjectMapper.class, String.class);
        m.setAccessible(true);
        TelemetryEvent evt = (TelemetryEvent) m.invoke(null, EntityResolutionJob.createMapper(), json);
        assertNotNull(evt);
        assertEquals("projects/p1/zones/us-central1-a/instances/web-1", evt.getEntityId());
        assertEquals("API_NODE", evt.getEntityType());
        assertEquals(1700000000000L, evt.getTimestamp());
    }

    @Test
    void parseEventToleratesUnknownFields() throws Exception {
        String json = "{\"entity_id\":\"x\",\"entity_type\":\"API_NODE\",\"future_field\":123}";
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
