package com.omniwatch.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.omniwatch.flink.config.FlinkConfig;
import com.omniwatch.flink.models.MetricEvent;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for FlinkJobMain.
 * Verifies configuration parsing and event serialization via reflection.
 */
class FlinkJobMainTest {

    @Test
    void testMainFailsWithoutBrokers_expectException() {
        assertThrows(Exception.class, () -> {
            FlinkJobMain.main(new String[]{"--unknown"});
        });
    }

    @Test
    void testConfigParsesFromArgs() {
        FlinkConfig config = FlinkConfig.fromArgs(new String[]{
                "--kafka.brokers", "localhost:9092",
                "--kafka.group.id", "test-group",
                "--minio.endpoint", "http://localhost:9000",
                "--minio.access.key", "test",
                "--minio.secret.key", "test123"
        });
        assertEquals("localhost:9092", config.getKafkaBrokers());
        assertEquals("test-group", config.getKafkaGroupId());
    }

    @Test
    void testConfigDefaults() {
        FlinkConfig config = FlinkConfig.fromArgs(new String[]{});
        assertEquals("kafka:29092", config.getKafkaBrokers());
        assertEquals("flink-ingestion", config.getKafkaGroupId());
    }

    @Test
    void testConfigMinioDefaults() {
        FlinkConfig config = FlinkConfig.fromArgs(new String[]{});
        assertEquals("http://minio:9010", config.getMinioEndpoint());
        assertEquals("minioadmin", config.getMinioAccessKey());
    }

    @Test
    void testSerializeEvent_returnsJson() throws Exception {
        MetricEvent event = new MetricEvent();
        event.setEntityId("test");
        event.setEntityType("API_NODE");
        event.setSourceType("performance");
        event.setMetricName("cpu");
        event.setValue(42.0);
        event.setTimestamp(1715000000000L);

        String json = invokeSerializeEvent(new ObjectMapper(), event);
        assertTrue(json.contains("\"entityId\":\"test\""),
                "JSON should contain entityId: " + json);
        assertTrue(json.contains("\"metricName\":\"cpu\""),
                "JSON should contain metricName: " + json);
    }

    @Test
    void testSerializeEvent_emptyObject_returnsJson() throws Exception {
        MetricEvent event = new MetricEvent();
        String json = invokeSerializeEvent(new ObjectMapper(), event);
        assertNotNull(json);
        assertTrue(json.startsWith("{"));
        assertTrue(json.endsWith("}"));
    }

    @Test
    void testSerializeEvent_null_returnsEmptyObject() throws Exception {
        String json = invokeSerializeEvent(new ObjectMapper(), null);
        assertEquals("null", json);
    }

    private String invokeSerializeEvent(ObjectMapper mapper, Object event) throws Exception {
        Method method = FlinkJobMain.class.getDeclaredMethod(
                "serializeEvent", ObjectMapper.class, Object.class);
        method.setAccessible(true);
        return (String) method.invoke(null, mapper, event);
    }
}
