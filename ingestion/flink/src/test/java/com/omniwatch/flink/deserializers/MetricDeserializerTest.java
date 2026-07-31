package com.omniwatch.flink.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.omniwatch.flink.models.MetricEvent;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MetricDeserializer.
 * Verifies that OTLP JSON metrics are correctly parsed into MetricEvent POJOs.
 */
class MetricDeserializerTest {

    private MetricDeserializer deserializer;
    private TestCollector collector;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        deserializer = new MetricDeserializer();
        collector = new TestCollector();
    }

    @Test
    void testDeserializeValidMetric() throws IOException {
        byte[] jsonBytes = Files.readAllBytes(Paths.get("src/test/resources/sample-metric.json"));
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("omniwatch.metrics.raw", 0, 0, null, jsonBytes);

        deserializer.deserialize(record, collector);

        MetricEvent event = collector.getCaptured();
        assertNotNull(event, "MetricEvent should not be null");
        assertEquals("frontend-service", event.getEntityId());
        assertEquals("API_NODE", event.getEntityType());
        assertEquals("http_requests_total", event.getMetricName());
        assertEquals(42.5, event.getValue(), 0.0001);
        assertEquals("count", event.getUnit());
        assertEquals("omniwatch.metrics.raw", event.getSourceTopic());
        assertEquals("performance", event.getSourceType());
        assertEquals(1715000000000L, event.getTimestamp());
    }

    @Test
    void testDeserializeMetricWithSum() throws Exception {
        String json = "{"
                + "\"resourceMetrics\": [{\"resource\": {\"attributes\": ["
                + "{\"key\": \"service.name\", \"value\": {\"stringValue\": \"payment-service\"}}"
                + "]}, \"scopeMetrics\": [{\"scope\": {}, \"metrics\": ["
                + "{\"name\": \"request_duration_ms\", \"unit\": \"ms\", \"sum\": {"
                + "\"dataPoints\": [{\"timeUnixNano\": \"1715000000000123456\", \"asDouble\": 150.0}]}"
                + "}]}]}]"
                + "}";
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.topic", 0, 0, null, json.getBytes());

        deserializer.deserialize(record, collector);

        MetricEvent event = collector.getCaptured();
        assertNotNull(event);
        assertEquals("payment-service", event.getEntityId());
        assertEquals("request_duration_ms", event.getMetricName());
        assertEquals(150.0, event.getValue(), 0.0001);
        assertEquals("ms", event.getUnit());
    }

    @Test
    void testDeserializeEmptyJson_emitsFallback() throws Exception {
        String json = "{\"resourceMetrics\": []}";
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.topic", 0, 0, null, json.getBytes());

        deserializer.deserialize(record, collector);

        MetricEvent event = collector.getCaptured();
        assertNotNull(event);
        assertEquals("unknown", event.getEntityId());
        assertEquals("unknown", event.getMetricName());
        assertEquals(0.0, event.getValue(), 0.0001);
    }

    @Test
    void testDeserializeInvalidJson_throwsIOException() {
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.topic", 0, 0, null, "not-json".getBytes());

        assertThrows(IOException.class, () -> deserializer.deserialize(record, collector));
    }

    @Test
    void testGetProducedType() {
        assertEquals(MetricEvent.class, deserializer.getProducedType().getTypeClass());
    }

    /**
     * Collector implementation that captures a single event for assertions.
     */
    private static class TestCollector implements Collector<MetricEvent> {
        private MetricEvent captured;

        @Override
        public void collect(MetricEvent event) {
            this.captured = event;
        }

        public MetricEvent getCaptured() {
            return captured;
        }

        @Override
        public void close() {
            // No-op for test collector
        }
    }
}
