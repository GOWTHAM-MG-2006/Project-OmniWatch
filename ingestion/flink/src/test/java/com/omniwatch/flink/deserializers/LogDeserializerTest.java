package com.omniwatch.flink.deserializers;

import com.omniwatch.flink.models.LogEvent;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for LogDeserializer.
 * Verifies OTLP JSON logs are correctly parsed into LogEvent POJOs.
 */
class LogDeserializerTest {

    private LogDeserializer deserializer;
    private TestCollector collector;

    @BeforeEach
    void setUp() {
        deserializer = new LogDeserializer();
        collector = new TestCollector();
    }

    @Test
    void testDeserializeValidLog() throws IOException {
        byte[] jsonBytes = Files.readAllBytes(Paths.get("src/test/resources/sample-log.json"));
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("omniwatch.logs.raw", 0, 0, null, jsonBytes);

        deserializer.deserialize(record, collector);

        LogEvent event = collector.getCaptured();
        assertNotNull(event);
        assertEquals("api-gateway", event.getEntityId());
        assertEquals("API_NODE", event.getEntityType());
        assertEquals("INFO", event.getSeverity());
        assertEquals("Request processed successfully", event.getBody());
        assertEquals("omniwatch.logs.raw", event.getSourceTopic());
        assertEquals("performance", event.getSourceType());
        assertEquals("api-gateway", event.getServiceName());
        assertTrue(event.getTimestamp() > 0);
    }

    @Test
    void testDeserializeLogWithErrorSeverity() throws Exception {
        String json = "{"
                + "\"resourceLogs\": [{\"resource\": {\"attributes\": ["
                + "{\"key\": \"service.name\", \"value\": {\"stringValue\": \"db-worker\"}}"
                + "]}, \"scopeLogs\": [{\"scope\": {}, \"logRecords\": ["
                + "{\"timeUnixNano\": \"1715000000000123456\", \"severityNumber\": 17, "
                + "\"body\": {\"stringValue\": \"Connection timeout\"}}"
                + "]}]}]"
                + "}";
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.topic", 0, 0, null, json.getBytes());

        deserializer.deserialize(record, collector);

        LogEvent event = collector.getCaptured();
        assertNotNull(event);
        assertEquals("ERROR", event.getSeverity());
        assertEquals("Connection timeout", event.getBody());
    }

    @Test
    void testDeserializeLogWithSeverityText() throws Exception {
        String json = "{"
                + "\"resourceLogs\": [{\"resource\": {\"attributes\": ["
                + "{\"key\": \"service.name\", \"value\": {\"stringValue\": \"web-frontend\"}}"
                + "]}, \"scopeLogs\": [{\"scope\": {}, \"logRecords\": ["
                + "{\"timeUnixNano\": \"1715000000000123456\", \"severityText\": \"WARN\", "
                + "\"body\": {\"stringValue\": \"High memory usage\"}}"
                + "]}]}]"
                + "}";
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.topic", 0, 0, null, json.getBytes());

        deserializer.deserialize(record, collector);

        LogEvent event = collector.getCaptured();
        assertEquals("WARN", event.getSeverity());
    }

    @Test
    void testDeserializeLogWithTraceAndSpanIds() throws Exception {
        String json = "{"
                + "\"resourceLogs\": [{\"resource\": {\"attributes\": ["
                + "{\"key\": \"service.name\", \"value\": {\"stringValue\": \"svc\"}}"
                + "]}, \"scopeLogs\": [{\"scope\": {}, \"logRecords\": ["
                + "{\"timeUnixNano\": \"1715000000000123456\", \"severityNumber\": 9, "
                + "\"traceId\": \"abc123\", \"spanId\": \"def456\", "
                + "\"body\": {\"stringValue\": \"test\"}}"
                + "]}]}]"
                + "}";
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.topic", 0, 0, null, json.getBytes());

        deserializer.deserialize(record, collector);

        LogEvent event = collector.getCaptured();
        assertEquals("abc123", event.getAttributes().get("traceId"));
        assertEquals("def456", event.getAttributes().get("spanId"));
    }

    @Test
    void testDeserializeEmptyLog_emitsFallback() throws Exception {
        String json = "{\"resourceLogs\": []}";
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.topic", 0, 0, null, json.getBytes());

        deserializer.deserialize(record, collector);

        LogEvent event = collector.getCaptured();
        assertNotNull(event);
        assertEquals("unknown", event.getEntityId());
        assertEquals("INFO", event.getSeverity());
        assertEquals("", event.getBody());
    }

    @Test
    void testDeserializeInvalidJson_throwsIOException() {
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.topic", 0, 0, null, "bad-json".getBytes());

        assertThrows(IOException.class, () -> deserializer.deserialize(record, collector));
    }

    @Test
    void testGetProducedType() {
        assertEquals(LogEvent.class, deserializer.getProducedType().getTypeClass());
    }

    private static class TestCollector implements Collector<LogEvent> {
        private LogEvent captured;

        @Override
        public void collect(LogEvent event) {
            this.captured = event;
        }

        public LogEvent getCaptured() {
            return captured;
        }

        @Override
        public void close() {
            // No-op for test collector
        }
    }
}
