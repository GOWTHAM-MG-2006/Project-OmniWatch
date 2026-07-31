package com.omniwatch.flink.deserializers;

import com.omniwatch.flink.models.TraceEvent;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for TraceDeserializer.
 * Verifies OTLP JSON traces are correctly parsed into TraceEvent POJOs.
 */
class TraceDeserializerTest {

    private TraceDeserializer deserializer;
    private TestCollector collector;

    @BeforeEach
    void setUp() {
        deserializer = new TraceDeserializer();
        collector = new TestCollector();
    }

    @Test
    void testDeserializeValidTrace() throws IOException {
        byte[] jsonBytes = Files.readAllBytes(Paths.get("src/test/resources/sample-trace.json"));
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("omniwatch.traces.raw", 0, 0, null, jsonBytes);

        deserializer.deserialize(record, collector);

        TraceEvent event = collector.getCaptured();
        assertNotNull(event);
        assertEquals("backend-worker", event.getEntityId());
        assertEquals("API_NODE", event.getEntityType());
        assertEquals("abcd1234abcd1234abcd1234abcd1234", event.getTraceId());
        assertEquals("ef01ef01ef01ef01", event.getSpanId());
        assertEquals("0000000000000000", event.getParentSpanId());
        assertEquals("process_request", event.getSpanName());
        assertEquals("OK", event.getStatus());
        assertEquals(0L, event.getDurationMs()); // diff=500000ns, 500000/1000000=0ms
        // Actually: 1715000000000500000 - 1715000000000000000 = 500000 ns = 0.5ms? No...
        // startTime: 1715000000000000000, endTime: 1715000000000500000
        // diff = 500000 ns = 0.5ms. But in code: (endTime - startTime)/1_000_000 = 500000/1000000 = 0
        // Wait, let me recalculate: 
        // start = 1715000000000000000, end = 1715000000000500000
        // diff = 500000
        // durationMs = 500000 / 1_000_000 = 0 (integer division)
        // Hmm, but in Java long division: 500000 / 1000000 = 0
        // The test expects 0 for durationMs
        // Actually the sample data has start=1715000000000000000, end=1715000000000500000
        // Let me recalculate: diff in nanos = 500000. durationMs = 500000/1000000 = 0
        
        // OK let me correct my sample data to have a more meaningful duration
        // Actually the test needs to work with what's in sample-trace.json
        // End: 1715000000000500000, Start: 1715000000000000000
        // Diff nanos = 500000, Diff millis = 0 
        // So durationMs = 0. That's correct with the sample data.
    }

    @Test
    void testDeserializeTraceWithAttributes() throws Exception {
        String json = "{"
                + "\"resourceSpans\": [{\"resource\": {\"attributes\": ["
                + "{\"key\": \"service.name\", \"value\": {\"stringValue\": \"api-gateway\"}}"
                + "]}, \"scopeSpans\": [{\"scope\": {}, \"spans\": ["
                + "{\"traceId\": \"aaaa\", \"spanId\": \"bbbb\", \"parentSpanId\": \"cccc\", "
                + "\"name\": \"authenticate\", \"startTimeUnixNano\": \"1000000\", "
                + "\"endTimeUnixNano\": \"2000000\", \"status\": {\"code\": 1}, "
                + "\"attributes\": [{\"key\": \"http.status\", \"value\": {\"intValue\": \"200\"}}]"
                + "}]}]}]"
                + "}";
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.topic", 0, 0, null, json.getBytes());

        deserializer.deserialize(record, collector);

        TraceEvent event = collector.getCaptured();
        assertNotNull(event);
        assertEquals("api-gateway", event.getEntityId());
        assertEquals("aaaa", event.getTraceId());
        assertEquals("bbbb", event.getSpanId());
        assertEquals("cccc", event.getParentSpanId());
        assertEquals("authenticate", event.getSpanName());
        assertEquals(1L, event.getDurationMs()); // 1000000/1000000 = 1
        assertEquals("OK", event.getStatus());
        assertTrue(event.getAttributes().containsKey("span.http.status"));
    }

    @Test
    void testDeserializeTraceWithErrorStatus() throws Exception {
        String json = "{"
                + "\"resourceSpans\": [{\"resource\": {\"attributes\": []}, "
                + "\"scopeSpans\": [{\"scope\": {}, \"spans\": ["
                + "{\"traceId\": \"t1\", \"spanId\": \"s1\", \"name\": \"failing-op\", "
                + "\"startTimeUnixNano\": \"1000000\", \"endTimeUnixNano\": \"3000000\", "
                + "\"status\": {\"code\": 2}}"
                + "]}]}]"
                + "}";
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.topic", 0, 0, null, json.getBytes());

        deserializer.deserialize(record, collector);

        TraceEvent event = collector.getCaptured();
        assertEquals("ERROR", event.getStatus());
        assertEquals(2L, event.getDurationMs());
    }

    @Test
    void testDeserializeEmptyTrace_emitsFallback() throws Exception {
        String json = "{\"resourceSpans\": []}";
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.topic", 0, 0, null, json.getBytes());

        deserializer.deserialize(record, collector);

        TraceEvent event = collector.getCaptured();
        assertNotNull(event);
        assertEquals("unknown", event.getEntityId());
        assertEquals("unknown", event.getSpanName());
        assertEquals("", event.getTraceId());
        assertEquals("", event.getSpanId());
        assertEquals(0, event.getDurationMs());
    }

    @Test
    void testDeserializeInvalidJson_throwsIOException() {
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.topic", 0, 0, null, "{bad".getBytes());

        assertThrows(IOException.class, () -> deserializer.deserialize(record, collector));
    }

    @Test
    void testGetProducedType() {
        assertEquals(TraceEvent.class, deserializer.getProducedType().getTypeClass());
    }

    private static class TestCollector implements Collector<TraceEvent> {
        private TraceEvent captured;

        @Override
        public void collect(TraceEvent event) {
            this.captured = event;
        }

        public TraceEvent getCaptured() {
            return captured;
        }

        @Override
        public void close() {
            // No-op for test collector
        }
    }
}
