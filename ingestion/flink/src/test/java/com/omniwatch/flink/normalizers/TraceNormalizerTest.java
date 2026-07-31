package com.omniwatch.flink.normalizers;

import com.omniwatch.flink.models.TraceEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for TraceNormalizer.
 * Verifies hex ID padding/truncation and status code normalization.
 */
class TraceNormalizerTest {

    private TraceEvent createTrace(String traceId, String spanId, String parentSpanId, String status) {
        TraceEvent event = new TraceEvent();
        event.setEntityId("test-service");
        event.setEntityType("API_NODE");
        event.setSourceType("performance");
        event.setTraceId(traceId);
        event.setSpanId(spanId);
        event.setParentSpanId(parentSpanId);
        event.setStatus(status);
        event.setSpanName("test-span");
        event.setTimestamp(System.currentTimeMillis());
        return event;
    }

    @Test
    void testTraceIdPaddedTo32Chars() {
        TraceEvent event = createTrace("abc", "def", null, "1");
        TraceNormalizer.normalize(event);
        assertEquals(32, event.getTraceId().length());
        assertTrue(event.getTraceId().endsWith("abc"));
        assertEquals("00000000000000000000000000000abc", event.getTraceId());
    }

    @Test
    void testTraceIdTruncatedTo32Chars() {
        String longId = "abcdef0123456789abcdef0123456789abcdef01";
        TraceEvent event = createTrace(longId, "span1", null, "1");
        TraceNormalizer.normalize(event);
        assertEquals(32, event.getTraceId().length());
        assertEquals("23456789abcdef0123456789abcdef01", event.getTraceId());
    }

    @Test
    void testTraceIdAlready32Chars() {
        String id = "abcdef0123456789abcdef0123456789";
        TraceEvent event = createTrace(id, "span1", null, "1");
        TraceNormalizer.normalize(event);
        assertEquals(id, event.getTraceId());
    }

    @Test
    void testTraceIdStripsNonHexChars() {
        TraceEvent event = createTrace("0xabcdef", "s1", null, "1");
        TraceNormalizer.normalize(event);
        assertEquals(32, event.getTraceId().length());
        assertTrue(event.getTraceId().endsWith("abcdef"));
    }

    @Test
    void testSpanIdPaddedTo16Chars() {
        TraceEvent event = createTrace("trace1", "ab", null, "1");
        TraceNormalizer.normalize(event);
        assertEquals(16, event.getSpanId().length());
        assertEquals("00000000000000ab", event.getSpanId());
    }

    @Test
    void testSpanIdTruncatedTo16Chars() {
        TraceEvent event = createTrace("trace1", "abcdef0123456789abcdef", null, "1");
        TraceNormalizer.normalize(event);
        assertEquals(16, event.getSpanId().length());
        assertEquals("0123456789abcdef", event.getSpanId());
    }

    @Test
    void testParentSpanIdNormalizedWhenPresent() {
        TraceEvent event = createTrace("t1", "s1", "ff", "1");
        TraceNormalizer.normalize(event);
        assertEquals(16, event.getParentSpanId().length());
        assertEquals("00000000000000ff", event.getParentSpanId());
    }

    @Test
    void testEmptyParentSpanIdLeftEmpty() {
        TraceEvent event = createTrace("t1", "s1", "", "1");
        TraceNormalizer.normalize(event);
        assertEquals("", event.getParentSpanId());
    }

    @Test
    void testNullParentSpanIdLeftNull() {
        TraceEvent event = createTrace("t1", "s1", null, "1");
        TraceNormalizer.normalize(event);
        assertNull(event.getParentSpanId());
    }

    @Test
    void testStatusNullMapsToUnset() {
        TraceEvent event = createTrace("t1", "s1", null, null);
        TraceNormalizer.normalize(event);
        assertEquals("UNSET", event.getStatus());
    }

    @Test
    void testStatusCode0MapsToUnset() {
        TraceEvent event = createTrace("t1", "s1", null, "0");
        TraceNormalizer.normalize(event);
        assertEquals("UNSET", event.getStatus());
    }

    @Test
    void testStatusCode1MapsToOk() {
        TraceEvent event = createTrace("t1", "s1", null, "1");
        TraceNormalizer.normalize(event);
        assertEquals("OK", event.getStatus());
    }

    @Test
    void testStatusCode2MapsToError() {
        TraceEvent event = createTrace("t1", "s1", null, "2");
        TraceNormalizer.normalize(event);
        assertEquals("ERROR", event.getStatus());
    }

    @Test
    void testStatusStringAlreadyCanonical() {
        TraceEvent event = createTrace("t1", "s1", null, "ERROR");
        TraceNormalizer.normalize(event);
        assertEquals("ERROR", event.getStatus());
    }

    @Test
    void testStatusCaseInsensitiveMapping() {
        TraceEvent event = createTrace("t1", "s1", null, "ok");
        TraceNormalizer.normalize(event);
        assertEquals("OK", event.getStatus());
    }

    @Test
    void testUnknownStatusDefaultsToUnset() {
        TraceEvent event = createTrace("t1", "s1", null, "INVALID");
        TraceNormalizer.normalize(event);
        assertEquals("UNSET", event.getStatus());
    }

    @Test
    void testNullEventReturnsNull() {
        assertNull(TraceNormalizer.normalize(null));
    }
}
