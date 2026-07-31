package com.omniwatch.flink.normalizers;

import com.omniwatch.flink.models.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for EventNormalizer.
 * Verifies unified normalization, required field validation, and SecurityEvent handling.
 */
class EventNormalizerTest {

    @Test
    void testNormalizeMetricEvent() {
        MetricEvent event = new MetricEvent();
        event.setEntityId("test-service");
        event.setEntityType("API_NODE");
        event.setSourceType("performance");
        event.setMetricName("cpu_usage");
        event.setValue(5000.0);
        event.setUnit("ms");
        event.setTimestamp(System.currentTimeMillis());

        MetricEvent result = EventNormalizer.normalize(event);
        assertNotNull(result);
        assertEquals(5.0, result.getNormalizedValue(), 0.0001); // ms → s
    }

    @Test
    void testNormalizeLogEvent() {
        LogEvent event = new LogEvent();
        event.setEntityId("svc");
        event.setEntityType("API_NODE");
        event.setSourceType("performance");
        event.setSeverity("WARN3");
        event.setBody("  test  ");
        event.setTimestamp(System.currentTimeMillis());

        LogEvent result = EventNormalizer.normalize(event);
        assertNotNull(result);
        assertEquals("WARN", result.getSeverity());
        assertEquals("test", result.getBody());
    }

    @Test
    void testNormalizeTraceEvent() {
        TraceEvent event = new TraceEvent();
        event.setEntityId("svc");
        event.setEntityType("API_NODE");
        event.setSourceType("performance");
        event.setTraceId("abc");
        event.setSpanId("d");
        event.setStatus("1");
        event.setTimestamp(System.currentTimeMillis());

        TraceEvent result = EventNormalizer.normalize(event);
        assertNotNull(result);
        assertEquals(32, result.getTraceId().length());
        assertEquals(16, result.getSpanId().length());
        assertEquals("OK", result.getStatus());
    }

    @Test
    void testNormalizeSecurityEvent() {
        SecurityEvent event = new SecurityEvent();
        event.setEntityId("db-server");
        event.setEntityType("SECURITY_NODE");
        event.setSourceType("security");
        event.setAttackType("brute_force");
        event.setConfidence(1.5); // Out of [0, 1]
        event.setTimestamp(System.currentTimeMillis());

        SecurityEvent result = EventNormalizer.normalize(event);
        assertNotNull(result);
        assertEquals("BRUTE_FORCE", result.getAttackType());
        assertEquals(1.0, result.getConfidence(), 0.0001); // Clamped
        assertNotNull(result.getEventId());
        assertNotNull(result.getDescription());
    }

    @Test
    void testNormalizeSecurityEventClampsConfidenceBelowZero() {
        SecurityEvent event = new SecurityEvent();
        event.setEntityId("svc");
        event.setEntityType("SECURITY_NODE");
        event.setSourceType("security");
        event.setAttackType("test");
        event.setConfidence(-0.5);
        event.setTimestamp(System.currentTimeMillis());

        SecurityEvent result = EventNormalizer.normalize(event);
        assertEquals(0.0, result.getConfidence(), 0.0001);
    }

    @Test
    void testNullEventReturnsNull() {
        assertNull(EventNormalizer.normalize((MetricEvent) null));
        assertNull(EventNormalizer.normalize((LogEvent) null));
        assertNull(EventNormalizer.normalize((TraceEvent) null));
        assertNull(EventNormalizer.normalize((SecurityEvent) null));
    }

    @Test
    void testNullEntityIdReturnsNull() {
        MetricEvent event = new MetricEvent();
        event.setEntityId(null);
        event.setEntityType("API_NODE");
        event.setSourceType("performance");
        event.setTimestamp(System.currentTimeMillis());

        assertNull(EventNormalizer.normalize(event));
    }

    @Test
    void testEmptyEntityIdReturnsNull() {
        MetricEvent event = new MetricEvent();
        event.setEntityId("");
        event.setEntityType("API_NODE");
        event.setSourceType("performance");
        event.setTimestamp(System.currentTimeMillis());

        assertNull(EventNormalizer.normalize(event));
    }

    @Test
    void testNullEntityTypeDefaultsToApiNode() {
        MetricEvent event = new MetricEvent();
        event.setEntityId("test");
        event.setEntityType(null);
        event.setSourceType("performance");
        event.setTimestamp(System.currentTimeMillis());

        MetricEvent result = EventNormalizer.normalize(event);
        assertNotNull(result);
        assertEquals("API_NODE", result.getEntityType());
    }

    @Test
    void testZeroTimestampReplacedWithCurrentTime() {
        MetricEvent event = new MetricEvent();
        event.setEntityId("test");
        event.setEntityType("API_NODE");
        event.setSourceType("performance");
        event.setTimestamp(0L);

        MetricEvent result = EventNormalizer.normalize(event);
        assertNotNull(result);
        assertTrue(result.getTimestamp() > 0);
    }

    @Test
    void testGetAttribute() {
        MetricEvent event = new MetricEvent();
        event.setEntityId("test");
        event.setEntityType("API_NODE");
        event.setSourceType("performance");
        event.setTimestamp(1L);
        event.setAttributes(java.util.Map.of("key1", "value1"));

        assertEquals("value1", EventNormalizer.getAttribute(event, "key1"));
        assertNull(EventNormalizer.getAttribute(event, "nonexistent"));
        assertNull(EventNormalizer.getAttribute(event, null));
    }

    @Test
    void testGenericNormalizeDispatchesToCorrectType() {
        NormalizedEvent metric = new MetricEvent();
        metric.setEntityId("svc");
        metric.setEntityType("API_NODE");
        metric.setSourceType("performance");
        metric.setTimestamp(1L);

        NormalizedEvent result = EventNormalizer.normalize(metric);
        assertTrue(result instanceof MetricEvent);
    }
}
