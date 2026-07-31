package com.omniwatch.flink.normalizers;

import com.omniwatch.flink.models.MetricEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MetricNormalizer.
 * Verifies unit conversions: bytes→MB, ms→s, us→s, ns→s, count unchanged.
 */
class MetricNormalizerTest {

    private MetricEvent createMetric(double value, String unit) {
        MetricEvent event = new MetricEvent();
        event.setEntityId("test-service");
        event.setEntityType("API_NODE");
        event.setSourceType("performance");
        event.setValue(value);
        event.setNormalizedValue(value);
        event.setUnit(unit);
        event.setMetricName("test_metric");
        event.setTimestamp(System.currentTimeMillis());
        return event;
    }

    @Test
    void testBytesToMegabytes() {
        MetricEvent event = createMetric(1_048_576.0, "By");
        MetricNormalizer.normalize(event);
        assertEquals(1.0, event.getNormalizedValue(), 0.0001);
    }

    @Test
    void testBytesUnitVariant() {
        MetricEvent event = createMetric(2_097_152.0, "bytes");
        MetricNormalizer.normalize(event);
        assertEquals(2.0, event.getNormalizedValue(), 0.0001);
    }

    @Test
    void testMillisecondsToSeconds() {
        MetricEvent event = createMetric(5000.0, "ms");
        MetricNormalizer.normalize(event);
        assertEquals(5.0, event.getNormalizedValue(), 0.0001);
    }

    @Test
    void testMillisecondsUnitVariant() {
        MetricEvent event = createMetric(1000.0, "milliseconds");
        MetricNormalizer.normalize(event);
        assertEquals(1.0, event.getNormalizedValue(), 0.0001);
    }

    @Test
    void testMicrosecondsToSeconds() {
        MetricEvent event = createMetric(1_000_000.0, "us");
        MetricNormalizer.normalize(event);
        assertEquals(1.0, event.getNormalizedValue(), 0.0001);
    }

    @Test
    void testMicrosecondsUnitVariant() {
        MetricEvent event = createMetric(5_000_000.0, "microseconds");
        MetricNormalizer.normalize(event);
        assertEquals(5.0, event.getNormalizedValue(), 0.0001);
    }

    @Test
    void testNanosecondsToSeconds() {
        MetricEvent event = createMetric(1_000_000_000.0, "ns");
        MetricNormalizer.normalize(event);
        assertEquals(1.0, event.getNormalizedValue(), 0.0001);
    }

    @Test
    void testNanosecondsUnitVariant() {
        MetricEvent event = createMetric(500_000_000.0, "nanoseconds");
        MetricNormalizer.normalize(event);
        assertEquals(0.5, event.getNormalizedValue(), 0.0001);
    }

    @Test
    void testCountUnitUnchanged() {
        MetricEvent event = createMetric(42.0, "count");
        MetricNormalizer.normalize(event);
        assertEquals(42.0, event.getNormalizedValue(), 0.0001);
    }

    @Test
    void testUnitOneUnchanged() {
        MetricEvent event = createMetric(100.0, "1");
        MetricNormalizer.normalize(event);
        assertEquals(100.0, event.getNormalizedValue(), 0.0001);
    }

    @Test
    void testEmptyUnitUnchanged() {
        MetricEvent event = createMetric(50.0, "");
        MetricNormalizer.normalize(event);
        assertEquals(50.0, event.getNormalizedValue(), 0.0001);
    }

    @Test
    void testNullUnitDefaultsToOne() {
        MetricEvent event = createMetric(99.0, null);
        MetricNormalizer.normalize(event);
        assertEquals(99.0, event.getNormalizedValue(), 0.0001);
    }

    @Test
    void testUnknownUnitUnchanged() {
        MetricEvent event = createMetric(77.0, "percent");
        MetricNormalizer.normalize(event);
        assertEquals(77.0, event.getNormalizedValue(), 0.0001);
    }

    @Test
    void testZeroValue() {
        MetricEvent event = createMetric(0.0, "ms");
        MetricNormalizer.normalize(event);
        assertEquals(0.0, event.getNormalizedValue(), 0.0001);
    }

    @Test
    void testNullEventReturnsNull() {
        assertNull(MetricNormalizer.normalize(null));
    }
}
