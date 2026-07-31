/*
 * OmniWatch — Windowing Layer
 * Component: FeatureVectorBuilderTest
 * Phase: 4
 * Purpose: Unit tests for feature vector building: merge logic across
 *          multiple WindowedFeatures, per-entity state isolation, multi-window
 *          size handling, and FeatureVector field correctness (all 15 fields).
 * Inputs: WindowedFeature stream (keyed by entityId)
 * Outputs: FeatureVector assertions
 */
package com.omniwatch.features.operators;

import com.omniwatch.features.models.FeatureVector;
import com.omniwatch.features.models.WindowedFeature;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FeatureVectorBuilderTest {

    // ---- Helpers ----

    private static WindowedFeature wf(String entityId, String windowSize,
                                       double p50, double p95, double p99,
                                       double avg, double min, double max,
                                       long count,
                                       long windowStart, long windowEnd) {
        WindowedFeature w = new WindowedFeature();
        w.setEntityId(entityId);
        w.setWindowSize(windowSize);
        w.setP50(p50);
        w.setP95(p95);
        w.setP99(p99);
        w.setAvg(avg);
        w.setMin(min);
        w.setMax(max);
        w.setCount(count);
        w.setWindowStart(windowStart);
        w.setWindowEnd(windowEnd);
        return w;
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, WindowedFeature, FeatureVector>
            openHarness(FeatureVectorBuilder builder) throws Exception {
        KeyedProcessOperator<String, WindowedFeature, FeatureVector> operator =
                new KeyedProcessOperator<>(builder);
        KeyedOneInputStreamOperatorTestHarness<String, WindowedFeature, FeatureVector> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        WindowedFeature::getEntityId,
                        TypeInformation.of(String.class));
        harness.open();
        return harness;
    }

    @SuppressWarnings("unchecked")
    private static FeatureVector emit(Object record) {
        return ((StreamRecord<FeatureVector>) record).getValue();
    }

    // ---- Tests ----

    @Test
    void emitsFeatureVectorWithAll15Fields() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, WindowedFeature, FeatureVector> h =
                openHarness(new FeatureVectorBuilder())) {
            WindowedFeature input = wf("entity-1", "5m",
                    10.0, 20.0, 30.0, 15.0, 5.0, 50.0, 100,
                    1672531200000L, 1672531500000L);
            h.processElement(new StreamRecord<>(input, 1L));

            assertEquals(1, h.getOutput().size());
            FeatureVector fv = emit(h.getOutput().poll());

            // Verify all 15 fields with correct types and values
            assertEquals("entity-1", fv.getEntityId());
            assertNotNull(fv.getWindowStart());
            assertNotNull(fv.getWindowEnd());
            assertEquals("5m", fv.getWindowSize());
            assertEquals(10.0, fv.getLatencyP50());
            assertEquals(20.0, fv.getLatencyP95());
            assertEquals(30.0, fv.getLatencyP99());
            assertEquals(15.0, fv.getLatencyAvg());
            assertEquals(5.0, fv.getLatencyMin());
            assertEquals(50.0, fv.getLatencyMax());
            assertEquals(0.0, fv.getErrorRate());
            assertEquals(100, fv.getRequestVolume());
            assertEquals(1, fv.getFeatureVersion());
            assertEquals(90, fv.getTtl());
            assertNotNull(fv.getTimestamp());

            // ISO-8601 format checks
            assertTrue(fv.getWindowStart().endsWith("Z"),
                    "windowStart should be ISO-8601: " + fv.getWindowStart());
            assertTrue(fv.getWindowEnd().endsWith("Z"),
                    "windowEnd should be ISO-8601: " + fv.getWindowEnd());
            assertTrue(fv.getTimestamp().endsWith("Z"),
                    "timestamp should be ISO-8601: " + fv.getTimestamp());
        }
    }

    @Test
    void mergesMultipleWindowedFeaturesSameWindowSize() throws Exception {
        FeatureVectorBuilder builder = new FeatureVectorBuilder();
        try (KeyedOneInputStreamOperatorTestHarness<String, WindowedFeature, FeatureVector> h =
                openHarness(builder)) {
            // First 5m window: p50=10, avg=15, min=5, max=50, count=100
            h.processElement(new StreamRecord<>(
                    wf("entity-1", "5m",
                            10.0, 20.0, 30.0, 15.0, 5.0, 50.0, 100,
                            1000L, 301000L),
                    1L));

            // Second 5m window: p50=12, avg=17, min=3, max=55, count=80
            h.processElement(new StreamRecord<>(
                    wf("entity-1", "5m",
                            12.0, 22.0, 32.0, 17.0, 3.0, 55.0, 80,
                            301000L, 601000L),
                    2L));

            assertEquals(2, h.getOutput().size());

            // --- First emission: single window stats ---
            FeatureVector fv1 = emit(h.getOutput().poll());
            assertEquals("entity-1", fv1.getEntityId());
            assertEquals("5m", fv1.getWindowSize());
            assertEquals(10.0, fv1.getLatencyP50());
            assertEquals(15.0, fv1.getLatencyAvg());
            assertEquals(5.0, fv1.getLatencyMin());
            assertEquals(50.0, fv1.getLatencyMax());
            assertEquals(100, fv1.getRequestVolume());
            assertEquals(1, fv1.getFeatureVersion());

            // --- Second emission: merged stats ---
            FeatureVector fv2 = emit(h.getOutput().poll());
            assertEquals("entity-1", fv2.getEntityId());
            assertEquals("5m", fv2.getWindowSize());

            // p50/p95/p99: latest values from second window
            assertEquals(12.0, fv2.getLatencyP50());
            assertEquals(22.0, fv2.getLatencyP95());
            assertEquals(32.0, fv2.getLatencyP99());

            // avg: weighted average = (15*100 + 17*80) / (100+80) = 2860/180
            double expectedAvg = (15.0 * 100 + 17.0 * 80) / 180.0;
            assertEquals(expectedAvg, fv2.getLatencyAvg(), 0.01);

            // min/max: global across windows
            assertEquals(3.0, fv2.getLatencyMin());
            assertEquals(55.0, fv2.getLatencyMax());

            // requestVolume: sum of counts
            assertEquals(180, fv2.getRequestVolume());

            // featureVersion: incremented
            assertEquals(2, fv2.getFeatureVersion());

            // window bounds: earliest start, latest end
            assertEquals("1970-01-01T00:00:01Z", fv2.getWindowStart());
            assertEquals("1970-01-01T00:10:01Z", fv2.getWindowEnd());
        }
    }

    @Test
    void handlesMultipleWindowSizes() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, WindowedFeature, FeatureVector> h =
                openHarness(new FeatureVectorBuilder())) {
            h.processElement(new StreamRecord<>(
                    wf("entity-1", "5m",
                            10.0, 20.0, 30.0, 15.0, 5.0, 50.0, 100,
                            1000L, 301000L),
                    1L));
            h.processElement(new StreamRecord<>(
                    wf("entity-1", "15m",
                            12.0, 25.0, 35.0, 18.0, 3.0, 60.0, 500,
                            1000L, 901000L),
                    2L));
            h.processElement(new StreamRecord<>(
                    wf("entity-1", "1h",
                            14.0, 28.0, 40.0, 20.0, 2.0, 70.0, 1000,
                            1000L, 3601000L),
                    3L));
            h.processElement(new StreamRecord<>(
                    wf("entity-1", "6h",
                            16.0, 30.0, 45.0, 22.0, 1.0, 80.0, 5000,
                            1000L, 21601000L),
                    4L));

            assertEquals(4, h.getOutput().size());

            FeatureVector fv1 = emit(h.getOutput().poll());
            assertEquals("5m", fv1.getWindowSize());
            assertEquals(100, fv1.getRequestVolume());

            FeatureVector fv2 = emit(h.getOutput().poll());
            assertEquals("15m", fv2.getWindowSize());
            assertEquals(500, fv2.getRequestVolume());

            FeatureVector fv3 = emit(h.getOutput().poll());
            assertEquals("1h", fv3.getWindowSize());
            assertEquals(1000, fv3.getRequestVolume());

            FeatureVector fv4 = emit(h.getOutput().poll());
            assertEquals("6h", fv4.getWindowSize());
            assertEquals(5000, fv4.getRequestVolume());
        }
    }

    @Test
    void emitsDistinctEntitiesIndependently() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, WindowedFeature, FeatureVector> h =
                openHarness(new FeatureVectorBuilder())) {
            h.processElement(new StreamRecord<>(
                    wf("entity-1", "5m",
                            10.0, 20.0, 30.0, 15.0, 5.0, 50.0, 100,
                            1000L, 301000L),
                    1L));
            h.processElement(new StreamRecord<>(
                    wf("entity-2", "5m",
                            5.0, 10.0, 15.0, 8.0, 2.0, 25.0, 200,
                            1000L, 301000L),
                    2L));

            assertEquals(2, h.getOutput().size());

            // Each entity gets its own featureVersion starting at 1
            FeatureVector fv1 = emit(h.getOutput().poll());
            FeatureVector fv2 = emit(h.getOutput().poll());

            assertEquals("entity-1", fv1.getEntityId());
            assertEquals(1, fv1.getFeatureVersion());
            assertEquals(100, fv1.getRequestVolume());

            assertEquals("entity-2", fv2.getEntityId());
            assertEquals(1, fv2.getFeatureVersion());
            assertEquals(200, fv2.getRequestVolume());
        }
    }

    @Test
    void defaultTtlIsNinety() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, WindowedFeature, FeatureVector> h =
                openHarness(new FeatureVectorBuilder())) {
            h.processElement(new StreamRecord<>(
                    wf("entity-1", "5m",
                            10.0, 20.0, 30.0, 15.0, 5.0, 50.0, 100,
                            1000L, 301000L),
                    1L));

            FeatureVector fv = emit(h.getOutput().poll());
            assertEquals(90, fv.getTtl());
        }
    }

    @Test
    void featureVersionIncrementsPerEmission() throws Exception {
        FeatureVectorBuilder builder = new FeatureVectorBuilder();
        try (KeyedOneInputStreamOperatorTestHarness<String, WindowedFeature, FeatureVector> h =
                openHarness(builder)) {
            h.processElement(new StreamRecord<>(
                    wf("entity-1", "5m",
                            10.0, 20.0, 30.0, 15.0, 5.0, 50.0, 100,
                            1000L, 301000L),
                    1L));
            h.processElement(new StreamRecord<>(
                    wf("entity-1", "5m",
                            12.0, 22.0, 32.0, 17.0, 3.0, 55.0, 80,
                            301000L, 601000L),
                    2L));
            h.processElement(new StreamRecord<>(
                    wf("entity-1", "5m",
                            14.0, 24.0, 34.0, 19.0, 1.0, 60.0, 120,
                            601000L, 901000L),
                    3L));

            assertEquals(3, h.getOutput().size());

            assertEquals(1, emit(h.getOutput().poll()).getFeatureVersion());
            assertEquals(2, emit(h.getOutput().poll()).getFeatureVersion());
            assertEquals(3, emit(h.getOutput().poll()).getFeatureVersion());
        }
    }

    @Test
    void skipsNullWindowSize() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, WindowedFeature, FeatureVector> h =
                openHarness(new FeatureVectorBuilder())) {
            WindowedFeature input = wf("entity-1", null,
                    10.0, 20.0, 30.0, 15.0, 5.0, 50.0, 100,
                    1000L, 301000L);
            h.processElement(new StreamRecord<>(input, 1L));

            // No output — null windowSize is rejected
            assertEquals(0, h.getOutput().size());
        }
    }

    @Test
    void windowBoundsTrackEarliestStartAndLatestEnd() throws Exception {
        FeatureVectorBuilder builder = new FeatureVectorBuilder();
        try (KeyedOneInputStreamOperatorTestHarness<String, WindowedFeature, FeatureVector> h =
                openHarness(builder)) {
            // First window: start=5000, end=305000
            h.processElement(new StreamRecord<>(
                    wf("entity-1", "15m",
                            10.0, 20.0, 30.0, 15.0, 5.0, 50.0, 100,
                            5000L, 305000L),
                    1L));

            // Second window: start=1000 (earlier), end=601000 (later)
            h.processElement(new StreamRecord<>(
                    wf("entity-1", "15m",
                            12.0, 22.0, 32.0, 17.0, 3.0, 55.0, 80,
                            1000L, 601000L),
                    2L));

            assertEquals(2, h.getOutput().size());
            h.getOutput().poll(); // discard first emission (windowStart=5000)
            FeatureVector fv = emit(h.getOutput().poll()); // second emission

            // windowStart = earliest (1000), windowEnd = latest (601000)
            assertEquals("1970-01-01T00:00:01Z", fv.getWindowStart());
            assertEquals("1970-01-01T00:10:01Z", fv.getWindowEnd());
        }
    }
}
