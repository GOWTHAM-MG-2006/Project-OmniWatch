/*
 * OmniWatch — Windowing Layer + Feature Store
 * Component: TumblingWindowAggregatorTest
 * Phase: 4
 * Purpose: Unit tests for tumbling window aggregation — verifies min/max/avg/
 *          count/sum correctness, merge behaviour, and edge cases.
 * Inputs: TumblingWindowAggregator direct function calls
 * Outputs: WindowedFeature assertion results
 */
package com.omniwatch.features.operators;

import com.omniwatch.features.models.MetricsEvent;
import com.omniwatch.features.models.WindowedFeature;
import com.omniwatch.features.operators.TumblingWindowAggregator.MetricAccum;
import com.omniwatch.features.operators.TumblingWindowAggregator.TumblingAccumulator;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Direct AggregateFunction unit tests (no Flink test harness needed).
 * Instantiates the function and calls createAccumulator / add / getResult / merge
 * directly — simpler and faster than OneInputStreamOperatorTestHarness.
 */
class TumblingWindowAggregatorTest {

    private static final long WINDOW_START = 0L;
    private static final long WINDOW_END = 60_000L; // 1 minute

    private static MetricsEvent event(String metricName, double value, long timestamp) {
        MetricsEvent e = new MetricsEvent();
        e.setEntityId("entity-1");
        e.setMetricName(metricName);
        e.setValue(value);
        e.setTimestamp(timestamp);
        e.setError(false);
        e.setSourceType("performance");
        return e;
    }

    // ------------------------------------------------------------------ //
    //  Basic aggregation                                                   //
    // ------------------------------------------------------------------ //

    @Test
    void aggregatesSingleMetricCorrectly() {
        TumblingWindowAggregator agg =
                new TumblingWindowAggregator("1m", WINDOW_START, WINDOW_END);

        TumblingAccumulator acc = agg.createAccumulator();
        // 5 events: values 10, 20, 30, 40, 50
        acc = agg.add(event("latency", 10, 0), acc);
        acc = agg.add(event("latency", 20, 1000), acc);
        acc = agg.add(event("latency", 30, 2000), acc);
        acc = agg.add(event("latency", 40, 3000), acc);
        acc = agg.add(event("latency", 50, 4000), acc);

        WindowedFeature result = agg.getResult(acc);

        assertEquals("latency", result.getMetricName());
        assertEquals("1m", result.getWindowSize());
        assertEquals(WINDOW_START, result.getWindowStart());
        assertEquals(WINDOW_END, result.getWindowEnd());
        assertEquals(5, result.getCount());
        assertEquals(10.0, result.getMin(), 1e-9);
        assertEquals(50.0, result.getMax(), 1e-9);
        assertEquals(150.0, result.getSum(), 1e-9); // 10+20+30+40+50
        assertEquals(30.0, result.getAvg(), 1e-9);  // 150/5
    }

    @Test
    void avgComputedAtGetResult() {
        TumblingWindowAggregator agg =
                new TumblingWindowAggregator("5m", 1000L, 310_000L);
        TumblingAccumulator acc = agg.createAccumulator();

        acc = agg.add(event("cpu", 100, 1000), acc);
        acc = agg.add(event("cpu", 200, 2000), acc);

        WindowedFeature result = agg.getResult(acc);
        // avg = 300/2 = 150
        assertEquals(150.0, result.getAvg(), 1e-9);
        assertEquals("5m", result.getWindowSize());
    }

    // ------------------------------------------------------------------ //
    //  Multiple metrics in the same window                                 //
    // ------------------------------------------------------------------ //

    @Test
    void handlesMultipleMetricsInAccumulator() {
        TumblingWindowAggregator agg =
                new TumblingWindowAggregator("1m", WINDOW_START, WINDOW_END);
        TumblingAccumulator acc = agg.createAccumulator();

        acc = agg.add(event("latency", 10, 0), acc);
        acc = agg.add(event("cpu", 80, 0), acc);
        acc = agg.add(event("latency", 20, 1000), acc);
        acc = agg.add(event("cpu", 90, 1000), acc);

        // Both metrics present in accumulator
        assertEquals(2, acc.metrics.size());

        WindowedFeature result = agg.getResult(acc);
        // getResult returns the first metric encountered; verify it's valid
        assertNotNull(result.getMetricName());
        assertTrue(result.getCount() > 0);
    }

    // ------------------------------------------------------------------ //
    //  Edge cases                                                          //
    // ------------------------------------------------------------------ //

    @Test
    void emptyAccumulatorReturnsBlankFeature() {
        TumblingWindowAggregator agg =
                new TumblingWindowAggregator("15m", 0L, 900_000L);
        TumblingAccumulator acc = agg.createAccumulator();

        WindowedFeature result = agg.getResult(acc);

        assertEquals("15m", result.getWindowSize());
        assertEquals(0L, result.getWindowStart());
        assertEquals(900_000L, result.getWindowEnd());
        assertEquals(0, result.getCount());
        assertEquals(0.0, result.getAvg(), 1e-9);
    }

    @Test
    void singleEventAccumulation() {
        TumblingWindowAggregator agg =
                new TumblingWindowAggregator("1m", WINDOW_START, WINDOW_END);
        TumblingAccumulator acc = agg.createAccumulator();

        acc = agg.add(event("errors", 42.0, 500), acc);
        WindowedFeature result = agg.getResult(acc);

        assertEquals(1, result.getCount());
        assertEquals(42.0, result.getMin(), 1e-9);
        assertEquals(42.0, result.getMax(), 1e-9);
        assertEquals(42.0, result.getSum(), 1e-9);
        assertEquals(42.0, result.getAvg(), 1e-9);
    }

    @Test
    void negativeValuesHandledCorrectly() {
        TumblingWindowAggregator agg =
                new TumblingWindowAggregator("1m", WINDOW_START, WINDOW_END);
        TumblingAccumulator acc = agg.createAccumulator();

        acc = agg.add(event("delta", -10, 0), acc);
        acc = agg.add(event("delta", 5, 1000), acc);
        acc = agg.add(event("delta", -3, 2000), acc);

        WindowedFeature result = agg.getResult(acc);
        assertEquals(3, result.getCount());
        assertEquals(-10.0, result.getMin(), 1e-9);
        assertEquals(5.0, result.getMax(), 1e-9);
        assertEquals(-8.0, result.getSum(), 1e-9);   // -10 + 5 + (-3)
        assertEquals(-8.0 / 3.0, result.getAvg(), 1e-9);
    }

    @Test
    void duplicateValuesCountedCorrectly() {
        TumblingWindowAggregator agg =
                new TumblingWindowAggregator("1m", WINDOW_START, WINDOW_END);
        TumblingAccumulator acc = agg.createAccumulator();

        acc = agg.add(event("mem", 100, 0), acc);
        acc = agg.add(event("mem", 100, 1000), acc);
        acc = agg.add(event("mem", 100, 2000), acc);

        WindowedFeature result = agg.getResult(acc);
        assertEquals(3, result.getCount());
        assertEquals(100.0, result.getMin(), 1e-9);
        assertEquals(100.0, result.getMax(), 1e-9);
        assertEquals(100.0, result.getAvg(), 1e-9);
    }

    // ------------------------------------------------------------------ //
    //  Merge (required by AggregateFunction for session windows)           //
    // ------------------------------------------------------------------ //

    @Test
    void mergeCombinesTwoAccumulatorsCorrectly() {
        TumblingWindowAggregator agg =
                new TumblingWindowAggregator("1m", WINDOW_START, WINDOW_END);

        // Build accumulator 1: latency [10, 20]
        TumblingAccumulator acc1 = agg.createAccumulator();
        acc1 = agg.add(event("latency", 10, 0), acc1);
        acc1 = agg.add(event("latency", 20, 1000), acc1);

        // Build accumulator 2: latency [30, 40]
        TumblingAccumulator acc2 = agg.createAccumulator();
        acc2 = agg.add(event("latency", 30, 2000), acc2);
        acc2 = agg.add(event("latency", 40, 3000), acc2);

        TumblingAccumulator merged = agg.merge(acc1, acc2);
        WindowedFeature result = agg.getResult(merged);

        assertEquals("latency", result.getMetricName());
        assertEquals(4, result.getCount());
        assertEquals(10.0, result.getMin(), 1e-9);
        assertEquals(40.0, result.getMax(), 1e-9);
        assertEquals(100.0, result.getSum(), 1e-9);  // 10+20+30+40
        assertEquals(25.0, result.getAvg(), 1e-9);   // 100/4
    }

    @Test
    void mergeHandlesDifferentMetricNames() {
        TumblingWindowAggregator agg =
                new TumblingWindowAggregator("5m", WINDOW_START, WINDOW_END);

        TumblingAccumulator acc1 = agg.createAccumulator();
        acc1 = agg.add(event("latency", 10, 0), acc1);

        TumblingAccumulator acc2 = agg.createAccumulator();
        acc2 = agg.add(event("cpu", 80, 0), acc2);

        TumblingAccumulator merged = agg.merge(acc1, acc2);

        // Both metrics present after merge
        assertEquals(2, merged.metrics.size());
        assertTrue(merged.metrics.containsKey("latency"));
        assertTrue(merged.metrics.containsKey("cpu"));
    }

    @Test
    void mergeWithEmptyAccumulator() {
        TumblingWindowAggregator agg =
                new TumblingWindowAggregator("1m", WINDOW_START, WINDOW_END);

        TumblingAccumulator acc1 = agg.createAccumulator();
        acc1 = agg.add(event("latency", 10, 0), acc1);

        TumblingAccumulator acc2 = agg.createAccumulator(); // empty

        TumblingAccumulator merged = agg.merge(acc1, acc2);
        WindowedFeature result = agg.getResult(merged);

        assertEquals(1, result.getCount());
        assertEquals(10.0, result.getSum(), 1e-9);
    }

    @Test
    void mergeEmptyIntoNonEmpty() {
        TumblingWindowAggregator agg =
                new TumblingWindowAggregator("1m", WINDOW_START, WINDOW_END);

        TumblingAccumulator empty = agg.createAccumulator();
        TumblingAccumulator nonEmpty = agg.createAccumulator();
        nonEmpty = agg.add(event("cpu", 50, 0), nonEmpty);

        TumblingAccumulator merged = agg.merge(empty, nonEmpty);
        WindowedFeature result = agg.getResult(merged);

        assertEquals(1, result.getCount());
        assertEquals(50.0, result.getSum(), 1e-9);
    }

    @Test
    void mergeBothEmpty() {
        TumblingWindowAggregator agg =
                new TumblingWindowAggregator("1m", WINDOW_START, WINDOW_END);

        TumblingAccumulator acc1 = agg.createAccumulator();
        TumblingAccumulator acc2 = agg.createAccumulator();

        TumblingAccumulator merged = agg.merge(acc1, acc2);
        WindowedFeature result = agg.getResult(merged);

        assertEquals(0, result.getCount());
        assertEquals("1m", result.getWindowSize());
    }

    // ------------------------------------------------------------------ //
    //  Window bounds and label                                             //
    // ------------------------------------------------------------------ //

    @Test
    void windowBoundsAndLabelPropagated() {
        long start = 1_000_000L;
        long end = 1_060_000L;
        TumblingWindowAggregator agg =
                new TumblingWindowAggregator("15m", start, end);

        assertEquals("15m", agg.getWindowLabel());
        assertEquals(start, agg.getWindowStart());
        assertEquals(end, agg.getWindowEnd());

        TumblingAccumulator acc = agg.createAccumulator();
        acc = agg.add(event("net", 7.5, start), acc);
        WindowedFeature result = agg.getResult(acc);

        assertEquals(start, result.getWindowStart());
        assertEquals(end, result.getWindowEnd());
        assertEquals("15m", result.getWindowSize());
    }

    // ------------------------------------------------------------------ //
    //  Large value set — simulates a 2-minute span (t=0..9, t=61..70)     //
    // ------------------------------------------------------------------ //

    @Test
    void tenEventsAcrossTwoMinutesInOneMinuteWindow() {
        long minuteMs = 60_000L;
        TumblingWindowAggregator agg =
                new TumblingWindowAggregator("1m", 0, minuteMs);
        TumblingAccumulator acc = agg.createAccumulator();

        // t=0..9 — all within the first 1-minute window (0ms–60000ms)
        double[] values = {10, 20, 30, 40, 50, 15, 25, 35, 45, 55};
        for (int i = 0; i < 10; i++) {
            acc = agg.add(event("latency", values[i], (long) i * 1000), acc);
        }

        // t=61..70 — would fall outside the 1-minute window in real Flink
        // but in direct AggregateFunction testing we just feed them in
        // (the window boundary is conceptual — Flink handles filtering)
        for (int i = 0; i < 10; i++) {
            acc = agg.add(event("latency", values[i] + 100,
                    minuteMs + (long) i * 1000), acc);
        }

        WindowedFeature result = agg.getResult(acc);

        assertEquals("latency", result.getMetricName());
        assertEquals(20, result.getCount());
        assertEquals(10.0, result.getMin(), 1e-9);
        assertEquals(155.0, result.getMax(), 1e-9);  // 55+100
        // sum = (10+20+30+40+50+15+25+35+45+55) + (110+120+130+140+150+115+125+135+145+155)
        double expectedSum = (10+20+30+40+50+15+25+35+45+55)
                + (110+120+130+140+150+115+135+145+155+125);
        assertEquals(expectedSum, result.getSum(), 1e-9);
        assertEquals(expectedSum / 20.0, result.getAvg(), 1e-9);
    }

    // ------------------------------------------------------------------ //
    //  Accumulator internals                                               //
    // ------------------------------------------------------------------ //

    @Test
    void metricAccumDefaultsAreCorrect() {
        MetricAccum m = new MetricAccum();
        assertEquals(Double.MAX_VALUE, m.min, 1e-9);
        assertEquals(Double.MIN_VALUE, m.max, 1e-9);
        assertEquals(0.0, m.sum, 1e-9);
        assertEquals(0, m.count);
    }

    @Test
    void tumblingAccumulatorStartsWithEmptyMap() {
        TumblingAccumulator acc = new TumblingAccumulator();
        assertNotNull(acc.metrics);
        assertTrue(acc.metrics.isEmpty());
    }
}
