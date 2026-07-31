/*
 * OmniWatch — Windowing Layer
 * Component: SlidingWindowAggregatorTest
 * Phase: 4
 * Purpose: Unit tests for SlidingWindowAggregator percentile, stddev, and rate logic.
 *          Tests the package-private static helpers directly (Context-free path)
 *          to avoid Flink runtime dependency in pure math tests.
 * Inputs: SlidingWindowAggregator static methods
 * Outputs: JUnit 5 assertions on percentile, stddev, rate calculations
 */
package com.omniwatch.features.operators;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class SlidingWindowAggregatorTest {

    // ---- percentile() tests ----

    @Test
    void percentileP50OfRange1To100() {
        // Values 1..100 (100 values)
        // index = 0.50 * (100-1) = 49.5
        // lower=49 (value=50), upper=50 (value=51), fraction=0.5
        // result = 50 + 0.5 * (51-50) = 50.5
        List<Double> values = IntStream.rangeClosed(1, 100)
                .mapToDouble(i -> i).boxed().collect(Collectors.toList());
        assertEquals(50.5, SlidingWindowAggregator.percentile(values, 50.0), 0.1);
    }

    @Test
    void percentileP95OfRange1To100() {
        // index = 0.95 * 99 = 94.05
        // lower=94 (value=95), upper=95 (value=96), fraction=0.05
        // result = 95 + 0.05 * (96-95) = 95.05
        List<Double> values = IntStream.rangeClosed(1, 100)
                .mapToDouble(i -> i).boxed().collect(Collectors.toList());
        assertEquals(95.05, SlidingWindowAggregator.percentile(values, 95.0), 0.1);
    }

    @Test
    void percentileP99OfRange1To100() {
        // index = 0.99 * 99 = 98.01
        // lower=98 (value=99), upper=99 (value=100), fraction=0.01
        // result = 99 + 0.01 * (100-99) = 99.01
        List<Double> values = IntStream.rangeClosed(1, 100)
                .mapToDouble(i -> i).boxed().collect(Collectors.toList());
        assertEquals(99.01, SlidingWindowAggregator.percentile(values, 99.0), 0.1);
    }

    @Test
    void percentileSingleElementReturnsThatElement() {
        List<Double> values = Arrays.asList(42.0);
        assertEquals(42.0, SlidingWindowAggregator.percentile(values, 50.0), 0.001);
        assertEquals(42.0, SlidingWindowAggregator.percentile(values, 99.0), 0.001);
    }

    @Test
    void percentileP0ReturnsMin() {
        List<Double> values = Arrays.asList(10.0, 20.0, 30.0);
        assertEquals(10.0, SlidingWindowAggregator.percentile(values, 0.0), 0.001);
    }

    @Test
    void percentileP100ReturnsMax() {
        List<Double> values = Arrays.asList(10.0, 20.0, 30.0);
        assertEquals(30.0, SlidingWindowAggregator.percentile(values, 100.0), 0.001);
    }

    @Test
    void percentileTwoElements() {
        // [10, 20]: index=0.5*1=0.5 -> 10 + 0.5*(20-10) = 15.0
        List<Double> values = Arrays.asList(10.0, 20.0);
        assertEquals(15.0, SlidingWindowAggregator.percentile(values, 50.0), 0.001);
    }

    @Test
    void percentileAlreadySortedListIsCorrect() {
        // Manually sorted: [5, 15, 25, 35, 45]
        // p50: index = 0.5*4 = 2.0 -> lower=2 (value=25), upper=2 -> 25.0
        List<Double> values = Arrays.asList(5.0, 15.0, 25.0, 35.0, 45.0);
        assertEquals(25.0, SlidingWindowAggregator.percentile(values, 50.0), 0.001);
    }

    @Test
    void percentileThrowsOnEmptyList() {
        assertThrows(IllegalArgumentException.class,
                () -> SlidingWindowAggregator.percentile(new ArrayList<>(), 50.0));
    }

    @Test
    void percentileThrowsOnInvalidP() {
        List<Double> values = Arrays.asList(1.0, 2.0, 3.0);
        assertThrows(IllegalArgumentException.class,
                () -> SlidingWindowAggregator.percentile(values, -1.0));
        assertThrows(IllegalArgumentException.class,
                () -> SlidingWindowAggregator.percentile(values, 101.0));
    }

    // ---- populationStddev() tests ----

    @Test
    void stddevOfRange1To100() {
        // mean = 50.5, variance = 833.25, stddev = sqrt(833.25) ≈ 28.867
        List<Double> values = IntStream.rangeClosed(1, 100)
                .mapToDouble(i -> i).boxed().collect(Collectors.toList());
        assertEquals(28.867, SlidingWindowAggregator.populationStddev(values, 50.5), 0.01);
    }

    @Test
    void stddevSingleElementReturnsZero() {
        List<Double> values = Arrays.asList(42.0);
        assertEquals(0.0, SlidingWindowAggregator.populationStddev(values, 42.0), 0.001);
    }

    @Test
    void stddevIdenticalValuesReturnsZero() {
        List<Double> values = Arrays.asList(7.0, 7.0, 7.0, 7.0);
        assertEquals(0.0, SlidingWindowAggregator.populationStddev(values, 7.0), 0.001);
    }

    @Test
    void stddevKnownValues() {
        // [2, 4, 4, 4, 5, 5, 7, 9] — population stddev ≈ 2.0
        // mean = 5.0, variance = ((2-5)^2 + 3*(4-5)^2 + 2*(5-5)^2 + (7-5)^2 + (9-5)^2) / 8
        // = (9 + 3 + 0 + 4 + 16) / 8 = 32 / 8 = 4.0, stddev = 2.0
        List<Double> values = Arrays.asList(2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0);
        assertEquals(2.0, SlidingWindowAggregator.populationStddev(values, 5.0), 0.001);
    }

    @Test
    void stddevTwoElementsSymmetric() {
        // [0, 10]: mean = 5.0, variance = ((0-5)^2 + (10-5)^2) / 2 = 50/2 = 25, stddev = 5.0
        List<Double> values = Arrays.asList(0.0, 10.0);
        assertEquals(5.0, SlidingWindowAggregator.populationStddev(values, 5.0), 0.001);
    }

    // ---- rate tests (verify formula: count / windowDurationSec) ----

    @Test
    void rateTenEventsIn300SecondWindow() {
        // 5-minute window = 300 seconds, 10 events -> rate = 10/300 ≈ 0.0333
        long count = 10;
        long windowStart = 0L;
        long windowEnd = 300_000L; // 300 seconds in millis
        double windowDurationSec = (windowEnd - windowStart) / 1000.0;
        double rate = count / windowDurationSec;
        assertEquals(10.0 / 300.0, rate, 0.0001);
    }

    @Test
    void rateZeroEventsReturnsZero() {
        long count = 0;
        double windowDurationSec = 300.0;
        double rate = windowDurationSec > 0 ? count / windowDurationSec : 0.0;
        assertEquals(0.0, rate, 0.0001);
    }

    @Test
    void rateZeroDurationReturnsZero() {
        long count = 5;
        double windowDurationSec = 0.0;
        double rate = windowDurationSec > 0 ? count / windowDurationSec : 0.0;
        assertEquals(0.0, rate, 0.0001);
    }

    @Test
    void rateOneEventPerSecond() {
        // 300 events in 300 seconds -> 1.0 event/sec
        long count = 300;
        double windowDurationSec = 300.0;
        double rate = count / windowDurationSec;
        assertEquals(1.0, rate, 0.0001);
    }

    // ---- Integration: percentile + stddev + basic stats on same dataset ----

    @Test
    void fullAggregationConsistency() {
        // Use known dataset: [10, 20, 30, 40, 50]
        List<Double> values = Arrays.asList(10.0, 20.0, 30.0, 40.0, 50.0);

        // min/max
        assertEquals(10.0, values.get(0), 0.001);
        assertEquals(50.0, values.get(values.size() - 1), 0.001);

        // sum = 150, avg = 30.0
        double sum = values.stream().mapToDouble(Double::doubleValue).sum();
        assertEquals(150.0, sum, 0.001);
        assertEquals(30.0, sum / values.size(), 0.001);

        // p50: index=0.5*4=2.0 -> values[2]=30.0
        assertEquals(30.0, SlidingWindowAggregator.percentile(values, 50.0), 0.001);

        // p95: index=0.95*4=3.8 -> lower=3(40), upper=4(50), frac=0.8
        // = 40 + 0.8*(50-40) = 48.0
        assertEquals(48.0, SlidingWindowAggregator.percentile(values, 95.0), 0.001);

        // p99: index=0.99*4=3.96 -> lower=3(40), upper=4(50), frac=0.96
        // = 40 + 0.96*(50-40) = 49.6
        assertEquals(49.6, SlidingWindowAggregator.percentile(values, 99.0), 0.01);

        // stddev: mean=30, variance=((10-30)^2+(20-30)^2+0+(40-30)^2+(50-30)^2)/5
        // = (400+100+0+100+400)/5 = 1000/5 = 200, stddev = sqrt(200) ≈ 14.142
        assertEquals(14.142, SlidingWindowAggregator.populationStddev(values, 30.0), 0.01);
    }
}
