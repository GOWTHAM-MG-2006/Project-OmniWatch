/*
 * OmniWatch — Windowing Layer + Feature Store
 * Component: WindowedFeature model
 * Phase: 4
 * Purpose: POJO representing a windowed aggregation result produced by
 *          TumblingWindowAggregator, SlidingWindowAggregator, and
 *          SessionWindowDetector. Published to the windowed Kafka topics.
 * Inputs: Window operators (TumblingWindowAggregator, etc.)
 * Outputs: Kafka topics omniwatch.features.windowed_{1m,5m,15m} (JSON)
 */
package com.omniwatch.features.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Windowed feature record. Contains pre-aggregated statistics for a single
 * metric within a time window. Window operators fill the subset of fields
 * they compute; unused fields remain at their default (0.0/0L).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WindowedFeature {

    private String entityId;
    private long windowStart;
    private long windowEnd;
    private String windowSize;
    private String metricName;
    private double min;
    private double max;
    private double avg;
    private long count;
    private double sum;
    private double p50;
    private double p95;
    private double p99;
    private double stddev;
    private double rate;

    /** Required no-arg constructor for Flink POJO serialization + Jackson. */
    public WindowedFeature() {
    }

    // ---- Getters / Setters ----

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public String getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(String windowSize) {
        this.windowSize = windowSize;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public double getAvg() {
        return avg;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public double getP50() {
        return p50;
    }

    public void setP50(double p50) {
        this.p50 = p50;
    }

    public double getP95() {
        return p95;
    }

    public void setP95(double p95) {
        this.p95 = p95;
    }

    public double getP99() {
        return p99;
    }

    public void setP99(double p99) {
        this.p99 = p99;
    }

    public double getStddev() {
        return stddev;
    }

    public void setStddev(double stddev) {
        this.stddev = stddev;
    }

    public double getRate() {
        return rate;
    }

    public void setRate(double rate) {
        this.rate = rate;
    }
}
