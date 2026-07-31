/*
 * OmniWatch — Windowing Layer + Feature Store
 * Component: FeatureVector model
 * Phase: 4
 * Purpose: POJO representing the final 15-column feature vector written to
 *          the ClickHouse feature_vectors table by FeatureStoreWriter and
 *          published to the windowed_15m Kafka topic by FeatureVectorBuilder.
 * Inputs: FeatureVectorBuilder operator
 * Outputs: ClickHouse feature_vectors table + Kafka omniwatch.features.windowed_15m
 */
package com.omniwatch.features.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Final feature vector for ML consumption (Phase 6 anomaly detection).
 * 15 fields matching the ClickHouse {@code feature_vectors} table schema
 * and the Feature Store API pydantic model exactly. Timestamps are ISO-8601
 * strings (serialized as such by Jackson).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureVector {

    private String entityId;
    private String windowStart;
    private String windowEnd;
    private String windowSize;
    private double latencyP50;
    private double latencyP95;
    private double latencyP99;
    private double latencyAvg;
    private double latencyMin;
    private double latencyMax;
    private double errorRate;
    private long requestVolume;
    private int featureVersion;
    private int ttl;
    private String timestamp;

    /** Required no-arg constructor for Flink POJO serialization + Jackson. */
    public FeatureVector() {
        this.ttl = 90;
    }

    // ---- Getters / Setters ----

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public String getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(String windowStart) {
        this.windowStart = windowStart;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }

    public String getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(String windowSize) {
        this.windowSize = windowSize;
    }

    public double getLatencyP50() {
        return latencyP50;
    }

    public void setLatencyP50(double latencyP50) {
        this.latencyP50 = latencyP50;
    }

    public double getLatencyP95() {
        return latencyP95;
    }

    public void setLatencyP95(double latencyP95) {
        this.latencyP95 = latencyP95;
    }

    public double getLatencyP99() {
        return latencyP99;
    }

    public void setLatencyP99(double latencyP99) {
        this.latencyP99 = latencyP99;
    }

    public double getLatencyAvg() {
        return latencyAvg;
    }

    public void setLatencyAvg(double latencyAvg) {
        this.latencyAvg = latencyAvg;
    }

    public double getLatencyMin() {
        return latencyMin;
    }

    public void setLatencyMin(double latencyMin) {
        this.latencyMin = latencyMin;
    }

    public double getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(double latencyMax) {
        this.latencyMax = latencyMax;
    }

    public double getErrorRate() {
        return errorRate;
    }

    public void setErrorRate(double errorRate) {
        this.errorRate = errorRate;
    }

    public long getRequestVolume() {
        return requestVolume;
    }

    public void setRequestVolume(long requestVolume) {
        this.requestVolume = requestVolume;
    }

    public int getFeatureVersion() {
        return featureVersion;
    }

    public void setFeatureVersion(int featureVersion) {
        this.featureVersion = featureVersion;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
