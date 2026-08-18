/*
 * OmniWatch — Windowing Layer + Feature Store
 * Component: MetricsEvent model
 * Phase: 4
 * Purpose: POJO representing a normalized metrics event consumed from the
 *          omniwatch.metrics.normalized Kafka topic. Parsed from JSON via
 *          Jackson with SNAKE_CASE naming strategy (matches the snake_case
 *          JSON contract: entity_id, metric_name, source_type).
 * Inputs: Kafka topic omniwatch.metrics.normalized (JSON)
 * Outputs: TumblingWindowAggregator / SlidingWindowAggregator / SessionWindowDetector
 */
package com.omniwatch.features.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Normalized metrics event. Fields mirror the Kafka JSON payload
 * (snake_case keys: entity_id, metric_name, value, timestamp, is_error,
 * source_type). Jackson's {@code SNAKE_CASE} naming strategy handles
 * the mapping to Java camelCase getters/setters.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetricsEvent {

    private String entityId;
    private String metricName;
    private double value;
    private long timestamp;
    @JsonProperty("is_error")
    private boolean error;
    private String sourceType;

    /** Required no-arg constructor for Flink POJO serialization + Jackson. */
    public MetricsEvent() {
    }

    public MetricsEvent(String entityId, String metricName, double value,
                        long timestamp, boolean isError, String sourceType) {
        this.entityId = entityId;
        this.metricName = metricName;
        this.value = value;
        this.timestamp = timestamp;
        this.error = isError;
        this.sourceType = sourceType;
    }

    // ---- Getters / Setters ----

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isError() {
        return error;
    }

    public void setError(boolean error) {
        this.error = error;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }
}
