package com.omniwatch.flink.models;

import java.util.Map;

/**
 * Normalized metric telemetry event.
 * Carries a single metric data point with its original and normalized value.
 */
public class MetricEvent extends NormalizedEvent {

    private String metricName;
    private double value;
    private double normalizedValue;
    private String unit;

    public MetricEvent() {
        super();
    }

    public MetricEvent(String entityId, String entityType, long timestamp,
                       String sourceType, String sourceTopic,
                       Map<String, String> attributes,
                       String metricName, double value, double normalizedValue,
                       String unit) {
        super(entityId, entityType, timestamp, sourceType, sourceTopic, attributes);
        this.metricName = metricName;
        this.value = value;
        this.normalizedValue = normalizedValue;
        this.unit = unit;
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

    public double getNormalizedValue() {
        return normalizedValue;
    }

    public void setNormalizedValue(double normalizedValue) {
        this.normalizedValue = normalizedValue;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }
}
