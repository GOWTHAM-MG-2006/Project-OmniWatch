package com.omniwatch.flink.models;

import java.util.Map;

/**
 * Normalized trace span telemetry event.
 * Represents a single span within a distributed trace.
 */
public class TraceEvent extends NormalizedEvent {

    private String traceId;
    private String spanId;
    private String parentSpanId;
    private String spanName;
    private long startTime;
    private long durationMs;
    private String status;

    public TraceEvent() {
        super();
    }

    public TraceEvent(String entityId, String entityType, long timestamp,
                      String sourceType, String sourceTopic,
                      Map<String, String> attributes,
                      String traceId, String spanId, String parentSpanId,
                      String spanName, long startTime, long durationMs,
                      String status) {
        super(entityId, entityType, timestamp, sourceType, sourceTopic, attributes);
        this.traceId = traceId;
        this.spanId = spanId;
        this.parentSpanId = parentSpanId;
        this.spanName = spanName;
        this.startTime = startTime;
        this.durationMs = durationMs;
        this.status = status;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getSpanId() {
        return spanId;
    }

    public void setSpanId(String spanId) {
        this.spanId = spanId;
    }

    public String getParentSpanId() {
        return parentSpanId;
    }

    public void setParentSpanId(String parentSpanId) {
        this.parentSpanId = parentSpanId;
    }

    public String getSpanName() {
        return spanName;
    }

    public void setSpanName(String spanName) {
        this.spanName = spanName;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getDurationMs() {
        return durationMs;
    }

    public void setDurationMs(long durationMs) {
        this.durationMs = durationMs;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
