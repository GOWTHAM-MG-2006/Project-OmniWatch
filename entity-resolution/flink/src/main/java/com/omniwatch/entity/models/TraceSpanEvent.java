/*
 * OmniWatch — Entity Resolution Layer
 * Component: TraceSpanEvent model
 * Phase: 3
 * Purpose: Minimal trace span view used by RelationshipBuilder to link a
 *          child span's entity to its parent span's entity (CALLS edge).
 * Inputs: Parsed telemetry events carrying trace fields
 * Outputs: RelationshipBuilder
 */
package com.omniwatch.entity.models;

/**
 * Lightweight span record extracted from normalized trace telemetry.
 * entityId is the raw entity identifier observed on the span.
 */
public class TraceSpanEvent {

    private String entityId;
    private String traceId;
    private String spanId;
    private String parentSpanId;
    private long durationMs;
    private String status;

    public TraceSpanEvent() {
    }

    public TraceSpanEvent(String entityId, String traceId, String spanId,
                          String parentSpanId, long durationMs, String status) {
        this.entityId = entityId;
        this.traceId = traceId;
        this.spanId = spanId;
        this.parentSpanId = parentSpanId;
        this.durationMs = durationMs;
        this.status = status;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
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
