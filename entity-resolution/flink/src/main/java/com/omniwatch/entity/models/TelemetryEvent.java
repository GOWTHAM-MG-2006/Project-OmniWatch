/*
 * OmniWatch — Entity Resolution Layer
 * Component: TelemetryEvent model
 * Phase: 3
 * Purpose: Generic view over a normalized telemetry event (metrics/logs/
 *          traces/events/security) read from Kafka as JSON. Mirrors the
 *          Phase 2 normalized models so ObjectMapper can deserialize them.
 * Inputs: Kafka topics omniwatch.{metrics,logs,traces,events,security}.normalized
 * Outputs: ResourceIdParser / RelationshipBuilder
 */
package com.omniwatch.entity.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Union shape of all normalized telemetry events. Trace-only fields are null
 * for non-trace events.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TelemetryEvent {

    private String entityId;
    private String entityType;
    private long timestamp;
    private String sourceType;
    private String sourceTopic;
    private Map<String, String> attributes = new HashMap<>();

    // Trace-only fields (present when the event is a trace span)
    private String traceId;
    private String spanId;
    private String parentSpanId;
    private String spanName;
    private long startTime;
    private long durationMs;
    private String status;

    public TelemetryEvent() {
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public void setSourceTopic(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes == null ? new HashMap<>() : attributes;
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

    /** True when this event carries enough span context to build a relationship. */
    public boolean isTraceSpan() {
        return traceId != null && spanId != null && !traceId.isEmpty() && !spanId.isEmpty();
    }
}
