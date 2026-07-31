package com.omniwatch.flink.models;

import java.util.HashMap;
import java.util.Map;

/**
 * Abstract base class for all telemetry events normalized by the Flink ingestion pipeline.
 * Every event flowing through the system carries these common fields.
 */
public abstract class NormalizedEvent {

    private String entityId;
    private String entityType;
    private long timestamp;
    private String sourceType;
    private String sourceTopic;
    private Map<String, String> attributes;

    protected NormalizedEvent() {
        this.attributes = new HashMap<>();
    }

    protected NormalizedEvent(String entityId, String entityType, long timestamp,
                              String sourceType, String sourceTopic,
                              Map<String, String> attributes) {
        this.entityId = entityId;
        this.entityType = entityType;
        this.timestamp = timestamp;
        this.sourceType = sourceType;
        this.sourceTopic = sourceTopic;
        this.attributes = attributes != null ? attributes : new HashMap<>();
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
        this.attributes = attributes;
    }
}
