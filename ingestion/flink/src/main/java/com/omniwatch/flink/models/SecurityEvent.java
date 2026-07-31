package com.omniwatch.flink.models;

import java.util.Map;

/**
 * Normalized security telemetry event.
 * Captures security-related observations such as brute force attempts,
 * privilege escalation, config drift, or data exfiltration.
 */
public class SecurityEvent extends NormalizedEvent {

    private String eventId;
    private String attackType;
    private double confidence;
    private String sourceIp;
    private String description;

    public SecurityEvent() {
        super();
    }

    public SecurityEvent(String entityId, String entityType, long timestamp,
                         String sourceType, String sourceTopic,
                         Map<String, String> attributes,
                         String eventId, String attackType, double confidence,
                         String sourceIp, String description) {
        super(entityId, entityType, timestamp, sourceType, sourceTopic, attributes);
        this.eventId = eventId;
        this.attackType = attackType;
        this.confidence = confidence;
        this.sourceIp = sourceIp;
        this.description = description;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getAttackType() {
        return attackType;
    }

    public void setAttackType(String attackType) {
        this.attackType = attackType;
    }

    public double getConfidence() {
        return confidence;
    }

    public void setConfidence(double confidence) {
        this.confidence = confidence;
    }

    public String getSourceIp() {
        return sourceIp;
    }

    public void setSourceIp(String sourceIp) {
        this.sourceIp = sourceIp;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
