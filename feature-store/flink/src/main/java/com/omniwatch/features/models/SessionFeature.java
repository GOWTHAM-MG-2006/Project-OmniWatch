/*
 * OmniWatch — Windowing Layer + Feature Store
 * Component: SessionFeature model
 * Phase: 4
 * Purpose: POJO representing a session window detection result produced by
 *          SessionWindowDetector. Tracks per-entity error bursts within
 *          a session (gap-based) window.
 * Inputs: SessionWindowDetector operator
 * Outputs: Kafka topic omniwatch.features.windowed_15m (JSON, via serialization)
 */
package com.omniwatch.features.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Session window feature. Produced when the SessionWindowDetector identifies
 * a session boundary (gap > 30s between events for the same entity).
 * {@code burstFlag} is true when error count exceeds the threshold (3).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SessionFeature {

    private String entityId;
    private long sessionStart;
    private long sessionEnd;
    private int errorCount;
    private boolean burstFlag;

    /** Required no-arg constructor for Flink POJO serialization + Jackson. */
    public SessionFeature() {
    }

    public SessionFeature(String entityId, long sessionStart, long sessionEnd,
                          int errorCount, boolean burstFlag) {
        this.entityId = entityId;
        this.sessionStart = sessionStart;
        this.sessionEnd = sessionEnd;
        this.errorCount = errorCount;
        this.burstFlag = burstFlag;
    }

    // ---- Getters / Setters ----

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public long getSessionStart() {
        return sessionStart;
    }

    public void setSessionStart(long sessionStart) {
        this.sessionStart = sessionStart;
    }

    public long getSessionEnd() {
        return sessionEnd;
    }

    public void setSessionEnd(long sessionEnd) {
        this.sessionEnd = sessionEnd;
    }

    public int getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(int errorCount) {
        this.errorCount = errorCount;
    }

    public boolean isBurstFlag() {
        return burstFlag;
    }

    public void setBurstFlag(boolean burstFlag) {
        this.burstFlag = burstFlag;
    }
}
