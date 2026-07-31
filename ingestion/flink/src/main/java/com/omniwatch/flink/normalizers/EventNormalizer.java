package com.omniwatch.flink.normalizers;

import com.omniwatch.flink.models.*;

import java.util.Map;

/**
 * Unified normalizer for all event types.
 * Provides static normalize methods for each event type that
 * delegate to the type-specific normalizers.
 * Additionally validates required fields and normalizes common
 * fields like timestamps.
 */
public class EventNormalizer {

    private EventNormalizer() {
        // Utility class - prevent instantiation
    }

    /**
     * Normalizes any NormalizedEvent: validates required fields and
     * ensures timestamp is in epoch millis.
     *
     * @param event the event to normalize
     * @param <T>   the event type
     * @return the normalized event, or null if validation fails
     */
    @SuppressWarnings("unchecked")
    public static <T extends NormalizedEvent> T normalize(T event) {
        if (event == null) {
            return null;
        }

        // Validate required fields
        if (!validateRequired(event)) {
            return null;
        }

        // Normalize timestamp (ensure positive epoch millis)
        if (event.getTimestamp() <= 0) {
            event.setTimestamp(System.currentTimeMillis());
        }

        // Delegate to type-specific normalizer
        if (event instanceof MetricEvent) {
            return (T) MetricNormalizer.normalize((MetricEvent) event);
        } else if (event instanceof LogEvent) {
            return (T) LogNormalizer.normalize((LogEvent) event);
        } else if (event instanceof TraceEvent) {
            return (T) TraceNormalizer.normalize((TraceEvent) event);
        } else if (event instanceof SecurityEvent) {
            return (T) normalize((SecurityEvent) event);
        }

        return event;
    }

    /**
     * Normalizes a MetricEvent (delegates to MetricNormalizer).
     */
    public static MetricEvent normalize(MetricEvent event) {
        if (event == null) return null;
        if (!validateRequired(event)) return null;
        if (event.getTimestamp() <= 0) {
            event.setTimestamp(System.currentTimeMillis());
        }
        return MetricNormalizer.normalize(event);
    }

    /**
     * Normalizes a LogEvent (delegates to LogNormalizer).
     */
    public static LogEvent normalize(LogEvent event) {
        if (event == null) return null;
        if (!validateRequired(event)) return null;
        if (event.getTimestamp() <= 0) {
            event.setTimestamp(System.currentTimeMillis());
        }
        return LogNormalizer.normalize(event);
    }

    /**
     * Normalizes a TraceEvent (delegates to TraceNormalizer).
     */
    public static TraceEvent normalize(TraceEvent event) {
        if (event == null) return null;
        if (!validateRequired(event)) return null;
        if (event.getTimestamp() <= 0) {
            event.setTimestamp(System.currentTimeMillis());
        }
        return TraceNormalizer.normalize(event);
    }

    /**
     * Normalizes a SecurityEvent.
     * Ensures eventId is present, attackType is canonical,
     * and confidence is in [0, 1] range.
     */
    public static SecurityEvent normalize(SecurityEvent event) {
        if (event == null) return null;
        if (!validateRequired(event)) return null;
        if (event.getTimestamp() <= 0) {
            event.setTimestamp(System.currentTimeMillis());
        }

        // Ensure eventId
        if (event.getEventId() == null || event.getEventId().isEmpty()) {
            event.setEventId(java.util.UUID.randomUUID().toString());
        }

        // Ensure attackType
        if (event.getAttackType() == null || event.getAttackType().isEmpty()) {
            event.setAttackType("UNKNOWN_ATTACK");
        } else {
            event.setAttackType(event.getAttackType().toUpperCase());
        }

        // Clamp confidence to [0, 1]
        double conf = event.getConfidence();
        if (conf < 0.0) conf = 0.0;
        if (conf > 1.0) conf = 1.0;
        event.setConfidence(conf);

        // Ensure description
        if (event.getDescription() == null) {
            event.setDescription("");
        }

        return event;
    }

    /**
     * Validates that required fields are present on the event.
     * At minimum, entityId must be non-null and non-empty.
     *
     * @param event the event to validate
     * @return true if valid, false otherwise
     */
    private static boolean validateRequired(NormalizedEvent event) {
        if (event.getEntityId() == null || event.getEntityId().isEmpty()) {
            return false;
        }
        if (event.getEntityType() == null || event.getEntityType().isEmpty()) {
            event.setEntityType("API_NODE");
        }
        if (event.getSourceType() == null || event.getSourceType().isEmpty()) {
            event.setSourceType("performance");
        }
        return true;
    }

    /**
     * Extracts a string attribute from the event's attributes map.
     *
     * @param event the event
     * @param key   the attribute key
     * @return the attribute value, or null if not found
     */
    public static String getAttribute(NormalizedEvent event, String key) {
        Map<String, String> attrs = event.getAttributes();
        if (attrs != null && key != null) {
            return attrs.get(key);
        }
        return null;
    }
}
