package com.omniwatch.flink.normalizers;

import com.omniwatch.flink.models.LogEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * Normalizes log events to a canonical format.
 * Standardizes severity levels, strips excess whitespace from body,
 * and resolves entity type from service name hints.
 */
public class LogNormalizer {

    private static final Map<String, String> SEVERITY_MAP = new HashMap<>();

    static {
        SEVERITY_MAP.put("TRACE", "TRACE");
        SEVERITY_MAP.put("TRACE2", "TRACE");
        SEVERITY_MAP.put("TRACE3", "TRACE");
        SEVERITY_MAP.put("TRACE4", "TRACE");
        SEVERITY_MAP.put("DEBUG", "DEBUG");
        SEVERITY_MAP.put("DEBUG2", "DEBUG");
        SEVERITY_MAP.put("DEBUG3", "DEBUG");
        SEVERITY_MAP.put("DEBUG4", "DEBUG");
        SEVERITY_MAP.put("INFO", "INFO");
        SEVERITY_MAP.put("INFO2", "INFO");
        SEVERITY_MAP.put("INFO3", "INFO");
        SEVERITY_MAP.put("INFO4", "INFO");
        SEVERITY_MAP.put("WARN", "WARN");
        SEVERITY_MAP.put("WARN2", "WARN");
        SEVERITY_MAP.put("WARN3", "WARN");
        SEVERITY_MAP.put("WARN4", "WARN");
        SEVERITY_MAP.put("ERROR", "ERROR");
        SEVERITY_MAP.put("ERROR2", "ERROR");
        SEVERITY_MAP.put("ERROR3", "ERROR");
        SEVERITY_MAP.put("ERROR4", "ERROR");
        SEVERITY_MAP.put("FATAL", "FATAL");
        SEVERITY_MAP.put("FATAL2", "FATAL");
        SEVERITY_MAP.put("FATAL3", "FATAL");
        SEVERITY_MAP.put("FATAL4", "FATAL");
        SEVERITY_MAP.put("CRITICAL", "FATAL");
        SEVERITY_MAP.put("ALERT", "FATAL");
        SEVERITY_MAP.put("EMERGENCY", "FATAL");
        SEVERITY_MAP.put("NOTICE", "INFO");
    }

    private static final Map<String, String> SERVICE_TYPE_MAP = new HashMap<>();

    static {
        SERVICE_TYPE_MAP.put("api", "API_NODE");
        SERVICE_TYPE_MAP.put("gateway", "API_NODE");
        SERVICE_TYPE_MAP.put("frontend", "API_NODE");
        SERVICE_TYPE_MAP.put("proxy", "API_NODE");
        SERVICE_TYPE_MAP.put("database", "DATABASE_NODE");
        SERVICE_TYPE_MAP.put("db", "DATABASE_NODE");
        SERVICE_TYPE_MAP.put("postgres", "DATABASE_NODE");
        SERVICE_TYPE_MAP.put("mysql", "DATABASE_NODE");
        SERVICE_TYPE_MAP.put("redis", "DATABASE_NODE");
        SERVICE_TYPE_MAP.put("mongodb", "DATABASE_NODE");
        SERVICE_TYPE_MAP.put("worker", "WORKER_NODE");
        SERVICE_TYPE_MAP.put("background", "WORKER_NODE");
        SERVICE_TYPE_MAP.put("queue", "WORKER_NODE");
        SERVICE_TYPE_MAP.put("cache", "INFRASTRUCTURE");
        SERVICE_TYPE_MAP.put("loadbalancer", "INFRASTRUCTURE");
        SERVICE_TYPE_MAP.put("dns", "INFRASTRUCTURE");
    }

    private LogNormalizer() {
        // Utility class - prevent instantiation
    }

    /**
     * Normalizes a LogEvent to canonical form.
     * Standardizes severity to one of: DEBUG, INFO, WARN, ERROR, FATAL.
     * Strips excess whitespace from body text.
     * Infers entityType from service name if not already set.
     *
     * @param raw the raw log event
     * @return the normalized log event
     */
    public static LogEvent normalize(LogEvent raw) {
        if (raw == null) {
            return null;
        }

        // Standardize severity
        String severity = raw.getSeverity();
        if (severity != null) {
            String canonical = SEVERITY_MAP.get(severity.toUpperCase());
            if (canonical != null) {
                raw.setSeverity(canonical);
            } else {
                // Fall back: map unknown severity to INFO
                raw.setSeverity("INFO");
            }
        } else {
            raw.setSeverity("INFO");
        }

        // Strip excess whitespace from body
        if (raw.getBody() != null) {
            raw.setBody(raw.getBody().replaceAll("\\s+", " ").trim());
        }

        // Infer entity type from service name if not default
        String entityType = raw.getEntityType();
        if (entityType == null || "API_NODE".equals(entityType)) {
            String serviceName = raw.getServiceName();
            if (serviceName != null) {
                String inferredType = inferEntityType(serviceName);
                if (inferredType != null) {
                    raw.setEntityType(inferredType);
                }
            }
        }

        return raw;
    }

    /**
     * Infers entity type from a service name using prefix/keyword matching.
     */
    private static String inferEntityType(String serviceName) {
        if (serviceName == null) return null;
        String lower = serviceName.toLowerCase();
        for (Map.Entry<String, String> entry : SERVICE_TYPE_MAP.entrySet()) {
            if (lower.contains(entry.getKey())) {
                return entry.getValue();
            }
        }
        return "API_NODE";
    }
}
