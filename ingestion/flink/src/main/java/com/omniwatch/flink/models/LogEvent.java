package com.omniwatch.flink.models;

import java.util.Map;

/**
 * Normalized log telemetry event.
 * Represents a single log line from any monitored service.
 */
public class LogEvent extends NormalizedEvent {

    private String severity;
    private String body;
    private String serviceName;

    public LogEvent() {
        super();
    }

    public LogEvent(String entityId, String entityType, long timestamp,
                    String sourceType, String sourceTopic,
                    Map<String, String> attributes,
                    String severity, String body, String serviceName) {
        super(entityId, entityType, timestamp, sourceType, sourceTopic, attributes);
        this.severity = severity;
        this.body = body;
        this.serviceName = serviceName;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }
}
