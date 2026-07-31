package com.omniwatch.flink.deserializers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.omniwatch.flink.models.LogEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Deserializes OTLP JSON logs from Kafka into LogEvent POJOs.
 * Reads resource.scopeLogs[].logRecords[] and maps severityNumber to canonical level strings.
 * Extracts service.name from resource attributes.
 */
public class LogDeserializer implements KafkaRecordDeserializationSchema<LogEvent> {

    private static final long serialVersionUID = 1L;
    private transient ObjectMapper objectMapper;

    private ObjectMapper getMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<LogEvent> out) throws IOException {
        try {
            ObjectMapper mapper = getMapper();
            JsonNode root = mapper.readTree(record.value());

            String serviceName = "unknown";
            Map<String, String> attributes = new HashMap<>();
            String sourceTopic = record.topic();

            JsonNode resourceLogs = root.get("resourceLogs");
            if (resourceLogs != null && resourceLogs.isArray() && resourceLogs.size() > 0) {
                JsonNode resourceLogsItem = resourceLogs.get(0);
                JsonNode resource = resourceLogsItem.get("resource");
                if (resource != null) {
                    JsonNode attrs = resource.get("attributes");
                    if (attrs != null && attrs.isArray()) {
                        for (JsonNode attr : attrs) {
                            String key = attr.get("key") != null ? attr.get("key").asText() : null;
                            if (key != null) {
                                String val = extractAttributeValue(attr.get("value"));
                                if (val != null) {
                                    attributes.put(key, val);
                                    if ("service.name".equals(key)) {
                                        serviceName = val;
                                    }
                                }
                            }
                        }
                    }
                }

                JsonNode scopeLogs = resourceLogsItem.get("scopeLogs");
                if (scopeLogs != null && scopeLogs.isArray() && scopeLogs.size() > 0) {
                    JsonNode scopeLogsItem = scopeLogs.get(0);
                    JsonNode logRecords = scopeLogsItem.get("logRecords");
                    if (logRecords != null && logRecords.isArray() && logRecords.size() > 0) {
                        JsonNode logRecord = logRecords.get(0);

                        String body = extractBody(logRecord.get("body"));

                        String severity = "INFO";
                        if (logRecord.has("severityNumber")) {
                            int severityNumber = logRecord.get("severityNumber").asInt();
                            severity = mapSeverityNumber(severityNumber);
                        } else if (logRecord.has("severityText")) {
                            severity = logRecord.get("severityText").asText();
                        }

                        long timestamp = System.currentTimeMillis();
                        if (logRecord.has("timeUnixNano")) {
                            timestamp = Long.parseLong(logRecord.get("timeUnixNano").asText()) / 1_000_000;
                        } else if (logRecord.has("observedTimeUnixNano")) {
                            timestamp = Long.parseLong(logRecord.get("observedTimeUnixNano").asText()) / 1_000_000;
                        }

                        if (logRecord.has("traceId")) {
                            attributes.put("traceId", logRecord.get("traceId").asText());
                        }
                        if (logRecord.has("spanId")) {
                            attributes.put("spanId", logRecord.get("spanId").asText());
                        }

                        LogEvent event = new LogEvent();
                        event.setEntityId(serviceName);
                        event.setEntityType("API_NODE");
                        event.setTimestamp(timestamp);
                        event.setSourceType("performance");
                        event.setSourceTopic(sourceTopic);
                        event.setAttributes(attributes);
                        event.setSeverity(severity);
                        event.setBody(body);
                        event.setServiceName(serviceName);
                        out.collect(event);
                        return;
                    }
                }
            }

            // Fallback
            LogEvent fallback = new LogEvent();
            fallback.setEntityId(serviceName);
            fallback.setEntityType("API_NODE");
            fallback.setTimestamp(System.currentTimeMillis());
            fallback.setSourceType("performance");
            fallback.setSourceTopic(sourceTopic);
            fallback.setAttributes(attributes);
            fallback.setSeverity("INFO");
            fallback.setBody("");
            fallback.setServiceName(serviceName);
            out.collect(fallback);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize log event", e);
        }
    }

    private String mapSeverityNumber(int severityNumber) {
        if (severityNumber >= 1 && severityNumber <= 4) return "TRACE";
        if (severityNumber >= 5 && severityNumber <= 8) return "DEBUG";
        if (severityNumber >= 9 && severityNumber <= 12) return "INFO";
        if (severityNumber >= 13 && severityNumber <= 16) return "WARN";
        if (severityNumber >= 17) return "ERROR";
        return "INFO";
    }

    private String extractBody(JsonNode bodyNode) {
        if (bodyNode == null) return "";
        if (bodyNode.has("stringValue")) {
            return bodyNode.get("stringValue").asText();
        }
        return bodyNode.asText();
    }

    private String extractAttributeValue(JsonNode valueNode) {
        if (valueNode == null) return null;
        if (valueNode.has("stringValue")) {
            return valueNode.get("stringValue").asText();
        }
        if (valueNode.has("intValue")) {
            return valueNode.get("intValue").asText();
        }
        if (valueNode.has("doubleValue")) {
            return valueNode.get("doubleValue").asText();
        }
        if (valueNode.has("boolValue")) {
            return String.valueOf(valueNode.get("boolValue").asBoolean());
        }
        return valueNode.asText();
    }

    @Override
    public TypeInformation<LogEvent> getProducedType() {
        return TypeInformation.of(LogEvent.class);
    }
}
