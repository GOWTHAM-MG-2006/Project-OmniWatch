package com.omniwatch.flink.deserializers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.omniwatch.flink.models.SecurityEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Deserializes security event JSON from Kafka into SecurityEvent POJOs.
 * Supports both the legacy omniwatch.security.events format and the
 * new standardized security event format.
 */
public class SecurityEventDeserializer implements KafkaRecordDeserializationSchema<SecurityEvent> {

    private static final long serialVersionUID = 1L;
    private transient ObjectMapper objectMapper;

    private ObjectMapper getMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<SecurityEvent> out) throws IOException {
        try {
            ObjectMapper mapper = getMapper();
            JsonNode root = mapper.readTree(record.value());

            Map<String, String> attributes = new HashMap<>();
            String sourceTopic = record.topic();

            String entityId = getField(root, "entity_id", "entityId", "unknown");
            String attackType = getField(root, "attack_type", "attackType", null);
            if (attackType == null) {
                attackType = getField(root, "event_type", "eventType", "UNKNOWN_ATTACK");
            }

            double confidence = 0.0;
            String confStr = getField(root, "confidence", null, null);
            if (confStr != null) {
                try {
                    confidence = Double.parseDouble(confStr);
                } catch (NumberFormatException e) {
                    confidence = 0.0;
                }
            } else if (root.has("confidence")) {
                confidence = root.get("confidence").asDouble(0.0);
            }

            String sourceIp = getField(root, "source_ip", "sourceIp", null);
            String description = getField(root, "description", null, "");

            if (root.has("severity")) {
                attributes.put("severity", root.get("severity").asText());
            }

            long timestamp = System.currentTimeMillis();
            String tsStr = getField(root, "timestamp", null, null);
            if (tsStr != null) {
                try {
                    timestamp = Long.parseLong(tsStr);
                } catch (NumberFormatException e) {
                    timestamp = System.currentTimeMillis();
                }
            }

            root.fieldNames().forEachRemaining(fieldName -> {
                if (!fieldName.equals("entity_id") && !fieldName.equals("entityId")
                        && !fieldName.equals("attack_type") && !fieldName.equals("attackType")
                        && !fieldName.equals("event_type") && !fieldName.equals("eventType")
                        && !fieldName.equals("confidence") && !fieldName.equals("source_ip")
                        && !fieldName.equals("sourceIp") && !fieldName.equals("description")
                        && !fieldName.equals("severity") && !fieldName.equals("timestamp")) {
                    JsonNode val = root.get(fieldName);
                    if (val != null) {
                        attributes.put(fieldName, val.asText());
                    }
                }
            });

            String eventId = getField(root, "event_id", "eventId", UUID.randomUUID().toString());

            SecurityEvent event = new SecurityEvent();
            event.setEntityId(entityId);
            event.setEntityType("SECURITY_NODE");
            event.setTimestamp(timestamp);
            event.setSourceType("security");
            event.setSourceTopic(sourceTopic);
            event.setAttributes(attributes);
            event.setEventId(eventId);
            event.setAttackType(normalizeAttackType(attackType));
            event.setConfidence(confidence);
            event.setSourceIp(sourceIp);
            event.setDescription(description);
            out.collect(event);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize security event", e);
        }
    }

    private String getField(JsonNode node, String snakeCase, String camelCase, String defaultValue) {
        if (snakeCase != null && node.has(snakeCase)) {
            return node.get(snakeCase).asText();
        }
        if (camelCase != null && node.has(camelCase)) {
            return node.get(camelCase).asText();
        }
        return defaultValue;
    }

    private String normalizeAttackType(String raw) {
        if (raw == null) return "UNKNOWN_ATTACK";
        String upper = raw.toUpperCase();
        switch (upper) {
            case "BRUTE_FORCE":
            case "BRUTE_FORCE_ATTEMPT":
            case "BRUTE_FORCE_ATTACK":
                return "BRUTE_FORCE";
            case "PRIVILEGE_ESCALATION":
            case "PRIVILEGE_ESCALATION_ATTEMPT":
                return "PRIVILEGE_ESCALATION";
            case "CONFIG_DRIFT":
            case "UNAUTHORIZED_CONFIG_CHANGE":
                return "CONFIG_DRIFT";
            case "DATA_EXFILTRATION":
            case "POTENTIAL_DATA_EXFILTRATION":
                return "DATA_EXFILTRATION";
            default:
                return upper;
        }
    }

    @Override
    public TypeInformation<SecurityEvent> getProducedType() {
        return TypeInformation.of(SecurityEvent.class);
    }
}
