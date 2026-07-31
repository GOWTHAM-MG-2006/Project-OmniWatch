package com.omniwatch.flink.deserializers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.omniwatch.flink.models.TraceEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Deserializes OTLP JSON traces from Kafka into TraceEvent POJOs.
 * Reads resource.scopeSpans[].spans[] and extracts span metadata.
 * Converts startTimeUnixNano/endTimeUnixNano to duration in milliseconds.
 */
public class TraceDeserializer implements KafkaRecordDeserializationSchema<TraceEvent> {

    private static final long serialVersionUID = 1L;
    private transient ObjectMapper objectMapper;

    private ObjectMapper getMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<TraceEvent> out) throws IOException {
        try {
            ObjectMapper mapper = getMapper();
            JsonNode root = mapper.readTree(record.value());

            String serviceName = "unknown";
            Map<String, String> attributes = new HashMap<>();
            String sourceTopic = record.topic();

            JsonNode resourceSpans = root.get("resourceSpans");
            if (resourceSpans != null && resourceSpans.isArray() && resourceSpans.size() > 0) {
                JsonNode resourceSpansItem = resourceSpans.get(0);
                JsonNode resource = resourceSpansItem.get("resource");
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

                JsonNode scopeSpans = resourceSpansItem.get("scopeSpans");
                if (scopeSpans != null && scopeSpans.isArray() && scopeSpans.size() > 0) {
                    JsonNode scopeSpansItem = scopeSpans.get(0);
                    JsonNode spans = scopeSpansItem.get("spans");
                    if (spans != null && spans.isArray() && spans.size() > 0) {
                        JsonNode span = spans.get(0);

                        String traceId = span.get("traceId") != null ? span.get("traceId").asText() : "";
                        String spanId = span.get("spanId") != null ? span.get("spanId").asText() : "";
                        String parentSpanId = span.get("parentSpanId") != null ? span.get("parentSpanId").asText() : "";
                        String spanName = span.get("name") != null ? span.get("name").asText() : "unknown";
                        long startTimeUnixNano = span.has("startTimeUnixNano")
                                ? Long.parseLong(span.get("startTimeUnixNano").asText()) : 0L;
                        long endTimeUnixNano = span.has("endTimeUnixNano")
                                ? Long.parseLong(span.get("endTimeUnixNano").asText()) : startTimeUnixNano;
                        long durationMs = (endTimeUnixNano - startTimeUnixNano) / 1_000_000L;
                        long timestamp = startTimeUnixNano / 1_000_000L;

                        String status = "UNSET";
                        JsonNode statusNode = span.get("status");
                        if (statusNode != null && statusNode.has("code")) {
                            int statusCode = statusNode.get("code").asInt();
                            switch (statusCode) {
                                case 0: status = "UNSET"; break;
                                case 1: status = "OK"; break;
                                case 2: status = "ERROR"; break;
                                default: status = "UNSET";
                            }
                        }

                        JsonNode spanAttrs = span.get("attributes");
                        if (spanAttrs != null && spanAttrs.isArray()) {
                            for (JsonNode attr : spanAttrs) {
                                String key = attr.get("key") != null ? attr.get("key").asText() : null;
                                if (key != null) {
                                    String val = extractAttributeValue(attr.get("value"));
                                    if (val != null) {
                                        attributes.put("span." + key, val);
                                    }
                                }
                            }
                        }

                        TraceEvent event = new TraceEvent();
                        event.setEntityId(serviceName);
                        event.setEntityType("API_NODE");
                        event.setTimestamp(timestamp);
                        event.setSourceType("performance");
                        event.setSourceTopic(sourceTopic);
                        event.setAttributes(attributes);
                        event.setTraceId(traceId);
                        event.setSpanId(spanId);
                        event.setParentSpanId(parentSpanId);
                        event.setSpanName(spanName);
                        event.setStartTime(timestamp);
                        event.setDurationMs(durationMs);
                        event.setStatus(status);
                        out.collect(event);
                        return;
                    }
                }
            }

            // Fallback
            TraceEvent fallback = new TraceEvent();
            fallback.setEntityId(serviceName);
            fallback.setEntityType("API_NODE");
            fallback.setTimestamp(System.currentTimeMillis());
            fallback.setSourceType("performance");
            fallback.setSourceTopic(sourceTopic);
            fallback.setAttributes(attributes);
            fallback.setTraceId("");
            fallback.setSpanId("");
            fallback.setParentSpanId("");
            fallback.setSpanName("unknown");
            fallback.setStartTime(System.currentTimeMillis());
            fallback.setDurationMs(0);
            fallback.setStatus("UNSET");
            out.collect(fallback);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize trace event", e);
        }
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
    public TypeInformation<TraceEvent> getProducedType() {
        return TypeInformation.of(TraceEvent.class);
    }
}
