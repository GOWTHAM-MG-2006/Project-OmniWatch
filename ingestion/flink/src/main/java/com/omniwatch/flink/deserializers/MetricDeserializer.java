package com.omniwatch.flink.deserializers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.omniwatch.flink.models.MetricEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Deserializes OTLP JSON metrics from Kafka into MetricEvent POJOs.
 * Parses the OpenTelemetry Protocol JSON format with resource.scopeMetrics[].metrics[].
 * Extracts service.name from resource attributes.
 */
public class MetricDeserializer implements KafkaRecordDeserializationSchema<MetricEvent> {

    private static final long serialVersionUID = 1L;
    private transient ObjectMapper objectMapper;

    private ObjectMapper getMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<MetricEvent> out) throws IOException {
        try {
            ObjectMapper mapper = getMapper();
            JsonNode root = mapper.readTree(record.value());

            String serviceName = "unknown";
            Map<String, String> attributes = new HashMap<>();
            String sourceTopic = record.topic();

            JsonNode resourceMetrics = root.get("resourceMetrics");
            if (resourceMetrics != null && resourceMetrics.isArray() && resourceMetrics.size() > 0) {
                JsonNode resourceMetricsItem = resourceMetrics.get(0);
                JsonNode resource = resourceMetricsItem.get("resource");
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

                JsonNode scopeMetrics = resourceMetricsItem.get("scopeMetrics");
                if (scopeMetrics != null && scopeMetrics.isArray() && scopeMetrics.size() > 0) {
                    JsonNode scopeMetricsItem = scopeMetrics.get(0);
                    JsonNode metrics = scopeMetricsItem.get("metrics");
                    if (metrics != null && metrics.isArray() && metrics.size() > 0) {
                        JsonNode metric = metrics.get(0);
                        String metricName = metric.get("name") != null ? metric.get("name").asText() : "unknown";
                        String unit = metric.get("unit") != null ? metric.get("unit").asText() : "1";

                        double value = extractMetricValue(metric);

                        long timestamp = System.currentTimeMillis();
                        JsonNode gauge = metric.get("gauge");
                        if (gauge != null && gauge.has("dataPoints") && gauge.get("dataPoints").isArray()
                                && gauge.get("dataPoints").size() > 0) {
                            JsonNode dp = gauge.get("dataPoints").get(0);
                            if (dp.has("timeUnixNano")) {
                                timestamp = Long.parseLong(dp.get("timeUnixNano").asText()) / 1_000_000;
                            }
                        }
                        JsonNode sum = metric.get("sum");
                        if (sum != null && sum.has("dataPoints") && sum.get("dataPoints").isArray()
                                && sum.get("dataPoints").size() > 0) {
                            JsonNode dp = sum.get("dataPoints").get(0);
                            if (dp.has("timeUnixNano")) {
                                timestamp = Long.parseLong(dp.get("timeUnixNano").asText()) / 1_000_000;
                            }
                        }

                        MetricEvent event = new MetricEvent();
                        event.setEntityId(serviceName);
                        event.setEntityType("API_NODE");
                        event.setTimestamp(timestamp);
                        event.setSourceType("performance");
                        event.setSourceTopic(sourceTopic);
                        event.setAttributes(attributes);
                        event.setMetricName(metricName);
                        event.setValue(value);
                        event.setNormalizedValue(value);
                        event.setUnit(unit);
                        out.collect(event);
                        return;
                    }
                }
            }

            // Fallback
            MetricEvent fallback = new MetricEvent();
            fallback.setEntityId(serviceName);
            fallback.setEntityType("API_NODE");
            fallback.setTimestamp(System.currentTimeMillis());
            fallback.setSourceType("performance");
            fallback.setSourceTopic(sourceTopic);
            fallback.setAttributes(attributes);
            fallback.setMetricName("unknown");
            fallback.setValue(0.0);
            fallback.setNormalizedValue(0.0);
            fallback.setUnit("1");
            out.collect(fallback);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize metric event", e);
        }
    }

    private double extractMetricValue(JsonNode metric) {
        JsonNode gauge = metric.get("gauge");
        if (gauge != null) {
            JsonNode dataPoints = gauge.get("dataPoints");
            if (dataPoints != null && dataPoints.isArray() && dataPoints.size() > 0) {
                return extractDataPointValue(dataPoints.get(0));
            }
        }
        JsonNode sum = metric.get("sum");
        if (sum != null) {
            JsonNode dataPoints = sum.get("dataPoints");
            if (dataPoints != null && dataPoints.isArray() && dataPoints.size() > 0) {
                return extractDataPointValue(dataPoints.get(0));
            }
        }
        return 0.0;
    }

    private double extractDataPointValue(JsonNode dataPoint) {
        if (dataPoint.has("asDouble")) {
            return dataPoint.get("asDouble").asDouble();
        }
        if (dataPoint.has("asInt")) {
            return dataPoint.get("asInt").asDouble();
        }
        if (dataPoint.has("asLong")) {
            return dataPoint.get("asLong").asDouble();
        }
        return 0.0;
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
    public TypeInformation<MetricEvent> getProducedType() {
        return TypeInformation.of(MetricEvent.class);
    }
}
