package com.omniwatch.flink.producers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.omniwatch.flink.models.SecurityEvent;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes security events to two Kafka topics:
 * - "omniwatch.security.normalized" (new format)
 * - "omniwatch.security.events" (legacy format for backward compatibility)
 *
 * Uses a ProcessFunction to collect events and send to both topics
 * via separate KafkaSinks. The SecurityEventRouter is configured as
 * a two-branch sink pattern in the pipeline rather than a single operator.
 *
 * Factory methods provide KafkaSink instances for each target topic.
 */
public class SecurityEventRouter {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityEventRouter.class);

    private static final String TOPIC_NORMALIZED = "omniwatch.security.normalized";
    private static final String TOPIC_LEGACY = "omniwatch.security.events";

    private SecurityEventRouter() {
        // Factory class - prevent instantiation
    }

    /**
     * Creates a KafkaSink for the normalized security events topic.
     *
     * @param brokers Kafka bootstrap servers
     * @return configured KafkaSink<String>
     */
    public static KafkaSink<String> createNormalizedSink(String brokers) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(TOPIC_NORMALIZED)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    /**
     * Creates a KafkaSink for the legacy security events topic.
     *
     * @param brokers Kafka bootstrap servers
     * @return configured KafkaSink<String>
     */
    public static KafkaSink<String> createLegacySink(String brokers) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(TOPIC_LEGACY)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    /**
     * ProcessFunction that serializes a SecurityEvent to JSON and emits it
     * as a string. Used in the pipeline to bridge typed events to the
     * string-based Kafka sink.
     *
     * Usage:
     * <pre>
     * securityStream
     *     .process(new SecurityEventRouter.SerializeFunction())
     *     .sinkTo(SecurityEventRouter.createNormalizedSink(brokers));
     * </pre>
     */
    public static class SerializeFunction extends ProcessFunction<SecurityEvent, String> {

        private static final long serialVersionUID = 1L;
        private transient ObjectMapper objectMapper;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            objectMapper = new ObjectMapper();
        }

        @Override
        public void processElement(SecurityEvent event,
                                    ProcessFunction<SecurityEvent, String>.Context ctx,
                                    Collector<String> out) {
            try {
                String json = objectMapper.writeValueAsString(event);
                out.collect(json);
            } catch (Exception e) {
                LOG.error("Failed to serialize SecurityEvent: {}", event.getEventId(), e);
            }
        }
    }
}
