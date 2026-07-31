package com.omniwatch.flink.producers;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

/**
 * Factory for creating a KafkaSink that writes normalized events
 * (general-purpose event stream) to the "omniwatch.events.normalized" topic.
 */
public class NormalizedEventsProducer {

    private static final String TOPIC = "omniwatch.events.normalized";

    private NormalizedEventsProducer() {
        // Factory class - prevent instantiation
    }

    /**
     * Creates a KafkaSink configured for the normalized events topic.
     *
     * @param brokers Kafka bootstrap servers (comma-separated)
     * @return configured KafkaSink<String>
     */
    public static KafkaSink<String> createSink(String brokers) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
