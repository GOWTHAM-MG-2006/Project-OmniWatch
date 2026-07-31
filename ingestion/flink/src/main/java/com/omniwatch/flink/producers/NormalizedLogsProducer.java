package com.omniwatch.flink.producers;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

/**
 * Factory for creating a KafkaSink that writes normalized log events
 * to the "omniwatch.logs.normalized" topic.
 */
public class NormalizedLogsProducer {

    private static final String TOPIC = "omniwatch.logs.normalized";

    private NormalizedLogsProducer() {
        // Factory class - prevent instantiation
    }

    /**
     * Creates a KafkaSink configured for the normalized logs topic.
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
