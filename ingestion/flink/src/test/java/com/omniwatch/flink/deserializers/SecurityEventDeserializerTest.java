package com.omniwatch.flink.deserializers;

import com.omniwatch.flink.models.SecurityEvent;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SecurityEventDeserializer.
 * Verifies security events are correctly parsed into SecurityEvent POJOs.
 */
class SecurityEventDeserializerTest {

    private SecurityEventDeserializer deserializer;
    private TestCollector collector;

    @BeforeEach
    void setUp() {
        deserializer = new SecurityEventDeserializer();
        collector = new TestCollector();
    }

    @Test
    void testDeserializeValidSecurityEvent() throws IOException {
        byte[] jsonBytes = Files.readAllBytes(Paths.get("src/test/resources/sample-security-event.json"));
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("omniwatch.security.events", 0, 0, null, jsonBytes);

        deserializer.deserialize(record, collector);

        SecurityEvent event = collector.getCaptured();
        assertNotNull(event);
        assertEquals("postgresql-database", event.getEntityId());
        assertEquals("SECURITY_NODE", event.getEntityType());
        assertEquals("BRUTE_FORCE", event.getAttackType());
        assertEquals(0.85, event.getConfidence(), 0.0001);
        assertEquals("192.168.1.100", event.getSourceIp());
        assertEquals("Multiple failed login attempts detected", event.getDescription());
        assertEquals("security", event.getSourceType());
        assertEquals("omniwatch.security.events", event.getSourceTopic());
        assertNotNull(event.getEventId());
    }

    @Test
    void testDeserializeSecurityEventSnakeCaseFields() throws Exception {
        String json = "{"
                + "\"entity_id\": \"web-server\", "
                + "\"attack_type\": \"PRIVILEGE_ESCALATION_ATTEMPT\", "
                + "\"confidence\": 0.92, "
                + "\"timestamp\": \"1715000000000\""
                + "}";
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.topic", 0, 0, null, json.getBytes());

        deserializer.deserialize(record, collector);

        SecurityEvent event = collector.getCaptured();
        assertEquals("web-server", event.getEntityId());
        assertEquals("PRIVILEGE_ESCALATION", event.getAttackType());
        assertEquals(0.92, event.getConfidence(), 0.0001);
    }

    @Test
    void testDeserializeSecurityEventCamelCaseFields() throws Exception {
        String json = "{"
                + "\"entityId\": \"app-server\", "
                + "\"attackType\": \"CONFIG_DRIFT\", "
                + "\"confidence\": 0.75, "
                + "\"severity\": \"CRITICAL\""
                + "}";
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.topic", 0, 0, null, json.getBytes());

        deserializer.deserialize(record, collector);

        SecurityEvent event = collector.getCaptured();
        assertEquals("app-server", event.getEntityId());
        assertEquals("CONFIG_DRIFT", event.getAttackType());
        assertTrue(event.getAttributes().containsKey("severity"));
        assertEquals("CRITICAL", event.getAttributes().get("severity"));
    }

    @Test
    void testDeserializeSecurityEventWithEventType() throws Exception {
        String json = "{"
                + "\"entity_id\": \"db\", "
                + "\"event_type\": \"DATA_EXFILTRATION\", "
                + "\"confidence\": 0.5"
                + "}";
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.topic", 0, 0, null, json.getBytes());

        deserializer.deserialize(record, collector);

        SecurityEvent event = collector.getCaptured();
        assertEquals("DATA_EXFILTRATION", event.getAttackType());
    }

    @Test
    void testDeserializeSecurityEventWithExtraFields() throws Exception {
        String json = "{"
                + "\"entity_id\": \"cache-node\", "
                + "\"attack_type\": \"BRUTE_FORCE\", "
                + "\"confidence\": 0.3, "
                + "\"region\": \"us-east-1\", "
                + "\"user\": \"unknown\""
                + "}";
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.topic", 0, 0, null, json.getBytes());

        deserializer.deserialize(record, collector);

        SecurityEvent event = collector.getCaptured();
        assertEquals("us-east-1", event.getAttributes().get("region"));
        assertEquals("unknown", event.getAttributes().get("user"));
    }

    @Test
    void testDeserializeEmptyJson_stillEmitsEvent() throws Exception {
        String json = "{}";
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.topic", 0, 0, null, json.getBytes());

        deserializer.deserialize(record, collector);

        SecurityEvent event = collector.getCaptured();
        assertNotNull(event);
        assertEquals("unknown", event.getEntityId());
        assertEquals("UNKNOWN_ATTACK", event.getAttackType());
        assertNotNull(event.getEventId());
    }

    @Test
    void testDeserializeInvalidJson_throwsIOException() {
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test.topic", 0, 0, null, "!!!".getBytes());

        assertThrows(IOException.class, () -> deserializer.deserialize(record, collector));
    }

    @Test
    void testGetProducedType() {
        assertEquals(SecurityEvent.class, deserializer.getProducedType().getTypeClass());
    }

    private static class TestCollector implements Collector<SecurityEvent> {
        private SecurityEvent captured;

        @Override
        public void collect(SecurityEvent event) {
            this.captured = event;
        }

        public SecurityEvent getCaptured() {
            return captured;
        }

        @Override
        public void close() {
            // No-op for test collector
        }
    }
}
