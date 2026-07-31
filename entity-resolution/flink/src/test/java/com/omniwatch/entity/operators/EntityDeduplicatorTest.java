/*
 * OmniWatch — Entity Resolution Layer
 * Component: EntityDeduplicatorTest
 * Phase: 3
 * Purpose: Unit tests for entity deduplication, merge and TTL expiry
 * Inputs: keyed UnifiedEntity stream
 * Outputs: emitted/merged entity assertions
 */
package com.omniwatch.entity.operators;

import com.omniwatch.entity.models.UnifiedEntity;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EntityDeduplicatorTest {

    private static UnifiedEntity entity(String id, String lastSeen, String... rawIds) {
        UnifiedEntity e = new UnifiedEntity();
        e.setEntityId(id);
        e.setEntityType("API_NODE");
        e.setProvider("gcp");
        e.setName(id);
        e.setLastSeen(lastSeen);
        for (String raw : rawIds) {
            e.getRawIdentifiers().add(raw);
        }
        return e;
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, UnifiedEntity, UnifiedEntity>
            openHarness(EntityDeduplicator dedup) throws Exception {
        KeyedProcessOperator<String, UnifiedEntity, UnifiedEntity> operator =
                new KeyedProcessOperator<>(dedup);
        KeyedOneInputStreamOperatorTestHarness<String, UnifiedEntity, UnifiedEntity> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        UnifiedEntity::getEntityId,
                        TypeInformation.of(String.class));
        harness.open();
        return harness;
    }

    @Test
    void emitsFirstObservation() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, UnifiedEntity, UnifiedEntity> h =
                openHarness(new EntityDeduplicator())) {
            UnifiedEntity e = entity("gcp:API_NODE/web-1", "2026-01-01T00:00:00Z", "raw-1");
            h.processElement(new StreamRecord<>(e, 1L));
            assertEquals(1, h.getOutput().size());
            UnifiedEntity out = ((StreamRecord<UnifiedEntity>) h.getOutput().poll()).getValue();
            assertEquals("gcp:API_NODE/web-1", out.getEntityId());
            assertEquals(1, out.getRawIdentifiers().size());
        }
    }

    @Test
    void suppressesDuplicateAndMergesLastSeen() throws Exception {
        EntityDeduplicator dedup = new EntityDeduplicator();
        try (KeyedOneInputStreamOperatorTestHarness<String, UnifiedEntity, UnifiedEntity> h =
                openHarness(dedup)) {
            h.processElement(new StreamRecord<>(
                    entity("gcp:API_NODE/web-1", "2026-01-01T00:00:00Z", "raw-1"), 1L));
            h.processElement(new StreamRecord<>(
                    entity("gcp:API_NODE/web-1", "2026-01-01T00:05:00Z", "raw-2"), 2L));
            // second observation must NOT be emitted (dedup within active window)
            assertEquals(1, h.getOutput().size());
            // emitted record is the first-sight snapshot, unchanged
            UnifiedEntity emitted =
                    ((StreamRecord<UnifiedEntity>) h.getOutput().poll()).getValue();
            assertEquals("2026-01-01T00:00:00Z", emitted.getLastSeen());
            assertEquals(1, emitted.getRawIdentifiers().size());
            // state must reflect the merge (refreshed lastSeen + union rawIdentifiers)
            UnifiedEntity state = dedup.activeState();
            assertNotNull(state);
            assertEquals("2026-01-01T00:05:00Z", state.getLastSeen());
            assertEquals(2, state.getRawIdentifiers().size());
        }
    }

    @Test
    void mergesRawIdentifiersWithoutDuplicates() throws Exception {
        EntityDeduplicator dedup = new EntityDeduplicator();
        try (KeyedOneInputStreamOperatorTestHarness<String, UnifiedEntity, UnifiedEntity> h =
                openHarness(dedup)) {
            h.processElement(new StreamRecord<>(
                    entity("gcp:API_NODE/web-1", "t1", "raw-a", "raw-b"), 1L));
            h.processElement(new StreamRecord<>(
                    entity("gcp:API_NODE/web-1", "t2", "raw-b", "raw-c"), 2L));
            // only the first observation is emitted
            assertEquals(1, h.getOutput().size());
            // merged state unions raw identifiers without duplicates
            UnifiedEntity state = dedup.activeState();
            assertNotNull(state);
            assertEquals(3, state.getRawIdentifiers().size());
            assertTrue(state.getRawIdentifiers().contains("raw-a"));
            assertTrue(state.getRawIdentifiers().contains("raw-b"));
            assertTrue(state.getRawIdentifiers().contains("raw-c"));
        }
    }

    @Test
    void emitsDistinctEntitiesIndependently() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, UnifiedEntity, UnifiedEntity> h =
                openHarness(new EntityDeduplicator())) {
            h.processElement(new StreamRecord<>(
                    entity("gcp:API_NODE/web-1", "t1", "raw-1"), 1L));
            h.processElement(new StreamRecord<>(
                    entity("aws:DATABASE_NODE/db-1", "t1", "raw-2"), 2L));
            assertEquals(2, h.getOutput().size());
        }
    }

    @Test
    void reEmitsAfterTtlExpiry() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, UnifiedEntity, UnifiedEntity> h =
                openHarness(new EntityDeduplicator())) {
            h.setProcessingTime(0L);
            h.processElement(new StreamRecord<>(
                    entity("gcp:API_NODE/web-1", "t1", "raw-1"), 1L));
            assertEquals(1, h.getOutput().size());
            // advance processing time beyond the 5-minute active window
            h.setProcessingTime(Time.minutes(6).toMilliseconds());
            h.processElement(new StreamRecord<>(
                    entity("gcp:API_NODE/web-1", "t2", "raw-1"), 2L));
            // expired state -> treated as a fresh observation -> emitted again
            assertEquals(2, h.getOutput().size());
        }
    }

    @Test
    void activeWindowIsFiveMinutes() {
        assertEquals(5L, EntityDeduplicator.ACTIVE_WINDOW_MINUTES);
    }
}
