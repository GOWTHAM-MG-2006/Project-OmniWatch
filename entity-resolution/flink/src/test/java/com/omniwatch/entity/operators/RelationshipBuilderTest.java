/*
 * OmniWatch — Entity Resolution Layer
 * Component: RelationshipBuilderTest
 * Phase: 3
 * Purpose: Unit tests for span -> CALLS edge building with parent resolution
 * Inputs: keyed TraceSpanEvent stream (keyed by traceId)
 * Outputs: EntityRelationship assertions
 */
package com.omniwatch.entity.operators;

import com.omniwatch.entity.config.EntityConfig;
import com.omniwatch.entity.models.EntityRelationship;
import com.omniwatch.entity.models.TraceSpanEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RelationshipBuilderTest {

    private static EntityConfig config;

    @BeforeAll
    static void setUp() {
        config = EntityConfig.load();
    }

    private static TraceSpanEvent span(String traceId, String spanId, String parentSpanId,
                                       String rawEntityId, long durationMs, String status) {
        return new TraceSpanEvent(rawEntityId, traceId, spanId, parentSpanId, durationMs, status);
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, TraceSpanEvent, EntityRelationship>
            openHarness() throws Exception {
        KeyedProcessOperator<String, TraceSpanEvent, EntityRelationship> operator =
                new KeyedProcessOperator<>(new RelationshipBuilder(config));
        KeyedOneInputStreamOperatorTestHarness<String, TraceSpanEvent, EntityRelationship> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        TraceSpanEvent::getTraceId,
                        TypeInformation.of(String.class));
        harness.open();
        return harness;
    }

    private static final String WEB_1 = "projects/p1/zones/us-central1-a/instances/web-1";
    private static final String CHECKOUT_1 = "projects/p1/zones/us-central1-a/instances/checkout-1";

    @Test
    void emitsCallsEdgeWhenParentResolves() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, TraceSpanEvent, EntityRelationship> h = openHarness()) {
            h.processElement(new StreamRecord<>(span("trace-1", "span-root", null, WEB_1, 0L, "OK"), 1L));
            assertEquals(0, h.getOutput().size(), "root span with no parent must not emit");
            h.processElement(new StreamRecord<>(span("trace-1", "span-child", "span-root", CHECKOUT_1, 42L, "OK"), 2L));
            assertEquals(1, h.getOutput().size());
            EntityRelationship rel = ((StreamRecord<EntityRelationship>) h.getOutput().poll()).getValue();
            assertEquals("gcp:API_NODE/web-1", rel.getSourceEntityId());
            assertEquals("gcp:API_NODE/checkout-1", rel.getTargetEntityId());
            assertEquals("CALLS", rel.getRelationshipType());
            assertEquals("42", rel.getProperties().get("latency_ms"));
            assertEquals("OK", rel.getProperties().get("status"));
            assertEquals("trace-1", rel.getProperties().get("trace_id"));
            assertNotNull(rel.getTimestamp());
        }
    }

    @Test
    void noEdgeWhenParentSpanUnknown() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, TraceSpanEvent, EntityRelationship> h = openHarness()) {
            h.processElement(new StreamRecord<>(span("trace-1", "span-child", "span-missing", CHECKOUT_1, 5L, "OK"), 1L));
            assertEquals(0, h.getOutput().size(), "child with unknown parent must not emit");
        }
    }

    @Test
    void skipsSelfEdges() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, TraceSpanEvent, EntityRelationship> h = openHarness()) {
            h.processElement(new StreamRecord<>(span("trace-1", "span-a", "span-a", WEB_1, 5L, "OK"), 1L));
            assertEquals(0, h.getOutput().size(), "span whose parent is itself must not emit");
        }
    }

    @Test
    void separatesTracesByTraceId() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, TraceSpanEvent, EntityRelationship> h = openHarness()) {
            h.processElement(new StreamRecord<>(span("trace-1", "span-root", null, WEB_1, 0L, "OK"), 1L));
            h.processElement(new StreamRecord<>(span("trace-2", "span-other", null, WEB_1, 0L, "OK"), 2L));
            // span-root belongs to trace-1 only; trace-2's root has no parent -> no edge
            assertEquals(0, h.getOutput().size());
            h.processElement(new StreamRecord<>(span("trace-2", "span-child", "span-other", CHECKOUT_1, 7L, "ERROR"), 3L));
            assertEquals(1, h.getOutput().size());
            EntityRelationship rel = ((StreamRecord<EntityRelationship>) h.getOutput().poll()).getValue();
            assertEquals("ERROR", rel.getProperties().get("status"));
        }
    }

    @Test
    void canonicalizesRawEntityIdsForBothEndpoints() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, TraceSpanEvent, EntityRelationship> h = openHarness()) {
            h.processElement(new StreamRecord<>(span("trace-1", "span-root", null, "gcp:compute:instance/web-1", 0L, null), 1L));
            h.processElement(new StreamRecord<>(span("trace-1", "span-child", "span-root",
                    "gcp:compute:instance/checkout-1", 10L, null), 2L));
            assertEquals(1, h.getOutput().size());
            EntityRelationship rel = ((StreamRecord<EntityRelationship>) h.getOutput().poll()).getValue();
            assertEquals("gcp:API_NODE/web-1", rel.getSourceEntityId());
            assertEquals("gcp:API_NODE/checkout-1", rel.getTargetEntityId());
            assertFalse(rel.getProperties().containsKey("status"), "null status must be omitted");
        }
    }

    @Test
    void traceWindowIsTenMinutes() {
        assertEquals(10L, RelationshipBuilder.TRACE_WINDOW_MINUTES);
    }
}
