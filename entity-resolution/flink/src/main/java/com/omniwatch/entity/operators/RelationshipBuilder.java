/*
 * OmniWatch — Entity Resolution Layer
 * Component: RelationshipBuilder
 * Phase: 3
 * Purpose: Stage 5 of the entity resolution pipeline. Keyed by trace_id,
 *          tracks spanId -> canonical entityId mappings and emits a CALLS
 *          relationship (parent entity -> child entity) when a child span's
 *          parentSpanId is already known within the same trace.
 * Inputs: TraceSpanEvent (keyed stream by traceId)
 * Outputs: EntityRelationship -> omniwatch.entities.relationships
 */
package com.omniwatch.entity.operators;

import com.omniwatch.entity.config.EntityConfig;
import com.omniwatch.entity.models.EntityRelationship;
import com.omniwatch.entity.models.ParsedResource;
import com.omniwatch.entity.models.TraceSpanEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds dependency edges from trace span parent/child relationships.
 * Only spans whose parent was observed earlier in the same trace window
 * produce an edge; unknown parents are tracked for later spans.
 */
public class RelationshipBuilder extends KeyedProcessFunction<String, TraceSpanEvent, EntityRelationship> {

    private static final long serialVersionUID = 1L;
    static final long TRACE_WINDOW_MINUTES = 10L;

    private final EntityConfig config;
    private transient MapState<String, String> spanEntityState;

    public RelationshipBuilder(EntityConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.minutes(TRACE_WINDOW_MINUTES))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        MapStateDescriptor<String, String> descriptor =
                new MapStateDescriptor<>("span-entity", String.class, String.class);
        descriptor.enableTimeToLive(ttl);
        spanEntityState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(TraceSpanEvent span, Context ctx, Collector<EntityRelationship> out)
            throws Exception {
        String childEntityId = canonicalEntityId(span.getEntityId());
        String parentEntityId = null;

        if (span.getParentSpanId() != null && !span.getParentSpanId().isEmpty()) {
            parentEntityId = spanEntityState.get(span.getParentSpanId());
        }

        // Track this span regardless, so later child spans can resolve us.
        spanEntityState.put(span.getSpanId(), childEntityId);

        if (parentEntityId != null && !parentEntityId.equals(childEntityId)) {
            Map<String, String> properties = new LinkedHashMap<>();
            properties.put("latency_ms", String.valueOf(span.getDurationMs()));
            if (span.getStatus() != null) {
                properties.put("status", span.getStatus());
            }
            properties.put("trace_id", span.getTraceId());
            out.collect(new EntityRelationship(
                    parentEntityId, childEntityId, "CALLS", properties, Instant.now().toString()));
        }
    }

    /** Resolves the canonical entity id for a raw identifier. */
    private String canonicalEntityId(String rawEntityId) {
        ParsedResource parsed = ResourceIdParser.parseRaw(rawEntityId, null, config);
        return CloudProviderMapper.canonicalEntityId(parsed);
    }
}
