/*
 * OmniWatch — Entity Resolution Layer
 * Component: EntityDeduplicator
 * Phase: 3
 * Purpose: Stage 4 of the entity resolution pipeline. Keyed by entity_id,
 *          merges repeated observations of the same entity within a 5-minute
 *          active window into a single record (updates last_seen, unions raw
 *          identifiers) and only emits when the entity is first seen (or its
 *          active state has expired).
 * Inputs: UnifiedEntity (keyed stream by entityId)
 * Outputs: UnifiedEntity (deduplicated, one record per active window)
 */
package com.omniwatch.entity.operators;

import com.omniwatch.entity.models.UnifiedEntity;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Deduplicates entity observations per the GAP-3 style rule: if the same
 * entity_id has been active in the last 5 minutes, update the existing
 * record instead of creating a new one.
 */
public class EntityDeduplicator extends KeyedProcessFunction<String, UnifiedEntity, UnifiedEntity> {

    private static final long serialVersionUID = 1L;
    static final long ACTIVE_WINDOW_MINUTES = 5L;

    private transient ValueState<UnifiedEntity> activeEntityState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<UnifiedEntity> descriptor =
                new ValueStateDescriptor<>("active-entity", UnifiedEntity.class);
        activeEntityState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(UnifiedEntity value, Context ctx, Collector<UnifiedEntity> out)
            throws Exception {
        UnifiedEntity existing = activeEntityState.value();
        if (existing == null) {
            // First observation within the active window: emit a first-sight
            // snapshot and track the entity until the window elapses.
            activeEntityState.update(value);
            long expiry = ctx.timerService().currentProcessingTime()
                    + Time.minutes(ACTIVE_WINDOW_MINUTES).toMilliseconds();
            ctx.timerService().registerProcessingTimeTimer(expiry);
            out.collect(snapshotOf(value));
            return;
        }
        // Active entity already seen: merge, do not emit a new record.
        mergeInto(existing, value);
        activeEntityState.update(existing);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<UnifiedEntity> out)
            throws Exception {
        // Active window elapsed: forget the entity so a later observation is
        // treated as a fresh sighting (and re-emitted).
        activeEntityState.clear();
    }

    /** Package-private test accessor: current merged entity state (null when inactive). */
    UnifiedEntity activeState() throws Exception {
        return activeEntityState.value();
    }

    /** Copies the incoming observation so the emitted record is a stable first-sight snapshot. */
    private static UnifiedEntity snapshotOf(UnifiedEntity value) {
        UnifiedEntity copy = new UnifiedEntity();
        copy.setEntityId(value.getEntityId());
        copy.setEntityType(value.getEntityType());
        copy.setProvider(value.getProvider());
        copy.setRegion(value.getRegion());
        copy.setName(value.getName());
        Map<String, String> tags = new HashMap<>();
        if (value.getBusinessTags() != null) {
            tags.putAll(value.getBusinessTags());
        }
        copy.setBusinessTags(tags);
        copy.setRawIdentifiers(new ArrayList<>());
        if (value.getRawIdentifiers() != null) {
            copy.getRawIdentifiers().addAll(value.getRawIdentifiers());
        }
        copy.setFirstSeen(value.getFirstSeen());
        copy.setLastSeen(value.getLastSeen());
        return copy;
    }

    private static void mergeInto(UnifiedEntity existing, UnifiedEntity incoming) {
        // Keep earliest first_seen, refresh last_seen.
        if (incoming.getLastSeen() != null) {
            existing.setLastSeen(incoming.getLastSeen());
        }
        // Union raw identifiers.
        List<String> identifiers = existing.getRawIdentifiers();
        if (incoming.getRawIdentifiers() != null) {
            for (String id : incoming.getRawIdentifiers()) {
                if (id != null && !identifiers.contains(id)) {
                    identifiers.add(id);
                }
            }
        }
        existing.setRawIdentifiers(identifiers);
    }
}
