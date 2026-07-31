/*
 * OmniWatch — Entity Resolution Layer
 * Component: CloudProviderMapper
 * Phase: 3
 * Purpose: Stage 2 of the entity resolution pipeline. Converts a
 *          ParsedResource into a canonical UnifiedEntity with a stable
 *          entity_id ("provider:entityType/name"), raw identifier retention
 *          and first/last seen timestamps.
 * Inputs: ParsedResource
 * Outputs: UnifiedEntity (pre-enrichment)
 */
package com.omniwatch.entity.operators;

import com.omniwatch.entity.models.ParsedResource;
import com.omniwatch.entity.models.UnifiedEntity;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Maps parsed resource metadata onto the canonical UnifiedEntity contract.
 */
public class CloudProviderMapper implements MapFunction<ParsedResource, UnifiedEntity> {

    private static final long serialVersionUID = 1L;

    @Override
    public UnifiedEntity map(ParsedResource resource) {
        return toUnifiedEntity(resource);
    }

    /** Static conversion, reusable by other operators and tests. */
    public static UnifiedEntity toUnifiedEntity(ParsedResource resource) {
        String entityId = canonicalEntityId(resource);
        String now = Instant.now().toString();
        UnifiedEntity entity = new UnifiedEntity();
        entity.setEntityId(entityId);
        entity.setEntityType(resource.getEntityType());
        entity.setProvider(resource.getProvider());
        entity.setRegion(resource.getRegion());
        entity.setName(resource.getName());
        entity.setRawIdentifiers(new ArrayList<>(Collections.singletonList(resource.getRawEntityId())));
        entity.setFirstSeen(now);
        entity.setLastSeen(now);
        return entity;
    }

    /**
     * Builds the stable canonical identifier used for deduplication and as the
     * Neo4j node key in later phases: "{provider}:{entityType}/{name}".
     */
    public static String canonicalEntityId(ParsedResource resource) {
        String provider = resource.getProvider() == null ? "unknown" : resource.getProvider();
        String type = resource.getEntityType() == null || resource.getEntityType().isEmpty()
                ? "UNKNOWN_NODE" : resource.getEntityType();
        String name = resource.getName() == null || resource.getName().isEmpty()
                ? resource.getRawEntityId() : resource.getName();
        return provider + ":" + type + "/" + name;
    }
}
