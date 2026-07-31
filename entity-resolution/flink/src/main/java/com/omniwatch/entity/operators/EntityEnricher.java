/*
 * OmniWatch — Entity Resolution Layer
 * Component: EntityEnricher
 * Phase: 3
 * Purpose: Stage 3 of the entity resolution pipeline. Attaches business tags
 *          (service_name, environment, owner_team, criticality, sla_tier) by
 *          entity_type defaults overridden by first-matching name rule.
 * Inputs: UnifiedEntity (from CloudProviderMapper)
 * Outputs: UnifiedEntity (with businessTags populated)
 */
package com.omniwatch.entity.operators;

import com.omniwatch.entity.config.EntityConfig;
import com.omniwatch.entity.config.EntityConfig.TagRule;
import com.omniwatch.entity.models.UnifiedEntity;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Enriches an entity with business context. Tag precedence (lowest to
 * highest): global defaults -> per-entity-type defaults -> first matching
 * name rule.
 */
public class EntityEnricher implements MapFunction<UnifiedEntity, UnifiedEntity> {

    private static final long serialVersionUID = 1L;

    private final EntityConfig config;

    public EntityEnricher(EntityConfig config) {
        this.config = config;
    }

    @Override
    public UnifiedEntity map(UnifiedEntity entity) {
        Map<String, String> tags = resolveTags(entity.getEntityType(), entity.getName());
        entity.setBusinessTags(tags);
        return entity;
    }

    /** Resolves the effective business tags for an entity. */
    public Map<String, String> resolveTags(String entityType, String name) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.putAll(config.getDefaultTags());

        Map<String, Map<String, String>> byType = config.getTagsByType();
        if (entityType != null && byType.containsKey(entityType)) {
            tags.putAll(byType.get(entityType));
        }

        for (TagRule rule : config.getTagRules()) {
            if (rule.getEntityType() != null && !rule.getEntityType().equals(entityType)) {
                continue;
            }
            if (rule.getNamePattern() != null
                    && (name == null || !rule.getNamePattern().matcher(name).find())) {
                continue;
            }
            tags.putAll(rule.getTags());
            break; // first matching rule wins
        }
        return tags;
    }
}
