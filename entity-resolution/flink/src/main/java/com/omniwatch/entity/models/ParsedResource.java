/*
 * OmniWatch — Entity Resolution Layer
 * Component: ParsedResource model
 * Phase: 3
 * Purpose: Intermediate result of ResourceIdParser — raw identifier broken
 *          down into provider, region, entity_type and canonical name.
 * Inputs: ResourceIdParser
 * Outputs: CloudProviderMapper
 */
package com.omniwatch.entity.models;

/**
 * Extraction result for a single raw entity identifier.
 * {@code matched} is false when no configured pattern matched; in that case
 * provider is "unknown" and the raw id is carried through as the name.
 */
public class ParsedResource {

    private final String rawEntityId;
    private final String provider;
    private final String region;
    private final String entityType;
    private final String name;
    private final boolean matched;

    public ParsedResource(String rawEntityId, String provider, String region,
                          String entityType, String name, boolean matched) {
        this.rawEntityId = rawEntityId;
        this.provider = provider;
        this.region = region;
        this.entityType = entityType;
        this.name = name;
        this.matched = matched;
    }

    public String getRawEntityId() {
        return rawEntityId;
    }

    public String getProvider() {
        return provider;
    }

    public String getRegion() {
        return region;
    }

    public String getEntityType() {
        return entityType;
    }

    public String getName() {
        return name;
    }

    public boolean isMatched() {
        return matched;
    }
}
