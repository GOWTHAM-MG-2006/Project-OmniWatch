/*
 * OmniWatch — Entity Resolution Layer
 * Component: UnifiedEntity model
 * Phase: 3
 * Purpose: Canonical entity representation produced by the entity resolution
 *          pipeline and published to omniwatch.entities.resolved.
 * Inputs: CloudProviderMapper / EntityEnricher / EntityDeduplicator
 * Outputs: JSON on omniwatch.entities.resolved (via ObjectMapper)
 */
package com.omniwatch.entity.models;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unified entity record. An entity is uniquely identified by {@code entityId}
 * (canonical form "provider:entityType/name"). Raw identifiers observed across
 * telemetry sources are preserved in {@code rawIdentifiers} for traceability.
 */
public class UnifiedEntity {

    private String entityId;
    private String entityType;
    private String provider;
    private String region;
    private String name;
    private Map<String, String> businessTags = new HashMap<>();
    private List<String> rawIdentifiers = new ArrayList<>();
    private String firstSeen;
    private String lastSeen;

    public UnifiedEntity() {
    }

    public UnifiedEntity(String entityId, String entityType, String provider,
                         String region, String name,
                         Map<String, String> businessTags,
                         List<String> rawIdentifiers,
                         String firstSeen, String lastSeen) {
        this.entityId = entityId;
        this.entityType = entityType;
        this.provider = provider;
        this.region = region;
        this.name = name;
        if (businessTags != null) {
            this.businessTags = businessTags;
        }
        if (rawIdentifiers != null) {
            this.rawIdentifiers = rawIdentifiers;
        }
        this.firstSeen = firstSeen;
        this.lastSeen = lastSeen;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getBusinessTags() {
        return businessTags;
    }

    public void setBusinessTags(Map<String, String> businessTags) {
        this.businessTags = businessTags == null ? new HashMap<>() : businessTags;
    }

    public List<String> getRawIdentifiers() {
        return rawIdentifiers;
    }

    public void setRawIdentifiers(List<String> rawIdentifiers) {
        this.rawIdentifiers = rawIdentifiers == null ? new ArrayList<>() : rawIdentifiers;
    }

    public String getFirstSeen() {
        return firstSeen;
    }

    public void setFirstSeen(String firstSeen) {
        this.firstSeen = firstSeen;
    }

    public String getLastSeen() {
        return lastSeen;
    }

    public void setLastSeen(String lastSeen) {
        this.lastSeen = lastSeen;
    }
}
