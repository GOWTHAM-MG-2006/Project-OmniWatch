/*
 * OmniWatch — Entity Resolution Layer
 * Component: EntityRelationship model
 * Phase: 3
 * Purpose: Dependency/call edge between two resolved entities, published to
 *          omniwatch.entities.relationships for the causal graph engine.
 * Inputs: RelationshipBuilder
 * Outputs: JSON on omniwatch.entities.relationships (via ObjectMapper)
 */
package com.omniwatch.entity.models;

import java.util.HashMap;
import java.util.Map;

/**
 * Directed relationship between a source entity and a target entity.
 * relationshipType uses the shared vocabulary CALLS / READS_FROM / DEPENDS_ON.
 */
public class EntityRelationship {

    private String sourceEntityId;
    private String targetEntityId;
    private String relationshipType;
    private Map<String, String> properties = new HashMap<>();
    private String timestamp;

    public EntityRelationship() {
    }

    public EntityRelationship(String sourceEntityId, String targetEntityId,
                              String relationshipType,
                              Map<String, String> properties,
                              String timestamp) {
        this.sourceEntityId = sourceEntityId;
        this.targetEntityId = targetEntityId;
        this.relationshipType = relationshipType;
        if (properties != null) {
            this.properties = properties;
        }
        this.timestamp = timestamp;
    }

    public String getSourceEntityId() {
        return sourceEntityId;
    }

    public void setSourceEntityId(String sourceEntityId) {
        this.sourceEntityId = sourceEntityId;
    }

    public String getTargetEntityId() {
        return targetEntityId;
    }

    public void setTargetEntityId(String targetEntityId) {
        this.targetEntityId = targetEntityId;
    }

    public String getRelationshipType() {
        return relationshipType;
    }

    public void setRelationshipType(String relationshipType) {
        this.relationshipType = relationshipType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties == null ? new HashMap<>() : properties;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
