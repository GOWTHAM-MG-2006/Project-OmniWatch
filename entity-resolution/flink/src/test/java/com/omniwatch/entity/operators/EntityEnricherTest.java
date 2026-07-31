/*
 * OmniWatch — Entity Resolution Layer
 * Component: EntityEnricherTest
 * Phase: 3
 * Purpose: Unit tests for business tag resolution (defaults, by-type, first-match rules)
 * Inputs: entity type + name
 * Outputs: businessTags map assertions
 */
package com.omniwatch.entity.operators;

import com.omniwatch.entity.config.EntityConfig;
import com.omniwatch.entity.models.UnifiedEntity;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EntityEnricherTest {

    private static EntityConfig config;
    private static EntityEnricher enricher;

    @BeforeAll
    static void setUp() {
        config = EntityConfig.load();
        enricher = new EntityEnricher(config);
    }

    @Test
    void databaseNodeGetsDataTierTags() {
        Map<String, String> tags = enricher.resolveTags("DATABASE_NODE", "postgres-1");
        assertEquals("HIGH", tags.get("criticality")); // by_entity_type override
        assertEquals("24x7", tags.get("sla_tier"));
        assertEquals("data-tier", tags.get("service_name"));
        assertEquals("platform-db", tags.get("owner_team"));
        assertEquals("prod", tags.get("environment"));
    }

    @Test
    void paymentsServiceGetsCriticalityOverride() {
        Map<String, String> tags = enricher.resolveTags("API_NODE", "payment-svc");
        assertEquals("payments", tags.get("service_name"));
        assertEquals("payments-team", tags.get("owner_team"));
        assertEquals("CRITICAL", tags.get("criticality"));
        assertEquals("24x7", tags.get("sla_tier"));
    }

    @Test
    void catalogServiceGetsCatalogTags() {
        Map<String, String> tags = enricher.resolveTags("API_NODE", "catalog-api");
        assertEquals("catalog", tags.get("service_name"));
        assertEquals("catalog-team", tags.get("owner_team"));
        assertEquals("MEDIUM", tags.get("criticality"));
    }

    @Test
    void frontendServiceMatchesFirstWinningRule() {
        // rule 4 (^frontend -> frontend/web-team) precedes rule 5 which also matches frontend
        Map<String, String> tags = enricher.resolveTags("API_NODE", "frontend");
        assertEquals("frontend", tags.get("service_name"));
        assertEquals("web-team", tags.get("owner_team"));
    }

    @Test
    void recommendationServiceGetsShopTags() {
        Map<String, String> tags = enricher.resolveTags("API_NODE", "recommendation-1");
        assertEquals("shop", tags.get("service_name"));
        assertEquals("ecommerce-team", tags.get("owner_team"));
    }

    @Test
    void unknownApiNodeGetsDefaultService() {
        Map<String, String> tags = enricher.resolveTags("API_NODE", "mystery-service");
        assertEquals("default-service", tags.get("service_name"));
        assertEquals("platform-team", tags.get("owner_team"));
        assertEquals("MEDIUM", tags.get("criticality"));
        assertEquals("Business Hours", tags.get("sla_tier"));
    }

    @Test
    void unknownEntityTypeGetsDefaultsOnly() {
        Map<String, String> tags = enricher.resolveTags("STORAGE_NODE", "bucket-1");
        assertEquals("LOW", tags.get("criticality"));
        assertEquals("Best Effort", tags.get("sla_tier"));
        assertNull(tags.get("service_name"));
        assertNull(tags.get("owner_team"));
    }

    @Test
    void nullNameSkipsNameRules() {
        Map<String, String> tags = enricher.resolveTags("API_NODE", null);
        assertEquals("MEDIUM", tags.get("criticality"));
        assertNull(tags.get("service_name"));
    }

    @Test
    void mapFunctionAttachesResolvedTags() {
        UnifiedEntity e = new UnifiedEntity("gcp:API_NODE/web-1", "API_NODE", "gcp", "us-central1",
                "web-1", null, null, null, null);
        UnifiedEntity enriched = enricher.map(e);
        assertEquals("default-service", enriched.getBusinessTags().get("service_name"));
        assertEquals("platform-team", enriched.getBusinessTags().get("owner_team"));
    }
}
