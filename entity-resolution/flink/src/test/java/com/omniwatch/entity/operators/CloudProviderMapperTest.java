/*
 * OmniWatch — Entity Resolution Layer
 * Component: CloudProviderMapperTest
 * Phase: 3
 * Purpose: Unit tests for provider/type/name -> canonical UnifiedEntity mapping
 * Inputs: ParsedResource instances
 * Outputs: UnifiedEntity + canonical entity id assertions
 */
package com.omniwatch.entity.operators;

import com.omniwatch.entity.models.ParsedResource;
import com.omniwatch.entity.models.UnifiedEntity;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CloudProviderMapperTest {

    private static ParsedResource resource(String raw, String provider, String region,
                                          String entityType, String name, boolean matched) {
        return new ParsedResource(raw, provider, region, entityType, name, matched);
    }

    @Test
    void mapsGcpComputeResourceToUnifiedEntity() {
        ParsedResource r = resource("projects/p1/zones/us-central1-a/instances/web-1",
                "gcp", "us-central1", "API_NODE", "web-1", true);
        UnifiedEntity e = CloudProviderMapper.toUnifiedEntity(r);
        assertEquals("gcp:API_NODE/web-1", e.getEntityId());
        assertEquals("gcp", e.getProvider());
        assertEquals("API_NODE", e.getEntityType());
        assertEquals("us-central1", e.getRegion());
        assertEquals("web-1", e.getName());
        assertEquals(1, e.getRawIdentifiers().size());
        assertEquals("projects/p1/zones/us-central1-a/instances/web-1", e.getRawIdentifiers().get(0));
        assertNotNull(e.getFirstSeen());
        assertNotNull(e.getLastSeen());
    }

    @Test
    void mapsUnmatchedResourceToUnknownProvider() {
        ParsedResource r = resource("random-id", "unknown", "", "API_NODE", "random-id", false);
        UnifiedEntity e = CloudProviderMapper.toUnifiedEntity(r);
        assertEquals("unknown:API_NODE/random-id", e.getEntityId());
        assertEquals("unknown", e.getProvider());
    }

    @Test
    void canonicalIdForDatabaseNode() {
        ParsedResource r = resource("arn:aws:rds:eu-west-1:123456789012:db:orders-db",
                "aws", "eu-west-1", "DATABASE_NODE", "orders-db", true);
        assertEquals("aws:DATABASE_NODE/orders-db", CloudProviderMapper.canonicalEntityId(r));
    }

    @Test
    void canonicalIdFallsBackToUnknownProvider() {
        ParsedResource r = resource("x", null, null, "API_NODE", "svc", true);
        assertEquals("unknown:API_NODE/svc", CloudProviderMapper.canonicalEntityId(r));
    }

    @Test
    void canonicalIdFallsBackToUnknownNodeType() {
        ParsedResource r = resource("x", "gcp", null, null, "svc", true);
        assertEquals("gcp:UNKNOWN_NODE/svc", CloudProviderMapper.canonicalEntityId(r));
    }

    @Test
    void canonicalIdFallsBackToRawIdForEmptyName() {
        ParsedResource r = resource("gcp:compute:instance/web-1", "gcp", null, "API_NODE", "", true);
        assertEquals("gcp:API_NODE/gcp:compute:instance/web-1", CloudProviderMapper.canonicalEntityId(r));
    }

    @Test
    void toUnifiedEntityUsesCanonicalIdWithRawFallbackName() {
        ParsedResource r = resource("gcp:compute:instance/web-1", "gcp", "", "API_NODE", "", true);
        UnifiedEntity e = CloudProviderMapper.toUnifiedEntity(r);
        assertEquals("gcp:API_NODE/gcp:compute:instance/web-1", e.getEntityId());
        assertEquals("", e.getName()); // name kept as-is; raw id only in canonical entityId
    }

    @Test
    void mapsAzureVmResource() {
        ParsedResource r = resource("/subscriptions/s/rg-prod/providers/Microsoft.Compute/virtualMachines/web-vm",
                "azure", "", "API_NODE", "web-vm", true);
        UnifiedEntity e = CloudProviderMapper.toUnifiedEntity(r);
        assertEquals("azure:API_NODE/web-vm", e.getEntityId());
        assertEquals("web-vm", e.getName());
    }

    @Test
    void mapsK8sPodResource() {
        ParsedResource r = resource("default/web-1-ab12c", "k8s", "", "API_NODE", "web-1", true);
        UnifiedEntity e = CloudProviderMapper.toUnifiedEntity(r);
        assertEquals("k8s:API_NODE/web-1", e.getEntityId());
    }
}
