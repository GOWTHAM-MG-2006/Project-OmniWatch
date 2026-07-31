/*
 * OmniWatch — Entity Resolution Layer
 * Component: ResourceIdParserTest
 * Phase: 3
 * Purpose: Unit tests for resource identifier parsing (GCP/AWS/Azure/K8s)
 * Inputs: raw entity IDs + entity type hints + EntityConfig mappings
 * Outputs: ParsedResource assertions
 */
package com.omniwatch.entity.operators;

import com.omniwatch.entity.config.EntityConfig;
import com.omniwatch.entity.models.ParsedResource;
import com.omniwatch.entity.models.TelemetryEvent;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ResourceIdParserTest {

    private static EntityConfig config;
    private static ResourceIdParser parser;

    @BeforeAll
    static void setUp() {
        config = EntityConfig.load();
        parser = new ResourceIdParser(config);
    }

    private static ParsedResource parse(String raw, String hint) {
        return ResourceIdParser.parseRaw(raw, hint, config);
    }

    @Test
    void parsesGcpComputeInstanceFullPath() {
        ParsedResource r = parse("projects/omniproject/zones/us-central1-a/instances/web-1", "API_NODE");
        assertTrue(r.isMatched());
        assertEquals("gcp", r.getProvider());
        assertEquals("us-central1", r.getRegion());
        assertEquals("API_NODE", r.getEntityType());
        assertEquals("web-1", r.getName());
        assertEquals("projects/omniproject/zones/us-central1-a/instances/web-1", r.getRawEntityId());
    }

    @Test
    void parsesGcpComputeShortForm() {
        ParsedResource r = parse("gcp:compute:instance/web-1", "API_NODE");
        assertTrue(r.isMatched());
        assertEquals("gcp", r.getProvider());
        assertEquals("API_NODE", r.getEntityType());
        assertEquals("web-1", r.getName());
    }

    @Test
    void mapsGcpZoneToRegionWhenUnknownZoneFallsBackToZone() {
        // europe-west2-a is not in zone_to_region map -> region stays as the zone
        ParsedResource r = parse("projects/p1/zones/europe-west2-a/instances/worker-3", null);
        assertTrue(r.isMatched());
        assertEquals("europe-west2-a", r.getRegion());
    }

    @Test
    void parsesAwsArnEc2Instance() {
        ParsedResource r = parse("arn:aws:ec2:us-east-1:123456789012:instance/i-0a1b2c3d4e5f67890", null);
        assertTrue(r.isMatched());
        assertEquals("aws", r.getProvider());
        assertEquals("us-east-1", r.getRegion());
        assertEquals("API_NODE", r.getEntityType());
        assertEquals("i-0a1b2c3d4e5f67890", r.getName());
    }

    @Test
    void parsesAwsShortForm() {
        ParsedResource r = parse("aws:ec2:instance/i-1234", null);
        assertTrue(r.isMatched());
        assertEquals("aws", r.getProvider());
        assertEquals("API_NODE", r.getEntityType());
        assertEquals("i-1234", r.getName());
    }

    @Test
    void parsesAwsInstanceIdPatternWithoutNamedGroups() {
        ParsedResource r = parse("i-0abcdef123456789", null);
        assertTrue(r.isMatched());
        assertEquals("aws", r.getProvider());
        assertEquals("API_NODE", r.getEntityType());
        // no named groups -> name falls back to raw id, region empty
        assertEquals("i-0abcdef123456789", r.getName());
        assertEquals("", r.getRegion());
    }

    @Test
    void parsesAzureVmFullPath() {
        ParsedResource r = parse("/subscriptions/sub-1/resourceGroups/rg-prod/providers/Microsoft.Compute/virtualMachines/web-vm", null);
        assertTrue(r.isMatched());
        assertEquals("azure", r.getProvider());
        assertEquals("API_NODE", r.getEntityType());
        assertEquals("web-vm", r.getName());
    }

    @Test
    void parsesAzureShortForm() {
        ParsedResource r = parse("azure:vm:web-vm", null);
        assertTrue(r.isMatched());
        assertEquals("azure", r.getProvider());
        assertEquals("web-vm", r.getName());
    }

    @Test
    void parsesK8sPodNamespacePattern() {
        ParsedResource r = parse("default/web-1-ab12c", null);
        assertTrue(r.isMatched());
        assertEquals("k8s", r.getProvider());
        assertEquals("API_NODE", r.getEntityType());
        assertEquals("web-1", r.getName());
    }

    @Test
    void parsesK8sPodShortForm() {
        ParsedResource r = parse("k8s:pod:default/web-1", null);
        assertTrue(r.isMatched());
        assertEquals("k8s", r.getProvider());
        assertEquals("web-1", r.getName());
    }

    @Test
    void parsesGcpDatabaseInstance() {
        ParsedResource r = parse("projects/omniproject/instances/postgres-1", null);
        assertTrue(r.isMatched());
        assertEquals("gcp", r.getProvider());
        assertEquals("DATABASE_NODE", r.getEntityType());
        assertEquals("postgres-1", r.getName());
    }

    @Test
    void parsesAwsRdsDatabase() {
        ParsedResource r = parse("arn:aws:rds:eu-west-1:123456789012:db:orders-db", null);
        assertTrue(r.isMatched());
        assertEquals("aws", r.getProvider());
        assertEquals("eu-west-1", r.getRegion());
        assertEquals("DATABASE_NODE", r.getEntityType());
        assertEquals("orders-db", r.getName());
    }

    @Test
    void parsesK8sServicePattern() {
        ParsedResource r = parse("service/default/shop", null);
        assertTrue(r.isMatched());
        assertEquals("k8s", r.getProvider());
        assertEquals("API_NODE", r.getEntityType());
        assertEquals("shop", r.getName());
    }

    @Test
    void returnsUnmatchedResourceForUnknownId() {
        ParsedResource r = parse("some/random/thing", "API_NODE");
        assertFalse(r.isMatched());
        assertEquals("unknown", r.getProvider());
        assertEquals("", r.getRegion());
        assertEquals("API_NODE", r.getEntityType());
        assertEquals("some/random/thing", r.getName());
    }

    @Test
    void returnsEmptyResourceForNullRawId() {
        ParsedResource r = parse(null, null);
        assertFalse(r.isMatched());
        assertEquals("unknown", r.getProvider());
        assertEquals("", r.getEntityType());
    }

    @Test
    void mapFunctionDelegatesToParseRaw() {
        TelemetryEvent evt = new TelemetryEvent();
        evt.setEntityId("projects/omniproject/zones/us-central1-a/instances/web-1");
        evt.setEntityType("API_NODE");
        ParsedResource r = parser.map(evt);
        assertTrue(r.isMatched());
        assertEquals("gcp", r.getProvider());
        assertEquals("web-1", r.getName());
    }
}
