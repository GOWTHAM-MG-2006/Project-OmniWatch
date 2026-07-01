# abstraction — Cloud Abstraction Layer (Phase 3)

## Purpose
Normalize all telemetry into a unified model and map cloud-specific resources
to entity types for cross-cloud analytics.

## Components

### 1. resource_normalizer.py
Maps cloud-specific service names to unified entity types.
- Loads mappings from entity_mappings.yaml
- Returns enriched events with entity_id, entity_type, cloud_provider
- Handles unknown entities with UNKNOWN_NODE type

### 2. entity_mapper.py
Enriches entities with business metadata from topology.
- Loads topology.json for service dependencies
- Adds criticality, SLA tier, upstream/downstream dependencies
- Provides full business context for each entity

### 3. cross_cloud_model.py
Unified schema for multi-cloud resources.
- Defines dataclasses: UnifiedMetric, UnifiedLog, UnifiedTrace, UnifiedSecurityEvent
- Builds type-safe event objects from raw telemetry
- Conforms to AGENTS.md data contracts

### 4. mappings/entity_mappings.yaml
Configurable mapping of service names to entity types.
- Service mappings (simulated services)
- Cloud provider mappings (AWS, Azure, GCP)
- Default values for unknown entities

## How to Run

```powershell
cd E:\Project-OmniWatch
simulation\.venv\Scripts\Activate.ps1

# Normalize an entity
py abstraction\resource_normalizer.py normalize --entity api-gateway

# List all mappings
py abstraction\resource_normalizer.py list

# Get entity info
py abstraction\entity_mapper.py map --entity api-gateway

# Get dependencies
py abstraction\entity_mapper.py deps --entity api-gateway
```
