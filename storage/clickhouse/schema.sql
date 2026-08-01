-- =============================================================================
-- OmniWatch — Storage Layer
-- Component: ClickHouse Schema
-- Phase: 5
-- Purpose: DDL for the ClickHouse Unified Storage Layer — the primary time-series
--          database for windowed timestamped data from cloud services, Kubernetes
--          pods, and security systems (Dataflow.md Tool 6).
-- Inputs: Telemetry (metrics, logs, traces) from ingestion, anomaly records from
--         predictive/, incident records from prioritization/, and resolved incident
--         outcomes from the continuous learning loop.
-- Outputs: 7 MergeTree tables in database `omniwatch` with daily partitioning
--          (toYYYYMMDD) and TTL retention policies. feature_vectors is NOT created
--          here — it is owned by Phase 4 (FeatureStoreWriter.java creates it).
-- Idempotent: safe to run multiple times (CREATE DATABASE/TABLE IF NOT EXISTS).
-- =============================================================================

CREATE DATABASE IF NOT EXISTS omniwatch;

-- -----------------------------------------------------------------------------
-- metrics — raw time-series metric storage (MergeTree)
-- Partitioning: toYYYYMMDD(timestamp); TTL: 90 days (Built Plan line 534)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS omniwatch.metrics
(
    entity_id    String,             -- normalized entity ID (e.g. postgresql-database)
    entity_type  String,             -- API_NODE, DATABASE_NODE, etc.
    metric_name  String,             -- e.g. cpu_usage, latency_p95
    value        Float64,            -- metric value at this timestamp
    tags         Map(String, String),-- dimension labels (pod, region, host)
    source_type  String,             -- "performance" or "security"
    timestamp    DateTime            -- UTC event time; partition + TTL key
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (entity_id, timestamp)
TTL timestamp + INTERVAL 90 DAY;

-- -----------------------------------------------------------------------------
-- logs — structured log events (MergeTree + bloom filter)
-- Partitioning: toYYYYMMDD(timestamp); TTL: 30 days (Built Plan line 534)
-- bloom_filter index accelerates WHERE entity_id = '...' scans.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS omniwatch.logs
(
    entity_id    String,             -- owning entity ID (log source)
    log_level    String,             -- DEBUG, INFO, WARN, ERROR, FATAL
    message      String,             -- log message body
    service_name String,             -- emitting service
    trace_id     String,             -- distributed tracing correlation ID
    timestamp    DateTime,           -- UTC event time; partition + TTL key
    INDEX idx_entity entity_id TYPE bloom_filter GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (entity_id, timestamp)
TTL timestamp + INTERVAL 30 DAY;

-- -----------------------------------------------------------------------------
-- traces — distributed trace spans (MergeTree)
-- Partitioning: toYYYYMMDD(timestamp); TTL: 30 days (Built Plan line 534)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS omniwatch.traces
(
    trace_id       String,           -- root trace identifier
    span_id        String,           -- this span's identifier
    parent_span_id String,           -- parent span (empty for root spans)
    service_name   String,           -- service that produced the span
    operation      String,           -- operation / route name
    duration_ms    Float64,          -- span duration in milliseconds
    timestamp      DateTime          -- UTC span start time; partition + TTL key
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (trace_id, span_id)
TTL timestamp + INTERVAL 30 DAY;

-- -----------------------------------------------------------------------------
-- anomalies — detected anomaly records (MergeTree)
-- Partitioning: toYYYYMMDD(timestamp); TTL: 90 days
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS omniwatch.anomalies
(
    anomaly_id               String,   -- unique anomaly identifier (UUID)
    entity_id                String,   -- affected entity ID
    entity_type              String,   -- API_NODE, DATABASE_NODE, etc.
    metric_name              String,   -- metric that deviated
    anomaly_score            Float64,  -- 0.0 to 1.0 detection score
    confidence               Float64,  -- 0 to 100 classifier confidence
    deviation_from_baseline  Float64,  -- signed deviation magnitude
    source_type              String,   -- "performance" or "security"
    status                   String,   -- active, resolved, deduplicated, etc.
    timestamp                DateTime  -- UTC detection time; partition + TTL key
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (entity_id, timestamp)
TTL timestamp + INTERVAL 90 DAY;

-- -----------------------------------------------------------------------------
-- incidents — incident records (MergeTree)
-- Partitioning: toYYYYMMDD(created_at); TTL: 365 days (Built Plan line 534)
-- NOTE: this table's time column is `created_at` (no `timestamp` column), so the
--       TTL key references created_at to keep the DDL valid ClickHouse syntax.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS omniwatch.incidents
(
    incident_id           String,     -- unique incident ID (UUID)
    severity              String,     -- P1, P2, P3, P4
    business_impact_score Float64,    -- 0 to 100
    root_cause_entity     String,     -- root cause entity ID
    entity_type           String,     -- root cause entity type
    confidence            Float64,    -- root cause confidence (0 to 100)
    fault_path            String,     -- serialized [root -> ... -> symptom] path
    impacted_services     String,     -- serialized list of impacted services
    status                String,     -- OPEN, RESOLVING, RESOLVED, ESCALATED
    deduplicated_count    UInt32,     -- alerts folded into this incident
    sla_breach_risk       String,     -- HIGH, MEDIUM, LOW
    assigned_to           String,     -- "auto-remediation" or engineer name
    created_at            DateTime    -- UTC creation time; partition + TTL key
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (created_at)
TTL created_at + INTERVAL 365 DAY;

-- -----------------------------------------------------------------------------
-- pending_approvals — human-in-the-loop approval queue (MergeTree)
-- No TTL: approvals must persist until decided (Built Plan line 530).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS omniwatch.pending_approvals
(
    approval_id  String,               -- unique approval ID (UUID)
    incident_id  String,               -- incident this approval belongs to
    action_type  String,               -- remediation action awaiting approval
    entity_id    String,               -- target entity of the action
    proposed_by  String,               -- "auto-remediation" or proposing engine
    status       String,               -- pending, approved, rejected, expired
    created_at   DateTime,             -- UTC creation time
    decided_at   Nullable(DateTime)    -- UTC decision time (NULL while pending)
)
ENGINE = MergeTree
ORDER BY (created_at);

-- -----------------------------------------------------------------------------
-- knowledge_base — resolved incident outcomes for continuous learning (MergeTree)
-- No TTL: knowledge persists as the learning loop's source of truth.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS omniwatch.knowledge_base
(
    kb_id                   String,   -- unique knowledge entry ID (UUID)
    incident_id             String,   -- originating incident ID
    root_cause_entity       String,   -- resolved root cause entity
    root_cause_entity_type  String,   -- root cause entity type
    resolution_summary      String,   -- how the incident was resolved
    actions_taken           String,   -- serialized list of remediation actions
    outcome                 String,   -- success/failure outcome of resolution
    created_at              DateTime  -- UTC entry creation time
)
ENGINE = MergeTree
ORDER BY (created_at);
