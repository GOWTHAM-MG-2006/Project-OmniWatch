-- ============================================================================
-- OmniWatch — ClickHouse Schema
-- Component: schema.sql
-- Phase: 3
-- Purpose: All ClickHouse table definitions for OmniWatch storage layer
-- Database: omniwatch
-- ============================================================================

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS omniwatch;

-- ============================================================================
-- Table: metrics
-- Stores all time-series metrics from simulators and real sources
-- Primary query: By entity_id + time range
-- ============================================================================
CREATE TABLE IF NOT EXISTS omniwatch.metrics
(
    timestamp DateTime64(3, 'UTC'),
    entity_id String,
    entity_type String,
    metric_name String,
    metric_value Float64,
    labels Map(String, String),
    source String DEFAULT 'simulation'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (entity_id, metric_name, timestamp);

-- ============================================================================
-- Table: logs
-- Stores all log events from applications and infrastructure
-- Primary query: By entity_id + log_level
-- ============================================================================
CREATE TABLE IF NOT EXISTS omniwatch.logs
(
    timestamp DateTime64(3, 'UTC'),
    entity_id String,
    entity_type String,
    log_level String,
    message String,
    labels Map(String, String),
    source String DEFAULT 'simulation'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (entity_id, log_level, timestamp);

-- ============================================================================
-- Table: anomalies
-- Stores detected anomaly records from predictive intelligence layer
-- Primary query: By status + timestamp
-- ============================================================================
CREATE TABLE IF NOT EXISTS omniwatch.anomalies
(
    anomaly_id UUID DEFAULT generateUUIDv4(),
    timestamp DateTime64(3, 'UTC'),
    entity_id String,
    anomaly_score Float64,
    confidence Float64,
    metric_name String,
    anomaly_type String,
    status String DEFAULT 'active'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (status, entity_id, timestamp);

-- ============================================================================
-- Table: incidents
-- Stores full incident records from prioritization engine
-- Primary query: By severity + status
-- ============================================================================
CREATE TABLE IF NOT EXISTS omniwatch.incidents
(
    incident_id UUID DEFAULT generateUUIDv4(),
    created_at DateTime64(3, 'UTC'),
    severity String,
    business_impact_score Float64,
    root_cause_entity String,
    status String DEFAULT 'OPEN',
    resolution_time Nullable(DateTime64(3, 'UTC')),
    auto_resolved Boolean DEFAULT false
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (severity, status, created_at);

-- ============================================================================
-- Table: pending_approvals
-- Stores incidents awaiting human approval
-- Primary query: By status = 'pending'
-- ============================================================================
CREATE TABLE IF NOT EXISTS omniwatch.pending_approvals
(
    approval_id UUID DEFAULT generateUUIDv4(),
    incident_id UUID,
    created_at DateTime64(3, 'UTC'),
    status String DEFAULT 'pending',
    assigned_to String DEFAULT 'unassigned',
    action_type String,
    entity_id String,
    confidence Float64,
    expires_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (status, created_at);

-- ============================================================================
-- Table: knowledge_base
-- Stores resolved incident outcomes for continuous learning
-- Primary query: By root_cause_entity_type
-- ============================================================================
CREATE TABLE IF NOT EXISTS omniwatch.knowledge_base
(
    entry_id UUID DEFAULT generateUUIDv4(),
    incident_id UUID,
    root_cause_entity String,
    root_cause_type String,
    resolution_action String,
    resolution_success Boolean,
    resolution_time_seconds Float64,
    created_at DateTime64(3, 'UTC'),
    metadata Map(String, String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (root_cause_type, created_at);
