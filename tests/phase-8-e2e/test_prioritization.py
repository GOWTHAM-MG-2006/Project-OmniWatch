"""
OmniWatch — Phase 8 E2E Tests

End-to-end tests for the Incident Prioritization Engine.
Covers severity classification, impact scoring, SLA risk, deduplication,
assignment routing, and the full pipeline.

No external dependencies (Kafka, MinIO, ClickHouse) required — the producer
is mocked and all internal components use real implementations.
"""

from __future__ import annotations

import time

from prioritization.models import IncidentRecord

# ---------------------------------------------------------------------------
# Severity Classification Tests
# ---------------------------------------------------------------------------


class TestP1DatabaseIncident:
    """P1: DATABASE entity + confidence >= 85 + impacted >= 3."""

    def test_p1_severity_assigned(self, engine, make_root_cause) -> None:
        rc = make_root_cause()
        incident = engine.process_root_cause(rc)

        assert incident.severity == "P1"
        published = engine._producer._published
        assert len(published) == 1
        assert isinstance(published[0], IncidentRecord)


class TestP1RequiresAllConditions:
    """P1 requires ALL three conditions: DATABASE entity, conf >= 85, impacted >= 3.

    If entity_type does not contain 'DATABASE', even high confidence should
    NOT produce P1 — it should fall through to P2.
    """

    def test_non_database_high_confidence_falls_to_p2(
        self, engine, make_root_cause
    ) -> None:
        rc = make_root_cause(
            entity_type="API_NODE",
            confidence=0.95,
            anomaly_score=0.95,
            impacted_count=5,
        )
        incident = engine.process_root_cause(rc)

        assert incident.severity != "P1"
        assert incident.severity == "P2"


class TestP1DatabaseLowImpact:
    """P1 also requires impacted_count >= 3.  If impacted_count < 3,
    falls through to P2."""

    def test_database_high_conf_low_impact_is_p2(self, engine, make_root_cause) -> None:
        rc = make_root_cause(
            entity_type="DATABASE_NODE",
            confidence=0.90,
            anomaly_score=0.90,
            impacted_count=1,
        )
        incident = engine.process_root_cause(rc)

        assert incident.severity != "P1"
        assert incident.severity == "P2"


class TestP2HighAnomaly:
    """P2: confidence >= 70 OR anomaly_score >= 0.7."""

    def test_p2_by_anomaly_score(self, engine, make_root_cause) -> None:
        rc = make_root_cause(
            entity_type="API_NODE",
            confidence=0.50,
            anomaly_score=0.80,
            impacted_count=1,
        )
        incident = engine.process_root_cause(rc)

        assert incident.severity == "P2"


class TestP3ModerateAnomaly:
    """P3: confidence >= 40 OR anomaly_score >= 0.4 (but not P2)."""

    def test_p3_by_confidence(self, engine, make_root_cause) -> None:
        rc = make_root_cause(
            entity_type="API_NODE",
            confidence=0.60,
            anomaly_score=0.30,
            impacted_count=1,
        )
        incident = engine.process_root_cause(rc)

        assert incident.severity == "P3"


class TestP4CatchAll:
    """P4: everything else (below all thresholds)."""

    def test_p4_low_confidence_anomaly(self, engine, make_root_cause) -> None:
        rc = make_root_cause(
            entity_type="API_NODE",
            confidence=0.20,
            anomaly_score=0.10,
            impacted_count=1,
        )
        incident = engine.process_root_cause(rc)

        assert incident.severity == "P4"


# ---------------------------------------------------------------------------
# Impact Scoring Tests
# ---------------------------------------------------------------------------


class TestImpactScoreCalculation:
    """Verify impact score components and clamping."""

    def test_impact_score_components(self, engine, make_root_cause) -> None:
        rc = make_root_cause(
            entity_type="API_NODE",
            confidence=0.50,
            anomaly_score=0.60,
            impacted_count=5,
            fault_path=["svc-a", "svc-b", "svc-c", "svc-d", "svc-e", "svc-f"],
            evidence={"log_snippets": ["e1", "e2", "e3", "e4", "e5"]},
        )
        incident = engine.process_root_cause(rc)

        assert incident.severity == "P3"

        # Impact = anomaly(0.6*40=24) + impacted(min(25, 5*5)=25) +
        #          conf(min(15, 50/100*15)=7.5) + P3 bonus(10) +
        #          fault_depth(min(10, 6*2)=10) + evidence(min(5, 5*1)=5)
        #        = 24 + 25 + 7.5 + 10 + 10 + 5 = 81.5
        assert 80.0 <= incident.business_impact_score <= 82.0

    def test_impact_score_clamped_to_100(self, engine, make_root_cause) -> None:
        rc = make_root_cause(
            entity_type="DATABASE_NODE",
            confidence=1.0,
            anomaly_score=1.0,
            impacted_count=10,
            fault_path=["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
            evidence={"log_snippets": ["a", "b", "c", "d", "e", "f", "g"]},
        )
        incident = engine.process_root_cause(rc)

        assert incident.business_impact_score <= 100.0


# ---------------------------------------------------------------------------
# SLA Risk Tests
# ---------------------------------------------------------------------------


class TestSLARiskCalculation:
    """P1 → HIGH, P2 → MEDIUM, P3/P4 → LOW, with impact elevation."""

    def test_p1_high_sla_risk(self, engine, make_root_cause) -> None:
        rc = make_root_cause()
        incident = engine.process_root_cause(rc)

        assert incident.severity == "P1"
        assert incident.sla_breach_risk == "HIGH"

    def test_p2_medium_sla_risk(self, engine, make_root_cause) -> None:
        rc = make_root_cause(
            entity_type="API_NODE",
            confidence=0.80,
            anomaly_score=0.80,
            impacted_count=1,
        )
        incident = engine.process_root_cause(rc)

        assert incident.severity == "P2"
        assert incident.sla_breach_risk == "MEDIUM"

    def test_sla_impact_elevation_p3_to_high(self, engine, make_root_cause) -> None:
        """P3 base risk is LOW, but impact >= 80 should elevate to HIGH."""
        rc = make_root_cause(
            entity_type="API_NODE",
            confidence=0.50,
            anomaly_score=0.60,
            impacted_count=5,
            fault_path=["a", "b", "c", "d", "e", "f"],
            evidence={"log_snippets": ["e1", "e2", "e3", "e4", "e5"]},
        )
        incident = engine.process_root_cause(rc)

        assert incident.severity == "P3"
        assert incident.business_impact_score >= 80.0
        assert incident.sla_breach_risk == "HIGH"


# ---------------------------------------------------------------------------
# Assignment Tests
# ---------------------------------------------------------------------------


class TestAssignmentRouting:
    """P1 + confidence_normalized >= 85 → auto-remediation; else oncall-engineer."""

    def test_p1_auto_remediation(self, engine, make_root_cause) -> None:
        rc = make_root_cause(
            entity_type="DATABASE_NODE",
            confidence=0.90,
            anomaly_score=0.90,
            impacted_count=3,
        )
        incident = engine.process_root_cause(rc)

        assert incident.severity == "P1"
        assert incident.assigned_to == "auto-remediation"

    def test_p2_oncall_engineer(self, engine, make_root_cause) -> None:
        rc = make_root_cause(
            entity_type="API_NODE",
            confidence=0.80,
            anomaly_score=0.80,
            impacted_count=1,
        )
        incident = engine.process_root_cause(rc)

        assert incident.severity == "P2"
        assert incident.assigned_to == "oncall-engineer"

    def test_p1_boundary_confidence_auto_remediation(
        self, engine, make_root_cause
    ) -> None:
        """P1 at exactly confidence=0.85 boundary → auto-remediation (boundary test)."""
        rc = make_root_cause(
            entity_type="DATABASE_NODE",
            confidence=0.85,  # exactly 85.0 on 0..100 scale
            anomaly_score=0.85,
            impacted_count=3,
        )
        incident = engine.process_root_cause(rc)

        assert incident.severity == "P1"
        assert incident.assigned_to == "auto-remediation"

    def test_p3_oncall_engineer(self, engine, make_root_cause) -> None:
        """P3 severity always goes to oncall-engineer (only P1+conf>=85 auto-remediates)."""
        rc = make_root_cause(
            entity_type="API_NODE",
            confidence=0.50,
            anomaly_score=0.30,
            impacted_count=1,
        )
        incident = engine.process_root_cause(rc)

        assert incident.severity == "P3"
        assert incident.assigned_to == "oncall-engineer"


# ---------------------------------------------------------------------------
# Deduplication Tests
# ---------------------------------------------------------------------------


class TestDeduplication:
    """GAP 3: Deduplication engine groups same-entity incidents within TTL."""

    def test_dedup_same_entity_increments_count(self, engine, make_root_cause) -> None:
        """Same root_cause_entity → 10 incidents merged, count = 10."""
        rc1 = make_root_cause(
            root_cause_entity="postgresql-database",
            confidence=0.85,
        )

        inc1 = engine.process_root_cause(rc1)
        assert inc1.deduplicated_count == 1

        inc = inc1
        for i in range(2, 11):
            rc = make_root_cause(
                incident_id=f"rc-test-{i:03d}",
                root_cause_entity="postgresql-database",
                confidence=0.90,
            )
            inc = engine.process_root_cause(rc)

        published = engine._producer._published
        assert len(published) == 10

        assert inc.deduplicated_count == 10
        assert inc.incident_id == inc1.incident_id

    def test_dedup_different_entities_no_merge(self, engine, make_root_cause) -> None:
        """Different root_cause_entity → separate incidents, count = 1 each."""
        rc1 = make_root_cause(
            root_cause_entity="postgresql-database",
        )
        rc2 = make_root_cause(
            incident_id="rc-test-002",
            root_cause_entity="redis-cache",
        )

        inc1 = engine.process_root_cause(rc1)
        inc2 = engine.process_root_cause(rc2)

        assert inc1.deduplicated_count == 1
        assert inc2.deduplicated_count == 1
        assert inc1.incident_id != inc2.incident_id

    def test_dedup_ttl_expiry_creates_new(
        self, engine_with_short_ttl, make_root_cause
    ) -> None:
        """After TTL expires, same entity → new incident (count = 1)."""
        rc1 = make_root_cause(
            root_cause_entity="db-expiring",
            confidence=0.85,
        )
        rc2 = make_root_cause(
            incident_id="rc-test-002",
            root_cause_entity="db-expiring",
            confidence=0.90,
        )

        inc1 = engine_with_short_ttl.process_root_cause(rc1)
        assert inc1.deduplicated_count == 1

        time.sleep(1.5)

        inc2 = engine_with_short_ttl.process_root_cause(rc2)

        assert inc2.deduplicated_count == 1
        assert inc2.incident_id != inc1.incident_id

    def test_dedup_disabled_passes_through(
        self, engine_dedup_disabled, make_root_cause
    ) -> None:
        """When dedup is disabled, every incident gets count = 1."""
        rc1 = make_root_cause(
            root_cause_entity="db-disabled",
            confidence=0.85,
        )
        rc2 = make_root_cause(
            incident_id="rc-test-002",
            root_cause_entity="db-disabled",
            confidence=0.90,
        )

        inc1 = engine_dedup_disabled.process_root_cause(rc1)
        inc2 = engine_dedup_disabled.process_root_cause(rc2)

        assert inc1.deduplicated_count == 1
        assert inc2.deduplicated_count == 1
        assert inc1.incident_id != inc2.incident_id


# ---------------------------------------------------------------------------
# Full Pipeline End-to-End Test
# ---------------------------------------------------------------------------


class TestFullPipeline:
    """End-to-end: RootCauseObject → factory → dedup → producer."""

    def test_normal_operation(self, engine, make_root_cause) -> None:
        """Normal (benign) root cause → P4, low impact, no anomalies."""
        rc = make_root_cause(
            entity_type="API_NODE",
            confidence=0.10,
            anomaly_score=0.05,
            impacted_count=1,
            fault_path=["api-gateway"],
            evidence={"log_snippets": []},
        )

        incident = engine.process_root_cause(rc)

        assert isinstance(incident, IncidentRecord)
        assert incident.severity == "P4"
        assert incident.sla_breach_risk == "LOW"
        assert incident.assigned_to == "oncall-engineer"
        assert incident.deduplicated_count == 1
        assert incident.status == "OPEN"
        assert 0.0 <= incident.business_impact_score <= 100.0

    def test_full_pipeline_produces_incident_record(
        self, engine, make_root_cause
    ) -> None:
        rc = make_root_cause()

        incident = engine.process_root_cause(rc)

        assert isinstance(incident, IncidentRecord)
        assert incident.status == "OPEN"
        assert incident.root_cause.root_cause_entity == "postgresql-database"
        assert incident.root_cause.entity_type == "DATABASE_NODE"
        assert incident.severity == "P1"
        assert 0.0 <= incident.business_impact_score <= 100.0
        assert incident.sla_breach_risk in ("HIGH", "MEDIUM", "LOW")
        assert incident.assigned_to in ("auto-remediation", "oncall-engineer")
        assert incident.deduplicated_count >= 1
        assert len(incident.incident_id) > 0
        assert len(incident.created_at) > 0

    def test_pipeline_stats_tracked(self, engine, make_root_cause) -> None:
        """Engine should track processed/published/deduplicated counters."""
        rc = make_root_cause()
        engine.process_root_cause(rc)

        stats = engine.get_stats()
        assert stats["processed"] == 1
        assert stats["published"] == 1
        assert stats["deduplicated"] == 0

    def test_pipeline_stats_after_dedup(self, engine, make_root_cause) -> None:
        """After dedup, the deduplicated counter should increment."""
        rc1 = make_root_cause(root_cause_entity="dup-db", confidence=0.85)
        rc2 = make_root_cause(
            incident_id="rc-2", root_cause_entity="dup-db", confidence=0.90
        )
        engine.process_root_cause(rc1)
        engine.process_root_cause(rc2)

        stats = engine.get_stats()
        assert stats["processed"] == 2
        assert stats["published"] == 2
        assert stats["deduplicated"] == 1
