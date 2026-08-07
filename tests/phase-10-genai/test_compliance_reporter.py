"""
OmniWatch — Generative AI Layer
Component: Compliance Reporter Tests
Phase: 10
Purpose: Unit tests for compliance_reporter.py — mocks ClickHouse and MinIO,
         validates report generation for all 3 report types, health endpoint,
         and CLI behavior.
Inputs: None (uses fixtures from conftest.py)
Outputs: Test results via pytest
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from genai.compliance_reporter import (
    ComplianceReporter,
    REPORT_TYPE_INCIDENT_RESPONSE,
    REPORT_TYPE_SECURITY_EVENT,
    REPORT_TYPE_SLA_COMPLIANCE,
    REPORT_TYPES,
)
from genai.models import ComplianceReportMeta


class TestComplianceReporterHealth:
    """Tests for ComplianceReporter.health_check."""

    def test_health_check_all_healthy(
        self,
        mock_ch_client: Any,
        mock_minio_client: Any,
    ) -> None:
        """Both ClickHouse and MinIO healthy returns all_healthy=True."""
        reporter = ComplianceReporter()
        reporter._ch_client = mock_ch_client
        reporter._minio_client = mock_minio_client

        result = reporter.health_check()

        assert result["clickhouse"] is True
        assert result["minio"] is True
        assert result["all_healthy"] is True

    def test_health_check_ch_down(
        self,
        mock_minio_client: Any,
    ) -> None:
        """ClickHouse down returns clickhouse=False."""
        reporter = ComplianceReporter()
        bad_ch = MagicMock()
        bad_ch.command.side_effect = ConnectionError("refused")
        reporter._ch_client = bad_ch
        reporter._minio_client = mock_minio_client

        result = reporter.health_check()

        assert result["clickhouse"] is False
        assert result["minio"] is True
        assert result["all_healthy"] is False

    def test_health_check_minio_down(
        self,
        mock_ch_client: Any,
    ) -> None:
        """MinIO down returns minio=False."""
        reporter = ComplianceReporter()
        reporter._ch_client = mock_ch_client
        bad_minio = MagicMock()
        bad_minio.bucket_exists.side_effect = ConnectionError("refused")
        reporter._minio_client = bad_minio

        result = reporter.health_check()

        assert result["clickhouse"] is True
        assert result["minio"] is False
        assert result["all_healthy"] is False


class TestComplianceReporterGetIncident:
    """Tests for ComplianceReporter.get_incident."""

    def test_get_incident_found(
        self,
        mock_ch_client: Any,
        sample_incident: dict[str, Any],
    ) -> None:
        """Returns incident dict when found."""
        mock_ch_client.incidents = [sample_incident]
        reporter = ComplianceReporter()
        reporter._ch_client = mock_ch_client

        result = reporter.get_incident("test-incident-001")

        assert result is not None
        assert result["incident_id"] == "test-incident-001"

    def test_get_incident_not_found(
        self,
        mock_ch_client: Any,
    ) -> None:
        """Returns None when incident not found."""
        mock_ch_client.incidents = []
        reporter = ComplianceReporter()
        reporter._ch_client = mock_ch_client

        result = reporter.get_incident("nonexistent-id")

        assert result is None


class TestComplianceReporterGenerateReport:
    """Tests for ComplianceReporter.generate_report."""

    def test_incident_response_report(
        self,
        mock_ch_client: Any,
        mock_minio_client: Any,
        sample_incident: dict[str, Any],
    ) -> None:
        """Generates Incident Response Evidence report."""
        mock_ch_client.incidents = [sample_incident]
        reporter = ComplianceReporter()
        reporter._ch_client = mock_ch_client
        reporter._minio_client = mock_minio_client

        meta = reporter.generate_report("test-incident-001", REPORT_TYPE_INCIDENT_RESPONSE)

        assert isinstance(meta, ComplianceReportMeta)
        assert meta.incident_id == "test-incident-001"
        assert meta.report_type == REPORT_TYPE_INCIDENT_RESPONSE
        assert meta.bucket == "omniwatch-audit-logs"
        # Verify MinIO was called
        assert len(mock_minio_client.put_calls) == 1
        assert "incident-response-evidence" in mock_minio_client.put_calls[0]["object_name"]

    def test_security_event_report(
        self,
        mock_ch_client: Any,
        mock_minio_client: Any,
        sample_incident: dict[str, Any],
    ) -> None:
        """Generates Security Event Summary report."""
        mock_ch_client.incidents = [sample_incident]
        reporter = ComplianceReporter()
        reporter._ch_client = mock_ch_client
        reporter._minio_client = mock_minio_client

        meta = reporter.generate_report("test-incident-001", REPORT_TYPE_SECURITY_EVENT)

        assert isinstance(meta, ComplianceReportMeta)
        assert meta.report_type == REPORT_TYPE_SECURITY_EVENT
        assert "security-event-summary" in mock_minio_client.put_calls[0]["object_name"]

    def test_sla_compliance_report(
        self,
        mock_ch_client: Any,
        mock_minio_client: Any,
        sample_incident: dict[str, Any],
    ) -> None:
        """Generates SLA Compliance Report."""
        mock_ch_client.incidents = [sample_incident]
        reporter = ComplianceReporter()
        reporter._ch_client = mock_ch_client
        reporter._minio_client = mock_minio_client

        meta = reporter.generate_report("test-incident-001", REPORT_TYPE_SLA_COMPLIANCE)

        assert isinstance(meta, ComplianceReportMeta)
        assert meta.report_type == REPORT_TYPE_SLA_COMPLIANCE
        assert "sla-compliance-report" in mock_minio_client.put_calls[0]["object_name"]

    def test_invalid_report_type_raises(
        self,
        mock_ch_client: Any,
        mock_minio_client: Any,
        sample_incident: dict[str, Any],
    ) -> None:
        """Invalid report type raises ValueError."""
        mock_ch_client.incidents = [sample_incident]
        reporter = ComplianceReporter()
        reporter._ch_client = mock_ch_client
        reporter._minio_client = mock_minio_client

        with pytest.raises(ValueError, match="report_type must be one of"):
            reporter.generate_report("test-incident-001", "Invalid Type")

    def test_missing_incident_raises(
        self,
        mock_ch_client: Any,
        mock_minio_client: Any,
    ) -> None:
        """Missing incident raises ValueError."""
        mock_ch_client.incidents = []
        reporter = ComplianceReporter()
        reporter._ch_client = mock_ch_client
        reporter._minio_client = mock_minio_client

        with pytest.raises(ValueError, match="not found"):
            reporter.generate_report("nonexistent-id")


class TestComplianceReportContent:
    """Tests for the actual Markdown content of generated reports."""

    def test_incident_response_contains_headings(
        self,
        mock_ch_client: Any,
        mock_minio_client: Any,
        sample_incident: dict[str, Any],
    ) -> None:
        """Incident Response report contains expected Markdown headings."""
        mock_ch_client.incidents = [sample_incident]
        reporter = ComplianceReporter()
        reporter._ch_client = mock_ch_client
        reporter._minio_client = mock_minio_client

        reporter.generate_report("test-incident-001", REPORT_TYPE_INCIDENT_RESPONSE)

        # Get the written content
        written = list(mock_minio_client.objects.values())[0].decode("utf-8")
        assert "# Incident Response Evidence" in written
        assert "## 1. Incident Summary" in written
        assert "## 2. Fault Path" in written
        assert "## 3. Impacted Services" in written
        assert "## 4. Audit Trail" in written
        assert "## 5. Compliance Checklist" in written

    def test_security_event_contains_headings(
        self,
        mock_ch_client: Any,
        mock_minio_client: Any,
        sample_incident: dict[str, Any],
    ) -> None:
        """Security Event report contains expected Markdown headings."""
        mock_ch_client.incidents = [sample_incident]
        reporter = ComplianceReporter()
        reporter._ch_client = mock_ch_client
        reporter._minio_client = mock_minio_client

        reporter.generate_report("test-incident-001", REPORT_TYPE_SECURITY_EVENT)

        written = list(mock_minio_client.objects.values())[0].decode("utf-8")
        assert "# Security Event Summary" in written
        assert "## 1. Security Event Overview" in written
        assert "## 2. Threat Analysis" in written
        assert "## 3. Evidence Collected" in written
        assert "## 4. Response Actions" in written
        assert "## 5. Compliance Status" in written

    def test_sla_compliance_contains_headings(
        self,
        mock_ch_client: Any,
        mock_minio_client: Any,
        sample_incident: dict[str, Any],
    ) -> None:
        """SLA Compliance report contains expected Markdown headings."""
        mock_ch_client.incidents = [sample_incident]
        reporter = ComplianceReporter()
        reporter._ch_client = mock_ch_client
        reporter._minio_client = mock_minio_client

        reporter.generate_report("test-incident-001", REPORT_TYPE_SLA_COMPLIANCE)

        written = list(mock_minio_client.objects.values())[0].decode("utf-8")
        assert "# SLA Compliance Report" in written
        assert "## 1. SLA Status" in written
        assert "## 2. Incident Timeline" in written
        assert "## 3. Impact Assessment" in written
        assert "## 4. Remediation Status" in written
        assert "## 5. Audit Evidence" in written
        assert "## 6. Compliance Checklist" in written

    def test_report_contains_incident_id(
        self,
        mock_ch_client: Any,
        mock_minio_client: Any,
        sample_incident: dict[str, Any],
    ) -> None:
        """All reports contain the incident ID."""
        mock_ch_client.incidents = [sample_incident]
        reporter = ComplianceReporter()
        reporter._ch_client = mock_ch_client
        reporter._minio_client = mock_minio_client

        reporter.generate_report("test-incident-001", REPORT_TYPE_INCIDENT_RESPONSE)

        written = list(mock_minio_client.objects.values())[0].decode("utf-8")
        assert "test-incident-001" in written

    def test_report_contains_framework(
        self,
        mock_ch_client: Any,
        mock_minio_client: Any,
        sample_incident: dict[str, Any],
    ) -> None:
        """Reports contain the compliance framework name."""
        mock_ch_client.incidents = [sample_incident]
        reporter = ComplianceReporter()
        reporter._ch_client = mock_ch_client
        reporter._minio_client = mock_minio_client

        reporter.generate_report("test-incident-001", REPORT_TYPE_INCIDENT_RESPONSE)

        written = list(mock_minio_client.objects.values())[0].decode("utf-8")
        assert "SOC2" in written or "ISO27001" in written


class TestReportTypes:
    """Tests for report type constants."""

    def test_all_report_types_defined(self) -> None:
        """All three report types are defined."""
        assert len(REPORT_TYPES) == 3
        assert REPORT_TYPE_INCIDENT_RESPONSE in REPORT_TYPES
        assert REPORT_TYPE_SECURITY_EVENT in REPORT_TYPES
        assert REPORT_TYPE_SLA_COMPLIANCE in REPORT_TYPES

    def test_report_type_names_exact(self) -> None:
        """Report type names match AGENTS.md GAP2 spec exactly."""
        assert REPORT_TYPE_INCIDENT_RESPONSE == "Incident Response Evidence"
        assert REPORT_TYPE_SECURITY_EVENT == "Security Event Summary"
        assert REPORT_TYPE_SLA_COMPLIANCE == "SLA Compliance Report"
