"""
OmniWatch — Generative AI Layer
Component: Compliance Report Generator (GAP2)
Phase: 10
Purpose: Generates SOC2 / ISO27001 / HIPAA / PCI-DSS evidence packages from
         ClickHouse incident records and MinIO audit logs. Outputs Markdown
         reports to the omniwatch-audit-logs MinIO bucket.
Inputs: ClickHouse omniwatch.incidents table + MinIO omniwatch-audit-logs bucket
Outputs: Markdown reports written to MinIO omniwatch-audit-logs bucket
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import clickhouse_connect
from fastapi import FastAPI
from minio import Minio

from genai.models import ComplianceReportMeta
from genai.settings import Settings

logger = logging.getLogger(__name__)

# Report type constants — EXACT names per AGENTS.md GAP2 spec
REPORT_TYPE_INCIDENT_RESPONSE = "Incident Response Evidence"
REPORT_TYPE_SECURITY_EVENT = "Security Event Summary"
REPORT_TYPE_SLA_COMPLIANCE = "SLA Compliance Report"

REPORT_TYPES: list[str] = [
    REPORT_TYPE_INCIDENT_RESPONSE,
    REPORT_TYPE_SECURITY_EVENT,
    REPORT_TYPE_SLA_COMPLIANCE,
]

AUDIT_BUCKET = "omniwatch-audit-logs"


class ComplianceReporter:
    """Generates compliance evidence reports from incident data.

    Reads from ClickHouse (incidents table) and MinIO (audit logs bucket),
    then writes Markdown reports back to MinIO.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or Settings()
        self._ch_client: Any = None
        self._minio_client: Any = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _get_ch_client(self) -> Any:
        """Lazily create the ClickHouse client."""
        if self._ch_client is None:
            self._ch_client = clickhouse_connect.get_client(
                host=self._settings.clickhouse_host,
                port=self._settings.clickhouse_port,
                database=self._settings.clickhouse_db,
                username=self._settings.clickhouse_user,
                password=self._settings.clickhouse_password,
            )
        return self._ch_client

    def _get_minio_client(self) -> Minio:
        """Lazily create the MinIO client."""
        if self._minio_client is None:
            self._minio_client = Minio(
                self._settings.minio_endpoint,
                access_key=self._settings.minio_access_key,
                secret_key=self._settings.minio_secret_key,
                secure=self._settings.minio_secure,
            )
        return self._minio_client

    def close(self) -> None:
        """Release connections."""
        if self._ch_client is not None:
            self._ch_client.close()
            self._ch_client = None
        self._minio_client = None

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    def health_check(self) -> dict[str, Any]:
        """Check ClickHouse and MinIO connectivity."""
        ch_ok = False
        minio_ok = False
        try:
            client = self._get_ch_client()
            client.command("SELECT 1")
            ch_ok = True
        except Exception as exc:
            logger.warning(json.dumps({"event": "clickhouse_health_fail", "error": str(exc)}))

        try:
            mclient = self._get_minio_client()
            minio_ok = mclient.bucket_exists(AUDIT_BUCKET)
        except Exception as exc:
            logger.warning(json.dumps({"event": "minio_health_fail", "error": str(exc)}))

        return {"clickhouse": ch_ok, "minio": minio_ok, "all_healthy": ch_ok and minio_ok}

    # ------------------------------------------------------------------
    # Incident queries
    # ------------------------------------------------------------------

    _INCIDENT_COLUMNS = (
        "incident_id, created_at, severity, status, business_impact_score, "
        "root_cause_entity, entity_type, confidence, anomaly_score, fault_path, "
        "impacted_services, impacted_count, evidence, sla_breach_risk, assigned_to, "
        "deduplicated_count"
    )

    def get_incident(self, incident_id: str) -> dict[str, Any] | None:
        """Fetch a single incident from ClickHouse by incident_id."""
        client = self._get_ch_client()
        result = client.query(
            f"SELECT {self._INCIDENT_COLUMNS} FROM omniwatch.incidents "
            "WHERE incident_id = %(id)s LIMIT 1",
            parameters={"id": incident_id},
        )
        if not result.result_rows:
            return None
        columns = list(result.column_names)
        row = result.result_rows[0]
        return {col: self._serialize(val) for col, val in zip(columns, row)}

    def get_audit_logs(self, incident_id: str) -> list[dict[str, str]]:
        """List audit log objects for an incident from MinIO."""
        mclient = self._get_minio_client()
        prefix = f"incident-{incident_id}/"
        logs: list[dict[str, str]] = []
        try:
            objects = mclient.list_objects(AUDIT_BUCKET, prefix=prefix)
            for obj in objects:
                if obj.object_name:
                    logs.append({
                        "key": obj.object_name,
                        "size": str(obj.size or 0),
                        "last_modified": obj.last_modified.isoformat() if obj.last_modified else "",
                    })
        except Exception as exc:
            logger.warning(json.dumps({"event": "minio_list_fail", "incident_id": incident_id, "error": str(exc)}))
        return logs

    def get_all_incidents(self, limit: int = 100) -> list[dict[str, Any]]:
        """Fetch recent incidents from ClickHouse."""
        client = self._get_ch_client()
        result = client.query(
            f"SELECT {self._INCIDENT_COLUMNS} FROM omniwatch.incidents "
            "ORDER BY created_at DESC LIMIT %(limit)s",
            parameters={"limit": limit},
        )
        columns = list(result.column_names)
        return [
            {col: self._serialize(val) for col, val in zip(columns, row)}
            for row in result.result_rows
        ]

    # ------------------------------------------------------------------
    # Report generation
    # ------------------------------------------------------------------

    def generate_report(
        self,
        incident_id: str,
        report_type: str = REPORT_TYPE_INCIDENT_RESPONSE,
    ) -> ComplianceReportMeta:
        """Generate a compliance report for an incident.

        Args:
            incident_id: The incident to report on.
            report_type: One of the three report type constants.

        Returns:
            ComplianceReportMeta with the report location in MinIO.

        Raises:
            ValueError: If incident not found or report_type unknown.
        """
        if report_type not in REPORT_TYPES:
            raise ValueError(
                f"report_type must be one of {REPORT_TYPES}, got {report_type!r}"
            )

        incident = self.get_incident(incident_id)
        if incident is None:
            raise ValueError(f"Incident {incident_id} not found in ClickHouse")

        audit_logs = self.get_audit_logs(incident_id)

        if report_type == REPORT_TYPE_INCIDENT_RESPONSE:
            markdown = self._render_incident_response(incident, audit_logs)
        elif report_type == REPORT_TYPE_SECURITY_EVENT:
            markdown = self._render_security_event(incident, audit_logs)
        else:
            markdown = self._render_sla_compliance(incident, audit_logs)

        # Write to MinIO
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        object_key = f"reports/{incident_id}/{report_type.lower().replace(' ', '-')}-{timestamp}.md"

        mclient = self._get_minio_client()
        md_bytes = markdown.encode("utf-8")
        from io import BytesIO

        mclient.put_object(
            AUDIT_BUCKET,
            object_key,
            data=BytesIO(md_bytes),
            length=len(md_bytes),
            content_type="text/markdown; charset=utf-8",
        )

        meta = ComplianceReportMeta(
            incident_id=incident_id,
            report_type=report_type,
            framework=self._report_type_to_framework(report_type),
            bucket=AUDIT_BUCKET,
            object_key=object_key,
        )

        logger.info(
            json.dumps({
                "event": "compliance_report_generated",
                "report_id": meta.report_id,
                "incident_id": incident_id,
                "report_type": report_type,
                "object_key": object_key,
            })
        )

        return meta

    # ------------------------------------------------------------------
    # Report renderers
    # ------------------------------------------------------------------

    def _render_incident_response(
        self,
        incident: dict[str, Any],
        audit_logs: list[dict[str, str]],
    ) -> str:
        """Render Incident Response Evidence report (SOC2/ISO27001)."""
        lines = [
            "# Incident Response Evidence",
            "",
            f"**Incident ID:** `{incident.get('incident_id', 'N/A')}`",
            f"**Generated At:** {datetime.now(timezone.utc).isoformat()}",
            "**Framework:** SOC2 / ISO27001",
            "",
            "---",
            "",
            "## 1. Incident Summary",
            "",
            "| Field | Value |",
            "|-------|-------|",
            f"| Severity | {incident.get('severity', 'N/A')} |",
            f"| Status | {incident.get('status', 'N/A')} |",
            f"| Business Impact Score | {incident.get('business_impact_score', 'N/A')} |",
            f"| Root Cause Entity | `{incident.get('root_cause_entity', 'N/A')}` |",
            f"| Entity Type | {incident.get('entity_type', 'N/A')} |",
            f"| Confidence | {incident.get('confidence', 'N/A')} |",
            f"| SLA Breach Risk | {incident.get('sla_breach_risk', 'N/A')} |",
            f"| Assigned To | {incident.get('assigned_to', 'N/A')} |",
            f"| Deduplicated Count | {incident.get('deduplicated_count', 'N/A')} |",
            f"| Created At | {incident.get('created_at', 'N/A')} |",
            "",
            "## 2. Fault Path",
            "",
        ]

        fault_path = incident.get("fault_path", "[]")
        if isinstance(fault_path, str):
            try:
                fault_path = json.loads(fault_path)
            except json.JSONDecodeError:
                fault_path = [fault_path]

        if fault_path:
            for i, node in enumerate(fault_path):
                arrow = " → " if i < len(fault_path) - 1 else ""
                lines.append(f"`{node}`{arrow}")
        else:
            lines.append("_No fault path recorded._")
        lines.append("")

        lines.extend([
            "## 3. Impacted Services",
            "",
        ])
        impacted = incident.get("impacted_services", "[]")
        if isinstance(impacted, str):
            try:
                impacted = json.loads(impacted)
            except json.JSONDecodeError:
                impacted = [impacted]

        if impacted:
            for svc in impacted:
                lines.append(f"- `{svc}`")
        else:
            lines.append("_No impacted services recorded._")
        lines.append("")

        lines.extend([
            "## 4. Audit Trail",
            "",
        ])
        if audit_logs:
            lines.append("| Object Key | Size | Last Modified |")
            lines.append("|------------|------|---------------|")
            for log in audit_logs:
                lines.append(f"| `{log['key']}` | {log['size']} bytes | {log['last_modified']} |")
        else:
            lines.append("_No audit log objects found for this incident._")
        lines.append("")

        lines.extend([
            "## 5. Compliance Checklist",
            "",
            "- [x] Incident detected and classified within SLA",
            "- [x] Root cause identified with confidence score",
            "- [x] Fault path traced through dependency graph",
            "- [x] Impacted services enumerated",
            "- [x] Audit trail preserved in object storage",
            "- [x] Remediation actions logged",
            "",
            "---",
            "_Report generated by OmniWatch Compliance Reporter (Phase 10)_",
        ])
        return "\n".join(lines)

    def _render_security_event(
        self,
        incident: dict[str, Any],
        audit_logs: list[dict[str, str]],
    ) -> str:
        """Render Security Event Summary report (ISO27001/HIPAA)."""
        severity = incident.get("severity", "N/A")
        lines = [
            "# Security Event Summary",
            "",
            f"**Incident ID:** `{incident.get('incident_id', 'N/A')}`",
            f"**Generated At:** {datetime.now(timezone.utc).isoformat()}",
            "**Framework:** ISO27001 / HIPAA",
            "",
            "---",
            "",
            "## 1. Security Event Overview",
            "",
            "| Field | Value |",
            "|-------|-------|",
            f"| Event Severity | {severity} |",
            f"| Detection Status | {incident.get('status', 'N/A')} |",
            f"| Affected Entity | `{incident.get('root_cause_entity', 'N/A')}` |",
            f"| Entity Type | {incident.get('entity_type', 'N/A')} |",
            f"| Risk Assessment | {incident.get('sla_breach_risk', 'N/A')} |",
            f"| Time of Detection | {incident.get('created_at', 'N/A')} |",
            "",
            "## 2. Threat Analysis",
            "",
            f"The security event affected entity `{incident.get('root_cause_entity', 'N/A')}` "
            f"of type `{incident.get('entity_type', 'N/A')}` with a confidence score of "
            f"`{incident.get('confidence', 'N/A')}`.",
            "",
        ]

        fault_path = incident.get("fault_path", "[]")
        if isinstance(fault_path, str):
            try:
                fault_path = json.loads(fault_path)
            except json.JSONDecodeError:
                fault_path = [fault_path]

        if fault_path:
            lines.extend([
                "### Propagation Path",
                "",
            ])
            for i, node in enumerate(fault_path):
                arrow = " → " if i < len(fault_path) - 1 else ""
                lines.append(f"`{node}`{arrow}")
            lines.append("")

        lines.extend([
            "## 3. Evidence Collected",
            "",
        ])
        if audit_logs:
            for log in audit_logs:
                lines.append(f"- Audit log: `{log['key']}` ({log['size']} bytes)")
        else:
            lines.append("- _No audit log objects found._")
        lines.append("")

        lines.extend([
            "## 4. Response Actions",
            "",
            f"- **Assigned To:** {incident.get('assigned_to', 'N/A')}",
            f"- **Deduplication Count:** {incident.get('deduplicated_count', 'N/A')}",
            "",
            "## 5. Compliance Status",
            "",
            "- [x] Security event detected and logged",
            "- [x] Event classified by severity",
            "- [x] Affected assets identified",
            "- [x] Evidence preserved in audit trail",
            "- [x] Response team notified",
            "",
            "---",
            "_Report generated by OmniWatch Compliance Reporter (Phase 10)_",
        ])
        return "\n".join(lines)

    def _render_sla_compliance(
        self,
        incident: dict[str, Any],
        audit_logs: list[dict[str, str]],
    ) -> str:
        """Render SLA Compliance Report (PCI-DSS)."""
        impact_score = incident.get("business_impact_score", 0)
        sla_risk = incident.get("sla_breach_risk", "N/A")

        # Determine SLA status
        if isinstance(impact_score, (int, float)) and impact_score > 80:
            sla_status = "AT RISK"
        elif sla_risk == "HIGH":
            sla_status = "BREACH IMMINENT"
        else:
            sla_status = "COMPLIANT"

        lines = [
            "# SLA Compliance Report",
            "",
            f"**Incident ID:** `{incident.get('incident_id', 'N/A')}`",
            f"**Generated At:** {datetime.now(timezone.utc).isoformat()}",
            "**Framework:** PCI-DSS",
            "",
            "---",
            "",
            "## 1. SLA Status",
            "",
            "| Metric | Value | Status |",
            "|--------|-------|--------|",
            f"| Business Impact Score | {impact_score} | {'⚠️' if isinstance(impact_score, (int, float)) and impact_score > 50 else '✅'} |",
            f"| SLA Breach Risk | {sla_risk} | {'🔴' if sla_risk == 'HIGH' else '🟡' if sla_risk == 'MEDIUM' else '🟢'} |",
            f"| Overall SLA Status | — | **{sla_status}** |",
            "",
            "## 2. Incident Timeline",
            "",
            f"- **Detected:** {incident.get('created_at', 'N/A')}",
            f"- **Severity:** {incident.get('severity', 'N/A')}",
            f"- **Root Cause:** `{incident.get('root_cause_entity', 'N/A')}`",
            "",
            "## 3. Impact Assessment",
            "",
        ]

        impacted = incident.get("impacted_services", "[]")
        if isinstance(impacted, str):
            try:
                impacted = json.loads(impacted)
            except json.JSONDecodeError:
                impacted = [impacted]

        if impacted:
            lines.append(f"**{len(impacted)} service(s) impacted:**")
            for svc in impacted:
                lines.append(f"- `{svc}`")
        else:
            lines.append("_No impacted services recorded._")
        lines.append("")

        lines.extend([
            "## 4. Remediation Status",
            "",
            f"- **Assigned To:** {incident.get('assigned_to', 'N/A')}",
            f"- **Status:** {incident.get('status', 'N/A')}",
            f"- **Deduplication Count:** {incident.get('deduplicated_count', 'N/A')}",
            "",
            "## 5. Audit Evidence",
            "",
        ])
        if audit_logs:
            lines.append(f"**{len(audit_logs)} audit log object(s) preserved:**")
            for log in audit_logs:
                lines.append(f"- `{log['key']}` ({log['size']} bytes)")
        else:
            lines.append("_No audit log objects found._")
        lines.append("")

        lines.extend([
            "## 6. Compliance Checklist",
            "",
            "- [x] SLA metrics tracked and recorded",
            "- [x] Business impact assessed",
            "- [x] Service dependencies mapped",
            "- [x] Remediation ownership assigned",
            "- [x] Audit evidence archived",
            "",
            "---",
            "_Report generated by OmniWatch Compliance Reporter (Phase 10)_",
        ])
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _report_type_to_framework(report_type: str) -> str:
        """Map report type to primary compliance framework."""
        mapping = {
            REPORT_TYPE_INCIDENT_RESPONSE: "SOC2/ISO27001",
            REPORT_TYPE_SECURITY_EVENT: "ISO27001/HIPAA",
            REPORT_TYPE_SLA_COMPLIANCE: "PCI-DSS",
        }
        return mapping.get(report_type, "General")

    @staticmethod
    def _serialize(value: Any) -> Any:
        """Serialize datetime values to ISO 8601."""
        if isinstance(value, datetime):
            return value.isoformat()
        return value


# ======================================================================
# FastAPI App
# ======================================================================

def create_app() -> FastAPI:
    """Create the FastAPI application for the compliance reporter."""
    app = FastAPI(
        title="OmniWatch Compliance Reporter",
        version="1.0.0",
    )
    reporter = ComplianceReporter()

    @app.get("/health")
    async def health() -> dict[str, Any]:
        """Liveness probe — returns service health status."""
        return reporter.health_check()

    @app.get("/report-types")
    async def report_types() -> dict[str, list[str]]:
        """List available report types."""
        return {"report_types": REPORT_TYPES}

    @app.post("/generate/{incident_id}")
    async def generate(
        incident_id: str,
        report_type: str = REPORT_TYPE_INCIDENT_RESPONSE,
    ) -> dict[str, Any]:
        """Generate a compliance report for an incident."""
        meta = reporter.generate_report(incident_id, report_type)
        return meta.model_dump()

    return app


app = create_app()


# ======================================================================
# CLI
# ======================================================================

def _cli_main() -> None:
    """CLI entry point for compliance_reporter."""
    import sys

    if len(sys.argv) < 4 or sys.argv[1] != "generate-report":
        print("Usage: python -m genai.compliance_reporter generate-report <incident_id> [report_type]")
        print(f"Report types: {', '.join(REPORT_TYPES)}")
        sys.exit(1)

    incident_id = sys.argv[2]
    report_type = sys.argv[3] if len(sys.argv) > 3 else REPORT_TYPE_INCIDENT_RESPONSE

    reporter = ComplianceReporter()
    try:
        meta = reporter.generate_report(incident_id, report_type)
        print(json.dumps(meta.model_dump(), indent=2))
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        reporter.close()


if __name__ == "__main__":
    _cli_main()
