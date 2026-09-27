# importer — Per-Workspace OTLP/HTTP Ingest Service (exporter-importer-split)

Port **4320**. Authenticates exporter Bearer tokens against identity
`POST /importers/verify`, resolves the workspace, and writes telemetry ONLY
to that workspace's ClickHouse database (`omniwatch_ws_<slug>`).

## Endpoints

| Method | Path | Auth | Success | Errors |
|--------|------|------|---------|--------|
| GET | `/health` | no | 200 `{"status":"ok"}` | — |
| POST | `/ingest` | `Authorization: Bearer <importer-token>` | 202 `{accepted, workspace_slug, inserted}` | 401 no token, 403 bad token, 422 unknown type, 503 identity/storage down |
| GET | `/activity/by-workspace/{id}` | `Authorization: Bearer <user JWT>` (forwarded to identity for ownership) | 200 `{workspace_slug, activity: [{entity_id, telemetry_type, last_seen}]}` | 401 no token, 403 non-owner |

Activity is in-memory (last accepted insert per workspace/entity/type);
a restart clears it and exporters re-announce on their next heartbeat.
Status is always DERIVED from `last_seen` recency — never stored, never faked.

## Body

```json
{
  "telemetry_type": "metrics",
  "entity_id": "aws-box-agent",
  "entity_type": "API_NODE",
  "metrics": [{"metric_name": "cpu_usage", "value": 41.2}],
  "logs": [],
  "traces": []
}
```

`telemetry_type` is one of 10: `metrics`, `logs`, `traces`, `metadata`,
`state`, `audit_logs`, `profiling`, `siem`, `auth_logs`, `security_alerts`.
Log-ish types route to `logs`; `profiling` routes to `traces`; `metadata` /
`state` without log lines are stored as one INFO envelope row.

## Environment

| Var | Default | Notes |
|-----|---------|-------|
| `OMNIWATCH_IMPORTER_PORT` | `4320` | serve port |
| `OMNIWATCH_IDENTITY_URL` | `http://localhost:8012` | identity verify endpoint |
| `IMPORTER_INTERNAL_SECRET` | dev-only default + WARN | must match identity |
| `OMNIWATCH_CLICKHOUSE_*` | compose defaults | workspace DBs live here |

## Run

```bash
pip install -r importer/requirements.txt
python -m uvicorn importer.main:app --port 4320

# tests (verify + CH layers are mocked)
pytest importer/tests/ -v
```
