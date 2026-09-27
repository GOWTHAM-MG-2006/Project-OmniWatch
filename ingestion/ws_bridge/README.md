# ws-bridge — Workspace Bridge Consumer

Consumes shared OTLP raw Kafka topics, keeps signals tagged with resource
attribute `omniwatch.workspace`, and persists them into per-workspace
ClickHouse databases (`omniwatch_ws_<slug>`).

## Inputs

Kafka `omniwatch.{metrics,logs,traces}.raw` (OTLP JSON), broker `kafka:29092`.

## Outputs

Rows in `omniwatch_ws_<slug>.{metrics,logs,traces}` (DB/tables auto-created
from the shared schema). Untagged signals are skipped — the shared path owns
them. Stats log line per flush (`routed`/`skipped`/`errors`).

## Run

```bash
docker compose up -d ws-bridge
curl http://localhost:8090/health
```
