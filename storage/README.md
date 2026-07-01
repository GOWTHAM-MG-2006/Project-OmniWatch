# storage — Unified Storage Layer (Phase 3)

## Purpose
Store all telemetry in the right databases for efficient querying and analysis.

## Components

### ClickHouse (storage/clickhouse/)
- **schema.sql** — All table definitions (metrics, logs, anomalies, incidents, etc.)
- **client.py** — Read/write client with insert and query methods

### Neo4j (storage/neo4j/)
- **topology_loader.py** — Loads topology.json into Neo4j property graph
- **client.py** — Cypher query client for graph operations

### MinIO (storage/minio/)
- **bucket_setup.py** — Creates all required buckets
- **client.py** — Object storage client for archive, incidents, audit logs

## Buckets

| Bucket | Contents |
|--------|----------|
| omniwatch-telemetry-archive | Aged telemetry > 30 days |
| omniwatch-incidents | Full incident JSON records |
| omniwatch-audit-logs | All remediation action logs |
| omniwatch-ml-datasets | Historical data for model training |
| omniwatch-runbooks | Generated runbooks and playbooks |

## How to Run

```powershell
cd E:\Project-OmniWatch
simulation\.venv\Scripts\Activate.ps1

# Install dependencies
py -m pip install clickhouse-driver neo4j minio

# Setup ClickHouse schema
py storage\clickhouse\client.py schema

# Load topology into Neo4j
py storage\neo4j\topology_loader.py load

# Create MinIO buckets
py storage\minio\bucket_setup.py

# Check storage status
py storage\clickhouse\client.py status
py storage\neo4j\client.py status
py storage\minio\client.py status
```
