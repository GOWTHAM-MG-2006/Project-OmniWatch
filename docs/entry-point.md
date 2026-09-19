# OmniWatch — Entry-Point Guide

> End-to-end journey through the identity layer: register, login, create a
> workspace, switch workspace, complete onboarding, and run a scoped query.
> All commands are verbatim — copy-paste against the running service on
> `http://localhost:8012`. The isolation contract lives in
> [`workspace-isolation.md`](workspace-isolation.md).

---

## Prerequisites

| Requirement | Detail |
|-------------|--------|
| Service running | `uvicorn identity.main:app --host 0.0.0.0 --port 8012` (or `python -m identity.main`) |
| curl | Any version with `--json` support |
| jq | Optional, for JSON pretty-printing |

---

## Route table

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/health` | No | Liveness check |
| `POST` | `/auth/register` | No | Create user account |
| `POST` | `/auth/login` | No | Get JWT tokens |
| `POST` | `/auth/refresh` | No | Refresh access token |
| `GET` | `/auth/me` | Yes | Current user info |
| `POST` | `/auth/logout` | Yes | Invalidate refresh token |
| `POST` | `/workspaces` | Yes | Create workspace (provisions CH/MinIO/Neo4j/Kafka) |
| `GET` | `/workspaces` | Yes | List user's workspaces |
| `GET` | `/workspaces/{id}` | Yes | Get workspace details |
| `PATCH` | `/workspaces/{id}` | Yes | Rename workspace |
| `DELETE` | `/workspaces/{id}` | Yes | Tombstone workspace |
| `POST` | `/workspaces/{id}/switch` | Yes | Switch active workspace (JWT claim) |
| `POST` | `/workspaces/{id}/onboarding` | Yes | Submit onboarding wizard answers |
| `GET` | `/workspaces/{id}/onboarding` | Yes | Retrieve onboarding answers |

---

## Step 1 — Register a user

```bash
curl --json '{ "email": "alice@example.com", "password": "securepass10" }' \
  http://localhost:8012/auth/register
```

Expected response (201):

```json
{
  "user_id": "u_abc123",
  "email": "alice@example.com",
  "created_at": "2026-09-19T12:00:00Z"
}
```

Password policy: minimum 10 characters, maximum 128 characters.

---

## Step 2 — Login (get JWT tokens)

```bash
curl --json '{ "email": "alice@example.com", "password": "securepass10" }' \
  http://localhost:8012/auth/login
```

Expected response (200):

```json
{
  "access_token": "eyJ...",
  "refresh_token": "eyJ...",
  "token_type": "bearer",
  "expires_in": 3600
}
```

Save both tokens for subsequent requests:

```bash
ACCESS_TOKEN="eyJ..."    # paste from response
REFRESH_TOKEN="eyJ..."   # paste from response
```

---

## Step 3 — Verify identity

```bash
curl -H "Authorization: Bearer $ACCESS_TOKEN" \
  http://localhost:8012/auth/me
```

Expected response (200):

```json
{
  "user_id": "u_abc123",
  "email": "alice@example.com",
  "created_at": "2026-09-19T12:00:00Z"
}
```

---

## Step 4 — Create a workspace

```bash
curl --json '{ "name": "Acme Backend" }' \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  http://localhost:8012/workspaces
```

The slug is auto-generated from the name: `"acme-backend"`. On collision, the
system appends `-2`, `-3`, etc.

Expected response (201):

```json
{
  "workspace": {
    "workspace_id": "ws_xyz789",
    "slug": "acme-backend",
    "name": "Acme Backend",
    "created_at": "2026-09-19T12:01:00Z",
    "kafka_topic_prefix": "ws_acme-backend.",
    "clickhouse_database": "omniwatch_ws_acme-backend",
    "minio_prefix": "workspaces/acme-backend/",
    "neo4j_workspace": "acme-backend",
    "k8s_namespace": "omniwatch-ws-acme-backend"
  },
  "provisioning": { "row": "...", "clickhouse": "...", "minio": "...", "neo4j": "..." },
  "connection_bundle": { "kafka_topic_prefix": "...", "clickhouse_database": "...", "minio_prefix": "...", "neo4j_workspace": "...", "k8s_namespace": "...", "otlp_note": "...", "agent_config": { ... } }
}
```

Save the workspace ID from `workspace.workspace_id`:

```bash
WS_ID="ws_xyz789"    # paste from response.workspace.workspace_id
```

---

## Step 5 — List workspaces

```bash
curl -H "Authorization: Bearer $ACCESS_TOKEN" \
  http://localhost:8012/workspaces
```

Expected response (200):

```json
[
  {
    "workspace_id": "ws_xyz789",
    "slug": "acme-backend",
    "name": "Acme Backend",
    "created_at": "2026-09-19T12:01:00Z",
    "kafka_topic_prefix": "ws_acme-backend.",
    "clickhouse_database": "omniwatch_ws_acme-backend",
    "minio_prefix": "workspaces/acme-backend/",
    "neo4j_workspace": "acme-backend",
    "k8s_namespace": "omniwatch-ws-acme-backend"
  }
]
```

---

## Step 6 — Switch active workspace

The `POST /workspaces/{id}/switch` endpoint issues a new token pair with an
updated `ws` claim. All subsequent requests use the switched token to scope
Kafka topics, ClickHouse queries, MinIO prefixes, and Neo4j traversals.

```bash
curl -X POST \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  http://localhost:8012/workspaces/$WS_ID/switch
```

Expected response (200):

```json
{
  "access_token": "eyJ...(new)",
  "refresh_token": "eyJ...(new)",
  "token_type": "bearer",
  "expires_in": 3600,
  "workspace_id": "ws_xyz789"
}
```

Update your token variable with the new access token:

```bash
ACCESS_TOKEN="eyJ...(new)"    # paste from response
```

---

## Step 7 — Submit onboarding wizard

The onboarding wizard collects capacity and service details for the workspace.
All fields are required. Enum fields use the closed vocabularies listed below.

### Enum values

| Field | Allowed values |
|-------|----------------|
| `app_type` | `api`, `worker`, `ml`, `iot`, `custom` |
| `cloud_provider` | `aws`, `azure`, `gcp`, `onprem`, `other` |
| `expected_eps` | `<100`, `100-1k`, `1k-10k`, `>10k` |
| `log_volume` | `<100`, `100-1k`, `1k-10k`, `>10k` |

```bash
curl --json '{
  "app_name": "order-service",
  "app_type": "api",
  "cloud_provider": "gcp",
  "service_endpoints": ["https://orders.acme.example/api/v1", "https://orders.acme.example/health"],
  "expected_eps": "1k-10k",
  "log_volume": "100-1k",
  "retention_days": 90,
  "alert_contact": "oncall@acme.example.com"
}' \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  http://localhost:8012/workspaces/$WS_ID/onboarding
```

Expected response (200):

```json
{
  "workspace_id": "ws_xyz789",
  "slug": "acme-backend",
  "answers": {
    "app_name": "order-service",
    "app_type": "api",
    "cloud_provider": "gcp",
    "service_endpoints": ["https://orders.acme.example/api/v1", "https://orders.acme.example/health"],
    "expected_eps": "1k-10k",
    "log_volume": "100-1k",
    "retention_days": 90,
    "alert_contact": "oncall@acme.example.com"
  },
  "suggested_config": {
    "queue_depth": 8000,
    "batch_size": 500,
    "poll_interval_s": 15,
    "scrape_interval_s": 15
  },
  "config_path": "omniwatch-incidents/workspaces/acme-backend/config.json",
  "updated_at": "2026-09-19T12:05:00Z"
}
```

Resubmission overwrites (upsert semantics, never duplicates).

---

## Step 8 — Retrieve onboarding answers

```bash
curl -H "Authorization: Bearer $ACCESS_TOKEN" \
  http://localhost:8012/workspaces/$WS_ID/onboarding
```

Returns the same payload as Step 7 (200). If onboarding has not been submitted
yet, returns 404.

---

## Step 9 — Refresh a token

```bash
curl --json "{ \"refresh_token\": \"$REFRESH_TOKEN\" }" \
  http://localhost:8012/auth/refresh
```

Expected response (200):

```json
{
  "access_token": "eyJ...(refreshed)",
  "refresh_token": "eyJ...(new-refresh)",
  "token_type": "bearer",
  "expires_in": 3600
}
```

---

## Step 10 — Rename a workspace

```bash
curl --json '{ "name": "Acme Orders API" }' \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -X PATCH \
  http://localhost:8012/workspaces/$WS_ID
```

The `slug` does not change on rename — all provisioned resources key off the slug.

---

## Step 11 — Delete a workspace (tombstone)

```bash
curl -H "Authorization: Bearer $ACCESS_TOKEN" \
  -X DELETE \
  http://localhost:8012/workspaces/$WS_ID
```

Returns 200 on success. The workspace is tombstoned (`deleted=1`) — data is
retained for 7 days (see [workspace-isolation.md](workspace-isolation.md) section 5).

---

## Step 12 — Logout

```bash
curl --json "{ \"refresh_token\": \"$REFRESH_TOKEN\" }" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  http://localhost:8012/auth/logout
```

---

## Error responses

| Code | Meaning | Example |
|------|---------|---------|
| `422` | Validation error (bad slug, short password, invalid enum) | `{ "detail": [{ "type": "value_error", ... }] }` |
| `401` | Missing or invalid `Authorization` header | `{ "detail": "Not authenticated" }` |
| `403` | Workspace exists but belongs to another user | `{ "detail": "Forbidden" }` |
| `404` | Onboarding not yet submitted, or tombstoned workspace | `{ "detail": "Not found" }` |

---

## Workspace isolation summary

Every resource is scoped to the active workspace via the JWT `ws` claim. The
full isolation contract is in [`workspace-isolation.md`](workspace-isolation.md).

| Layer | Scope pattern |
|-------|---------------|
| Kafka topics | `ws_<slug>.omniwatch.*` |
| ClickHouse | `omniwatch_ws_<slug>` |
| MinIO | `workspaces/<slug>/` |
| Neo4j | `:Workspace{slug}` + `BELONGS_TO` edges |
| OPA | `tenant_allow` checks `jwt_ws == resource_ws` |
| K8s | `omniwatch-ws-<slug>` (documented, not provisioned) |

---

## Agent configuration (per-workspace)

For deploying the `omniwatch-agent` to a workspace-specific namespace, set
these environment variables in the DaemonSet or deployment manifest:

```yaml
env:
  - name: OMNIWATCH_WORKSPACE_SLUG
    value: "acme-backend"
  - name: OMNIWATCH_K8S_NAMESPACE
    value: "omniwatch-ws-acme-backend"
  - name: OMNIWATCH_CLICKHOUSE_DATABASE
    value: "omniwatch_ws_acme-backend"
  - name: OMNIWATCH_KAFKA_TOPIC_PREFIX
    value: "ws_acme-backend."
  - name: OMNIWATCH_MINIO_PREFIX
    value: "workspaces/acme-backend/"
```

See the [omniwatch-agent README](../omniwatch-agent/README.md) for full agent
configuration and the [K8s manifests README](../k8s/omniwatch-agent/README.md)
for namespace mapping details.
