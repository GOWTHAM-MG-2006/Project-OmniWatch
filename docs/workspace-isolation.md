# OmniWatch — Workspace Isolation Contract (ENTRY-2)

> THE contract every later todo implements (todos 3–5 scope inputs through
> exactly these mappings; algorithms untouched). Code MUST match this document
> verbatim — tests are the authority, then code, then docs (docs conform to
> tested behavior, never reverse). Helpers live in `storage/config.py`
> (`workspace_kafka_prefix`, `workspace_database`, `workspace_prefix`,
> `workspace_k8s_namespace`); provisioning in `identity/provision.py`;
> routers in `identity/workspaces.py`.

## 1. Naming schemes (exact)

| Layer | Non-default workspace (`<slug>`) | `default` workspace (bootstrap) |
|-------|----------------------------------|---------------------------------|
| Kafka topics | `ws_<slug>.omniwatch.*` — e.g. base `omniwatch.anomalies.detected` → `ws_acme.omniwatch.anomalies.detected` | bare names unchanged: `omniwatch.anomalies.detected`, … (today's topics) |
| ClickHouse | database `omniwatch_ws_<slug>` (same `schema.sql` tables, cloned on provision) | database `omniwatch` (today's DB) |
| MinIO | prefix `workspaces/<slug>/` inside the EXISTING buckets (no new buckets) | bare keys (no prefix; today's layout) |
| Neo4j | scoping node `:Workspace{slug}` + `BELONGS_TO` edges from each entity node to its workspace node | scoping node `:Workspace{slug: 'default'}`; legacy nodes without `BELONGS_TO` read as `default` |
| OPA | rule `tenant_allow` checks JWT `sub`/`ws` against the resource workspace (see §4) | `ws: default` (or absent `ws` on legacy tokens) maps to `default` |
| K8s | namespace `omniwatch-ws-<slug>` — DOCUMENTED mapping only, NOT provisioned (no automation creates namespaces) | namespace `omniwatch` (today's namespace) |

No new Kafka topics outside the `ws_<slug>.` prefix scheme; no new
ClickHouse databases outside `omniwatch_ws_<slug>`; no new Neo4j labels
outside `:Workspace` scoping; no new MinIO buckets (prefixes only).

## 2. Slug rules

- Allowed: lowercase alphanumeric plus hyphen/underscore: `^[a-z0-9][a-z0-9-_]{0,62}$`
  (max 63 chars). Anything else → `422`.
- Auto-slug from `name`: lowercase, runs of `[^a-z0-9]+` → `-`, strip
  leading/trailing `-_`, truncate to 63, fallback `workspace` when empty.
- Slugs are GLOBALLY unique (they key CH databases): on collision the
  provisioner appends `-2`, `-3`, … — never 409s on slug.
- `name` is unique PER USER: duplicate name for the same user → `409`.
  Different users may reuse the same name (slugs disambiguate via suffix).
- Slugs are IMMUTABLE: rename changes `name` only, never `slug`
  (databases/topics/prefixes key off the slug).

## 3. Ownership + error discipline

- Every workspace row carries `user_id` (owner). All single-workspace
  endpoints (`GET/PATCH/DELETE /workspaces/{id}`, `POST .../switch`) resolve
  the caller via `require_user` (or the default-bootstrap identity, §6).
- Non-existent id OR id owned by another user → `403` (never 404-leak,
  never 200). Rationale: 404 vs 403 would oracle workspace existence.
- Malformed slug (explicit `slug` field, when supplied) → `422`.
- Tombstoned (`deleted=1`) workspaces behave as absent for the owner:
  single-get/switch/rename → `403` (same non-leak rule); list excludes them.

## 4. OPA `tenant_allow` (addition only — existing rules untouched)

New rule file `orchestration/policies/tenant_allow.rego`,
`package omniwatch`, evaluated BEFORE the existing allow/needs_approval
rules (decision client queries `tenant_allow` first; deny short-circuits):

```rego
tenant_allow if {
  input.resource_ws == "default"        # legacy/unscoped resources
  input.jwt_ws == "default"
}
tenant_allow if {
  input.resource_ws == input.jwt_ws     # exact workspace match
  input.jwt_sub == input.resource_owner # caller owns the workspace
}
```

Inputs: `jwt_sub` (JWT `sub`), `jwt_ws` (JWT `ws`, absent → `default`),
`resource_ws` (workspace slug owning the resource),
`resource_owner` (workspace `user_id`). Existing `policy.rego` bytes are
preserved verbatim.

## 5. Provision chain (`POST /workspaces`)

Order (each step best-effort-degraded, never failing the 201):

1. validate (name non-empty, slug rules → 422; per-user name dup → 409)
2. create row (`workspaces` table, migration `003_workspaces.py`)
3. ClickHouse: `CREATE DATABASE IF NOT EXISTS omniwatch_ws_<slug>` +
   clone `schema.sql` tables (default: skip — uses `omniwatch`)
4. MinIO: ensure prefixes `workspaces/<slug>/` (marker objects) in the
   existing buckets (default: skip — bare keys)
5. Neo4j: `MERGE (:Workspace{slug})` scoping node (default: the `default` node)
6. return connection bundle: workspace fields + `kafka_topic_prefix`
   (`ws_<slug>.`), `clickhouse_database`, `minio_prefix`, `neo4j_workspace`,
   `k8s_namespace` (documentation value only) + agent snippet VALUES
   (endpoint note, never secrets)

Delete is a tombstone: `deleted=1` set, row + all provisioned data RETAINED
for 7 days (documented retention window; NO purge job implemented, NO data
wiped on delete). Queryable-during-window is the contract.

## 6. Default bootstrap (single-tenant regression boundary)

First boot / legacy path auto-provisions user `local-dev` + workspace
`default` (slug `default`): zero-config topics/DBs map to today's bare
names (table above, right column). Unauthenticated calls to `/workspaces*`
resolve to this bootstrap identity, so existing compose boot, tests, and E2E
keep working byte-identical with no `Authorization` header. Authenticated
calls scope strictly to the caller's own workspaces (§3).

## 7. K8s namespace mapping (documented, NOT provisioned)

Template: `k8s/workspaces/namespace-template.yaml` renders namespace
`omniwatch-ws-<slug>` with the standard labels. No controller, job, or
endpoint creates namespaces automatically — cluster deploy maps workspaces
to namespaces by hand (or a future todo). `default` → `omniwatch`.
