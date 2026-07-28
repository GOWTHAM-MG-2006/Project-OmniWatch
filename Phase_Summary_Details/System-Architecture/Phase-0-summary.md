# Phase 0 — Environment Setup (Summary)

## Goal
Create a production-realistic local development + GCP sandbox environment with all infrastructure defined-as-code and reproducible from a single `docker-compose up -d` command.

## Completed

### 1. Local Infrastructure (`docker-compose.yml`)
All 11 core services defined with health checks, persistent volumes, and network isolation:

| Service | Image | Port(s) | Purpose |
|---------|-------|---------|---------|
| Zookeeper | confluentinc/cp-zookeeper:7.5.0 | 2181 | Kafka coordination |
| Kafka | confluentinc/cp-kafka:7.5.0 | 9092 | Event bus |
| ClickHouse | clickhouse/clickhouse-server:23.8 | 9000, 8123 | Time-series DB |
| Neo4j | neo4j:5.13-community | 7474, 7687 | Graph DB |
| MinIO | minio/minio:latest | 9001, 9010 | Object storage |
| Prometheus | prom/prometheus:v2.47.0 | 9090 | Metrics |
| Loki | grafana/loki:2.9.0 | 3100 | Logs |
| Jaeger | jaegertracing/all-in-one:1.50 | 16686, 4317, 4318 | Traces |
| Ollama | ollama/ollama:latest | 11434 | LLM inference |
| Redis | redis:7.2-alpine | 6379 | Feature store / cache |
| OPA | openpolicyagent/opa:latest-static | 8181 | Policy engine |

### 2. Environment Configuration (`.env.example`)
60+ environment variables defined for all service endpoints, ports, and credentials:
- Service hostnames and ports (INTERNAL_* and EXTERNAL_* variants for Docker/K8s)
- Database names, users, passwords
- Kafka topic configuration
- Object storage buckets (5 buckets defined)
- Ollama model configuration

### 3. Prometheus Configuration (`config/prometheus.yml`)
- Scrape job for `omniwatch-otel-collector` (port 8889 via Docker DNS)
- Scrape interval: 15s
- Target: `otel-collector:8889`

### 4. GCP Infrastructure (`config/gcp/terraform/`)
4 Terraform files for automated GCP provisioning:

| File | Lines | Contents |
|------|-------|----------|
| `main.tf` | 211 | GCP provider, GKE Autopilot cluster, Artifact Registry, VPC, firewall rules, Service Account, IAM bindings, Workload Identity, K8s namespace + SA |
| `variables.tf` | — | Input vars: project_id, region, cluster_name, node_count, environment |
| `outputs.tf` | — | Output: cluster_endpoint, cluster_ca_cert, artifact_registry_url, sa_email |
| `versions.tf` | — | Provider constraints: google ~> 5.0, kubernetes ~> 2.0, random |

**Passes:** `terraform init -backend=false` + `terraform fmt -check` + `terraform validate`

### 5. K8s Infrastructure Manifests (`k8s/infra/*/` + `config/k8s/`)
24 YAML files across 12 service directories, each with `deployment.yaml` + `service.yaml`:

| Service Dir | Deployment | Service | Notes |
|-------------|-----------|---------|-------|
| `zookeeper/` | ✅ | ✅ | Single replica, 2181 port |
| `kafka/` | ✅ | ✅ | KAFKA_ADVERTISED_LISTENERS with internal/external |
| `clickhouse/` | ✅ | ✅ | 9000 (native) + 8123 (HTTP) |
| `neo4j/` | ✅ | ✅ | 7474 (browser) + 7687 (bolt) |
| `minio/` | ✅ | ✅ | 9001 (console) + 9010 (API) |
| `prometheus/` | ✅ (+ ConfigMap) | ✅ | ConfigMap mounted at /etc/prometheus |
| `loki/` | ✅ | ✅ | 3100 for log ingestion |
| `jaeger/` | ✅ | ✅ | All-in-one: 16686 UI, 4317 gRPC, 4318 HTTP |
| `redis/` | ✅ | ✅ | 6379, health check via redis-cli |
| `ollama/` | ✅ | ✅ | 11434 with persistent volume |
| `opa/` | ✅ | ✅ | 8181 with /policies volume mount |

### 6. E2E Test (`tests/phase-0-e2e/test_infra_connectivity.py`)
Comprehensive test suite:
- **TestPhase0Structure**: Validates all files, directories, and configs exist
- **TestDockerComposeConnectivity**: Validates running services respond (HTTP health, Kafka topic CRUD, ClickHouse queries, Redis ping, MinIO bucket list)
- **TestTerraformConfig**: Validates Terraform syntax via `terraform fmt` + `init` + `validate`
- **TestK8sManifests**: Validates all K8s manifests have required YAML structure
- **Standalone CLI**: `python tests/phase-0-e2e/test_infra_connectivity.py` for quick checks

### 7. Documentation Updates
- `AGENTS.md` — Updated repository structure, phase table, deleted paths
- `README.md` — Written from scratch with architecture overview, quick start, development guide
- `.omo/plans/OmniWatch-Built Plan.md` — Complete 12-phase build plan

## Files Created This Phase
```
config/
├── gcp/
│   └── terraform/
│       ├── main.tf
│       ├── variables.tf
│       ├── outputs.tf
│       └── versions.tf
├── k8s/
│   └── namespace.yaml
├── prometheus.yml                    (updated)
├── docker-compose.yml                (updated)
├── .env.example                      (populated)

k8s/infra/
├── zookeeper/{deployment,service}.yaml
├── kafka/{deployment,service}.yaml
├── clickhouse/{deployment,service}.yaml
├── neo4j/{deployment,service}.yaml
├── minio/{deployment,service}.yaml
├── prometheus/{deployment,service,configmap}.yaml
├── loki/{deployment,service}.yaml
├── jaeger/{deployment,service}.yaml
├── redis/{deployment,service}.yaml
├── ollama/{deployment,service}.yaml
├── opa/{deployment,service}.yaml

tests/
└── phase-0-e2e/
    └── test_infra_connectivity.py
```

## Architecture Reference

```
                         ┌──── LOCAL DEV ────┐
                         │  docker-compose up │
                         │  ───────────────── │
                         │  Kafka  ClickHouse  │
                         │  Neo4j  MinIO       │
                         │  Prometheus  Loki    │
                         │  Jaeger  Ollama    │
                         │  Redis  OPA  ZK     │
                         └────────────────────┘
                                  │
                         ┌─── GCP SANDBOX ────┐
                         │  terraform apply   │
                         │  ───────────────── │
                         │  GKE Autopilot      │
                         │  Artifact Registry  │
                         │  VPC + Firewall     │
                         │  Service Accounts   │
                         └────────────────────┘
                                  │
                         ┌── K8S INFRA ──────┐
                         │  kubectl apply     │
                         │  ───────────────── │
                         │  omniwatch ns      │
                         │  Same 11 services  │
                         └────────────────────┘
```

## Phase 0 Checklist
- [x] Docker Compose with all 11 services
- [x] `.env.example` with 60+ environment variables
- [x] Prometheus scrape config for otel-collector
- [x] GCP Terraform (GKE, VPC, IAM, Artifact Registry)
- [x] K8s namespace + 11 service manifests
- [x] E2E infra connectivity test suite
- [x] AGENTS.md updated
- [x] README.md written
- [x] Build plan verified by Momus
- [x] Security leak check script available

## Gate to Phase 1
✅ Phase 0 E2E test created at `tests/phase-0-e2e/test_infra_connectivity.py`
✅ Phase 0 summary saved to `Phase_Summary_Details/System-Architecture/Phase-0-summary.md`
→ Ready to begin **Phase 1: Microservices + OTel Instrumentation**