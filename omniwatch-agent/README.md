# omniwatch-agent

Host-level telemetry collector for OmniWatch. Runs as a DaemonSet (one pod per node) and collects **logs**, **events**, **metadata/state**, **metrics (host-level)**, and **traces (via Beyla sidecar)** from Kubernetes nodes and pods, exporting everything to the OpenTelemetry Collector via OTLP gRPC.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start (Local)](#quick-start-local)
- [Docker Build](#docker-build)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Configuration](#configuration)
- [Log Format: JSON Structured Logging](#log-format-json-structured-logging)
- [BCC Fallback (Optional, for Restricted-eBPF Environments)](#bcc-fallback-optional-for-restricted-ebpf-environments)
- [Beyla Integration](#beyla-integration)
- [Data Types Covered vs Other Components](#data-types-covered-vs-other-components)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

---

## Overview

`omniwatch-agent` is a lightweight Go binary that runs on every Kubernetes node. It:

1. **Collects** host-level metrics (CPU, memory, disk, network), container logs, Kubernetes object state, and audit logs.
2. **Self-instruments** via the OTel SDK — emitting its own traces, metrics, and logs as heartbeats every collection interval.
3. **Exports** all telemetry to the OpenTelemetry Collector (`otelcol`) via OTLP gRPC.
4. **Runs a Beyla sidecar** for eBPF-based auto-instrumentation of application HTTP/gRPC traffic (no code changes required).

The agent is built as a distroless container (non-root, read-only root filesystem) and is designed for production Kubernetes clusters.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Kubernetes Node                                                │
│                                                                 │
│  ┌─────────────────────────┐   ┌─────────────────────────────┐  │
│  │  omniwatch-agent        │   │  beyla (sidecar)            │  │
│  │  ─────────────────────  │   │  ─────────────────────────  │  │
│  │  hostmetrics receiver   │   │  eBPF auto-instrumentation │  │
│  │  filelog receiver       │   │  HTTP/gRPC traces           │  │
│  │  k8s_objects receiver   │   │                             │  │
│  │  k8s_cluster receiver   │   │  → OTLP → otelcol:4317     │  │
│  │                         │   │                             │  │
│  │  → OTLP gRPC            │   └─────────────────────────────┘  │
│  │  → otelcol:4317         │                                    │
│  └─────────────────────────┘                                    │
│                                                                 │
│  ┌─────────────────────────┐                                    │
│  │  Health Probe           │                                    │
│  │  :8080/health           │                                    │
│  │  :8080/ready            │                                    │
│  └─────────────────────────┘                                    │
└─────────────────────────────────────────────────────────────────┘
                          │
                          │ OTLP gRPC
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│  otelcol (OpenTelemetry Collector)                              │
│  Receives OTLP → processes → exports to backends                │
└─────────────────────────────────────────────────────────────────┘
```

**Pipeline wiring** (hardcoded in `internal/collector/collector.go`):

```
Receivers:  [hostmetrics, filelog, k8s_cluster, k8s_objects]
Processors: [batch]
Exporters:  [otlp]
```

---

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Go | 1.23+ | `E:\Go\bin\go.exe` on this machine |
| Docker | 24+ | For building the container image |
| Kubernetes | 1.25+ | For DaemonSet deployment |
| kubectl | 1.28+ | For manifest application |
| OpenTelemetry Collector | 0.85+ | Target endpoint (receives OTLP gRPC) |

---

## Quick Start (Local)

### 1. Build and run with Docker Compose

```bash
# From the repository root — starts the agent + otelcol + supporting services
docker compose up -d omniwatch-agent

# Verify the agent is healthy
curl http://localhost:8080/health
# → {"status":"ok","collector_status":"running"}

curl http://localhost:8080/ready
# → {"status":"ready","collector_status":"running"}
```

### 2. Build and run standalone (no Compose)

```bash
# Build the binary
cd omniwatch-agent
go build -o bin/agent.exe ./cmd/agent

# Run with default config
./bin/agent.exe

# Run with custom config path
./bin/agent.exe -config configs/agent.yaml

# Override OTLP endpoint via env var
OMNIWATCH_EXPORTER_OTLP_ENDPOINT=localhost:4317 ./bin/agent.exe
```

### 3. Run tests

```bash
cd omniwatch-agent
go test ./... -count=1
```

---

## Docker Build

### Build the image

```bash
# From the repository root
docker build -t omniwatch-agent:latest ./omniwatch-agent
```

### Dockerfile (2-stage, distroless)

```dockerfile
# Stage 1: Build
FROM golang:1.23-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /agent ./cmd/agent

# Stage 2: Runtime (distroless, non-root)
FROM gcr.io/distroless/static-debian12
COPY --from=builder /agent /agent
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/agent"]
```

### Verify the image

```bash
# Check the image was built
docker images omniwatch-agent

# Run and verify health
docker run -d --name agent-test -p 8080:8080 omniwatch-agent:latest
curl http://localhost:8080/health
docker stop agent-test && docker rm agent-test
```

### Supply Chain (SBOM / cosign / reproducible build)

```bash
# Reproducible build (pinned epoch → same digest across rebuilds)
SOURCE_DATE_EPOCH=$(git log -1 --format=%ct) make build-reproducible

# SPDX 2.3 SBOM (requires syft)
make sbom   # → sbom.spdx.json

# Keyless sign + verify (requires cosign, OIDC in CI)
make sign
make verify   # E2E pipeline note: run `cosign verify <image>` after deploy
```

Tag pushes (`v*`) trigger `.github/workflows/supply-chain.yml`: reproducible
build → SBOM (anchore/sbom-action) → keyless cosign sign → SBOM attached to
the release.

---

## Kubernetes Deployment

### Manifest files

| File | What it does |
|------|--------------|
| `k8s/omniwatch-agent/rbac.yaml` | ServiceAccount + ClusterRole + ClusterRoleBinding |
| `k8s/omniwatch-agent/daemonset.yaml` | DaemonSet (one agent pod per node) + inline Beyla ConfigMap |
| `k8s/omniwatch-agent/service.yaml` | ClusterIP Service exposing health port (8080) |

### Apply (order matters: RBAC first)

```bash
# Provide the image the DaemonSet references
docker build -t omniwatch-agent:latest ./omniwatch-agent

# Apply RBAC first so the ServiceAccount exists
kubectl apply -f k8s/omniwatch-agent/rbac.yaml
kubectl apply -f k8s/omniwatch-agent/daemonset.yaml
kubectl apply -f k8s/omniwatch-agent/service.yaml
```

### Validate without a cluster (dry-run)

```bash
kubectl apply --dry-run=client -f k8s/omniwatch-agent/rbac.yaml
kubectl apply --dry-run=client -f k8s/omniwatch-agent/daemonset.yaml
kubectl apply --dry-run=client -f k8s/omniwatch-agent/service.yaml
```

### Live checks (needs a running cluster)

```bash
kubectl -n omniwatch get daemonset omniwatch-agent
kubectl -n omniwatch get pods -l app=omniwatch-agent
kubectl -n omniwatch get svc omniwatch-agent

# Health from inside the cluster
kubectl -n omniwatch exec -it <pod-name> -c omniwatch-agent -- \
  wget -qO- http://localhost:8080/health

# Health from outside (port-forward)
kubectl -n omniwatch port-forward svc/omniwatch-agent 8080:8080 &
curl http://localhost:8080/health
curl http://localhost:8080/ready
```

### Security context

- `runAsNonRoot: true`
- `runAsUser: 65532` (UID `nonroot` — verified in distroless image via `/etc/passwd`)
- `readOnlyRootFilesystem: true`
- No privileged containers (agent), no `hostNetwork`, no `hostPID`, no root user
- The agent mounts `/var/log`, `/var/lib/docker/containers`, and `/var/log/kubernetes/audit` **read-only** from the host

---

## Configuration

Configuration is loaded from a YAML file (default: `configs/agent.yaml`) with environment variable overrides. If the YAML file is missing, defaults are used with env overrides applied.

### YAML config (`configs/agent.yaml`)

```yaml
agent:
  collection_interval: 60s
  log_level: info

receiver:
  hostmetrics:
    collection_interval: 60s
  filelog:
    include:
      - /var/log/pods/*/*/*.log
      - /var/log/containers/*/*.log
      - /var/log/kubernetes/audit/*.log
    exclude:
      - /var/log/pods/*/*/**.gz
    start_at: beginning
    operators:
      - type: json_parser
        timestamp:
          parse_from: attributes.time
          layout: RFC3339
        severity:
          parse_from: attributes.level
  k8s_objects:
    collection_interval: 15m
    mode: pull
    objects:
      - name: pods
      - name: nodes
      - name: services
      - name: endpoints
      - name: namespaces
      - name: deployments
      - name: replicasets
      - name: daemonsets
      - name: statefulsets
  k8s_cluster:
    collection_interval: 10m
    node_conditions_to_report:
      - Ready
      - MemoryPressure
      - DiskPressure
      - PIDPressure
      - NetworkUnavailable

exporter:
  otlp:
    endpoint: otel-collector:4317
    insecure: true
```

### Environment variable overrides

All overrides use the `OMNIWATCH_` prefix. Set these to override YAML values without editing the config file.

| Env Var | Type | Default | Description |
|---------|------|---------|-------------|
| `OMNIWATCH_AGENT_COLLECTION_INTERVAL` | duration | `60s` | How often the collector emits heartbeats |
| `OMNIWATCH_AGENT_LOG_LEVEL` | string | `info` | Log level (`debug`, `info`, `warn`, `error`) |
| `OMNIWATCH_RECEIVER_HOSTMETRICS_COLLECTION_INTERVAL` | duration | `60s` | Host metrics collection interval |
| `OMNIWATCH_RECEIVER_FILELOG_INCLUDE` | comma-list | (see YAML) | Log file glob patterns |
| `OMNIWATCH_RECEIVER_K8S_OBJECTS_COLLECTION_INTERVAL` | duration | `15m` | K8s object polling interval |
| `OMNIWATCH_RECEIVER_K8S_OBJECTS_MODE` | string | `pull` | K8s objects mode (`pull` or `watch`) |
| `OMNIWATCH_RECEIVER_K8S_OBJECTS_LABEL_SELECTOR` | string | (empty) | K8s label selector filter |
| `OMNIWATCH_RECEIVER_K8S_OBJECTS_FIELD_SELECTOR` | string | (empty) | K8s field selector filter |
| `OMNIWATCH_RECEIVER_K8S_CLUSTER_COLLECTION_INTERVAL` | duration | `10m` | K8s cluster receiver interval |
| `OMNIWATCH_RECEIVER_K8S_CLUSTER_NODE_CONDITIONS_TO_REPORT` | comma-list | `Ready,MemoryPressure,...` | Node conditions to report |
| `OMNIWATCH_EXPORTER_OTLP_ENDPOINT` | string | `otel-collector:4317` | OTLP gRPC endpoint |
| `OMNIWATCH_EXPORTER_OTLP_INSECURE` | bool | `true` | Skip TLS for OTLP gRPC |

### CLI flags

```
-config <path>    Path to agent config YAML (default: configs/agent.yaml)
-health-addr <addr>  Health probe listen address (default: :8080)
```

---

## Log Format: JSON Structured Logging

All OmniWatch components emit **JSON structured logs** to stdout. This is a project-wide convention.

### Format

```json
{
  "time": "2026-09-18T10:30:00Z",
  "level": "info",
  "message": "collector starting",
  "caller": "internal/collector/collector.go:109",
  "service.name": "omniwatch-agent"
}
```

### Fields

| Field | Type | Source |
|-------|------|--------|
| `time` | RFC3339 string | Go `slog` handler |
| `level` | string | Go `slog` handler |
| `message` | string | Go `slog` handler |
| `caller` | string | Go `slog` handler (with source enabled) |
| `service.name` | string | OTel resource attribute |

### Rationale

- **Machine-parseable**: JSON logs are trivially parsed by downstream systems (filelog receiver, log aggregators).
- **Native OTel support**: The filelog receiver's `json_parser` operator extracts fields from JSON logs without custom parsing.
- **Matches filelog config**: The agent's own `filelog` receiver operators are configured to parse this exact JSON format (`json_parser` with `attributes.time` and `attributes.level`).
- **Consistent across components**: Every OmniWatch component emits the same JSON structure, enabling unified log processing.

### How it works

The agent uses Go's `log/slog` with a JSON handler:

```go
// cmd/agent/main.go
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level:     slog.LevelInfo,
    AddSource: true,
}))
```

The filelog receiver in `configs/agent.yaml` parses these logs:

```yaml
operators:
  - type: json_parser
    timestamp:
      parse_from: attributes.time
      layout: RFC3339
    severity:
      parse_from: attributes.level
```

---

## BCC Fallback (Optional, for Restricted-eBPF Environments)

> **OPTIONAL / Better-to-Have** — Only needed if your environment restricts BPF/eBPF (e.g., hardened kernels, certain container runtimes, or security policies that block `CAP_BPF`). Most standard Kubernetes clusters do not need this.

### When to use

Use the BCC fallback receiver **only if** the primary eBPF-based receivers (`hostmetrics`, `k8s_objects`, Beyla) cannot initialize due to kernel or runtime restrictions. The BCC (BPF Compiler Collection) fallback uses older BPF syscall patterns that may work on restricted kernels where modern eBPF is blocked.

### How to activate

Set `receiver.ebpf.enabled: true` in your agent config:

```yaml
receiver:
  ebpf:
    enabled: true
    # Additional BCC-specific config (see BCC documentation)
```

Or via environment variable:

```bash
OMNIWATCH_RECEIVER_EBPF_ENABLED=true
```

### What it does

- Replaces the standard `hostmetrics` receiver with a BCC-based equivalent for CPU/memory/disk/network metrics.
- Does **not** replace `k8s_objects`, `filelog`, or Beyla — those remain as-is.
- Produces the same metric names and structure, so downstream dashboards and anomaly detection are unaffected.

### Limitations

- Requires the BCC tools package installed on the host node (not available in distroless images).
- Higher CPU overhead than native eBPF receivers.
- Not recommended for production unless eBPF is explicitly blocked by policy.

---

## Beyla Integration

The agent ships with a **Beyla sidecar** for eBPF-based auto-instrumentation. Beyla captures HTTP and gRPC traces from all processes on the node without requiring code changes or SDK instrumentation.

### What Beyla does

- Attaches eBPF probes to all user-space processes on the node.
- Captures incoming and outgoing HTTP/gRPC requests as OpenTelemetry traces.
- Enriches traces with Kubernetes metadata (`k8s.namespace.name`, `k8s.pod.name`, `k8s.deployment.name`).
- Exports traces to the OTel Collector via OTLP gRPC (same endpoint as the agent).

### Docker Compose usage

The Beyla sidecar is included in the `docker-compose.yml`:

```bash
# Start Beyla alongside the agent
docker compose up -d beyla

# Beyla config is mounted from:
#   ./omniwatch-agent/configs/beyla-config.yaml → /etc/beyla/config.yaml
```

Beyla config (`configs/beyla-config.yaml`):

```yaml
executable_name: ""
openTelemetry:
  attributes:
    k8s.namespace.name: true
    k8s.pod.name: true
    k8s.deployment.name: true
  exporter:
    otlp:
      endpoint: otelcol:4317
      insecure: true
log_level: INFO
kernel_version_min: "5.10"
discovery:
  instrument:
    - k8s_namespace: "*"
    - exe_path: "*"
    - open_ports: 80,443,8000-8999
  exclude_instrument:
    - k8s_namespace: kube-system
ebpf:
  wakeup_len: 100
otel_traces_export:
  endpoint: http://otelcol:4317
  protocol: grpc
```

> **Schema note**: the `openTelemetry` block above is the legacy 1.x schema, kept for reference. The running Beyla image (v3.x) reads `otel_traces_export` — the endpoint needs an explicit scheme (`http://otelcol:4317` + `protocol: grpc` = plaintext gRPC, matching the old `insecure: true`); a bare `otelcol:4317` fails with `URL must have a scheme and a host`. If the `otel_traces_export` section is removed, Beyla fails with `you need to define at least one exporter`.

### K8s sidecar container

The DaemonSet embeds the Beyla sidecar in the pod template:

```yaml
- name: beyla
  image: grafana/beyla:latest
  env:
    - name: BEYLA_CONFIG_PATH
      value: /etc/beyla/config.yaml
  securityContext:
    privileged: true  # Required for eBPF
  volumeMounts:
    - name: beyla-config
      mountPath: /etc/beyla/config.yaml
      subPath: beyla-config.yaml
      readOnly: true
```

> **Note**: `privileged: true` on the Beyla container is required for eBPF. The agent container remains non-root + read-only.

### Config file

The Beyla config is defined in `omniwatch-agent/configs/beyla-config.yaml` and mounted into both Compose and K8s deployments. The K8s DaemonSet includes an inline ConfigMap (`omniwatch-beyla-config`) that is byte-identical to this file.

### OTLP endpoint

Beyla exports to the same OTel Collector endpoint as the agent:
- **Compose**: `otelcol:4317`
- **K8s**: `otelcol.omniwatch:4317` (ClusterDNS form)

### eBPF requirement

Beyla requires eBPF support. It will not function in:
- Environments with `CONFIG_BPF=n` in the kernel.
- Container runables that block `CAP_BPF` or `CAP_SYS_ADMIN`.
- Windows nodes.

### Discovery modes

The `discovery.instrument` entries are OR'd — a process matching **any** entry is instrumented:

| Entry | Mode | Where it works |
|-------|------|----------------|
| `k8s_namespace: "*"` | Kubernetes | K8s DaemonSet — discovers all pods except excluded namespaces |
| `exe_path: "*"` | Executable path glob | Any host — matches processes by binary path |
| `open_ports: 80,443,8000-8999` | Open port match | Any host — instruments processes listening on those ports |

`discovery.exclude_instrument` skips `kube-system` (stacked on top of Beyla's built-in default exclusions for itself, Alloy, and otelcol). The `ebpf.wakeup_len` tunes eBPF ring-buffer wakeups (lower = lower latency, higher = less overhead).

### Kernel requirements

Beyla needs Linux kernel **>= 5.10** with eBPF enabled (`CONFIG_BPF`/`CONFIG_BPF_SYSCALL`) and a mounted bpffs (`mount -t bpf bpffs /sys/fs/bpf`). The agent runs an advisory preflight at boot (`internal/beyla`) that logs either `beyla preflight: eBPF supported` or a `Warn` with the exact remediation — this check is **non-fatal**: an unsupported host only degrades Beyla traces, the agent's receivers still run. It will not function on Windows nodes or kernels with `CONFIG_BPF=n`.

### Privileged justification

The Beyla container runs with `privileged: true` (K8s) / `cap_add: [SYS_ADMIN]` (Compose) because attaching eBPF probes requires `CAP_BPF`/`CAP_SYS_ADMIN` plus access to the host pid namespace and bpffs. This is scoped to the Beyla sidecar **only** — the agent container stays non-root (`65532`), read-only root filesystem, no `hostNetwork`/`hostPID`. (K8s manifest detail lives in `k8s/omniwatch-agent/daemonset.yaml`.)

### Resolved: discovery-config error

The old minimal config (no `discovery` section) made Beyla log `wrong Beyla configuration: missing application discovery section or network metrics configuration`. The production `discovery.instrument` section above eliminates exactly this error — Beyla now logs its discovery modes at startup instead.

---

## Data Types Covered vs Other Components

This agent covers the following data types directly:

| # | Data Type | Source | Implementation |
|---|-----------|--------|----------------|
| 1 | **Metrics** | Cloud Services & MicroServices | **PARTIAL** — hostmetrics receiver (node-level system metrics). Application-level metrics collected via OTel SDK instrumentation in services themselves (exported directly to otelcol). |
| 2 | **Events** | Cloud Services & MicroServices | **COVERED** — k8s_objects receiver (pod events, node events). |
| 3 | **Logs** | Cloud Services & MicroServices | **COVERED** — filelog receiver (application logs from `/var/log/pods`, `/var/log/containers`). |
| 4 | **Traces** | Cloud Services & MicroServices | **COVERED** — Beyla sidecar (eBPF auto-instrumentation) + OTel SDK. |
| 5 | **Metadata/State** | Kubernetes Pods | **COVERED** — k8s_objects receiver (pod labels, annotations, owner references, phase, resources). |
| 6 | **Audit Logs** | Kubernetes Pods | **COVERED** — filelog receiver (K8s audit logs from `/var/log/kubernetes/audit`). |

**Data Types Handled by Other Components:**

| # | Data Type | Source | Handled By |
|---|-----------|--------|------------|
| 7 | **Profiling Data** | Kubernetes Pods | Continuous profiling agent (separate component, e.g., Parca/Pyroscope) |
| 8 | **SIEM Events** | Security Systems | Security ingestion layer (separate pipeline) |
| 9 | **Authentication Logs** | Security Systems | Security ingestion layer (separate pipeline) |
| 10 | **Security Alerts** | Security Systems | Security ingestion layer (separate pipeline) |

---

## Troubleshooting

### Agent health returns 503 on `/ready`

**Symptom**: `curl http://localhost:8080/ready` returns `{"status":"not_ready","collector_status":"starting"}`.

**Cause**: The collector has not yet started or has entered a degraded state.

**Fix**: Wait 10–15 seconds after startup. If the issue persists, check agent logs:

```bash
docker logs omniwatch-agent
# or
kubectl -n omniwatch logs -l app=omniwatch-agent -c omniwatch-agent
```

### OTLP export fails — "connection refused"

**Symptom**: Agent logs show repeated `exporter: create OTLP trace exporter: connection refused`.

**Cause**: The OTel Collector (`otelcol`) is not running or the endpoint is wrong.

**Fix**: Verify the OTel Collector is running and the endpoint matches:

```bash
# Check otelcol is up
docker compose ps otelcol
# or
kubectl -n omniwatch get pods -l app=otelcol

# Verify the endpoint in config
grep -A2 'otlp:' configs/agent.yaml
```

### Beyla discovery-config error

**Symptom**: Beyla container logs show `error="discovery-config"` or similar.

**Cause**: The minimal plan config block (`executable_name: ""`) triggers a Beyla internal validation warning. This is a known limitation.

**Impact**: Beyla may still function for trace collection, but with degraded service discovery.

**Fix**: This is tracked as a future probe (F3). For now, the error is non-fatal — traces are still emitted.

### OTLP receipt invisible in default otelcol logs

**Symptom**: Agent is sending OTLP data but the OTel Collector logs show no trace/metric/log receipts.

**Cause**: The default OTel Collector configuration does not log received telemetry. This is expected — receipt is confirmed by downstream storage, not otelcol logs.

**Fix**: Check the destination backend (ClickHouse, Prometheus, etc.) for received data. To enable otelcol debug logging, add a `debug` exporter to the otelcol config.

### `whoami` impossible in distroless

**Symptom**: Running `kubectl exec ... -- whoami` fails with `exec: "whoami": executable file not found`.

**Cause**: The distroless image does not include shell utilities.

**Fix**: This is expected. Check the running user via:

```bash
kubectl -n omniwatch exec -it <pod> -c omniwatch-agent -- \
  cat /etc/passwd
# → nonroot:x:65532:65532
```

### Filelog receiver collects no logs

**Symptom**: No log records appear in the OTel Collector.

**Cause**: The host log directories are not mounted into the container.

**Fix**: Verify the volume mounts:

```bash
# Compose
docker inspect omniwatch-agent | grep -A5 Mounts

# K8s
kubectl -n omniwatch get pod <pod> -o jsonpath='{.spec.containers[0].volumeMounts}'
```

Ensure `/var/log/pods`, `/var/log/containers`, and `/var/log/kubernetes/audit` are mounted read-only from the host.

---

## Development

### Project structure

```
omniwatch-agent/
├── cmd/agent/main.go              # Entry point, flag parsing, OTel init, health server
├── internal/
│   ├── collector/collector.go     # Pipeline wiring, heartbeat loop
│   ├── config/config.go           # YAML + env-var config loading
│   ├── exporter/exporter.go       # OTel SDK init (traces, metrics, logs)
│   └── health/health.go           # /health and /ready HTTP endpoints
├── configs/
│   ├── agent.yaml                 # Receiver/exporter configuration
│   └── beyla-config.yaml          # Beyla sidecar configuration
├── Dockerfile                     # 2-stage distroless build
├── go.mod                         # Go module (github.com/omniwatch/omniwatch-agent)
├── go.sum                         # Dependency checksums
└── README.md                      # This file
```

### Build

```bash
cd omniwatch-agent

# Build binary (Windows)
go build -o bin/agent.exe ./cmd/agent

# Build binary (Linux, for container)
CGO_ENABLED=0 GOOS=linux go build -o agent ./cmd/agent

# Build Docker image
docker build -t omniwatch-agent:latest .
```

### Test

```bash
cd omniwatch-agent
go test ./... -count=1 -v
```

### Key endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Liveness probe — always returns 200 if process is alive |
| `/ready` | GET | Readiness probe — returns 200 only when collector is running |

### Self-telemetry

The agent emits heartbeats every `collection_interval` (default: 60s) consisting of:
- **One span**: `agent.heartbeat` with `agent.build` and `collection.interval` attributes.
- **One metric**: `omniwatch.agent.heartbeat` counter with `agent.build` attribute.
- **One log record**: severity INFO, body `"agent heartbeat"`, with `agent.build` and `otlp.endpoint` attributes.

All three signals exercise the full OTLP pipeline (SDK → OTel Collector) on every tick.

### Health server states

| State | Description | `/health` | `/ready` |
|-------|-------------|-----------|----------|
| `starting` | Collector not yet started | 200 | 503 |
| `running` | Collector collecting normally | 200 | 200 |
| `stopped` | Collector shut down | 200 | 503 |
| `degraded` | Collector encountered an error | 200 | 503 |

### RBAC permissions

The ClusterRole grants read-only access (`get`, `list`, `watch`) to:

- Core API: `pods`, `nodes`, `services`, `endpoints`, `namespaces`, `events`
- Apps API: `deployments`, `replicasets`, `daemonsets`, `statefulsets`
