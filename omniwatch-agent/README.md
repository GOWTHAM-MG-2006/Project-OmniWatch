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
- [Alerting and SLOs](#alerting-and-slos)
- [Runbooks](#runbooks)
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
    collection_interval: 3m
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
    collection_interval: 3m
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
| `OMNIWATCH_RECEIVER_K8S_OBJECTS_COLLECTION_INTERVAL` | duration | `3m` | K8s object polling interval |
| `OMNIWATCH_RECEIVER_K8S_OBJECTS_MODE` | string | `pull` | K8s objects mode (`pull` or `watch`) |
| `OMNIWATCH_RECEIVER_K8S_OBJECTS_LABEL_SELECTOR` | string | (empty) | K8s label selector filter |
| `OMNIWATCH_RECEIVER_K8S_OBJECTS_FIELD_SELECTOR` | string | (empty) | K8s field selector filter |
| `OMNIWATCH_RECEIVER_K8S_CLUSTER_COLLECTION_INTERVAL` | duration | `3m` | K8s cluster receiver interval |
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

## Alerting and SLOs

Alert rules live in `configs/alerts/prometheusrules.yaml` (PrometheusRule shape:
groups + rules with `expr`/`for`/`labels`/`annotations`). There is no
Prometheus server or Alertmanager in this stack — the otelcol rule processor
loads the file and evaluates the rules against OTLP metrics from the agent.
The `internal/alerting` package loads the same file at boot (`LoadRules`) and
re-exports every rule as an OTLP log record (`RuleExporter`: body
`alerting rule <name>: <expr>`, attributes `alert.name`, `alert.severity`,
`alert.expr`, `alert.group`, `alert.for`, `alert.runbook`) so rule delivery
itself is observable on the existing log pipeline.

| Rule | Signal | Meaning |
|------|--------|---------|
| `AgentHealthDown` | `up{job="omniwatch-agent"} == 0` for 2m | `:8080/health` probe failing |
| `AgentHeartbeatMissing` | `absent(omniwatch_agent_heartbeat_total)` for 5m | No heartbeats arriving at otelcol |
| `AgentOTLPExportFailing` | `rate(omniwatch_agent_heartbeat_total[5m]) == 0` for 5m | OTLP export success collapsed |
| `AgentExportLatencyHigh` | p99 export latency `> 5` for 10m | Export latency above SLO |
| `AgentQueueDropping` | `rate(omniwatch_agent_queue_dropped_total[5m]) > 0` for 5m | Bounded queue (drop-oldest, IND-2) full |
| `AgentCircuitBreakerOpen` | `omniwatch_agent_breaker_state == 1` for 1m | Gobreaker circuit open (5 errs / 30s, IND-2) |

The breaker gauge is derived from `CircuitBreaker.State()`: `0` closed,
`1` open, `2` half-open (see `internal/alerting.BreakerStateMetric`).

### SLOs

| SLO | Target | Window | Rule that guards it |
|-----|--------|--------|---------------------|
| `/health` availability | **99.9%** | 30 days | `AgentHealthDown`, `AgentHeartbeatMissing` |
| OTLP export success | **99%** | 30 days | `AgentOTLPExportFailing` |
| OTLP export latency p99 | **< 5s** | 7 days | `AgentExportLatencyHigh` |

Availability math lives in `internal/alerting` (`Availability`,
`SLO.Meets`, `P99Within`) and is covered by table tests.

---

## Runbooks

### Runbook: AgentHealthDown

**Symptom**: `up{job="omniwatch-agent"} == 0` for 2 minutes.

**Diagnosis steps**:
1. Check the pod is scheduled: `kubectl -n omniwatch get pods -l app=omniwatch-agent`.
2. Check liveness directly: `curl http://localhost:8080/health` (expect 200).
3. Check recent restarts/OOMKills: `kubectl -n omniwatch describe pod <pod>` (see resource limits, IND-4).
4. Read agent logs for panics: `kubectl -n omniwatch logs -l app=omniwatch-agent -c omniwatch-agent`.

### Runbook: AgentHeartbeatMissing

**Symptom**: no `omniwatch.agent.heartbeat` datapoints for 5 minutes.

**Diagnosis steps**:
1. Verify the agent process is alive: `curl http://localhost:8080/health`.
2. Verify the collector loop started: look for `collector starting` in logs.
3. Check the OTLP endpoint in config: `grep -A2 'otlp:' configs/agent.yaml`.
4. Check otelcol is receiving: inspect the destination backend for recent datapoints.

### Runbook: AgentOTLPExportFailing

**Symptom**: heartbeat export rate is zero over 5 minutes.

**Diagnosis steps**:
1. Verify otelcol is up: `docker compose ps otelcol` or `kubectl -n omniwatch get pods -l app=otelcol`.
2. Look for `heartbeat export failed (guarded)` warnings in agent logs.
3. Check for TLS errors: without SPIRE, secure mode exits non-zero; dev mode needs `insecure: true`.
4. Check whether the breaker opened (`AgentCircuitBreakerOpen` firing) — if so, follow that runbook.

### Runbook: AgentExportLatencyHigh

**Symptom**: p99 export latency above the 5s SLO for 10 minutes.

**Diagnosis steps**:
1. Check otelcol load and downstream backend latency (ClickHouse/Prometheus).
2. Check whether retry backoff is saturated: `100ms → 500ms → 2s`, 3 attempts (IND-2).
3. Check network latency to the OTLP endpoint: time a `ForceFlush` cycle.
4. Consider raising `resilience.retry_max_attempts` or lowering collection pressure.

### Runbook: AgentQueueDropping

**Symptom**: `omniwatch.agent.queue_dropped` increasing — the bounded heartbeat
queue is full and oldest emissions are dropped.

**Diagnosis steps**:
1. Confirm export is the bottleneck: `AgentOTLPExportFailing` or high latency usually fires alongside.
2. Check queue depth via `Collector.QueueLen()` / `QueueDropped()` (exposed for observability).
3. Fix the export path first (endpoint, TLS, otelcol health) — drops stop once drain keeps up.
4. If drops persist with a healthy endpoint, raise `resilience.queue_size` (default 1000).

### Runbook: AgentCircuitBreakerOpen

**Symptom**: `omniwatch_agent_breaker_state == 1` — the gobreaker circuit
(5 consecutive errors / 30s window, 30s open, IND-2) is open and exports fail
fast without touching the endpoint.

**Diagnosis steps**:
1. Fix the underlying endpoint failure (otelcol down, TLS misconfig, wrong endpoint).
2. Wait for the 30s open timeout, then watch for the half-open probe: success closes the circuit.
3. Check `breaker.Counts()` consecutive failures to confirm the failure class (auth vs network).
4. Do not restart the agent to "clear" the breaker — the open state is protecting the endpoint.

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
│   ├── alerting/                # Rule loading, OTLP rule export, SLO math (IND-5)
│   ├── collector/collector.go     # Pipeline wiring, heartbeat loop
│   ├── config/config.go           # YAML + env-var config loading
│   ├── exporter/exporter.go       # OTel SDK init (traces, metrics, logs)
│   └── health/health.go           # /health and /ready HTTP endpoints
├── configs/
│   ├── agent.yaml                 # Receiver/exporter configuration
│   ├── alerts/prometheusrules.yaml # Alert rules for otelcol rule processor (IND-5)
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

---

## TLS / mTLS

The agent supports TLS for OTLP gRPC export to the OpenTelemetry Collector.

### Configuration

```yaml
# configs/agent.yaml
tls:
  enabled: true
  cert_file: /etc/omniwatch/tls/tls.crt
  key_file: /etc/omniwatch/tls/tls.key
  ca_file: /etc/omniwatch/tls/ca.crt        # optional: custom CA
  insecure_skip_verify: false                # never true in production
```

Environment variable overrides:

| Env Var | Type | Default | Description |
|---------|------|---------|-------------|
| `OMNIWATCH_TLS_ENABLED` | bool | `false` | Enable TLS for OTLP gRPC export |
| `OMNIWATCH_TLS_CERT_FILE` | string | (empty) | Path to client certificate |
| `OMNIWATCH_TLS_KEY_FILE` | string | (empty) | Path to client private key |
| `OMNIWATCH_TLS_CA_FILE` | string | (empty) | Path to CA certificate (system CA if empty) |
| `OMNIWATCH_TLS_INSECURE_SKIP_VERIFY` | bool | `false` | Skip server certificate verification (dev only) |

### SPIRE integration (mTLS)

In production clusters with SPIRE, the agent mounts the SVID at:

```
unix:///tmp/spire-agent/public/api.sock
```

The SPIRE agent socket is mounted read-only into the pod:

```yaml
volumeMounts:
  - name: spire-agent-socket
    mountPath: /tmp/spire-agent/public
    readOnly: true
volumes:
  - name: spire-agent-socket
    hostPath:
      path: /run/spire/sockets
      type: Directory
```

When `tls.enabled: true` and the SPIRE socket is available, the agent automatically fetches X.509 SVIDs for mTLS. Certificate rotation happens every **24 hours** (SPIRE default). The agent reloads certificates transparently — no restart required.

### Health endpoint

The health server (`:8080 /health`, `:8080 /ready`) always serves **plaintext HTTP** — it is not TLS-protected. This is intentional: Kubernetes liveness/readiness probes do not support client certificates.

---

## Auth

The agent authenticates to the OTLP endpoint using one of three modes, configured via `auth.mode` in `configs/agent.yaml`.

### Modes

| Mode | How it works | When to use |
|------|--------------|-------------|
| `dev` | No authentication; `insecure: true` required | Local development, Docker Compose |
| `apikey` | `Authorization: Bearer <key>` header on every OTLP export | Staging, simple production setups |
| `mtls` | X.509 client certificate via SPIRE SVID | Production clusters with SPIRE |

### Configuration

```yaml
# configs/agent.yaml
auth:
  mode: dev                  # dev | apikey | mtls
  api_key: ""                # required when mode=apikey
  health_handler: auto       # auto | require | optional
```

Environment variable overrides:

| Env Var | Type | Default | Description |
|---------|------|---------|-------------|
| `OMNIWATCH_AUTH_MODE` | string | `dev` | Authentication mode (`dev`, `apikey`, `mtls`) |
| `OMNIWATCH_AUTH_API_KEY` | string | (empty) | API key for `apikey` mode |
| `OMNIWATCH_AUTH_HEALTH_HANDLER` | string | `auto` | Health handler auth (`auto`, `require`, `optional`) |

### Health handler modes

The `health_handler` setting controls whether the `:8080` health endpoints require authentication:

- `auto` (default): No auth required in `dev` mode; auth required in `apikey`/`mtls` modes
- `require`: Always require auth on health endpoints (breaks K8s probes unless you configure probe headers)
- `optional`: Never require auth on health endpoints (safe for K8s probes regardless of `auth.mode`)

**K8s probe compatibility**: Set `health_handler: optional` if your health endpoints must remain unauthenticated for Kubernetes liveness/readiness probes, even when `auth.mode` is `apikey` or `mtls`.

### API key header

When `auth.mode: apikey`, the agent sends:

```
grpc-export POST /opentelemetry.proto.collector.trace.v1.TraceService/Export
Headers:
  Authorization: Bearer <OMNIWATCH_AUTH_API_KEY>
  Content-Type: application/grpc
```

The key is read at startup from the environment — it is **not** logged or emitted in telemetry.

---

## Resilience

The agent includes production resilience patterns to handle transient failures in the OTLP export path.

### Circuit breaker

The circuit breaker protects the OTLP endpoint from cascading failures:

| Parameter | Value | Source |
|-----------|-------|--------|
| Failure threshold | 5 consecutive errors | `gobreaker` default |
| Window | 30 seconds | Configurable via `resilience.breaker_window` |
| Open duration | 30 seconds | Before half-open probe |
| Half-open probes | 1 request | Success closes circuit |

```yaml
resilience:
  breaker_enabled: true
  breaker_window: 30s
```

**Metrics**: `omniwatch.agent.breaker_state` gauge (0=closed, 1=open, 2=half-open) is emitted on every export cycle. Alert rule `AgentCircuitBreakerOpen` fires when state == 1 for > 1 minute.

### Retry with backoff

Failed exports are retried with exponential backoff:

| Attempt | Delay | Cumulative |
|---------|-------|------------|
| 1st retry | 100ms | 100ms |
| 2nd retry | 500ms | 600ms |
| 3rd retry | 2s | 2.6s |

```yaml
resilience:
  retry_enabled: true
  retry_max_attempts: 3
  retry_base_delay: 100ms
  retry_max_delay: 2s
```

After 3 failed attempts, the error is counted by the circuit breaker. If the breaker is open, exports fail fast without attempting the connection.

### Bounded queue (drop-oldest)

The agent uses a bounded channel for heartbeat telemetry to prevent unbounded memory growth:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `queue_size` | 1000 | Maximum heartbeats in the buffer |
| Eviction | drop-oldest | When full, the oldest heartbeat is dropped |
| Metric | `omniwatch.agent.queue_dropped` | Counter of dropped heartbeats |

```yaml
resilience:
  queue_size: 1000
```

Drops occur only when export is slower than collection. If drops persist, fix the export path first (endpoint health, TLS, otelcol load). Alert rule `AgentQueueDropping` fires when `rate(omniwatch_agent_queue_dropped_total[5m]) > 0` for 5 minutes.

### Combined behavior

```
Collection tick → enqueue → export attempt
                              ↓
                         success? ──yes──→ done
                              │
                             no
                              ↓
                         retry (up to 3x with backoff)
                              ↓
                         still failing?
                              ↓
                         breaker counts error
                              ↓
                         5 consecutive errors → breaker OPEN
                              ↓
                         exports fail fast for 30s
                              ↓
                         half-open probe → success? → breaker CLOSED
```

---

## Resource Quotas

The agent enforces Kubernetes resource requests and limits to prevent node resource exhaustion.

### Agent container

| Resource | Request | Limit | Rationale |
|----------|---------|-------|-----------|
| CPU | 500m | 256Mi | Host metrics + log collection + OTel SDK overhead |
| Memory | 512Mi | 256Mi | Bounded by `queue_size` + OTel batch processor |
| oomScoreAdj | -100 | -100 | Prevents OOM-kill under node pressure (critical daemon) |

### Beyla sidecar

| Resource | Request | Limit | Rationale |
|----------|---------|-------|-----------|
| CPU | (none) | (none) | eBPF probe attach is bursty; no steady-state CPU |
| Memory | (none) | (none) | Kernel-managed eBPF maps; userspace footprint is small |
| privileged | `true` | — | Required for `CAP_BPF`/`CAP_SYS_ADMIN` |

### K8s DaemonSet resource block

```yaml
containers:
  - name: omniwatch-agent
    resources:
      requests:
        cpu: 500m
        memory: 256Mi
      limits:
        memory: 512Mi
    securityContext:
      oomScoreAdj: -100
  - name: beyla
    resources:
      requests: {}
      limits: {}
    securityContext:
      privileged: true
```

### OOM kill behavior

If the agent exceeds `memory.limit` (512Mi), Kubernetes OOM-kills the pod. The liveness probe (`:8080/health`) then fails, and the DaemonSet controller restarts the pod. The `oomScoreAdj: -100` setting tells the OOM killer to prefer killing other pods first — the agent is a critical daemon.

To monitor OOM kills:

```bash
# Check for OOMKilled restarts
kubectl -n omniwatch describe pod <pod-name> | grep -A3 "Last State"

# Check node memory pressure
kubectl describe node <node-name> | grep -A5 "Conditions"
```

---

## Beyla Production

### Version

Beyla **v3.35.0** is the validated version. The `kernel_version_min: "5.10"` setting in `configs/beyla-config.yaml` enforces the minimum Linux kernel version at startup.

### Kernel requirements

| Requirement | Minimum | Check command |
|-------------|---------|---------------|
| Linux kernel | >= 5.10 | `uname -r` |
| eBPF enabled | `CONFIG_BPF=y` | `grep CONFIG_BPF /boot/config-$(uname -r)` |
| BPF syscall | `CONFIG_BPF_SYSCALL=y` | same as above |
| bpffs mounted | yes | `mount -t bpf bpffs /sys/fs/bpf` |

### Preflight check

The agent runs an advisory preflight at boot (`internal/beyla`) that logs either:

- `beyla preflight: eBPF supported` — kernel is compatible
- `beyla preflight: <Warn>` with exact remediation — kernel is unsupported

This check is **non-fatal**: an unsupported host only degrades Beyla traces; the agent's receivers (hostmetrics, filelog, k8s_objects) still run.

### Production checklist

1. **Kernel**: Verify >= 5.10 with eBPF enabled
2. **Privileged**: Beyla sidecar must have `privileged: true` (agent container stays non-root)
3. **Config**: Use `otel_traces_export` (not legacy `openTelemetry.exporter.otlp`) with explicit scheme:
   ```yaml
   otel_traces_export:
     endpoint: http://otelcol:4317
     protocol: grpc
   ```
4. **Discovery**: `discovery.instrument` entries are OR'd — at least one must match target processes
5. **Exclusions**: `discovery.exclude_instrument` skips `kube-system` (Beyla also excludes itself, Alloy, otelcol)
6. **wakeup_len**: `100` is a balanced default — lower = lower latency, higher = less overhead

### Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `wrong Beyla configuration: missing application discovery section` | No `discovery` block in config | Add `discovery.instrument` section (see `configs/beyla-config.yaml`) |
| `you need to define at least one exporter` | Missing `otel_traces_export` section | Add `otel_traces_export.endpoint` with scheme (`http://...`) |
| `URL must have a scheme and a host` | Bare endpoint (`otelcol:4317`) without scheme | Use `http://otelcol:4317` + `protocol: grpc` |
| Beyla container crash-looping | Kernel < 5.10 or `CONFIG_BPF=n` | Upgrade kernel or use BCC fallback receiver |

---

## Leader Election

Leader election ensures exactly one replica processes at a time for singleton daemons (causal, predictive, learning). The agent itself does **not** use leader election — it runs on every node. This section documents the shared `common/leaderelection.py` used by other OmniWatch components.

### How it works

Uses Kubernetes `coordination.k8s.io/v1` Lease objects:

1. All replicas attempt to acquire the Lease
2. One replica wins and becomes leader
3. Leader renews the Lease every `renew_interval` (default: `lease_duration / 3` = 5s)
4. On leader failure, the Lease expires after `lease_duration` (default: 15s)
5. A follower detects expiry and acquires the Lease

### Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `OMNIWATCH_LEADER_ELECTION_ENABLED` | `false` | Enable leader election |
| `OMNIWATCH_LEADER_ELECTION_LEASE_NAME` | `omniwatch-<service>-leader` | Lease object name |
| `OMNIWATCH_LEADER_ELECTION_LEASE_NAMESPACE` | `omniwatch` | Namespace for the Lease |
| `OMNIWATCH_LEADER_ELECTION_LEASE_DURATION` | `15` | Lease hold time in seconds |

### Usage

```python
from common.leaderelection import build_elector, run_leader_gated
import threading

elector = build_elector(service="causal")
elector.start()

stop_event = threading.Event()
run_leader_gated(
    elector=elector,
    run_fn=my_processing_loop,
    stop_fn=my_cleanup,
    stop_event=stop_event,
)
```

### Health endpoints are NOT gated

Health endpoints (`/health`, `/ready`) always serve regardless of leadership status — gate only the processing loop. This ensures Kubernetes probes work on all replicas.

### Standalone fallback

When `enabled=false` (default) or the K8s API is unavailable, the elector runs in standalone mode: `is_leader()` always returns `True`. No Lease traffic occurs.

---

## Supply Chain

The supply chain ensures reproducible builds, SBOM generation, and image signing for every release.

### Pipeline

Tag pushes (`v*`) trigger `.github/workflows/supply-chain.yml`:

```
SOURCE_DATE_EPOCH=$(git log -1 --format=%ct)
       ↓
make build-reproducible IMAGE="$IMAGE" SOURCE_DATE_EPOCH="$SOURCE_DATE_EPOCH"
       ↓
docker push "$IMAGE"
       ↓
anchore/sbom-action → sbom.spdx.json (SPDX 2.3)
       ↓
cosign sign --yes "$IMAGE" (keyless OIDC via GitHub)
       ↓
cosign verify "$IMAGE"
       ↓
softprops/action-gh-release → attach sbom.spdx.json to release
```

### Reproducible build

```bash
# Set SOURCE_DATE_EPOCH to the last commit timestamp
SOURCE_DATE_EPOCH=$(git log -1 --format=%ct) make build-reproducible

# Verify reproducibility
make verify-reproducible
```

The `SOURCE_DATE_EPOCH` env var pins the build timestamp, producing identical digests across rebuilds.

### SBOM

```bash
make sbom   # → sbom.spdx.json
```

Generated via `syft` in SPDX 2.3 format. The CI workflow validates the format:

```bash
python -c "import json;d=json.load(open('sbom.spdx.json'));assert d['spdxVersion'].startswith('SPDX-2.3')"
```

### Image signing

```bash
make sign    # cosign sign (keyless OIDC in CI)
make verify  # cosign verify
```

Keyless signing uses GitHub OIDC — no private keys or HSM required. The signature is stored in the transparency log (Rekor).

### E2E verification

After deploying the agent image, verify the signature:

```bash
cosign verify ghcr.io/<org>/omniwatch-agent:<tag>
```

This ensures the image was signed by the OmniWatch CI pipeline and has not been tampered with.

---

## E2E Test Guide

### Prerequisites

- Docker Desktop running
- `go test ./...` passes locally
- (Optional) Kubernetes cluster for full E2E

### Test levels

| Level | What it covers | Command |
|-------|----------------|---------|
| Unit | Individual functions, config loading, health states | `go test ./... -count=1` |
| Integration | Collector wiring, OTLP export, resilience patterns | `go test ./... -count=1 -tags=integration` |
| E2E | Full pipeline: agent → otelcol → backend | `make test-e2e` (or see below) |

### Running E2E tests

```bash
# 1. Start the full stack
docker compose up -d

# 2. Wait for all services to be healthy
docker compose ps
# All services should show "Up" status

# 3. Verify agent health
curl http://localhost:8080/health
# → {"status":"ok","collector_status":"running"}

curl http://localhost:8080/ready
# → {"status":"ready","collector_status":"running"}

# 4. Verify telemetry is flowing
# Check otelcol logs for received heartbeats
docker compose logs otelcol --tail=20 | grep heartbeat

# 5. Check for dropped telemetry (should be 0)
curl -s http://localhost:8080/metrics | grep queue_dropped
```

### Anomaly injection tests

Use the anomaly injector to test detection pipelines:

```bash
# Database cascade failure
python simulation/anomaly_injector.py --scenario database_cascade

# Memory leak
python simulation/anomaly_injector.py --scenario memory_leak

# Security attack
python simulation/anomaly_injector.py --scenario security_attack

# Config drift
python simulation/anomaly_injector.py --scenario config_drift
```

### Supply chain verification

```bash
# Verify image signature (requires cosign)
cosign verify ghcr.io/<org>/omniwatch-agent:<tag>

# Verify SBOM
make verify-sbom
```

### K8s E2E (with cluster)

```bash
# Deploy to cluster
kubectl apply -f k8s/omniwatch-agent/rbac.yaml
kubectl apply -f k8s/omniwatch-agent/daemonset.yaml
kubectl apply -f k8s/omniwatch-agent/service.yaml

# Verify all pods are Running
kubectl -n omniwatch get pods -l app=omniwatch-agent

# Check logs for startup
kubectl -n omniwatch logs -l app=omniwatch-agent -c omniwatch-agent --tail=20

# Verify health from inside cluster
kubectl -n omniwatch exec -it <pod-name> -c omniwatch-agent -- \
  wget -qO- http://localhost:8080/health
```

### Troubleshooting E2E failures

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Agent health 503 | Collector not started yet | Wait 10-15s; check logs |
| No heartbeats in otelcol | OTLP endpoint wrong | Verify `configs/agent.yaml` exporter section |
| Beyla crash-looping | Kernel < 5.10 or missing `discovery` | Check kernel version; add `discovery.instrument` |
| Queue drops increasing | Export bottleneck | Fix otelcol; check downstream latency |
| Circuit breaker open | 5+ consecutive errors | Fix endpoint; wait 30s for half-open probe |
