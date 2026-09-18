# omniwatch-agent

## Beyla Integration

[Beyla](https://grafana.com/oss/beyla/) is a Grafana eBPF-based auto-instrumentation tool that
produces OpenTelemetry traces and metrics with zero source-code changes. In OmniWatch it
runs as a **sidecar container** that instruments target services (HTTP/gRPC databases, Kafka
clients, etc.) at the kernel level and exports OTLP traces directly to the OTel Collector.

### Docker Compose Usage

The `beyla` service is defined in the root `docker-compose.yml`:

```bash
# Start Beyla (depends on otelcol being up)
docker compose up -d beyla

# Check logs
docker logs -f omniwatch-beyla

# Stop
docker compose down beyla
```

**Configuration file:** `omniwatch-agent/configs/beyla-config.yaml` (mounted to `/etc/beyla/config.yaml`).

The config enables Kubernetes metadata attributes (`k8s.namespace.name`, `k8s.pod.name`,
`k8s.deployment.name`) so traces produced in-cluster carry pod-level identity. In Docker
Compose mode (no K8s), Beyla still emits traces — the K8s attributes are simply empty.

**OTLP Endpoint:** `otelcol:4317` (insecure, same collector used by all OmniWatch services).

**eBPF Requirement:** Beyla requires a Linux kernel >= 5.10 with `SYS_ADMIN` capability.
On Windows Docker Desktop (WSL2-backed), eBPF may not be available — the container will
start but log an error. This is expected; production deployment targets GKE (Linux nodes).

### K8s Sidecar Container Snippet (T10 Embed)

The following YAML block is the exact sidecar container spec to embed in the
DaemonSet (T10) or the omniwatch-agent Pod spec:

```yaml
      - name: beyla
        image: grafana/beyla:latest
        env:
          - name: BEYLA_CONFIG_PATH
            value: /etc/beyla/config.yaml
        securityContext:
          privileged: true
        volumeMounts:
          - name: beyla-config
            mountPath: /etc/beyla/config.yaml
            subPath: beyla-config.yaml
            readOnly: true
```

Corresponding volume (add to the Pod spec `volumes:` list):

```yaml
      volumes:
        - name: beyla-config
          configMap:
            name: omniwatch-beyla-config
```

The ConfigMap (`omniwatch-beyla-config`) must be created from
`omniwatch-agent/configs/beyla-config.yaml`:

```bash
kubectl create configmap omniwatch-beyla-config \
  --from-file=beyla-config.yaml=omniwatch-agent/configs/beyla-config.yaml
```

**Key notes for T10:**
- `privileged: true` (or `SYS_ADMIN` capability) is required for eBPF.
- Beyla instruments all processes on the node (DaemonSet pattern) or
  specific pods via `BEYLA_OPEN_PORT` / `executable_name` filter.
- The `subPath` mount ensures Beyla sees a single file, not a directory.
- OTLP endpoint is `otelcol:4317` (service name in K8s matches the OTel
  Collector Service).

### Troubleshooting

```bash
# Beyla logs (compose)
docker logs omniwatch-beyla 2>&1 | tail -50

# Check OTLP connectivity (Beyla → otelcol)
docker exec omniwatch-beyla env | grep BEYLA_CONFIG

# Verify otelcol is receiving traces from Beyla
docker logs omniwatch-otelcol 2>&1 | grep -i beyla

# eBPF capability check (Linux only)
docker exec omniwatch-beyla cat /proc/1/status | grep CapEff
# Expected: CapEff: 00000000a80425fb (SYS_ADMIN = bit 21)
```
