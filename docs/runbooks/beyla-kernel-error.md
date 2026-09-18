# Runbook: Beyla Kernel Error

**Symptom**: Beyla container crash-looping with kernel compatibility error  
**Severity**: P3  
**Impact**: eBPF auto-instrumentation unavailable; host metrics and log receivers still work

---

## Symptoms

- Beyla container in `CrashLoopBackOff`
- Agent logs show: `beyla preflight: <Warn>` with kernel remediation
- No eBPF traces being generated

## Diagnosis

### 1. Check Beyla logs

```bash
kubectl -n omniwatch logs -l app=omniwatch-agent -c beyla --tail=50
```

Look for:
- `wrong Beyla configuration: missing application discovery section` — config issue
- `you need to define at least one exporter` — missing `otel_traces_export`
- `URL must have a scheme and a host` — endpoint missing `http://` prefix

### 2. Check kernel version

```bash
# From the node (requires SSH or node shell)
uname -r
# → 5.10.0+ required

# From inside the pod (may not reflect host kernel)
kubectl -n omniwatch exec -it <pod-name> -c beyla -- uname -r
```

### 3. Check eBPF support

```bash
# From the node
grep CONFIG_BPF /boot/config-$(uname -r)
# Should show:
# CONFIG_BPF=y
# CONFIG_BPF_SYSCALL=y
```

### 4. Check Beyla config

```bash
kubectl -n omniwatch get configmap beyla-config -o yaml
```

Verify:
- `discovery.instrument` section exists with at least one entry
- `otel_traces_export.endpoint` has scheme (`http://...`)
- `otel_traces_export.protocol` is set to `grpc`

## Fix

### Fix 1: Kernel too old (< 5.10)

```bash
# Option A: Upgrade the node kernel
sudo apt update && sudo apt install linux-generic-hwe-20.04  # Ubuntu
sudo yum update kernel  # RHEL/CentOS

# Option B: Use BCC fallback receiver (no eBPF needed)
# Edit configs/agent.yaml, replace beyla receiver with:
# receivers:
#   bcc:
#     type: tcp
#     interface: eth0
```

### Fix 2: Missing discovery section

```yaml
# Add to configs/beyla-config.yaml
discovery:
  instrument:
    - image: "api-gateway"
    - image: "order-service"
    - image: "user-service"
    - namespace: "default"
      annotation:
        omniwatch.io/beyla: "enabled"
```

### Fix 3: Missing otel_traces_export

```yaml
# Add to configs/beyla-config.yaml
otel_traces_export:
  endpoint: http://otelcol:4317
  protocol: grpc
  # Add headers for API key auth if needed
  # headers:
  #   Authorization: Bearer ${OMNIWATCH_AUTH_API_KEY}
```

### Fix 4: URL scheme missing

```yaml
# Wrong (bare endpoint):
otel_traces_export:
  endpoint: otelcol:4317

# Correct (with scheme):
otel_traces_export:
  endpoint: http://otelcol:4317
  protocol: grpc
```

### Fix 5: Beyla container restart

```bash
# Restart just the Beyla sidecar
kubectl -n omniwatch delete pod <pod-name>
# DaemonSet will recreate it
```

## Verify

```bash
# 1. Beyla container is Running
kubectl -n omniwatch get pods -l app=omniwatch-agent -o jsonpath='{.items[*].status.containerStatuses[*].name}:{.items[*].status.containerStatuses[*].ready}'
# → beyla:true omniwatch-agent:true

# 2. Preflight check passes
kubectl -n omniwatch logs -l app=omniwatch-agent -c beyla --tail=5 | grep preflight
# → beyla preflight: eBPF supported

# 3. Traces are being exported
kubectl -n omniwatch logs -l app=omniwatch-agent -c otelcol --tail=20 | grep trace
```

## Impact Note

Beyla is optional for the agent's core functionality. Even if Beyla fails:

- **Host metrics receiver**: Still collects CPU, memory, disk, network metrics
- **Filelog receiver**: Still collects container logs
- **K8s objects receiver**: Still collects pod/node/deployment metadata
- **Agent health**: Still reports OK on `/health` and `/ready`

Only eBPF auto-instrumentation (traces) is unavailable.
