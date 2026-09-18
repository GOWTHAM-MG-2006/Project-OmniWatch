# Runbook: Agent Health Degraded

**Alert**: `AgentHealthDown` — `omniwatch_agent_health == 0` for > 2 minutes  
**Severity**: P2  
**Impact**: Agent is not reporting health; telemetry collection may be degraded

---

## Symptoms

- Alert `AgentHealthDown` fires: `omniwatch_agent_health == 0` for > 2 minutes
- `curl http://localhost:8080/health` returns non-200 or connection refused
- Collector status shows `stopping` or `error` in agent logs

## Diagnosis

### 1. Check pod status

```bash
kubectl -n omniwatch get pods -l app=omniwatch-agent -o wide
kubectl -n omniwatch describe pod <pod-name>
```

Look for: `CrashLoopBackOff`, `OOMKilled`, `Error`, or restart counts > 0.

### 2. Check agent logs

```bash
kubectl -n omniwatch logs -l app=omniwatch-agent -c omniwatch-agent --tail=100
```

Look for:
- `otelcol failed to start` — collector failed to initialize
- `health check failed` — health endpoint not responding
- `OOMKilled` — memory exceeded limit (512Mi)

### 3. Check collector status

```bash
kubectl -n omniwatch logs -l app=omniwatch-agent -c otelcol --tail=50
```

Look for: `failed to start`, `address already in use`, `permission denied`.

### 4. Check resource usage

```bash
kubectl -n omniwatch top pod <pod-name>
```

Verify memory < 512Mi and CPU < 500m.

## Fix

### Fix 1: OOMKilled

The agent exceeded its 512Mi memory limit.

```bash
# Check current memory usage trend
kubectl -n omniwatch top pod <pod-name> --containers

# Increase memory limit temporarily
kubectl -n omniwatch patch deployment omniwatch-agent -p \
  '{"spec":{"template":{"spec":{"containers":[{"name":"omniwatch-agent","resources":{"limits":{"memory":"768Mi"}}}]}}}}'
```

### Fix 2: Collector failed to start

```bash
# Check collector config
kubectl -n omniwatch get configmap omniwatch-agent-config -o yaml

# Restart the pod
kubectl -n omniwatch delete pod <pod-name>
```

### Fix 3: Port conflict

Another process is using port 8080:

```bash
# Find the conflicting process
kubectl -n omniwatch exec -it <pod-name> -c omniwatch-agent -- \
  ss -tlnp | grep 8080

# Resolve conflict or change the agent's port
```

### Fix 4: RBAC issue

```bash
# Verify ClusterRole permissions
kubectl get clusterrole omniwatch-agent -o yaml

# Re-apply RBAC if needed
kubectl apply -f k8s/omniwatch-agent/rbac.yaml
```

## Verify

```bash
# 1. Health returns OK
curl http://localhost:8080/health
# → {"status":"ok","collector_status":"running"}

# 2. Ready returns OK
curl http://localhost:8080/ready
# → {"status":"ready","collector_status":"running"}

# 3. No OOMKilled restarts
kubectl -n omniwatch describe pod <pod-name> | grep -A3 "Last State"

# 4. Queue drops are zero
curl -s http://localhost:8080/metrics | grep queue_dropped
```
