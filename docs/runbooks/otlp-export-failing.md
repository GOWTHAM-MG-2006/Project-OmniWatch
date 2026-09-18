# Runbook: OTLP Export Failing

**Alert**: `AgentHeartbeatMissing` — no heartbeats received for > 5 minutes  
**Alert**: `AgentCircuitBreakerOpen` — breaker state == 1 for > 1 minute  
**Severity**: P2  
**Impact**: Telemetry is not reaching the collector; downstream systems receive no data

---

## Symptoms

- Alert `AgentHeartbeatMissing` fires
- `omniwatch_agent_heartbeat_total` counter not incrementing
- `omniwatch_agent.breaker_state == 1` (circuit breaker open)
- `omniwatch_agent.queue_dropped` counter incrementing

## Diagnosis

### 1. Check breaker state

```bash
curl -s http://localhost:8080/metrics | grep breaker_state
# → omniwatch_agent_breaker_state 1  (open = problem confirmed)
```

### 2. Check export errors

```bash
curl -s http://localhost:8080/metrics | grep export_errors
# → omniwatch_agent_export_errors_total 12
```

### 3. Check if OTLP endpoint is reachable

```bash
# From the agent pod
kubectl -n omniwatch exec -it <pod-name> -c omniwatch-agent -- \
  wget -qO- http://otelcol:4317/v1/traces 2>&1 || echo "UNREACHABLE"

# DNS resolution
kubectl -n omniwatch exec -it <pod-name> -c omniwatch-agent -- \
  nslookup otelcol
```

### 4. Check collector status

```bash
kubectl -n omniwatch logs -l app=omniwatch-agent -c otelcol --tail=50
```

Look for: `address already in use`, `connection refused`, `TLS handshake`.

### 5. Check TLS configuration

```bash
# Verify TLS is configured
kubectl -n omniwatch exec -it <pod-name> -c omniwatch-agent -- \
  cat /etc/omniwatch/agent.yaml | grep -A5 "^tls:"

# Verify certificate files exist
kubectl -n omniwatch exec -it <pod-name> -c omniwatch-agent -- \
  ls -la /etc/omniwatch/tls/
```

### 6. Check queue drops

```bash
curl -s http://localhost:8080/metrics | grep queue_dropped
# → omniwatch_agent_queue_dropped_total 47
```

## Fix

### Fix 1: OTLP endpoint down

```bash
# Restart the collector
kubectl -n omniwatch rollout restart deployment otelcol

# Wait for readiness
kubectl -n omniwatch rollout status deployment/otelcol --timeout=60s
```

### Fix 2: TLS mismatch

Agent has `tls.enabled: true` but collector expects plaintext (or vice versa):

```bash
# Check agent TLS config
kubectl -n omniwatch get configmap omniwatch-agent-config -o jsonpath='{.data.agent\.yaml}' | grep -A5 tls

# Temporarily disable TLS for testing (dev only)
kubectl -n omniwatch set env deployment/omniwatch-agent OMNIWATCH_TLS_ENABLED=false
```

### Fix 3: Circuit breaker auto-recovery

The circuit breaker opens for 30 seconds after 5 consecutive errors, then attempts a half-open probe:

```bash
# Wait for half-open probe
watch -n 5 'curl -s http://localhost:8080/metrics | grep breaker_state'
# → state transitions: 1 (open) → 2 (half-open) → 0 (closed) on success
```

If breaker stays open, fix the underlying export issue first.

### Fix 4: Queue drops

```bash
# Check drop rate
curl -s http://localhost:8080/metrics | grep queue_dropped

# If drops are persistent, increase queue size temporarily
kubectl -n omniwatch set env deployment/omniwatch-agent OMNIWATCH_RESILIENCE_QUEUE_SIZE=2000
```

## Verify

```bash
# 1. Breaker is closed
curl -s http://localhost:8080/metrics | grep breaker_state
# → omniwatch_agent_breaker_state 0

# 2. Heartbeats are flowing
curl -s http://localhost:8080/metrics | grep heartbeat_total

# 3. No queue drops
curl -s http://localhost:8080/metrics | grep queue_dropped
# → (empty or 0)

# 4. Collector logs show received data
kubectl -n omniwatch logs -l app=omniwatch-agent -c otelcol --tail=10 | grep heartbeat
```
