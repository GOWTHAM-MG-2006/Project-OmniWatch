# Runbook: Leader Election Flap

**Symptom**: Leader election toggling rapidly between replicas  
**Severity**: P3  
**Impact**: Processing pauses during transitions; duplicate work if two leaders active briefly

---

## Symptoms

- Logs show repeated `leader elected` / `leader lost` messages within seconds
- Lease object `omniwatch-causal-leader` (or similar) has rapid `acquireTime` changes
- Processing pauses visible as gaps in output

## Diagnosis

### 1. Check Lease object

```bash
kubectl -n omniwatch get lease omniwatch-causal-leader -o yaml
```

Look for:
- `acquireTime` changing within seconds
- `holderIdentity` switching between pod names
- `leaseDurationSeconds` set correctly (default: 15)

### 2. Check elector logs

```bash
kubectl -n omniwatch logs -l app=omniwatch-causal --tail=100 | grep -i "leader"
```

Look for:
- `leader elected: <pod>` followed quickly by `leader lost: <pod>`
- `lease renewal failed` — network or API server issue

### 3. Check K8s API server latency

```bash
# From the pod
kubectl -n omniwatch exec -it <pod-name> -c omniwatch-causal -- \
  wget -qO- https://kubernetes.default.svc/healthz
```

High latency (> 500ms) can cause renewal failures and lease expiry.

### 4. Check network connectivity

```bash
kubectl -n omniwatch exec -it <pod-name> -c omniwatch-causal -- \
  nslookup kubernetes.default.svc

kubectl -n omniwatch exec -it <pod-name> -c omniwatch-causal -- \
  curl -sk https://kubernetes.default.svc/version
```

### 5. Check clock skew

```bash
# From each node
date -u
```

Clock skew > `leaseDurationSeconds` (15s) causes false leader expiry.

## Fix

### Fix 1: Increase lease duration

If flapping due to API server latency, increase the lease duration:

```bash
kubectl -n omniwatch set env deployment/omniwatch-causal \
  OMNIWATCH_LEADER_ELECTION_LEASE_DURATION=30
```

### Fix 2: Reduce renewal interval

More frequent renewals prevent false expiry:

```bash
# Modify common/leaderelection.py if needed
# renew_interval = lease_duration / 5  (instead of default / 3)
```

### Fix 3: Disable leader election (temporary)

If flapping is blocking all processing:

```bash
kubectl -n omniwatch set env deployment/omniwatch-causal \
  OMNIWATCH_LEADER_ELECTION_ENABLED=false
```

**Warning**: This runs all replicas in standalone mode — no deduplication. Only use temporarily.

### Fix 4: Fix API server issues

```bash
# Check API server health
kubectl get componentstatuses

# Check API server logs (admin access required)
# Look for high latency, etcd issues, or admission controller errors
```

### Fix 5: Fix network issues

```bash
# Check pod network connectivity
kubectl -n omniwatch exec -it <pod-name> -c omniwatch-causal -- \
  ping -c 3 kubernetes.default.svc

# Check DNS resolution
kubectl -n omniwatch exec -it <pod-name> -c omniwatch-causal -- \
  cat /etc/resolv.conf
```

### Fix 6: Fix clock skew

```bash
# On nodes with skew, sync NTP
sudo ntpdate -u pool.ntp.org

# Ensure NTP is running
sudo systemctl enable --now chrony  # or ntpd
```

## Verify

```bash
# 1. Lease is stable
kubectl -n omniwatch get lease omniwatch-causal-leader -o yaml | grep acquireTime
# → Timestamp should be stable (not changing within seconds)

# 2. Leader logs are calm
kubectl -n omniwatch logs -l app=omniwatch-causal --tail=50 | grep leader
# → Single "leader elected" entry, no rapid toggling

# 3. Processing is continuous
# Check output logs for gaps — processing should flow without pauses
```

## Behavior Notes

- Leader election is **disabled by default** (`OMNIWATCH_LEADER_ELECTION_ENABLED=false`)
- Health endpoints (`/health`, `/ready`) are **not gated** — always serve regardless of leadership
- When `enabled=false` or K8s API unavailable: standalone mode (`is_leader()` always True)
- Lease renewal runs at `lease_duration / 3` intervals (default: every 5s)
- The agent itself does **not** use leader election — only causal/predictive/learning components
