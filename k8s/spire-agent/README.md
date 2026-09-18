# spire-agent — K8s manifests

SPIRE agent DaemonSet for OmniWatch workload attestation
(plan checkbox `- [ ] 1. TLS/mTLS module (SPIRE + mTLS for OTLP)`).
The omniwatch-agent fetches its mTLS identity from this DaemonSet's
Workload API instead of using shared cert files.

## Files

| File | What it does |
|------|--------------|
| `daemonset.yaml` | DaemonSet (one spire-agent per node, image `spire-agent:1.9+`) |
| `configmap.yaml` | `agent.conf` (trust domain `example.org`, k8s + unix attestors) |
| `rbac.yaml` | ServiceAccount + ClusterRole (pods/nodes read) + Binding |
| `README.md` | This file |

## Apply

```bash
kubectl apply -f k8s/spire-agent/rbac.yaml
kubectl apply -f k8s/spire-agent/configmap.yaml
kubectl apply -f k8s/spire-agent/daemonset.yaml

# Validate without a cluster:
kubectl apply --dry-run=client -f k8s/spire-agent/rbac.yaml
kubectl apply --dry-run=client -f k8s/spire-agent/configmap.yaml
kubectl apply --dry-run=client -f k8s/spire-agent/daemonset.yaml
```

## Wiring to omniwatch-agent

- Socket: spire-agent serves `unix:///tmp/spire-agent/public/api.sock`
  (SPIRE default), backed by hostPath `/tmp/spire-agent/public`.
- The omniwatch-agent DaemonSet must mount the SAME hostPath so its
  `tls.spiffe_socket_path` default works verbatim:

```yaml
volumeMounts:
- name: spire-socket
  mountPath: /tmp/spire-agent/public
  readOnly: true
volumes:
- name: spire-socket
  hostPath:
    path: /tmp/spire-agent/public
    type: DirectoryOrCreate
```

- Trust domain `example.org` matches `configs/agent.yaml` (`tls.trust_domain`)
  and `internal/config` defaults.
- Dev mode is unchanged: with `OMNIWATCH_EXPORTER_OTLP_INSECURE=true` the
  agent skips SPIRE entirely (loud warning in logs).

## Notes

- Requires a SPIRE server (`spire-server.omniwatch:8081`) with matching
  trust domain and a join token / PSAT attestation — server deployment is
  out of scope for IND-1.
- `hostPID`/`hostNetwork` on the DaemonSet are required for k8s workload
  attestation (same as upstream SPIRE examples).
