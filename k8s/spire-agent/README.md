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
- DYN-goagent: the omniwatch-agent DaemonSet now mounts the SAME hostPath
  (`spire-socket` volume, `DirectoryOrCreate`) so its
  `tls.spiffe_socket_path` default works verbatim. Follow-up closed.
  Pre-DYN trees still need the manual block below:

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

## mTLS handshake proof (future-checklist item 2)

### A. Local proof — no cluster needed (done)

```bash
cd omniwatch-agent
go test ./internal/tls/ -run TestMTLSHandshake -v
```

- `TestMTLSHandshakeMutualVerification` (`internal/tls/mtls_handshake_test.go`):
  mints a throwaway CA + server/client certs, starts a real gRPC server
  with `RequireAndVerifyClientCert`, dials it with the agent's own
  `NewTLSCredentials` (file-fallback path — same constructor OTLP export
  uses), and asserts the server observed the client's
  `spiffe://example.org/omniwatch/hs-client` ID.
- `TestMTLSHandshakeRejectsUncertifiedClient`: a client presenting no
  certificate never reaches `READY` — mutual verification is enforced,
  not optional.

### B. In-cluster 2-pod proof (needs a SPIRE server)

The local test above proves the handshake logic. This proves it with
real SVIDs issued by SPIRE. Requires a SPIRE server at
`spire-server.omniwatch:8081` with trust domain `example.org`
(server deployment itself is out of scope — see Notes).

```bash
# 1. Deploy the SPIRE agent (this directory)
kubectl apply -f k8s/spire-agent/rbac.yaml
kubectl apply -f k8s/spire-agent/configmap.yaml
kubectl apply -f k8s/spire-agent/daemonset.yaml
kubectl -n omniwatch rollout status daemonset/spire-agent

# 2. On the SPIRE server, register the two test workloads so both pods
#    get SVIDs under the same trust domain:
#    spiffe://example.org/ns/omniwatch/sa/mtls-probe-server
#    spiffe://example.org/ns/omniwatch/sa/mtls-probe-client

# 3. Launch pod-A (server) and pod-B (client) from the agent image.
#    Both mount the SAME spire-socket hostPath the DaemonSets share
#    (socket unix:///tmp/spire-agent/public/api.sock, trust domain
#    example.org — matches configs/agent.yaml tls.* defaults):
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: mtls-probe-server
  namespace: omniwatch
spec:
  serviceAccountName: spire-agent
  containers:
  - name: probe
    image: omniwatch-agent:latest
    env:
    - name: OMNIWATCH_AUTH_MODE
      value: mtls
    - name: OMNIWATCH_TLS_SPIFFE_SOCKET_PATH
      value: unix:///tmp/spire-agent/public/api.sock
    - name: OMNIWATCH_TLS_TRUST_DOMAIN
      value: example.org
    volumeMounts:
    - name: spire-socket
      mountPath: /tmp/spire-agent/public
      readOnly: true
  volumes:
  - name: spire-socket
    hostPath:
      path: /tmp/spire-agent/public
      type: DirectoryOrCreate
---
apiVersion: v1
kind: Pod
metadata:
  name: mtls-probe-client
  namespace: omniwatch
spec:
  serviceAccountName: spire-agent
  containers:
  - name: probe
    image: omniwatch-agent:latest
    env:
    - name: OMNIWATCH_AUTH_MODE
      value: mtls
    - name: OMNIWATCH_TLS_SPIFFE_SOCKET_PATH
      value: unix:///tmp/spire-agent/public/api.sock
    - name: OMNIWATCH_TLS_TRUST_DOMAIN
      value: example.org
    volumeMounts:
    - name: spire-socket
      mountPath: /tmp/spire-agent/public
      readOnly: true
  volumes:
  - name: spire-socket
    hostPath:
      path: /tmp/spire-agent/public
      type: DirectoryOrCreate
EOF

# 4. Verify: both pods attested and fetched a live SVID (no cert-file
#    fallback — OMNIWATCH_TLS_CERT_FILE / _KEY_FILE stay unset):
kubectl -n omniwatch logs mtls-probe-server -c probe | grep "mTLS identity acquired from SPIRE"
kubectl -n omniwatch logs mtls-probe-client -c probe | grep "mTLS identity acquired from SPIRE"

# 5. Verify mutual handshake: run the probe gRPC server in pod-A and the
#    probe client in pod-B (both via the agent's tls.Resolve path), then
#    confirm each side logged the peer's SPIFFE ID:
#    server sees spiffe://example.org/ns/omniwatch/sa/mtls-probe-client
#    client sees spiffe://example.org/ns/omniwatch/sa/mtls-probe-server

# 6. Clean up:
kubectl -n omniwatch delete pod mtls-probe-server mtls-probe-client
```

## Notes

- Requires a SPIRE server (`spire-server.omniwatch:8081`) with matching
  trust domain and a join token / PSAT attestation — server deployment is
  out of scope for IND-1.
- `hostPID`/`hostNetwork` on the DaemonSet are required for k8s workload
  attestation (same as upstream SPIRE examples).
