# omniwatch-agent — K8s manifests

DaemonSet + RBAC + Service for the OmniWatch host agent
(plan checkbox `- [ ] 10. K8s DaemonSet Manifest (Non-Root, ReadOnlyRootFilesystem)`).

## Files

| File | What it does |
|------|--------------|
| `daemonset.yaml` | DaemonSet (one agent pod per node) + inline `omniwatch-beyla-config` ConfigMap |
| `rbac.yaml` | ServiceAccount + ClusterRole + ClusterRoleBinding |
| `service.yaml` | ClusterIP Service exposing the agent health port (8080) |

## Apply

```bash
# Provide the image the DaemonSet references (T9 built this exact tag):
docker build -t omniwatch-agent:latest ./omniwatch-agent

# Apply (order matters: RBAC first so the DaemonSet's ServiceAccount exists)
kubectl apply -f k8s/omniwatch-agent/rbac.yaml
kubectl apply -f k8s/omniwatch-agent/daemonset.yaml
kubectl apply -f k8s/omniwatch-agent/service.yaml

# Validate without a cluster (plan QA scenario):
kubectl apply --dry-run=client -f k8s/omniwatch-agent/daemonset.yaml
kubectl apply --dry-run=client -f k8s/omniwatch-agent/rbac.yaml
kubectl apply --dry-run=client -f k8s/omniwatch-agent/service.yaml

# Live checks (needs a cluster):
kubectl -n omniwatch get daemonset omniwatch-agent
kubectl -n omniwatch get pods -l app=omniwatch-agent
curl http://<pod-ip>:8080/health
curl http://<pod-ip>:8080/ready
```

## Namespace mapping

Workspaces provision K8s namespaces using the pattern:

```
omniwatch-ws-<slug>
```

Where `<slug>` is the workspace slug auto-generated during provisioning
(`identity/provision.py`). For example, workspace `Acme Backend` → slug
`acme-backend` → namespace `omniwatch-ws-acme-backend`.

When deploying this DaemonSet into a workspace-scoped namespace, update
the `namespace` field in each manifest and set the `OMNIWATCH_*` env vars
(see [`docs/workspace-isolation.md`](../../docs/workspace-isolation.md)):

```bash
# Apply to a workspace namespace:
kubectl apply -f k8s/omniwatch-agent/rbac.yaml -n omniwatch-ws-acme-backend
kubectl apply -f k8s/omniwatch-agent/daemonset.yaml -n omniwatch-ws-acme-backend
kubectl apply -f k8s/omniwatch-agent/service.yaml -n omniwatch-ws-acme-backend
```

## Non-root notes

- Pod `securityContext`: `runAsNonRoot: true`, `runAsUser: 65532`,
  `readOnlyRootFilesystem: true`.
- `65532` (not the plan text's `65534`): T9 proved the distroless runtime user
  `nonroot` is UID 65532 (evidence `.omo/evidence/task-9-nonroot.txt`,
  `/etc/passwd` extraction `nonroot:x:65532:65532`), so the K8s identity
  matches the image USER.
- No privileged containers (agent), no hostNetwork, no hostPID, no root user.
  The agent container mounts `/var/log`, `/var/lib/docker/containers`, and
  `/var/log/kubernetes/audit` read-only from the host.

## Beyla sidecar note

- The pod template embeds the T8 Beyla sidecar verbatim
  (`grafana/beyla:latest`, `BEYLA_CONFIG_PATH=/etc/beyla/config.yaml`,
  `privileged: true`, subPath mount of `beyla-config`).
- `privileged: true` on the **Beyla container only** is required for eBPF;
  the agent container stays non-root + read-only.
- The `beyla-config` volume is sourced from the `omniwatch-beyla-config`
  ConfigMap defined as a second `---` document inside `daemonset.yaml`
  (self-contained choice — content is byte-identical to
  `omniwatch-agent/configs/beyla-config.yaml`). Alternative:
  `kubectl create configmap omniwatch-beyla-config --from-file=beyla-config.yaml=omniwatch-agent/configs/beyla-config.yaml`.
- Known limitation (T8/F3): Beyla logs a discovery-config error with the
  minimal plan config block; production tuning is an F3 probe.

## Env / wiring

- Agent image: `omniwatch-agent:latest` (tag T9 built via
  `docker build -t omniwatch-agent`; compose builds a project-prefixed tag —
  retag or rebuild with the exact tag before deploying).
- `OMNIWATCH_EXPORTER_OTLP_ENDPOINT=otelcol.omniwatch:4317` (cluster DNS form;
  compose uses `otelcol:4317`).
- Probes hit the agent's real endpoints on port 8080: liveness `/health`,
  readiness `/ready` (both proven 200 in T9).

## Resource quotas

The DaemonSet enforces requests and limits on the agent container to prevent node resource exhaustion:

| Resource | Request | Limit | Rationale |
|----------|---------|-------|-----------|
| CPU | 500m | (none) | Host metrics + log collection + OTel SDK overhead |
| Memory | 256Mi | 512Mi | Bounded by `queue_size` + OTel batch processor |
| oomScoreAdj | -100 | -100 | Prevents OOM-kill under node pressure (critical daemon) |

The Beyla sidecar has **no resource limits** — eBPF probe attach is bursty; its userspace footprint is small and kernel-managed.

```yaml
# From daemonset.yaml
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
    resources: {}   # no limits — eBPF attach is bursty
    securityContext:
      privileged: true
```

## Auth wiring

The agent authenticates to the OTLP endpoint via `auth.mode` in `configs/agent.yaml`:

| Mode | How it works | K8s env var |
|------|--------------|-------------|
| `dev` | No auth; `insecure: true` | `OMNIWATCH_AUTH_MODE=dev` |
| `apikey` | `Authorization: Bearer <key>` header | `OMNIWATCH_AUTH_MODE=apikey`, `OMNIWATCH_AUTH_API_KEY=<key>` |
| `mtls` | X.509 client cert via SPIRE SVID | `OMNIWATCH_AUTH_MODE=mtls` (requires SPIRE socket mount) |

**Health handler**: `health_handler` controls whether `:8080 /health` and `:8080 /ready` require authentication:

- `auto` (default): no auth in `dev` mode; auth required in `apikey`/`mtls`
- `require`: always require auth on health endpoints (breaks K8s probes unless you add probe headers)
- `optional`: never require auth on health endpoints (safe for K8s probes regardless of mode)

For K8s deployments, set `OMNIWATCH_AUTH_HEALTH_HANDLER=optional` to keep probes working when `auth.mode` is `apikey` or `mtls`.

## TLS / mTLS

The agent supports TLS for OTLP gRPC export. When `tls.enabled: true`, mount certificates into the pod:

```yaml
volumeMounts:
  - name: tls-certs
    mountPath: /etc/omniwatch/tls
    readOnly: true
volumes:
  - name: tls-certs
    secret:
      secretName: omniwatch-agent-tls
```

Environment variables to set:

| Env Var | Description |
|---------|-------------|
| `OMNIWATCH_TLS_ENABLED` | `true` to enable TLS |
| `OMNIWATCH_TLS_CERT_FILE` | Path to client certificate (e.g. `/etc/omniwatch/tls/tls.crt`) |
| `OMNIWATCH_TLS_KEY_FILE` | Path to client private key |
| `OMNIWATCH_TLS_CA_FILE` | Path to CA certificate (optional; uses system CA if empty) |

**SPIRE integration**: For mTLS in production clusters, mount the SPIRE agent socket:

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

Certificates rotate every **24 hours** (SPIRE default). The agent reloads them transparently — no restart required.

**Health endpoint**: `:8080 /health` and `:8080 /ready` always serve **plaintext HTTP** — Kubernetes liveness/readiness probes do not support client certificates.

## SPIRE socket note

When using SPIRE for mTLS, the SPIRE agent socket lives at:

```
unix:///tmp/spire-agent/public/api.sock
```

Mount the host's SPIRE agent socket directory read-only at `/tmp/spire-agent/public`. The trust domain for OmniWatch is `example.org` (matching the SPIRE server configuration).
