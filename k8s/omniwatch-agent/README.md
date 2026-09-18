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
