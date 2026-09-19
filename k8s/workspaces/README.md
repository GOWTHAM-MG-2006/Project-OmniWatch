# k8s/workspaces — workspace namespace mapping (ENTRY-2, todo 2)

Cluster-deploy mapping for isolated workspaces. **Nothing here is
provisioned automatically** — `POST /workspaces` never creates namespaces
(plan guardrail). An operator maps a workspace to a namespace by hand:

| Workspace slug | Namespace |
|----------------|-----------|
| `<slug>` | `omniwatch-ws-<slug>` |
| `default` | `omniwatch` (today's namespace) |

Files:

- `namespace-template.yaml` — Namespace manifest template. Replace `<slug>`
  with the workspace slug and `kubectl apply -f -`. Labels mirror the
  `k8s/learning/` style (`app`, `tier`-equivalent component labels).
- This README — the mapping note (deliberately not a controller).

Validation (no cluster touched):

```bash
kubectl apply --dry-run=client -f k8s/workspaces/namespace-template.yaml
```
