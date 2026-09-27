# autotune — Phase1 advisory self-tuning observer

Read-only control loop that **emits recommendations without applying them**.
Pure addition: no wiring into config paths, no behavior change.

## What it does

`Observe(Signals) []Recommendation` evaluates a point-in-time snapshot of
four signal families and returns proposed values + reasons:

| Signal family | Observed fields | Proposal when degraded |
|---|---|---|
| `queue` | `QueueLen/Capacity/Dropped` | `resilience.queue_size` → 2× (cap 10000) |
| `breaker` | `BreakerState/ConsecutiveFailures` | `resilience.retry_max_attempts` → +1 (cap 5) |
| `export` | `ExportP99Latency/FailRate` | `agent.collection_interval` → 2× (cap 5m) |
| `hostmetrics` | `HostmetricsInterval` + queue pressure | `receiver.hostmetrics.collection_interval` → 2× |

Healthy agent → empty slice → caller stays silent.

## Emission

`Observer.Emit(ctx, signals)` reports each recommendation once as:

- **log**: `slog.Info("omniwatch.agent.autotune.recommendation", param, current, proposed, reason, mode="advisory")`
- **metric**: OTel counter `omniwatch.agent.autotune.recommendation` +1 per
  recommendation with `param/current/proposed` attributes.

Both sinks are nil-tolerant (nil logger → `slog.Default`, nil counter →
log-only), matching the `resilience` package convention.

## Phase1 boundaries (must NOT do)

- No import of `config`, `collector`, or `exporter` — signals are copied in.
- No mutation of inputs, config structs, or live components.
- No goroutines, tickers, or background loops (Phase2 control-loop concern).
- No new config knobs or env vars.

## Future (not Phase1)

Phase2 wires a periodic `Emit` call from the collector drain path and
feeds `Signals` from live `QueueLen/QueueDropped`, `breaker.State/Counts`,
export latency, and receiver intervals. Phase3+ may close the loop
(apply with policy gates). This package stays the pure decision function.

## Test

```bash
cd omniwatch-agent
go test ./internal/autotune/ -count=1 -v
```
