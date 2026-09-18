// OmniWatch — Agent
// Component: alerting (rule exporter)
// Phase: industry-ready (IND-5)
// Purpose: Export PrometheusRule rules as OTLP log records to otelcol
// Inputs: Loaded RuleFile + OTel log.Logger emitter
// Outputs: One INFO log record per rule on the existing log pipeline
package alerting

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/log"
)

// instrumentationScope identifies log records produced by this exporter.
const instrumentationScope = "github.com/omniwatch/omniwatch-agent/internal/alerting"

// BreakerStateMetric is the gauge the otelcol rule processor derives from
// CircuitBreaker.State() (IND-2 exposes the accessor, not a counter):
// 0 = closed (healthy), 1 = open (failing fast), 2 = half-open (probing).
// The AgentCircuitBreakerOpen rule fires on value == 1.
const BreakerStateMetric = "omniwatch.agent.breaker_state"

// Emitter is the sink records are written to: the OTel log.Logger in
// production (it satisfies this interface structurally via Emit), a stub in
// tests. A dedicated interface is required because log.Logger carries an
// unexported method and cannot be implemented outside the OTel module.
type Emitter interface {
	Emit(ctx context.Context, rec log.Record)
}

// Options configures a RuleExporter. Emitter is never nil in practice — New
// rejects an empty Options so a missing logger fails fast at wiring time
// instead of silently dropping rules.
type Options struct {
	Emitter Emitter
}

// RuleExporter renders each PrometheusRule rule as an OTLP log record and
// emits it on the existing log pipeline — extending the agent heartbeat log
// schema with alert.* attributes, never replacing it. The otelcol rule
// processor picks the records up by the alert.name attribute prefix.
type RuleExporter struct {
	emitter Emitter
}

// New builds a RuleExporter from opts.
func New(opts Options) *RuleExporter {
	return &RuleExporter{emitter: opts.Emitter}
}

// ExportRule emits a single grouped rule as one INFO log record. The body
// carries the alert name and expression; group, severity, and runbook travel
// as attributes so the otelcol rule processor can route without parsing bodies.
func (e *RuleExporter) ExportRule(ctx context.Context, gr GroupedRule) error {
	r := gr.Rule
	group := gr.Group
	if e.emitter == nil {
		return fmt.Errorf("alerting: no log emitter configured")
	}
	if r.Alert == "" || r.Expr == "" {
		return fmt.Errorf("alerting: rule missing alert/expr")
	}
	rec := log.Record{}
	rec.SetSeverity(log.SeverityInfo)
	rec.SetBody(log.StringValue("alerting rule " + r.Alert + ": " + r.Expr))
	rec.AddAttributes(
		log.String("alert.name", r.Alert),
		log.String("alert.expr", r.Expr),
		log.String("alert.group", group),
		log.String("alert.severity", r.Labels["severity"]),
		log.String("alert.for", r.For),
		log.String("alert.runbook", r.Annotations["runbook"]),
		log.String("service.name", "omniwatch-agent"),
	)
	e.emitter.Emit(ctx, rec)
	return nil
}

// ExportAll emits every rule in f, stopping at the first error.
func (e *RuleExporter) ExportAll(ctx context.Context, f *RuleFile) error {
	if f == nil {
		return fmt.Errorf("alerting: nil rule file")
	}
	for _, gr := range f.Flatten() {
		if err := e.ExportRule(ctx, gr); err != nil {
			return err
		}
	}
	return nil
}
