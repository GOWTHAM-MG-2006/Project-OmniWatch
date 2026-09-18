// OmniWatch — Agent
// Component: alerting (SLOs)
// Phase: industry-ready (IND-5)
// Purpose: SLO targets and availability math for agent alerting
// Inputs: Good/total event counts per signal
// Outputs: Availability percentage + meets/misses verdict
package alerting

import "time"

// SLO is one service-level objective: the minimum good-event ratio over a
// window, expressed as a percentage (99.9 = three nines).
type SLO struct {
	Name   string
	Target float64
	Window time.Duration
}

// Production SLOs for the agent, documented in the README SLO table.
var (
	HealthAvailabilitySLO = SLO{Name: "health-availability", Target: 99.9, Window: 30 * 24 * time.Hour}
	ExportSuccessSLO      = SLO{Name: "otlp-export-success", Target: 99.0, Window: 30 * 24 * time.Hour}
	ExportLatencySLO      = SLO{Name: "otlp-export-p99-latency", Target: 5, Window: 7 * 24 * time.Hour}
)

// Availability returns good/total as a percentage in [0, 100]. A zero total
// yields 0 rather than NaN so empty windows read as "no evidence of health".
func Availability(good, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return float64(good) / float64(total) * 100
}

// Meets reports whether good/total reaches the SLO target. For the latency
// SLO the target is a seconds bound — compare p99 observations against it
// with P99Within instead.
func (s SLO) Meets(good, total uint64) bool {
	return Availability(good, total) >= s.Target
}

// P99Within reports whether an observed p99 latency in seconds is inside the
// export latency SLO (under 5s).
func P99Within(p99Seconds float64) bool {
	return p99Seconds < ExportLatencySLO.Target
}
