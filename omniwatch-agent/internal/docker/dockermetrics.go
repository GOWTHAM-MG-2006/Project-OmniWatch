// OmniWatch — Agent
// Component: docker dockermetrics (container stats poller)
// Phase: real-application-telemetry (Item 3)
// Purpose: Sample one-shot Engine stats for allowlisted containers and emit
// real per-container cpu/mem series. Same allowlist + enabled semantics as
// the log tailer: empty allowlist = disabled, heartbeat-only mode.
// Inputs: Docker Client + container allowlist
// Outputs: []direct.IngestBody (TypeMetrics), one batch per emission
package docker

import (
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/omniwatch/omniwatch-agent/internal/direct"
)

// Poller samples container stats for an allowlist of container names.
type Poller struct {
	client    *Client
	entityID  string
	allowlist map[string]bool
	logger    *slog.Logger
}

// NewPoller builds a stats poller. Empty containers disables polling —
// Collect then returns nil (backward compatible with old YAML).
func NewPoller(client *Client, entityID string, containers []string, logger *slog.Logger) *Poller {
	if logger == nil {
		logger = slog.Default()
	}
	allow := make(map[string]bool, len(containers))
	for _, n := range containers {
		if n = strings.TrimSpace(n); n != "" {
			allow[n] = true
		}
	}
	return &Poller{client: client, entityID: entityID, allowlist: allow, logger: logger}
}

// Enabled reports whether polling is configured (non-empty allowlist).
func (p *Poller) Enabled() bool { return len(p.allowlist) > 0 }

// Collect samples one-shot stats from allowlisted running containers and
// packs them into one metrics batch. SourceType is left empty so the
// importer records these as real performance series (unlike the
// SourceType=heartbeat liveness metrics). Failures degrade to nil.
func (p *Poller) Collect() []direct.IngestBody {
	if !p.Enabled() {
		return nil
	}
	containers, err := p.client.List()
	if err != nil {
		p.logger.Warn("docker poller: list failed, skipping emission", "error", err)
		return nil
	}
	sort.Slice(containers, func(i, j int) bool { return containers[i].Name() < containers[j].Name() })
	now := time.Now().UTC().Format(time.RFC3339)
	var points []direct.MetricPoint
	for _, c := range containers {
		name := c.Name()
		if !p.allowlist[name] || c.State != "running" {
			continue
		}
		st, err := p.client.Stats(name)
		if err != nil {
			p.logger.Warn("docker poller: stats failed", "container", name, "error", err)
			continue
		}
		tags := map[string]string{"container": name, "image": c.Image}
		points = append(points,
			direct.MetricPoint{MetricName: "container_cpu_percent", Value: st.CPUPercent, Tags: tags, Timestamp: now},
			direct.MetricPoint{MetricName: "container_mem_used_percent", Value: st.MemUsedPercent(), Tags: tags, Timestamp: now},
			direct.MetricPoint{MetricName: "container_mem_used_bytes", Value: float64(st.MemUsedBytes), Tags: tags, Timestamp: now},
		)
	}
	if len(points) == 0 {
		return nil
	}
	return []direct.IngestBody{{
		TelemetryType: direct.TypeMetrics,
		EntityID:      p.entityID,
		EntityType:    "API_NODE", // conservative: same as heartbeat until EntityResolutionJob mapping is verified
		Metrics:       points,
	}}
}
