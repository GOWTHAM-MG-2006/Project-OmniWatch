// OmniWatch — Agent
// Component: docker applogs (container log tailer)
// Phase: real-application-telemetry (Item 2)
// Purpose: Tail allowlisted container logs via the Engine API and convert new
// lines into importer logs batches. Bounded rings keep a t3.micro box safe;
// an absent container allowlist disables the tailer (heartbeat-only mode).
// Inputs: Docker Client + container allowlist + tail caps
// Outputs: []direct.IngestBody (TypeLogs), one batch per emission
package docker

import (
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/omniwatch/omniwatch-agent/internal/direct"
)

// MaxLinesPerContainer bounds lines taken from one container per emission.
// MaxLinesPerEmission bounds the whole logs batch (rate-budget guard: the
// importer allows 60 ingest requests/min; logs ride one request).
const (
	MaxLinesPerContainer = 200
	MaxLinesPerEmission  = 1000
)

// tailState tracks per-container progress between emissions.
type tailState struct {
	lastSeenUnix int64
	initialized  bool
}

// Tailer tails container logs for an allowlist of container names.
type Tailer struct {
	client    *Client
	entityID  string
	allowlist map[string]bool
	tail      int
	logger    *slog.Logger
	state     map[string]*tailState
}

// NewTailer builds a log tailer. Empty containers disables tailing — Collect
// then returns nil and the agent stays in heartbeat-only mode (backward
// compatible with old YAML that has no docker keys). tail <= 0 selects the
// default first-run cap.
func NewTailer(client *Client, entityID string, containers []string, tail int, logger *slog.Logger) *Tailer {
	if logger == nil {
		logger = slog.Default()
	}
	if tail <= 0 {
		tail = 100
	}
	allow := make(map[string]bool, len(containers))
	for _, n := range containers {
		if n = strings.TrimSpace(n); n != "" {
			allow[n] = true
		}
	}
	return &Tailer{client: client, entityID: entityID, allowlist: allow, tail: tail, logger: logger, state: map[string]*tailState{}}
}

// Enabled reports whether tailing is configured (non-empty allowlist).
func (t *Tailer) Enabled() bool { return len(t.allowlist) > 0 }

// Collect fetches new lines from allowlisted running containers and packs
// them into one logs batch. Failures degrade to "no new lines" (warned once
// per emission) — a dead socket must never break the heartbeat path.
func (t *Tailer) Collect() []direct.IngestBody {
	if !t.Enabled() {
		return nil
	}
	containers, err := t.client.List()
	if err != nil {
		t.logger.Warn("docker tailer: list failed, skipping emission", "error", err)
		return nil
	}
	sort.Slice(containers, func(i, j int) bool { return containers[i].Name() < containers[j].Name() })
	now := time.Now().UTC().Format(time.RFC3339)
	var points []direct.LogPoint
	for _, c := range containers {
		if len(points) >= MaxLinesPerEmission {
			break
		}
		name := c.Name()
		if !t.allowlist[name] || c.State != "running" {
			continue
		}
		st := t.state[name]
		if st == nil {
			st = &tailState{}
			t.state[name] = st
		}
		var lines []direct.LogPoint
		if !st.initialized {
			lines = t.fetch(name, 0, st) // first run: last N lines, no since filter
			st.initialized = true
		} else {
			lines = t.fetch(name, st.lastSeenUnix, st)
		}
		for _, lp := range lines {
			if len(points) >= MaxLinesPerEmission {
				break
			}
			lp.ServiceName = name
			lp.Timestamp = now
			points = append(points, lp)
		}
	}
	if len(points) == 0 {
		return nil
	}
	return []direct.IngestBody{{
		TelemetryType: direct.TypeLogs,
		EntityID:      t.entityID,
		EntityType:    "API_NODE", // conservative: same as heartbeat until EntityResolutionJob mapping is verified
		Logs:          points,
	}}
}

// fetch pulls up to MaxLinesPerContainer new lines and advances the cursor.
func (t *Tailer) fetch(name string, since int64, st *tailState) []direct.LogPoint {
	lines, err := t.client.Logs(name, since, MaxLinesPerContainer)
	if err != nil {
		t.logger.Warn("docker tailer: logs failed", "container", name, "error", err)
		return nil
	}
	var out []direct.LogPoint
	var maxTS int64 = since
	for _, ln := range lines {
		if ln.Message == "" {
			continue
		}
		out = append(out, direct.LogPoint{LogLevel: "INFO", Message: ln.Message})
		if ln.TimestampUnix > maxTS {
			maxTS = ln.TimestampUnix
		}
	}
	// Advance the cursor only forward; when the daemon gave no timestamps,
	// keep the old cursor so the next emission re-reads rather than skips.
	// (Duplicate lines are preferable to lost lines on clock skew.)
	if maxTS > st.lastSeenUnix {
		st.lastSeenUnix = maxTS
	}
	return out
}
