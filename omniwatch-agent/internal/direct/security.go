// OmniWatch — Agent
// Component: direct security tail
// Phase: exporter-importer-split
// Purpose: Best-effort security batches from real host auth logs. Lines are
// only ever forwarded when they exist on disk — absence yields zero batches,
// never synthetic rows.
// Inputs: /var/log/auth.log or /var/log/secure (Linux only)
// Outputs: one audit_logs ingest batch (send-once; the same host lines are
// not duplicated across auth/siem/alert types)
package direct

import (
	"bufio"
	"os"
	"runtime"
	"strings"
	"time"
)

var authLogPaths = []string{"/var/log/auth.log", "/var/log/secure"}

// suspiciousSubstrings marks lines worth forwarding (case-insensitive).
var suspiciousSubstrings = []string{
	"failed", "failure", "invalid", "denied", "unauthorized",
	"break-in", "intrusion", "attack", "exploit", "malware",
}

// TailSecurity scans the host auth log and returns one audit batch built
// only from lines actually present. Empty when the log is absent. Lines are
// sent once (not fanned out across auth/siem/alert types) so downstream
// counts and the ingest rate budget reflect real event volume.
func (c *Client) TailSecurity() []IngestBody {
	if runtime.GOOS != "linux" {
		return nil
	}
	lines := tailSuspicious(200)
	if len(lines) == 0 {
		return nil
	}
	return buildSecurityBatches(lines, c.cfg.EntityID, time.Now().UTC().Format(time.RFC3339))
}

// buildSecurityBatches is the pure line→batch mapping, unit-testable without
// touching host log paths.
func buildSecurityBatches(lines []string, entity, now string) []IngestBody {
	logs := make([]LogPoint, 0, len(lines))
	for _, l := range lines {
		logs = append(logs, LogPoint{
			LogLevel: "WARN", Message: l,
			ServiceName: "omniwatch-agent", Timestamp: now,
		})
	}
	return []IngestBody{
		{TelemetryType: TypeAuditLogs, EntityID: entity, EntityType: "API_NODE", Logs: logs},
	}
}

// tailSuspicious returns up to the last maxLines suspicious lines of the
// first readable auth log, oldest-first.
func tailSuspicious(maxLines int) []string {
	var path string
	for _, p := range authLogPaths {
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			path = p
			break
		}
	}
	if path == "" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	var hits []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 64*1024)
	for sc.Scan() {
		line := sc.Text()
		lower := strings.ToLower(line)
		matched := false
		for _, sub := range suspiciousSubstrings {
			if strings.Contains(lower, sub) {
				matched = true
				break
			}
		}
		if matched {
			hits = append(hits, line)
		}
	}
	if len(hits) > maxLines {
		hits = hits[len(hits)-maxLines:]
	}
	return hits
}
