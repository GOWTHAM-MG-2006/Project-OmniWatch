// OmniWatch — Agent
// Component: docker tests
// Phase: real-application-telemetry (Item 7)
// Purpose: Fixture-based unit tests for framing, parsing, allowlist, bounds.
// A fake Engine API over a real unix socket covers List/Logs/Stats; a missing
// socket proves graceful degradation. No daemon, no network needed.
package docker

import (
	"encoding/binary"
	"net"
	"net/http"
	"path/filepath"
	"testing"
)

// frame builds one multiplexed Docker log frame for fixtures.
func frame(stream byte, payload string) []byte {
	var hdr [8]byte
	hdr[0] = stream
	binary.BigEndian.PutUint32(hdr[4:8], uint32(len(payload)))
	return append(hdr[:], payload...)
}

func TestDemuxFramed(t *testing.T) {
	raw := append(frame(1, "hello\n"), frame(2, "world\n")...)
	if got := string(demux(raw)); got != "hello\nworld\n" {
		t.Fatalf("demux framed = %q", got)
	}
}

func TestDemuxRawPassthrough(t *testing.T) {
	raw := []byte("plain log line\n")
	if got := string(demux(raw)); got != string(raw) {
		t.Fatalf("demux raw = %q", got)
	}
}

func TestSplitLogLinesStripsTimestamps(t *testing.T) {
	payload := []byte("2024-05-01T10:00:00.123456789Z first line\nno-timestamp line\n\n")
	lines := splitLogLines(payload)
	if len(lines) != 2 {
		t.Fatalf("lines = %d, want 2", len(lines))
	}
	if lines[0].Message != "first line" || lines[0].TimestampUnix == 0 {
		t.Errorf("line0 = %+v", lines[0])
	}
	if lines[1].Message != "no-timestamp line" || lines[1].TimestampUnix != 0 {
		t.Errorf("line1 = %+v", lines[1])
	}
}

// fakeEngine serves canned Engine API responses over a unix socket.
func fakeEngine(t *testing.T) *Client {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "docker.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1.24/containers/json", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[
			{"Id":"aaa","Names":["/aws-frontend"],"Image":"frontend:1","State":"running"},
			{"Id":"bbb","Names":["/aws-valkey"],"Image":"valkey:8","State":"running"},
			{"Id":"ccc","Names":["/stopped-one"],"Image":"x:1","State":"exited"}
		]`))
	})
	mux.HandleFunc("/v1.24/containers/aws-frontend/logs", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("timestamps") != "true" {
			t.Errorf("timestamps param missing")
		}
		body := frame(1, "2024-05-01T10:00:01Z GET / 200\n") 
		body = append(body, frame(1, "2024-05-01T10:00:02Z GET /cart 200\n")...)
		_, _ = w.Write(body)
	})
	mux.HandleFunc("/v1.24/containers/aws-frontend/stats", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{
			"cpu_stats":{"cpu_usage":{"total_usage":2000000000,"percpu_usage":[1000000000,1000000000]},"system_cpu_usage":20000000000,"online_cpus":2},
			"precpu_stats":{"cpu_usage":{"total_usage":1000000000},"system_cpu_usage":10000000000},
			"memory_stats":{"usage":104857600,"limit":536870912}
		}`))
	})
	go func() { _ = http.Serve(ln, mux) }()
	t.Cleanup(func() { _ = ln.Close() })
	return New(sock)
}

func TestListNames(t *testing.T) {
	cs, err := fakeEngine(t).List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(cs) != 3 || cs[0].Name() != "aws-frontend" {
		t.Fatalf("containers = %+v", cs)
	}
}

func TestLogsAndStats(t *testing.T) {
	c := fakeEngine(t)
	lines, err := c.Logs("aws-frontend", 0, 100)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	if len(lines) != 2 || lines[0].Message != "GET / 200" || lines[1].TimestampUnix == 0 {
		t.Fatalf("lines = %+v", lines)
	}
	st, err := c.Stats("aws-frontend")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	// cpuDelta=1e9, sysDelta=1e10, 2 cpus → 20%.
	if st.CPUPercent < 19.9 || st.CPUPercent > 20.1 {
		t.Errorf("CPUPercent = %v, want ~20", st.CPUPercent)
	}
	if got := st.MemUsedPercent(); got < 19.5 || got > 19.6 {
		t.Errorf("MemUsedPercent = %v, want ~19.53", got)
	}
}

func TestTailerAllowlistAndBounds(t *testing.T) {
	c := fakeEngine(t)
	tr := NewTailer(c, "e1", []string{"aws-frontend"}, 50, nil)
	if !tr.Enabled() {
		t.Fatal("tailer should be enabled")
	}
	batches := tr.Collect()
	if len(batches) != 1 || batches[0].TelemetryType != "logs" {
		t.Fatalf("batches = %+v", batches)
	}
	logs := batches[0].Logs
	if len(logs) != 2 || logs[0].ServiceName != "aws-frontend" {
		t.Fatalf("logs = %+v", logs)
	}
	// Second emission uses since filter; fake ignores it, so lines repeat —
	// but cursor must have advanced (no re-read-from-zero regression).
	if tr.state["aws-frontend"].lastSeenUnix == 0 {
		t.Error("cursor did not advance")
	}
	// Stopped containers and non-allowlisted names are skipped.
	tr2 := NewTailer(c, "e1", []string{"stopped-one", "aws-valkey"}, 50, nil)
	b := tr2.Collect()
	if b != nil {
		t.Fatalf("expected nil (exited + no logs route), got %+v", b)
	}
	// Disabled tailer returns nil without touching the daemon.
	tr3 := NewTailer(New("/nonexistent.sock"), "e1", nil, 0, nil)
	if tr3.Enabled() {
		t.Fatal("empty allowlist must be disabled")
	}
	if tr3.Collect() != nil {
		t.Fatal("disabled tailer must return nil")
	}
}

func TestPollerCollect(t *testing.T) {
	c := fakeEngine(t)
	batches := NewPoller(c, "e1", []string{"aws-frontend"}, nil).Collect()
	if len(batches) != 1 || batches[0].TelemetryType != "metrics" {
		t.Fatalf("batches = %+v", batches)
	}
	names := map[string]bool{}
	for _, m := range batches[0].Metrics {
		names[m.MetricName] = true
		if m.Tags["container"] != "aws-frontend" {
			t.Errorf("metric missing container tag: %+v", m)
		}
		if m.SourceType != "" {
			t.Errorf("poller metrics must leave SourceType empty (real perf): %+v", m)
		}
	}
	for _, want := range []string{"container_cpu_percent", "container_mem_used_percent", "container_mem_used_bytes"} {
		if !names[want] {
			t.Errorf("missing metric %s", want)
		}
	}
}

func TestMissingSocketDegrades(t *testing.T) {
	c := New(filepath.Join(t.TempDir(), "nope.sock"))
	if _, err := c.List(); err == nil {
		t.Fatal("expected error on missing socket")
	}
	tr := NewTailer(c, "e1", []string{"x"}, 10, nil)
	if tr.Collect() != nil {
		t.Fatal("tailer must degrade to nil on dead socket")
	}
	p := NewPoller(c, "e1", []string{"x"}, nil)
	if p.Collect() != nil {
		t.Fatal("poller must degrade to nil on dead socket")
	}
}

func TestMemUsedPercentZeroLimit(t *testing.T) {
	var s ContainerStats
	if s.MemUsedPercent() != 0 {
		t.Fatal("zero limit must yield 0")
	}
}
