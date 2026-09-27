// OmniWatch — Agent
// Component: direct tests
// Phase: exporter-importer-split
// Purpose: Bearer auth, status handling, heartbeat shape, security restraint.
// Inputs: httptest servers (no network, no /proc dependency)
// Outputs: Unit-test verdicts for the direct importer client
package direct

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func testClient(t *testing.T, url string, check func(r *http.Request, body IngestBody)) *Client {
	t.Helper()
	cfg := Config{
		Endpoint: url, APIToken: "tok-123",
		ExporterNumber: 2, ExporterName: "web-01",
		EntityID: "exporter-2-test", Timeout: 5 * time.Second,
	}
	return New(cfg, nil)
}

func TestSendBearerAnd202(t *testing.T) {
	var gotAuth, gotCT string
	var gotBody IngestBody
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":true}`))
	}))
	defer srv.Close()

	c := testClient(t, srv.URL, nil)
	err := c.Send(context.Background(), IngestBody{
		TelemetryType: TypeMetrics, EntityID: "exporter-2-test",
		Metrics: []MetricPoint{{MetricName: "cpu_load_1m", Value: 0.5}},
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if gotAuth != "Bearer tok-123" {
		t.Errorf("auth header = %q", gotAuth)
	}
	if gotCT != "application/json" {
		t.Errorf("content-type = %q", gotCT)
	}
	if gotBody.TelemetryType != TypeMetrics || gotBody.EntityID != "exporter-2-test" {
		t.Errorf("body = %+v", gotBody)
	}
}

func TestSend403IsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"detail":"invalid importer token"}`))
	}))
	defer srv.Close()

	c := testClient(t, srv.URL, nil)
	if err := c.Send(context.Background(), IngestBody{TelemetryType: TypeLogs, EntityID: "x"}); err == nil {
		t.Fatal("expected error on 403, got nil")
	}
}

func TestBuildHeartbeatCoversCoreTypes(t *testing.T) {
	c := testClient(t, "http://localhost:9", nil)
	batches := c.BuildHeartbeat()
	seen := map[string]bool{}
	for _, b := range batches {
		seen[b.TelemetryType] = true
		if b.EntityID == "" {
			t.Errorf("batch %s has empty entity_id", b.TelemetryType)
		}
	}
	for _, want := range []string{TypeMetrics, TypeLogs, TypeTraces, TypeMetadata, TypeState, TypeProfiling} {
		if !seen[want] {
			t.Errorf("heartbeat missing %s batch", want)
		}
	}
	for _, b := range batches {
		if b.TelemetryType != TypeMetrics {
			continue
		}
		for _, m := range b.Metrics {
			if m.SourceType != "heartbeat" {
				t.Errorf("heartbeat metric %s source_type = %q, want heartbeat", m.MetricName, m.SourceType)
			}
			if m.MetricName == "go_goroutines" && m.Value <= 0 {
				t.Errorf("go_goroutines = %v, want live pprof count > 0", m.Value)
			}
		}
	}
	var hbTrace, hbSpan, profParent, profTrace string
	var profOp string
	var profDur float64
	for _, b := range batches {
		if len(b.Traces) == 0 {
			continue
		}
		for _, s := range b.Traces {
			switch b.TelemetryType {
			case TypeTraces:
				hbTrace, hbSpan = s.TraceID, s.SpanID
			case TypeProfiling:
				profTrace, profParent, profOp, profDur = s.TraceID, s.ParentSpanID, s.Operation, s.DurationMs
			}
		}
	}
	if profOp != "runtime.profile(self)" {
		t.Errorf("profile operation = %q, want runtime.profile(self)", profOp)
	}
	if profTrace == "" || profTrace != hbTrace {
		t.Errorf("profile trace %q does not join heartbeat trace %q", profTrace, hbTrace)
	}
	if profParent == "" || profParent != hbSpan {
		t.Errorf("profile parent %q is not the heartbeat span %q", profParent, hbSpan)
	}
	if profDur < 0 {
		t.Errorf("profile duration = %v, want >= 0 interval GC pause", profDur)
	}
}
func TestTailSecurityAbsentLogYieldsNothing(t *testing.T) {
	old := authLogPaths
	authLogPaths = []string{"/nonexistent-omniwatch-auth.log"}
	defer func() { authLogPaths = old }()
	c := testClient(t, "http://localhost:9", nil)
	if batches := c.TailSecurity(); len(batches) != 0 {
		t.Errorf("expected no security batches without a log file, got %d", len(batches))
	}
}

func TestBuildSecurityBatchesSendsOnce(t *testing.T) {
	lines := []string{
		"sshd[1]: Failed password for root",
		"sshd[2]: Invalid user admin",
	}
	batches := buildSecurityBatches(lines, "e1", "2026-09-26T00:00:00Z")
	if len(batches) != 1 {
		t.Fatalf("expected exactly 1 security batch (send-once), got %d", len(batches))
	}
	b := batches[0]
	if b.TelemetryType != TypeAuditLogs {
		t.Errorf("batch type = %q, want %q", b.TelemetryType, TypeAuditLogs)
	}
	if len(b.Logs) != len(lines) {
		t.Errorf("batch holds %d lines, want %d (no duplication, no loss)", len(b.Logs), len(lines))
	}
	for i, l := range b.Logs {
		if l.Message != lines[i] {
			t.Errorf("line %d = %q, want verbatim %q", i, l.Message, lines[i])
		}
		if l.LogLevel != "WARN" {
			t.Errorf("line %d level = %q, want WARN", i, l.LogLevel)
		}
	}
}
func TestExportWithResilienceShipsAll(t *testing.T) {
	var count int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	c := testClient(t, srv.URL, nil)
	batches := []IngestBody{
		{TelemetryType: TypeMetrics, EntityID: "x"},
		{TelemetryType: TypeLogs, EntityID: "x"},
	}
	if err := c.ExportWithResilience(context.Background(), batches); err != nil {
		t.Fatalf("ExportWithResilience: %v", err)
	}
	if count != 2 {
		t.Errorf("shipped %d batches, want 2", count)
	}
}
