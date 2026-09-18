// OmniWatch — Agent
// Component: alerting (tests)
// Phase: industry-ready (IND-5)
// Purpose: Table tests for rule loading, OTLP log export, and SLO math
// Inputs: configs/alerts/prometheusrules.yaml, stub Emitter
// Outputs: go test verdicts
package alerting

import (
	"context"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/otel/log"
)

func rulesPath(t *testing.T) string {
	t.Helper()
	return filepath.Join("..", "..", "configs", "alerts", "prometheusrules.yaml")
}

func TestLoadRules(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		wantRules int
		wantErr   bool
	}{
		{name: "production rules file loads", path: rulesPath(t), wantRules: 6, wantErr: false},
		{name: "missing file errors", path: filepath.Join(t.TempDir(), "nope.yaml"), wantRules: 0, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf, err := LoadRules(tt.path)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadRules: %v", err)
			}
			if got := rf.RuleCount(); got != tt.wantRules {
				t.Fatalf("RuleCount = %d, want %d", got, tt.wantRules)
			}
			for _, r := range rf.Flatten() {
				if r.Alert == "" || r.Expr == "" {
					t.Fatalf("rule missing alert/expr: %+v", r)
				}
				if r.For == "" {
					t.Fatalf("rule %q missing for duration", r.Alert)
				}
				if r.Labels["severity"] == "" {
					t.Fatalf("rule %q missing severity label", r.Alert)
				}
			}
		})
	}
}

func TestLoadRulesCoversSignals(t *testing.T) {
	rf, err := LoadRules(rulesPath(t))
	if err != nil {
		t.Fatalf("LoadRules: %v", err)
	}
	exprs := ""
	for _, r := range rf.Flatten() {
		exprs += r.Expr + "\n"
	}
	for _, want := range []string{
		"omniwatch_agent_queue_dropped_total",
		"omniwatch_agent_breaker_state",
		"omniwatch_agent_heartbeat_total",
	} {
		if !contains(exprs, want) {
			t.Errorf("no rule references %q", want)
		}
	}
}

// stubEmitter is a real in-memory log.Logger: Emit appends the record.
type stubEmitter struct{ records []log.Record }

func (s *stubEmitter) Emit(ctx context.Context, rec log.Record) {
	s.records = append(s.records, rec)
}

// TestExportRuleAsLogBody asserts the failing-first contract: a loaded rule
// marshals to the expected OTLP log body before the exporter exists.
func TestExportRuleAsLogBody(t *testing.T) {
	rf, err := LoadRules(rulesPath(t))
	if err != nil {
		t.Fatalf("LoadRules: %v", err)
	}
	stub := &stubEmitter{}
	exp := New(Options{Emitter: stub})
	rule := rf.Flatten()[0]
	if err := exp.ExportRule(context.Background(), rule); err != nil {
		t.Fatalf("ExportRule: %v", err)
	}
	if len(stub.records) != 1 {
		t.Fatalf("emitted %d records, want 1", len(stub.records))
	}
	rec := stub.records[0]
	if rec.Severity() != log.SeverityInfo {
		t.Errorf("severity = %v, want INFO", rec.Severity())
	}
	body := rec.Body().AsString()
	if !contains(body, rule.Alert) {
		t.Errorf("log body %q does not contain alert name %q", body, rule.Alert)
	}
	if !contains(body, rule.Expr) {
		t.Errorf("log body %q does not contain expr %q", body, rule.Expr)
	}
	found := map[string]string{}
	rec.WalkAttributes(func(kv log.KeyValue) bool {
		found[kv.Key] = kv.Value.AsString()
		return true
	})
	for _, k := range []string{"alert.name", "alert.severity", "alert.expr", "alert.group"} {
		if found[k] == "" {
			t.Errorf("log record missing attribute %q", k)
		}
	}
	if found["alert.name"] != rule.Alert {
		t.Errorf("alert.name = %q, want %q", found["alert.name"], rule.Alert)
	}
}

func TestExportAll(t *testing.T) {
	rf, err := LoadRules(rulesPath(t))
	if err != nil {
		t.Fatalf("LoadRules: %v", err)
	}
	stub := &stubEmitter{}
	exp := New(Options{Emitter: stub})
	if err := exp.ExportAll(context.Background(), rf); err != nil {
		t.Fatalf("ExportAll: %v", err)
	}
	if len(stub.records) != rf.RuleCount() {
		t.Errorf("emitted %d records, want %d", len(stub.records), rf.RuleCount())
	}
}

func TestAvailability(t *testing.T) {
	tests := []struct {
		name    string
		success uint64
		total   uint64
		want    float64
	}{
		{name: "perfect", success: 1000, total: 1000, want: 100},
		{name: "three nines", success: 999, total: 1000, want: 99.9},
		{name: "zero total", success: 0, total: 0, want: 0},
		{name: "half", success: 1, total: 2, want: 50},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Availability(tt.success, tt.total); got != tt.want {
				t.Errorf("Availability = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMeetsSLO(t *testing.T) {
	tests := []struct {
		name      string
		slo       SLO
		good      uint64
		total     uint64
		wantMeets bool
	}{
		{name: "health meets 99.9", slo: HealthAvailabilitySLO, good: 999, total: 1000, wantMeets: true},
		{name: "health misses 99.9", slo: HealthAvailabilitySLO, good: 998, total: 1000, wantMeets: false},
		{name: "export meets 99", slo: ExportSuccessSLO, good: 99, total: 100, wantMeets: true},
		{name: "export misses 99", slo: ExportSuccessSLO, good: 98, total: 100, wantMeets: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.slo.Meets(tt.good, tt.total); got != tt.wantMeets {
				t.Errorf("Meets = %v, want %v", got, tt.wantMeets)
			}
		})
	}
}

func contains(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
