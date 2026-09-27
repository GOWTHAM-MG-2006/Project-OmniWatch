// OmniWatch — Agent
// Component: autotune (tests: Phase1 advisory observer)
// Phase: 1
// Purpose: Unit tests for read-only Observe rules + log/metric emission
// Inputs: synthetic Signals snapshots
// Outputs: go test results
package autotune

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func healthySignals() Signals {
	return Signals{
		QueueLen:            10,
		QueueCapacity:       1000,
		QueueDropped:        0,
		BreakerState:        BreakerClosed,
		ConsecutiveFailures: 0,
		ExportP99Latency:    time.Second,
		ExportFailRate:      0,
		CollectionInterval:  60 * time.Second,
		HostmetricsInterval: 60 * time.Second,
		QueueSize:           1000,
		RetryMaxAttempts:    3,
	}
}

func TestObserveHealthySilent(t *testing.T) {
	if recs := Observe(healthySignals()); len(recs) != 0 {
		t.Fatalf("Observe(healthy) = %d recs, want 0: %v", len(recs), recs)
	}
}

func TestObserveQueueDropsProposeDouble(t *testing.T) {
	s := healthySignals()
	s.QueueLen = 900
	s.QueueDropped = 7
	recs := Observe(s)
	var found *Recommendation
	for i := range recs {
		if recs[i].Param == "resilience.queue_size" {
			found = &recs[i]
		}
	}
	if found == nil {
		t.Fatalf("no queue_size rec in %v", recs)
	}
	if found.Current != "1000" || found.Proposed != "2000" {
		t.Errorf("queue rec = %s -> %s, want 1000 -> 2000", found.Current, found.Proposed)
	}
	if found.Reason == "" {
		t.Errorf("queue rec missing reason")
	}
}

func TestObserveQueuePressureWithoutDrops(t *testing.T) {
	s := healthySignals()
	s.QueueLen = 850 // 85% of 1000, no drops yet
	recs := Observe(s)
	found := false
	for _, r := range recs {
		if r.Param == "resilience.queue_size" {
			found = true
		}
	}
	if !found {
		t.Fatalf("want pre-emptive queue_size rec at 85%% full, got %v", recs)
	}
}

func TestObserveQueueProposalCapped(t *testing.T) {
	s := healthySignals()
	s.QueueSize = 9000
	s.QueueCapacity = 9000
	s.QueueLen = 8500
	s.QueueDropped = 1
	recs := Observe(s)
	for _, r := range recs {
		if r.Param == "resilience.queue_size" && r.Proposed != "10000" {
			t.Errorf("proposed = %s, want cap 10000", r.Proposed)
		}
	}
}

func TestObserveBreakerOpenProposesRetry(t *testing.T) {
	s := healthySignals()
	s.BreakerState = BreakerOpen
	s.ConsecutiveFailures = 5
	recs := Observe(s)
	found := false
	for _, r := range recs {
		if r.Param == "resilience.retry_max_attempts" {
			found = true
			if r.Current != "3" || r.Proposed != "4" {
				t.Errorf("retry rec = %s -> %s, want 3 -> 4", r.Current, r.Proposed)
			}
			if r.Reason == "" {
				t.Errorf("retry rec missing reason")
			}
		}
	}
	if !found {
		t.Fatalf("no retry rec for open breaker, got %v", recs)
	}
}

func TestObserveRetryCappedAtMax(t *testing.T) {
	s := healthySignals()
	s.BreakerState = BreakerOpen
	s.RetryMaxAttempts = 5
	for _, r := range Observe(s) {
		if r.Param == "resilience.retry_max_attempts" {
			t.Fatalf("retry already at max must not propose more, got %v", r)
		}
	}
}

func TestObserveLatencyProposesSlowerCollection(t *testing.T) {
	s := healthySignals()
	s.ExportP99Latency = 8 * time.Second
	recs := Observe(s)
	found := false
	for _, r := range recs {
		if r.Param == "agent.collection_interval" {
			found = true
			if r.Current != "1m0s" || r.Proposed != "2m0s" {
				t.Errorf("interval rec = %s -> %s, want 1m0s -> 2m0s", r.Current, r.Proposed)
			}
		}
	}
	if !found {
		t.Fatalf("no collection_interval rec for 8s p99, got %v", recs)
	}
}

func TestObservePureNoMutation(t *testing.T) {
	s := healthySignals()
	s.QueueDropped = 3
	before := s
	_ = Observe(s)
	if s != before {
		t.Errorf("Observe mutated input: before %+v, after %+v", before, s)
	}
}

func TestEmitCountsMetricPerRecommendation(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	counter, err := provider.Meter("test").Int64Counter(RecommendationMetric)
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	o := New(discardLogger(), counter)
	s := healthySignals()
	s.QueueDropped = 2
	s.BreakerState = BreakerOpen
	s.ConsecutiveFailures = 5

	recs := o.Emit(context.Background(), s)
	if len(recs) == 0 {
		t.Fatalf("Emit returned 0 recs, want >0")
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	var sum int64
	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != RecommendationMetric {
				continue
			}
			found = true
			if data, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range data.DataPoints {
					sum += dp.Value
				}
			}
		}
	}
	if !found {
		t.Fatalf("metric %q not exported", RecommendationMetric)
	}
	if sum != int64(len(recs)) {
		t.Errorf("counter sum = %d, want %d (one per recommendation)", sum, len(recs))
	}
}

func TestEmitNilSafeAndSilentWhenHealthy(t *testing.T) {
	o := New(nil, nil) // nil logger + nil counter must not panic
	if recs := o.Emit(context.Background(), healthySignals()); len(recs) != 0 {
		t.Fatalf("Emit(healthy) = %d recs, want 0", len(recs))
	}
}
