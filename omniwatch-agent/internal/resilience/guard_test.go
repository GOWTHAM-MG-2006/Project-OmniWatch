// OmniWatch — Agent
// Component: resilience (tests: guarded execution / exporter wrap)
// Phase: industry-ready (IND-2)
// Purpose: Prove retry-inside-breaker composition incl. dead-endpoint fast fail
// Inputs: failing ops, unroutable TCP endpoint
// Outputs: go test results
package resilience

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func fastPolicy() *RetryPolicy {
	r := DefaultRetryPolicy()
	r.BaseDelay = time.Millisecond
	r.MaxDelay = 5 * time.Millisecond
	r.JitterFraction = 0
	return &r
}

func TestGuardedSuccess(t *testing.T) {
	b := NewCircuitBreaker(testBreakerSettings(5))
	r := fastPolicy()
	calls := 0
	err := Guarded(context.Background(), b, r, func(ctx context.Context) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("Guarded = %v, want nil", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1 (no retry on success)", calls)
	}
}

func TestGuardedRetriesThenReports(t *testing.T) {
	b := NewCircuitBreaker(testBreakerSettings(5))
	r := fastPolicy()
	r.MaxAttempts = 3
	calls := 0
	sentinel := errors.New("export failed")
	err := Guarded(context.Background(), b, r, func(ctx context.Context) error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Guarded = %v, want %v", err, sentinel)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3 retried attempts", calls)
	}
	// One Guarded call is one breaker request even though it retried.
	if got := b.Counts().Requests; got != 1 {
		t.Errorf("breaker Requests = %d, want 1 (retries collapse to one request)", got)
	}
}

func TestGuardedBreakerOpensOnRepeatedGuardedFailures(t *testing.T) {
	b := NewCircuitBreaker(testBreakerSettings(2))
	r := fastPolicy()
	r.MaxAttempts = 1
	ctx := context.Background()
	_ = Guarded(ctx, b, r, func(ctx context.Context) error { return errors.New("down") })
	_ = Guarded(ctx, b, r, func(ctx context.Context) error { return errors.New("down") })
	if st := b.State(); st.String() != "open" {
		t.Fatalf("State() = %v, want open after 2 guarded failures", st)
	}
	if err := Guarded(ctx, b, r, func(ctx context.Context) error { return nil }); !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("Guarded on open circuit = %v, want ErrCircuitOpen", err)
	}
}

func TestGuardedNilLegsPassThrough(t *testing.T) {
	if err := Guarded(context.Background(), nil, nil, func(ctx context.Context) error { return nil }); err != nil {
		t.Errorf("Guarded(nil,nil) success = %v, want nil", err)
	}
	sentinel := errors.New("boom")
	if err := Guarded(context.Background(), nil, nil, func(ctx context.Context) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Errorf("Guarded(nil,nil) failure = %v, want %v", err, sentinel)
	}
}

// TestExporterWrapDeadEndpoint simulates the exporter OTLP call against a dead
// endpoint (unroutable port 9, discard): the dial fails fast, retry exhausts,
// and the breaker records the failure. The agent must never hang here.
func TestExporterWrapDeadEndpoint(t *testing.T) {
	b := NewCircuitBreaker(testBreakerSettings(5))
	r := fastPolicy()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	err := Guarded(ctx, b, r, func(ctx context.Context) error {
		d := net.Dialer{Timeout: 500 * time.Millisecond}
		c, err := d.DialContext(ctx, "tcp", "127.0.0.1:9")
		if err != nil {
			return err
		}
		_ = c.Close()
		return nil
	})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("Guarded dial to dead endpoint = nil, want connection error")
	}
	if elapsed > 10*time.Second {
		t.Errorf("elapsed = %v, want fast fail well under ctx timeout", elapsed)
	}
	if got := b.Counts().TotalFailures; got != 1 {
		t.Errorf("breaker TotalFailures = %d, want 1", got)
	}
}
