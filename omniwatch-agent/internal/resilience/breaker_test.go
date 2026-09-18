// OmniWatch — Agent
// Component: resilience (tests: circuit breaker)
// Phase: industry-ready (IND-2)
// Purpose: Unit tests for breaker open/half-open/close transitions
// Inputs: synthetic failing/passing funcs
// Outputs: go test results
package resilience

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sony/gobreaker"
)

func testBreakerSettings(threshold uint32) BreakerSettings {
	return BreakerSettings{
		Name:              "test-breaker",
		FailureThreshold:  threshold,
		Interval:          time.Minute,
		OpenTimeout:       50 * time.Millisecond,
		MaxHalfOpenProbes: 1,
	}
}

func failFunc(err error) func(ctx context.Context) error {
	return func(ctx context.Context) error { return err }
}

func okFunc() func(ctx context.Context) error {
	return func(ctx context.Context) error { return nil }
}

func TestBreakerOpensAfterThreshold(t *testing.T) {
	tests := []struct {
		name      string
		threshold uint32
		failures  int
		wantOpen  bool
	}{
		{"below threshold stays closed", 3, 2, false},
		{"at threshold opens", 3, 3, true},
		{"plan default 5 errors opens", 5, 5, true},
		{"plan default 5 errors, 4 stays closed", 5, 4, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewCircuitBreaker(testBreakerSettings(tt.threshold))
			ctx := context.Background()
			sentinel := errors.New("export boom")
			for i := 0; i < tt.failures; i++ {
				_ = b.Do(ctx, failFunc(sentinel))
			}
			if got := b.State(); tt.wantOpen && got != gobreaker.StateOpen {
				t.Errorf("State() = %v, want Open after %d failures (threshold %d)", got, tt.failures, tt.threshold)
			} else if !tt.wantOpen && got != gobreaker.StateClosed {
				t.Errorf("State() = %v, want Closed after %d failures (threshold %d)", got, tt.failures, tt.threshold)
			}
		})
	}
}

func TestBreakerOpenRejectsWithErrCircuitOpen(t *testing.T) {
	b := NewCircuitBreaker(testBreakerSettings(1))
	ctx := context.Background()
	_ = b.Do(ctx, failFunc(errors.New("boom")))
	if st := b.State(); st != gobreaker.StateOpen {
		t.Fatalf("State() = %v, want Open", st)
	}
	called := false
	err := b.Do(ctx, func(ctx context.Context) error { called = true; return nil })
	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("Do on open circuit = %v, want errors.Is ErrCircuitOpen", err)
	}
	if called {
		t.Errorf("fn executed while circuit open, want rejection without execution")
	}
}

func TestBreakerHalfOpenProbeSuccessCloses(t *testing.T) {
	b := NewCircuitBreaker(testBreakerSettings(1))
	ctx := context.Background()
	_ = b.Do(ctx, failFunc(errors.New("boom")))
	if st := b.State(); st != gobreaker.StateOpen {
		t.Fatalf("State() = %v, want Open", st)
	}
	time.Sleep(80 * time.Millisecond) // exceed 50ms OpenTimeout
	if err := b.Do(ctx, okFunc()); err != nil {
		t.Fatalf("half-open probe = %v, want nil", err)
	}
	if st := b.State(); st != gobreaker.StateClosed {
		t.Errorf("State() = %v after successful probe, want Closed", st)
	}
}

func TestBreakerHalfOpenProbeFailureReopens(t *testing.T) {
	b := NewCircuitBreaker(testBreakerSettings(1))
	ctx := context.Background()
	_ = b.Do(ctx, failFunc(errors.New("boom")))
	time.Sleep(80 * time.Millisecond) // exceed 50ms OpenTimeout
	_ = b.Do(ctx, failFunc(errors.New("still down")))
	if st := b.State(); st != gobreaker.StateOpen {
		t.Errorf("State() = %v after failed probe, want Open", st)
	}
}

func TestBreakerSuccessResetsConsecutiveFailures(t *testing.T) {
	b := NewCircuitBreaker(testBreakerSettings(3))
	ctx := context.Background()
	sentinel := errors.New("boom")
	_ = b.Do(ctx, failFunc(sentinel))
	_ = b.Do(ctx, failFunc(sentinel))
	if err := b.Do(ctx, okFunc()); err != nil {
		t.Fatalf("success = %v, want nil", err)
	}
	// Two more failures must NOT open: the success reset the streak.
	_ = b.Do(ctx, failFunc(sentinel))
	_ = b.Do(ctx, failFunc(sentinel))
	if st := b.State(); st != gobreaker.StateClosed {
		t.Errorf("State() = %v, want Closed (streak was reset by success)", st)
	}
}

func TestBreakerContextCancelAborts(t *testing.T) {
	b := NewCircuitBreaker(testBreakerSettings(5))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	err := b.Do(ctx, func(ctx context.Context) error { called = true; return nil })
	if err == nil {
		t.Errorf("Do with cancelled ctx = nil, want abort error")
	}
	if called {
		t.Errorf("fn executed with cancelled ctx, want abort before execution")
	}
	if st := b.State(); st != gobreaker.StateClosed {
		t.Errorf("State() = %v, want Closed (cancel must not count as failure)", st)
	}
}

func TestBreakerZeroSettingsFallBackToDefaults(t *testing.T) {
	b := NewCircuitBreaker(BreakerSettings{})
	if b.Name() == "" {
		t.Errorf("Name() empty, want default name")
	}
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		_ = b.Do(ctx, failFunc(errors.New("boom")))
	}
	if st := b.State(); st != gobreaker.StateClosed {
		t.Errorf("State() = %v after 4 failures with defaults, want Closed", st)
	}
	_ = b.Do(ctx, failFunc(errors.New("boom")))
	if st := b.State(); st != gobreaker.StateOpen {
		t.Errorf("State() = %v after 5 failures with defaults, want Open", st)
	}
}
