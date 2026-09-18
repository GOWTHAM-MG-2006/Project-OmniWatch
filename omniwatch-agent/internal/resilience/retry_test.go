// OmniWatch — Agent
// Component: resilience (tests: retry policy)
// Phase: industry-ready (IND-2)
// Purpose: Unit tests for the 100ms/500ms/2s backoff schedule, attempt cap, jitter
// Inputs: synthetic failing/passing funcs
// Outputs: go test results
package resilience

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryBackoffSchedule(t *testing.T) {
	r := DefaultRetryPolicy()
	if got := r.BackoffForRetry(2); got != 100*time.Millisecond {
		t.Errorf("BackoffForRetry(2) = %v, want 100ms", got)
	}
	if got := r.BackoffForRetry(3); got != 500*time.Millisecond {
		t.Errorf("BackoffForRetry(3) = %v, want 500ms", got)
	}
	if got := r.BackoffForRetry(4); got != 2*time.Second {
		t.Errorf("BackoffForRetry(4) = %v, want 2s", got)
	}
	if got := r.BackoffForRetry(9); got != 2*time.Second {
		t.Errorf("BackoffForRetry(9) = %v, want cap 2s", got)
	}
	if got := r.BackoffForRetry(1); got != 0 {
		t.Errorf("BackoffForRetry(1) = %v, want 0 (no wait before first try)", got)
	}
}

func TestRetryMaxAttempts(t *testing.T) {
	r := DefaultRetryPolicy()
	r.JitterFraction = 0 // exact timing for the attempt-count assertion
	calls := 0
	errBoom := errors.New("boom")
	start := time.Now()
	err := r.Do(context.Background(), func(ctx context.Context) error {
		calls++
		return errBoom
	})
	elapsed := time.Since(start)
	if !errors.Is(err, errBoom) {
		t.Fatalf("Do = %v, want last error %v", err, errBoom)
	}
	if calls != DefaultRetryMaxAttempts {
		t.Errorf("calls = %d, want max %d attempts", calls, DefaultRetryMaxAttempts)
	}
	// Waits were 100ms + 500ms = 600ms of sleep.
	if elapsed < 600*time.Millisecond {
		t.Errorf("elapsed = %v, want >= 600ms (100ms+500ms schedule)", elapsed)
	}
	if elapsed > 10*time.Second {
		t.Errorf("elapsed = %v, want < 10s (no runaway sleep)", elapsed)
	}
}

func TestRetrySuccessStopsEarly(t *testing.T) {
	r := DefaultRetryPolicy()
	calls := 0
	err := r.Do(context.Background(), func(ctx context.Context) error {
		calls++
		if calls < 2 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Do = %v, want nil after transient failure", err)
	}
	if calls != 2 {
		t.Errorf("calls = %d, want 2 (stop on first success)", calls)
	}
}

func TestRetryJitterBounds(t *testing.T) {
	r := DefaultRetryPolicy() // jitter enabled by default
	for i := 0; i < 50; i++ {
		base := r.BackoffForRetry(2)
		got := r.JitteredBackoff(2)
		if got < base {
			t.Fatalf("JitteredBackoff(2) = %v below base %v", got, base)
		}
		if max := base + time.Duration(r.JitterFraction*float64(base)) + time.Nanosecond; got > max {
			t.Fatalf("JitteredBackoff(2) = %v above base+jitter %v", got, max)
		}
	}
	plain := r
	plain.JitterFraction = 0
	if got := plain.JitteredBackoff(3); got != 500*time.Millisecond {
		t.Errorf("JitteredBackoff(3) with jitter off = %v, want exactly 500ms", got)
	}
}

func TestRetryContextCancelAborts(t *testing.T) {
	r := DefaultRetryPolicy()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	err := r.Do(ctx, func(ctx context.Context) error {
		calls++
		return errors.New("boom")
	})
	if err == nil {
		t.Errorf("Do with cancelled ctx = nil, want abort error")
	}
	if calls != 0 {
		t.Errorf("calls = %d, want 0 (abort before first attempt)", calls)
	}
}
