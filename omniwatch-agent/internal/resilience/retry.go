// OmniWatch — Agent
// Component: resilience (retry policy)
// Phase: industry-ready (IND-2)
// Purpose: Context-aware exponential-backoff retry for export calls
// Inputs: RetryPolicy (max attempts, base delay, max delay cap)
// Outputs: fn result or last error after exhausting attempts
package resilience

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// Retry timing defaults per the industry-ready plan: base 100ms with
// exponential growth capped at 2s, max 3 attempts. The realized base delays
// are 100ms → 500ms → 2s (5x growth: 100ms, 100ms*5, capped 2500ms→2000ms),
// each with up to JitterFraction extra random spread to avoid thundering herds.
const (
	DefaultRetryMaxAttempts = 3
	DefaultRetryBaseDelay   = 100 * time.Millisecond
	DefaultRetryMaxDelay    = 2 * time.Second
	// DefaultRetryJitterFraction adds up to +50% random spread on each wait.
	DefaultRetryJitterFraction = 0.5
	retryGrowthFactor          = 5
)

// RetryPolicy describes exponential backoff: attempt n (1-based retry,
// i.e. the wait before the (n+1)-th try) sleeps
// min(BaseDelay * 5^(n-1), MaxDelay) plus jitter, aborting early on ctx.Done.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	// JitterFraction adds uniform [0, base*JitterFraction) extra delay per
	// wait (0 disables jitter). BackoffForRetry stays exact for assertions.
	JitterFraction float64
}

// DefaultRetryPolicy returns the plan defaults: 3 attempts, 100ms base,
// 2s cap, jitter enabled.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    DefaultRetryMaxAttempts,
		BaseDelay:      DefaultRetryBaseDelay,
		MaxDelay:       DefaultRetryMaxDelay,
		JitterFraction: DefaultRetryJitterFraction,
	}
}

// BackoffForRetry returns the wait before try number tryNum (tryNum >= 2,
// i.e. BackoffForRetry(2) is the wait after the first failure).
func (r RetryPolicy) BackoffForRetry(tryNum int) time.Duration {
	if tryNum < 2 {
		return 0
	}
	base := r.BaseDelay
	if base <= 0 {
		base = DefaultRetryBaseDelay
	}
	maxDelay := r.MaxDelay
	if maxDelay <= 0 {
		maxDelay = DefaultRetryMaxDelay
	}
	d := base
	for i := 2; i < tryNum; i++ {
		d *= retryGrowthFactor
		if d >= maxDelay {
			return maxDelay
		}
	}
	if d > maxDelay {
		return maxDelay
	}
	return d
}

// JitteredBackoff returns BackoffForRetry(tryNum) plus uniform random spread
// in [0, base*JitterFraction), so concurrent agents do not retry in lockstep.
func (r RetryPolicy) JitteredBackoff(tryNum int) time.Duration {
	base := r.BackoffForRetry(tryNum)
	if base <= 0 || r.JitterFraction <= 0 {
		return base
	}
	extra := time.Duration(rand.Float64() * r.JitterFraction * float64(base)) //nolint:gosec // non-crypto jitter
	return base + extra
}

// Do runs fn up to MaxAttempts times (a non-positive MaxAttempts is treated
// as 1). Between attempts it sleeps the jittered backoff delay, aborting with
// ctx.Err() when the context is cancelled. The last error is returned when
// attempts are exhausted.
func (r RetryPolicy) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	attempts := r.MaxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	var err error
	for try := 1; try <= attempts; try++ {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return fmt.Errorf("resilience: retry aborted on attempt %d: %w", try, ctx.Err())
			default:
			}
		}
		if err = fn(ctx); err == nil {
			return nil
		}
		if try == attempts {
			break
		}
		wait := r.JitteredBackoff(try + 1)
		if ctx != nil {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return fmt.Errorf("resilience: retry aborted while backing off: %w", ctx.Err())
			case <-timer.C:
			}
		} else {
			time.Sleep(wait)
		}
	}
	return err
}
