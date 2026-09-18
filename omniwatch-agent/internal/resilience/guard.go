// OmniWatch — Agent
// Component: resilience (guarded execution)
// Phase: industry-ready (IND-2)
// Purpose: Compose retry-inside-breaker for OTLP export calls
// Inputs: Breaker + RetryPolicy + fallible op
// Outputs: op result; breaker counts the retried outcome once
package resilience

import (
	"context"
)

// Guarded executes op with the retry policy first, then counts the retried
// outcome against the circuit breaker: the breaker wraps the whole retried
// call, so one Guarded invocation is one breaker request. A nil breaker or
// nil policy is skipped, so callers can disable either leg.
func Guarded(ctx context.Context, b *CircuitBreaker, r *RetryPolicy, op func(ctx context.Context) error) error {
	run := func(ctx context.Context) error {
		if r == nil {
			return op(ctx)
		}
		return r.Do(ctx, op)
	}
	if b == nil {
		return run(ctx)
	}
	return b.Do(ctx, run)
}
