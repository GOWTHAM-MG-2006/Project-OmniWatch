// OmniWatch — Agent
// Component: resilience (circuit breaker)
// Phase: industry-ready (IND-2)
// Purpose: Gobreaker-backed circuit breaker guarding OTLP export calls
// Inputs: BreakerSettings (failure threshold, count window, open timeout)
// Outputs: Error-gated execution with open/half-open/closed state machine
package resilience

import (
	"context"
	"fmt"
	"time"

	"github.com/sony/gobreaker"
)

// Breaker window defaults: the breaker opens after FailureThreshold
// consecutive export failures and stays open for OpenTimeout before a
// half-open probe is allowed. Interval resets the consecutive-failure
// counts so a single stale burst does not trip the breaker forever.
const (
	DefaultBreakerInterval    = 30 * time.Second
	DefaultBreakerOpenTimeout = 30 * time.Second
	DefaultMaxHalfOpenProbes  = uint32(1)
)

// BreakerSettings configures a CircuitBreaker. FailureThreshold is the
// number of consecutive failures that opens the circuit (5 errors per the
// industry-ready plan); Interval is the window over which counts are kept.
type BreakerSettings struct {
	Name             string
	FailureThreshold uint32
	Interval         time.Duration
	OpenTimeout      time.Duration
	MaxHalfOpenProbes uint32
}

// DefaultBreakerSettings returns the plan defaults: 5 errors / 30s window,
// 30s open, a single half-open probe.
func DefaultBreakerSettings() BreakerSettings {
	return BreakerSettings{
		Name:              "otlp-export",
		FailureThreshold:  5,
		Interval:          DefaultBreakerInterval,
		OpenTimeout:       DefaultBreakerOpenTimeout,
		MaxHalfOpenProbes: DefaultMaxHalfOpenProbes,
	}
}

// CircuitBreaker is a thin wrapper around github.com/sony/gobreaker — the
// only permitted external dependency of this package. It opens after
// FailureThreshold consecutive errors, allows MaxHalfOpenProbes through in
// half-open state, and closes on probe success (re-opens on probe failure).
type CircuitBreaker struct {
	cb       *gobreaker.CircuitBreaker
	settings BreakerSettings
}

// NewCircuitBreaker builds a breaker from settings; zero-value fields fall
// back to DefaultBreakerSettings.
func NewCircuitBreaker(s BreakerSettings) *CircuitBreaker {
	def := DefaultBreakerSettings()
	if s.Name == "" {
		s.Name = def.Name
	}
	if s.FailureThreshold == 0 {
		s.FailureThreshold = def.FailureThreshold
	}
	if s.Interval <= 0 {
		s.Interval = def.Interval
	}
	if s.OpenTimeout <= 0 {
		s.OpenTimeout = def.OpenTimeout
	}
	if s.MaxHalfOpenProbes == 0 {
		s.MaxHalfOpenProbes = def.MaxHalfOpenProbes
	}
	threshold := s.FailureThreshold
	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        s.Name,
		MaxRequests: s.MaxHalfOpenProbes,
		Interval:    s.Interval,
		Timeout:     s.OpenTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= threshold
		},
	})
	return &CircuitBreaker{cb: cb, settings: s}
}

// Do runs fn unless the circuit is open. A nil ctx aborts immediately; a
// cancelled ctx aborts before touching the breaker. Breaker-open rejection
// is returned as ErrCircuitOpen (wrapping gobreaker.ErrOpenState) so callers
// can distinguish "endpoint down" from "breaker protecting".
func (b *CircuitBreaker) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	if ctx == nil {
		return fmt.Errorf("resilience: nil context")
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("resilience: context done before breaker execute: %w", ctx.Err())
	default:
	}
	_, err := b.cb.Execute(func() (any, error) {
		return nil, fn(ctx)
	})
	if err == gobreaker.ErrOpenState || err == gobreaker.ErrTooManyRequests {
		return fmt.Errorf("%w: %w", ErrCircuitOpen, err)
	}
	return err
}

// State reports the current breaker state (Closed, Open, Half-Open).
func (b *CircuitBreaker) State() gobreaker.State { return b.cb.State() }

// Counts reports the breaker's internal success/failure counters.
func (b *CircuitBreaker) Counts() gobreaker.Counts { return b.cb.Counts() }

// Name returns the breaker name.
func (b *CircuitBreaker) Name() string { return b.cb.Name() }
