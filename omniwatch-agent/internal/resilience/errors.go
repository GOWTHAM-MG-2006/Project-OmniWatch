// OmniWatch — Agent
// Component: resilience (sentinel errors)
// Phase: industry-ready (IND-2)
// Purpose: Shared sentinel errors for the resilience package
// Inputs: none
// Outputs: ErrCircuitOpen for breaker-open rejection
package resilience

import "errors"

// ErrCircuitOpen is wrapped (with %w) around gobreaker.ErrOpenState /
// gobreaker.ErrTooManyRequests so callers can use errors.Is to detect
// breaker rejection distinct from a genuine export failure.
var ErrCircuitOpen = errors.New("resilience: circuit breaker is open")
