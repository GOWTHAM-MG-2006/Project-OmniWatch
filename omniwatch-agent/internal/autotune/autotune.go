// OmniWatch — Agent
// Component: autotune (Phase1 advisory self-tuning observer)
// Phase: 1
// Purpose: Read-only observer over queue/breaker/export/hostmetrics signals emitting recommendations
// Inputs: Signals snapshot (queue depth/drops, breaker state, export health, intervals)
// Outputs: Advisory Recommendation list via slog log + OTel metric; never mutates config
package autotune

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RecommendationMetric is the OTel counter incremented once per emitted
// recommendation. The alerting/queue convention (exact name asserted in
// tests) applies: do NOT rename without updating tests + docs.
const RecommendationMetric = "omniwatch.agent.autotune.recommendation"

// RecommendationLog is the slog message emitted once per recommendation.
const RecommendationLog = "omniwatch.agent.autotune.recommendation"

// Advisory thresholds. Conservative on purpose: Phase1 must stay quiet on a
// healthy agent and only speak up when a signal is clearly degraded.
const (
	// queuePressureRatio fires the queue_size proposal when the queue is
	// this full (>=80%) or any drop-oldest eviction has occurred.
	queuePressureRatio = 0.8
	// maxQueueProposal caps the proposed queue_size so a single burst
	// cannot recommend unbounded memory growth.
	maxQueueProposal = 10000
	// breakerTripWarn fires the retry/breaker proposal after this many
	// consecutive export failures (below the 5-error trip so advice
	// lands before the circuit opens).
	breakerTripWarn = uint32(3)
	// maxRetryProposal caps the proposed retry_max_attempts.
	maxRetryProposal = 5
	// exportP99SLO mirrors the AgentExportLatencyHigh rule (p99 < 5s).
	exportP99SLO = 5 * time.Second
	// minCollectionInterval is the floor the interval proposals respect.
	minCollectionInterval = 15 * time.Second
	// maxCollectionInterval caps the proposed collection/hostmetrics interval.
	maxCollectionInterval = 5 * time.Minute
)

// BreakerState describes the circuit breaker in a Signals snapshot.
// It mirrors gobreaker states without importing gobreaker, keeping this
// package dependency-free (read-only observer, no wiring into config paths).
type BreakerState int

const (
	BreakerClosed BreakerState = iota
	BreakerOpen
	BreakerHalfOpen
)

// Signals is a point-in-time, read-only snapshot of the control-loop inputs.
// All fields are observed values; Observe never mutates them or any config.
type Signals struct {
	// Queue signals (bounded heartbeat queue, IND-2).
	QueueLen      int
	QueueCapacity int
	QueueDropped  int64
	// Breaker signals (gobreaker guarding OTLP export, IND-2).
	BreakerState        BreakerState
	ConsecutiveFailures uint32
	// Export signals (OTLP export health).
	ExportP99Latency time.Duration
	ExportFailRate   float64 // 0.0-1.0 fraction of recent exports failing
	// Interval signals (current configured cadences, observed read-only).
	CollectionInterval  time.Duration
	HostmetricsInterval time.Duration
	QueueSize           int // current resilience.queue_size
	RetryMaxAttempts    int // current resilience.retry_max_attempts
}

// Recommendation is a single advisory proposal: what to change, from what
// to what, and why. Phase1 emits these only — nothing applies them.
type Recommendation struct {
	Param    string // config knob, e.g. "resilience.queue_size"
	Current  string // current value as string
	Proposed string // proposed value as string
	Reason   string // human-readable why
}

// Observe evaluates signals and returns advisory recommendations.
// It is pure and read-only: no logging, no metrics, no config mutation.
// An empty slice means "healthy, no advice" — callers must stay quiet.
func Observe(s Signals) []Recommendation {
	var out []Recommendation

	// 1. Queue pressure → grow resilience.queue_size (drop-oldest is lossy).
	if s.QueueCapacity > 0 {
		pressure := float64(s.QueueLen) / float64(s.QueueCapacity)
		if s.QueueDropped > 0 || pressure >= queuePressureRatio {
			current := s.QueueSize
			if current <= 0 {
				current = s.QueueCapacity
			}
			proposed := current * 2
			if proposed <= current {
				proposed = current + 1
			}
			if proposed > maxQueueProposal {
				proposed = maxQueueProposal
			}
			if proposed > current {
				out = append(out, Recommendation{
					Param:    "resilience.queue_size",
					Current:  itoa(current),
					Proposed: itoa(proposed),
					Reason:   queueReason(s.QueueLen, s.QueueCapacity, s.QueueDropped),
				})
			}
		}
	}

	// 2. Breaker stress → raise resilience.retry_max_attempts (one step).
	// Advisory only: never touches the breaker itself.
	if s.BreakerState == BreakerOpen || s.BreakerState == BreakerHalfOpen ||
		s.ConsecutiveFailures >= breakerTripWarn || s.ExportFailRate > 0 {
		current := s.RetryMaxAttempts
		if current <= 0 {
			current = 3 // plan default (IND-2)
		}
		if current < maxRetryProposal {
			out = append(out, Recommendation{
				Param:    "resilience.retry_max_attempts",
				Current:  itoa(current),
				Proposed: itoa(current + 1),
				Reason:   breakerReason(s.BreakerState, s.ConsecutiveFailures, s.ExportFailRate),
			})
		}
	}

	// 3. Export latency above SLO → slow the collection cadence (less pressure).
	if s.ExportP99Latency > exportP99SLO && s.CollectionInterval > 0 {
		proposed := s.CollectionInterval * 2
		if proposed <= s.CollectionInterval {
			proposed = s.CollectionInterval + minCollectionInterval
		}
		if proposed > maxCollectionInterval {
			proposed = maxCollectionInterval
		}
		if proposed > s.CollectionInterval {
			out = append(out, Recommendation{
				Param:    "agent.collection_interval",
				Current:  s.CollectionInterval.String(),
				Proposed: proposed.String(),
				Reason:   "export p99 " + s.ExportP99Latency.String() + " above 5s SLO; halving collection pressure (advisory only)",
			})
		}
	}

	// 4. Queue pressure + fast hostmetrics → slow hostmetrics cadence.
	if s.HostmetricsInterval > 0 && s.HostmetricsInterval < minCollectionInterval*2 &&
		s.QueueCapacity > 0 && (s.QueueDropped > 0 ||
			float64(s.QueueLen)/float64(s.QueueCapacity) >= queuePressureRatio) {
		proposed := s.HostmetricsInterval * 2
		if proposed > maxCollectionInterval {
			proposed = maxCollectionInterval
		}
		if proposed > s.HostmetricsInterval {
			out = append(out, Recommendation{
				Param:    "receiver.hostmetrics.collection_interval",
				Current:  s.HostmetricsInterval.String(),
				Proposed: proposed.String(),
				Reason:   "hostmetrics cadence faster than 30s while queue is pressured; slowing receiver (advisory only)",
			})
		}
	}

	if out == nil {
		return []Recommendation{}
	}
	return out
}

// Observer emits advisory recommendations as a structured log + OTel metric.
// Phase1 wiring: none — nothing constructs this outside tests/docs yet.
// A nil logger uses slog.Default; a nil counter skips metric export.
type Observer struct {
	logger  *slog.Logger
	counter metric.Int64Counter
}

// New builds an Observer. logger may be nil, counter may be nil.
func New(logger *slog.Logger, counter metric.Int64Counter) *Observer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Observer{logger: logger, counter: counter}
}

// Emit observes signals and reports each recommendation once via slog.Info
// (message omniwatch.agent.autotune.recommendation) plus one counter Add per
// recommendation with param/current/proposed attributes. It returns the
// recommendations for tests/callers. Zero recommendations → silent.
func (o *Observer) Emit(ctx context.Context, s Signals) []Recommendation {
	recs := Observe(s)
	for _, r := range recs {
		o.logger.Info(RecommendationLog,
			"param", r.Param,
			"current", r.Current,
			"proposed", r.Proposed,
			"reason", r.Reason,
			"mode", "advisory",
		)
		if o.counter != nil {
			o.counter.Add(ctx, 1, metric.WithAttributes(
				attribute.String("param", r.Param),
				attribute.String("current", r.Current),
				attribute.String("proposed", r.Proposed),
			))
		}
	}
	return recs
}

func queueReason(length, capacity int, dropped int64) string {
	if dropped > 0 {
		return "bounded queue dropped " + itoa64(dropped) + " oldest heartbeat(s) (" +
			itoa(length) + "/" + itoa(capacity) + " full); doubling queue_size absorbs export stalls (advisory only)"
	}
	return "queue " + itoa(length) + "/" + itoa(capacity) + " over 80% full; doubling queue_size before drops start (advisory only)"
}

func breakerReason(state BreakerState, failures uint32, failRate float64) string {
	switch state {
	case BreakerOpen:
		return "circuit breaker OPEN with " + itoaU32(failures) + " consecutive failures; one extra retry attempt rides out transient stalls — fix the endpoint first (advisory only)"
	case BreakerHalfOpen:
		return "circuit breaker half-open probing with " + itoaU32(failures) + " consecutive failures; one extra retry attempt steadies the probe (advisory only)"
	default:
		if failRate > 0 {
			return "export fail rate above zero with " + itoaU32(failures) + " consecutive failures; one extra retry attempt before breaker trips (advisory only)"
		}
		return itoaU32(failures) + " consecutive export failures approaching the 5-error trip; one extra retry attempt (advisory only)"
	}
}

func itoa(n int) string {
	neg := n < 0
	if neg {
		n = -n
	}
	buf := [20]byte{}
	i := len(buf)
	if n == 0 {
		i--
		buf[i] = '0'
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func itoa64(n int64) string {
	neg := n < 0
	if neg {
		n = -n
	}
	buf := [20]byte{}
	i := len(buf)
	if n == 0 {
		i--
		buf[i] = '0'
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func itoaU32(n uint32) string { return itoa64(int64(n)) }
