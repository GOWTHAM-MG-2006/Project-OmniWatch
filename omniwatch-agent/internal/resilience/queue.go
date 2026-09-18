// OmniWatch — Agent
// Component: resilience (bounded queue)
// Phase: industry-ready (IND-2)
// Purpose: Non-blocking bounded queue with drop-oldest backpressure
// Inputs: Items enqueued via TryEnqueue (never blocks the caller)
// Outputs: FIFO dequeue for a drain worker; drops logged + counted
package resilience

import (
	"context"
	"log/slog"
	"sync"

	"go.opentelemetry.io/otel/metric"
)

// QueueDroppedMetric is the exact counter name the T9 E2E test asserts on.
// Do NOT rename without updating the E2E test and the plan acceptance criteria.
const QueueDroppedMetric = "omniwatch.agent.queue_dropped"

// BoundedQueue is a fixed-capacity FIFO. TryEnqueue never blocks: when the
// queue is full it evicts the OLDEST item, logs a warning with the drop
// count, and increments the omniwatch.agent.queue_dropped OTel counter.
// A nil counter is allowed (drops are still counted locally and logged).
type BoundedQueue[T any] struct {
	mu       sync.Mutex
	items    []T
	capacity int
	logger   *slog.Logger
	counter  metric.Int64Counter
	dropped  int64
}

// NewBoundedQueue builds a queue holding at most capacity items. A
// non-positive capacity is clamped to 1. logger may be nil (slog.Default is
// used); counter may be nil (no OTel export, local count still kept).
func NewBoundedQueue[T any](capacity int, logger *slog.Logger, counter metric.Int64Counter) *BoundedQueue[T] {
	if capacity <= 0 {
		capacity = 1
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &BoundedQueue[T]{
		items:    make([]T, 0, capacity),
		capacity: capacity,
		logger:   logger,
		counter:  counter,
	}
}

// TryEnqueue appends item without ever blocking. On a full queue the oldest
// item is dropped first; the drop is logged at Warn with the running dropped
// total and reported to the OTel counter with a background context (counter
// Add is non-blocking and allocation-free on the hot path).
func (q *BoundedQueue[T]) TryEnqueue(item T) {
	q.mu.Lock()
	for len(q.items) >= q.capacity {
		var zero T
		q.items[0] = zero // release reference for GC
		q.items = q.items[1:]
		q.dropped++
		dropped := q.dropped
		capacity := q.capacity
		q.mu.Unlock()
		q.logger.Warn("resilience: queue full, dropped oldest item",
			"queue_capacity", capacity,
			"dropped_total", dropped,
		)
		if q.counter != nil {
			q.counter.Add(context.Background(), 1)
		}
		q.mu.Lock()
	}
	q.items = append(q.items, item)
	q.mu.Unlock()
}

// Dequeue removes and returns the oldest item, or false when empty.
func (q *BoundedQueue[T]) Dequeue() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		var zero T
		return zero, false
	}
	item := q.items[0]
	var zero T
	q.items[0] = zero // release reference for GC
	q.items = q.items[1:]
	return item, true
}

// Len returns the current number of queued items.
func (q *BoundedQueue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// Capacity returns the maximum number of items the queue holds.
func (q *BoundedQueue[T]) Capacity() int { return q.capacity }

// Dropped returns the running total of drop-oldest evictions.
func (q *BoundedQueue[T]) Dropped() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dropped
}
