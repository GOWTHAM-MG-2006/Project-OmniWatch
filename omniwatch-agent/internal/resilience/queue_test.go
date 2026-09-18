// OmniWatch — Agent
// Component: resilience (tests: bounded queue)
// Phase: industry-ready (IND-2)
// Purpose: Unit tests for drop-oldest backpressure + metric counting
// Inputs: synthetic enqueue bursts
// Outputs: go test results
package resilience

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// discardLogger is the Go 1.23 spelling of slog.DiscardHandler (added in 1.24).
func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestQueueFIFOOrder(t *testing.T) {
	q := NewBoundedQueue[int](3, discardLogger(), nil)
	q.TryEnqueue(1)
	q.TryEnqueue(2)
	q.TryEnqueue(3)
	for _, want := range []int{1, 2, 3} {
		got, ok := q.Dequeue()
		if !ok {
			t.Fatalf("Dequeue ok = false, want item %d", want)
		}
		if got != want {
			t.Errorf("Dequeue = %d, want %d", got, want)
		}
	}
	if _, ok := q.Dequeue(); ok {
		t.Errorf("Dequeue on empty ok = true, want false")
	}
}

func TestQueueDropOldestOnFull(t *testing.T) {
	tests := []struct {
		name      string
		capacity  int
		enqueues  []int
		wantItems []int
		wantDrop  int64
	}{
		{"size 1 rapid heartbeats keep newest", 1, []int{1, 2, 3}, []int{3}, 2},
		{"size 2 drops oldest", 2, []int{1, 2, 3, 4}, []int{3, 4}, 2},
		{"exact fit no drops", 3, []int{1, 2, 3}, []int{1, 2, 3}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewBoundedQueue[int](tt.capacity, discardLogger(), nil)
			for _, v := range tt.enqueues {
				q.TryEnqueue(v)
			}
			if got := q.Dropped(); got != tt.wantDrop {
				t.Errorf("Dropped() = %d, want %d", got, tt.wantDrop)
			}
			if got := q.Len(); got != len(tt.wantItems) {
				t.Fatalf("Len() = %d, want %d", got, len(tt.wantItems))
			}
			for _, want := range tt.wantItems {
				got, ok := q.Dequeue()
				if !ok {
					t.Fatalf("Dequeue ok = false, want %d", want)
				}
				if got != want {
					t.Errorf("Dequeue = %d, want %d (oldest must be dropped first)", got, want)
				}
			}
		})
	}
}

func TestQueueCapacityClamped(t *testing.T) {
	for _, cap := range []int{0, -5} {
		q := NewBoundedQueue[int](cap, discardLogger(), nil)
		if q.Capacity() != 1 {
			t.Errorf("Capacity() = %d for input %d, want clamp to 1", q.Capacity(), cap)
		}
	}
}

func TestQueueDroppedCounterIncrement(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	counter, err := provider.Meter("test").Int64Counter(QueueDroppedMetric)
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	q := NewBoundedQueue[int](1, discardLogger(), counter)
	q.TryEnqueue(1)
	q.TryEnqueue(2) // drop 1
	q.TryEnqueue(3) // drop 2

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	var sum int64
	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != QueueDroppedMetric {
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
		t.Fatalf("metric %q not exported", QueueDroppedMetric)
	}
	if sum != 2 {
		t.Errorf("counter sum = %d, want 2", sum)
	}
}

func TestQueueNeverBlocksMainGoroutine(t *testing.T) {
	q := NewBoundedQueue[int](4, discardLogger(), nil)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				q.TryEnqueue(g*1000 + i) // must never block
			}
		}(g)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done: // all 4000 enqueues complete; Len bounded, drops counted
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for concurrent enqueues (TryEnqueue must never block)")
	}
	if got := q.Len(); got != q.Capacity() {
		t.Errorf("Len() = %d, want full capacity %d after burst", got, q.Capacity())
	}
	if got, want := q.Dropped(), int64(4000-q.Capacity()); got != want {
		t.Errorf("Dropped() = %d, want %d", got, want)
	}
}
