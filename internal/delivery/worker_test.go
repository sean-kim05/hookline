package delivery

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sean-kim05/hookline/internal/event"
	"github.com/sean-kim05/hookline/internal/queue"
)

// stubDeliverer returns whatever its fn produces, recording call count.
type stubDeliverer struct {
	fn    func(queue.Message) Result
	calls atomic.Int64
}

func (s *stubDeliverer) Deliver(_ context.Context, m queue.Message) Result {
	s.calls.Add(1)
	return s.fn(m)
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newWorker wires a worker over q with a fixed clock and zero-jitter backoff so
// retry timing is exact.
func newWorker(t *testing.T, q queue.Queue, d Deliverer, sink DeadLetterSink, now func() time.Time, max int) *Worker {
	t.Helper()
	bo := NewBackoff(time.Second, time.Hour)
	bo.rand = func() float64 { return 0.9999999 } // deterministic: always the ceiling
	w, err := New(Config{
		Queue:       q,
		Deliverer:   d,
		Sink:        sink,
		Backoff:     bo,
		MaxAttempts: max,
		BatchSize:   10,
		LeaseFor:    time.Minute,
		Logger:      quietLogger(),
		Now:         now,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return w
}

func TestWorkerAcksOnSuccess(t *testing.T) {
	clk := &testClock{t: time.Unix(1_700_000_000, 0)}
	q := queue.NewMemoryQueue(queue.WithClock(clk.now))
	mustEnqueue(t, q, event.Event{Endpoint: "https://x.test", Payload: []byte("{}")})

	d := &stubDeliverer{fn: func(queue.Message) Result { return Result{Success: true, StatusCode: 200} }}
	w := newWorker(t, q, d, &MemoryDeadLetterSink{}, clk.now, 5)

	n, err := w.ProcessBatch(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("ProcessBatch = (%d, %v), want (1, nil)", n, err)
	}
	// Acked => gone from the queue.
	if got, _ := q.Lease(context.Background(), 10, time.Minute); len(got) != 0 {
		t.Errorf("message still queued after successful delivery: %d", len(got))
	}
}

func TestWorkerNacksWithBackoffOnFailure(t *testing.T) {
	clk := &testClock{t: time.Unix(1_700_000_000, 0)}
	q := queue.NewMemoryQueue(queue.WithClock(clk.now))
	mustEnqueue(t, q, event.Event{Endpoint: "https://x.test", Payload: []byte("{}")})

	d := &stubDeliverer{fn: func(queue.Message) Result { return Result{StatusCode: 503} }}
	w := newWorker(t, q, d, &MemoryDeadLetterSink{}, clk.now, 5)

	if _, err := w.ProcessBatch(context.Background()); err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}

	// First attempt (Attempts=1) failed; backoff ceiling = base*2^0 = 1s.
	// Not ready before the delay elapses.
	if got, _ := q.Lease(context.Background(), 10, time.Minute); len(got) != 0 {
		t.Fatal("message leasable before its backoff elapsed")
	}
	clk.advance(time.Second)
	got, _ := q.Lease(context.Background(), 10, time.Minute)
	if len(got) != 1 {
		t.Fatalf("got %d leases after backoff, want 1", len(got))
	}
	if got[0].Message.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2 on retry", got[0].Message.Attempts)
	}
}

func TestWorkerDeadLettersWhenExhausted(t *testing.T) {
	clk := &testClock{t: time.Unix(1_700_000_000, 0)}
	q := queue.NewMemoryQueue(queue.WithClock(clk.now))
	mustEnqueue(t, q, event.Event{Endpoint: "https://x.test", Payload: []byte("{}")})

	d := &stubDeliverer{fn: func(queue.Message) Result { return Result{StatusCode: 500} }}
	sink := &MemoryDeadLetterSink{}
	const maxAttempts = 3
	w := newWorker(t, q, d, sink, clk.now, maxAttempts)

	ctx := context.Background()
	// Drive attempts: each ProcessBatch leases the (now ready) message, fails,
	// and either reschedules or dead-letters. Advance past each backoff.
	for i := 0; i < maxAttempts; i++ {
		if _, err := w.ProcessBatch(ctx); err != nil {
			t.Fatalf("ProcessBatch %d: %v", i, err)
		}
		clk.advance(time.Hour) // skip any backoff so the retry is ready
	}

	if sink.Len() != 1 {
		t.Fatalf("dead-letter count = %d, want 1", sink.Len())
	}
	if d.calls.Load() != maxAttempts {
		t.Errorf("deliver calls = %d, want %d", d.calls.Load(), maxAttempts)
	}
	// Dead-lettered => removed from the queue.
	if got, _ := q.Lease(ctx, 10, time.Minute); len(got) != 0 {
		t.Errorf("message still queued after dead-letter: %d", len(got))
	}
	entries := sink.Entries()
	if entries[0].Message.Attempts != maxAttempts {
		t.Errorf("dead letter Attempts = %d, want %d", entries[0].Message.Attempts, maxAttempts)
	}
}

func TestWorkerConcurrentBatch(t *testing.T) {
	clk := &testClock{t: time.Unix(1_700_000_000, 0)}
	q := queue.NewMemoryQueue(queue.WithClock(clk.now))
	const total = 50
	for i := 0; i < total; i++ {
		mustEnqueue(t, q, event.Event{Endpoint: "https://x.test", Payload: []byte("{}")})
	}

	var delivered sync.Map
	d := &stubDeliverer{fn: func(m queue.Message) Result {
		delivered.Store(m.ID, true)
		return Result{Success: true, StatusCode: 200}
	}}
	w, err := New(Config{
		Queue: q, Deliverer: d, Sink: &MemoryDeadLetterSink{},
		BatchSize: total, Concurrency: 8, LeaseFor: time.Minute,
		Logger: quietLogger(), Now: clk.now,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	n, err := w.ProcessBatch(context.Background())
	if err != nil || n != total {
		t.Fatalf("ProcessBatch = (%d, %v), want (%d, nil)", n, err, total)
	}
	count := 0
	delivered.Range(func(_, _ any) bool { count++; return true })
	if count != total {
		t.Errorf("delivered %d distinct messages, want %d", count, total)
	}
	if got, _ := q.Lease(context.Background(), total, time.Minute); len(got) != 0 {
		t.Errorf("%d messages remain queued after a full successful batch", len(got))
	}
}

// mustEnqueue is a local helper mirroring the conformance suite's.
func mustEnqueue(t *testing.T, q queue.Queue, ev event.Event) string {
	t.Helper()
	id, err := q.Enqueue(context.Background(), ev)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return id
}

// testClock is a manually-advanced clock, safe for the concurrent batch test.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}
