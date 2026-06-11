package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sean-kim05/hookline/internal/event"
)

func newTestEvent() event.Event {
	return event.Event{
		Endpoint: "https://example.test/hook",
		Payload:  []byte(`{"hello":"world"}`),
	}
}

func TestEnqueueThenLease(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()

	id, err := q.Enqueue(ctx, newTestEvent())
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	leases, err := q.Lease(ctx, 10, time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("got %d leases, want 1", len(leases))
	}
	if leases[0].Message.ID != id {
		t.Errorf("leased message ID = %q, want %q", leases[0].Message.ID, id)
	}
	if leases[0].Token == 0 {
		t.Error("lease token must be non-zero")
	}
	if leases[0].Message.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1 on first lease", leases[0].Message.Attempts)
	}
}

func TestAckRemovesMessage(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()
	if _, err := q.Enqueue(ctx, newTestEvent()); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	leases, _ := q.Lease(ctx, 1, time.Minute)
	if err := q.Ack(ctx, leases[0].Message.ID, leases[0].Token); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	again, _ := q.Lease(ctx, 10, time.Minute)
	if len(again) != 0 {
		t.Errorf("got %d leases after ack, want 0", len(again))
	}
}

func TestNackReschedules(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	q := NewMemoryQueue()
	q.now = clock.now
	ctx := context.Background()
	if _, err := q.Enqueue(ctx, newTestEvent()); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	leases, _ := q.Lease(ctx, 1, time.Minute)
	if err := q.Nack(ctx, leases[0].Message.ID, leases[0].Token, 30*time.Second); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// Not ready until retryAfter elapses.
	if got, _ := q.Lease(ctx, 1, time.Minute); len(got) != 0 {
		t.Fatal("message leasable before retryAfter elapsed")
	}

	clock.advance(30 * time.Second)
	got, _ := q.Lease(ctx, 1, time.Minute)
	if len(got) != 1 {
		t.Fatalf("got %d leases after retry delay, want 1", len(got))
	}
	if got[0].Message.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2", got[0].Message.Attempts)
	}
}

func TestExpiredLeaseBecomesAvailable(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	q := NewMemoryQueue()
	q.now = clock.now
	ctx := context.Background()
	if _, err := q.Enqueue(ctx, newTestEvent()); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	first, _ := q.Lease(ctx, 1, time.Minute)
	if len(first) != 1 {
		t.Fatal("expected first lease")
	}

	// Still under a live lease: nobody else can take it.
	if got, _ := q.Lease(ctx, 1, time.Minute); len(got) != 0 {
		t.Fatal("message leasable while lease still live")
	}

	clock.advance(2 * time.Minute) // lease lapses
	second, _ := q.Lease(ctx, 1, time.Minute)
	if len(second) != 1 {
		t.Fatal("expired lease did not become available")
	}
	if second[0].Token <= first[0].Token {
		t.Errorf("re-lease token = %d, want > %d", second[0].Token, first[0].Token)
	}
}

// TestStaleLeaseFencing is the core correctness property: a worker whose lease
// has expired must not be able to Ack or Nack the message after it has been
// re-leased to someone else. This is exactly the double-delivery race that the
// fencing token prevents.
func TestStaleLeaseFencing(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	q := NewMemoryQueue()
	q.now = clock.now
	ctx := context.Background()
	if _, err := q.Enqueue(ctx, newTestEvent()); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	stale, _ := q.Lease(ctx, 1, time.Minute)
	clock.advance(2 * time.Minute) // stale worker's lease expires
	fresh, _ := q.Lease(ctx, 1, time.Minute)
	if len(fresh) != 1 {
		t.Fatal("expected re-lease after expiry")
	}

	// The stale worker tries to finish its work — both paths must be rejected.
	if err := q.Ack(ctx, stale[0].Message.ID, stale[0].Token); !errors.Is(err, ErrStaleLease) {
		t.Errorf("stale Ack error = %v, want ErrStaleLease", err)
	}
	if err := q.Nack(ctx, stale[0].Message.ID, stale[0].Token, time.Second); !errors.Is(err, ErrStaleLease) {
		t.Errorf("stale Nack error = %v, want ErrStaleLease", err)
	}

	// The fresh worker can still complete normally.
	if err := q.Ack(ctx, fresh[0].Message.ID, fresh[0].Token); err != nil {
		t.Errorf("fresh Ack error = %v, want nil", err)
	}
}

func TestLeaseRespectsLimit(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := q.Enqueue(ctx, newTestEvent()); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	leases, _ := q.Lease(ctx, 2, time.Minute)
	if len(leases) != 2 {
		t.Fatalf("got %d leases, want 2", len(leases))
	}
}

func TestAckUnknownMessage(t *testing.T) {
	q := NewMemoryQueue()
	if err := q.Ack(context.Background(), "does-not-exist", 1); !errors.Is(err, ErrNotFound) {
		t.Errorf("Ack unknown error = %v, want ErrNotFound", err)
	}
}

// fakeClock is a manually-advanced clock for deterministic time-based tests.
type fakeClock struct {
	t time.Time
}

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }
