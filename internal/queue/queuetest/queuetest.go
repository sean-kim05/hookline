// Package queuetest is the shared conformance suite for queue.Queue
// implementations. Every backend (in-memory, Postgres, WAL) must pass Run
// unmodified — that is what guarantees identical delivery semantics when one
// backend is swapped for another.
//
// The suite owns the clock: a Factory receives a now func and must build a
// queue that reads all time (readiness, lease expiry) from it.
package queuetest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sean-kim05/hookline/internal/event"
	"github.com/sean-kim05/hookline/internal/queue"
)

// Factory builds a fresh, empty Queue for one conformance subtest. The queue
// must use now as its only clock source and must be empty of messages. Use
// t.Cleanup to release resources.
type Factory func(t *testing.T, now func() time.Time) queue.Queue

// Run executes the full conformance suite against the implementation under
// test.
func Run(t *testing.T, f Factory) {
	t.Helper()

	tests := []struct {
		name string
		fn   func(t *testing.T, q queue.Queue, clk *Clock)
	}{
		{"EnqueueThenLease", testEnqueueThenLease},
		{"EventRoundTrip", testEventRoundTrip},
		{"LeaseEmptyQueue", testLeaseEmptyQueue},
		{"LeaseRespectsLimit", testLeaseRespectsLimit},
		{"LeaseFIFOOrder", testLeaseFIFOOrder},
		{"LiveLeaseIsExclusive", testLiveLeaseIsExclusive},
		{"ExpiredLeaseBecomesAvailable", testExpiredLeaseBecomesAvailable},
		{"AckRemovesMessage", testAckRemovesMessage},
		{"AckUnknownMessage", testAckUnknownMessage},
		{"NackUnknownMessage", testNackUnknownMessage},
		{"NackReschedules", testNackReschedules},
		{"NackZeroDelayIsImmediatelyReady", testNackZeroDelay},
		{"StaleLeaseFencing", testStaleLeaseFencing},
		{"LateAckWithCurrentTokenSucceeds", testLateAckCurrentToken},
		{"ConcurrentLeaseNoDoubleClaim", testConcurrentLeaseNoDoubleClaim},
		{"ModelBasedRandomOps", testModelBasedRandomOps},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clk := NewClock()
			q := f(t, clk.Now)
			tc.fn(t, q, clk)
		})
	}
}

func testEvent(i int) event.Event {
	return event.Event{
		Endpoint: fmt.Sprintf("https://example.test/hook/%d", i),
		Payload:  []byte(fmt.Sprintf(`{"seq":%d}`, i)),
	}
}

// mustEnqueue enqueues and fails the test on error.
func mustEnqueue(t *testing.T, q queue.Queue, ev event.Event) string {
	t.Helper()
	id, err := q.Enqueue(context.Background(), ev)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return id
}

// mustLease leases and fails the test on error.
func mustLease(t *testing.T, q queue.Queue, n int, d time.Duration) []queue.Lease {
	t.Helper()
	leases, err := q.Lease(context.Background(), n, d)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	return leases
}

func testEnqueueThenLease(t *testing.T, q queue.Queue, _ *Clock) {
	id := mustEnqueue(t, q, testEvent(0))

	leases := mustLease(t, q, 10, time.Minute)
	if len(leases) != 1 {
		t.Fatalf("got %d leases, want 1", len(leases))
	}
	l := leases[0]
	if l.Message.ID != id {
		t.Errorf("leased message ID = %q, want %q", l.Message.ID, id)
	}
	if l.Token == 0 {
		t.Error("lease token must be non-zero")
	}
	if l.Message.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1 on first lease", l.Message.Attempts)
	}
	if l.Message.EnqueuedAt.IsZero() {
		t.Error("EnqueuedAt must be set")
	}
}

func testEventRoundTrip(t *testing.T, q queue.Queue, _ *Clock) {
	in := event.Event{
		Endpoint:       "https://example.test/orders",
		Payload:        []byte(`{"order_id":42,"total":"19.99"}`),
		ContentType:    "application/json",
		IdempotencyKey: "order-42-created",
	}
	mustEnqueue(t, q, in)

	leases := mustLease(t, q, 1, time.Minute)
	if len(leases) != 1 {
		t.Fatalf("got %d leases, want 1", len(leases))
	}
	out := leases[0].Message.Event
	if out.ID == "" {
		t.Error("event ID must be assigned on enqueue")
	}
	if out.Endpoint != in.Endpoint {
		t.Errorf("Endpoint = %q, want %q", out.Endpoint, in.Endpoint)
	}
	if string(out.Payload) != string(in.Payload) {
		t.Errorf("Payload = %q, want %q", out.Payload, in.Payload)
	}
	if out.ContentType != in.ContentType {
		t.Errorf("ContentType = %q, want %q", out.ContentType, in.ContentType)
	}
	if out.IdempotencyKey != in.IdempotencyKey {
		t.Errorf("IdempotencyKey = %q, want %q", out.IdempotencyKey, in.IdempotencyKey)
	}
	if out.CreatedAt.IsZero() {
		t.Error("CreatedAt must be assigned on enqueue")
	}
}

func testLeaseEmptyQueue(t *testing.T, q queue.Queue, _ *Clock) {
	if leases := mustLease(t, q, 10, time.Minute); len(leases) != 0 {
		t.Errorf("got %d leases from empty queue, want 0", len(leases))
	}
}

func testLeaseRespectsLimit(t *testing.T, q queue.Queue, _ *Clock) {
	for i := 0; i < 5; i++ {
		mustEnqueue(t, q, testEvent(i))
	}
	if leases := mustLease(t, q, 2, time.Minute); len(leases) != 2 {
		t.Fatalf("got %d leases, want 2", len(leases))
	}
	// The remaining three are still available.
	if leases := mustLease(t, q, 10, time.Minute); len(leases) != 3 {
		t.Fatalf("got %d remaining leases, want 3", len(leases))
	}
}

func testLeaseFIFOOrder(t *testing.T, q queue.Queue, clk *Clock) {
	// Enqueue at strictly increasing times so FIFO order is well-defined.
	var ids []string
	for i := 0; i < 4; i++ {
		ids = append(ids, mustEnqueue(t, q, testEvent(i)))
		clk.Advance(time.Millisecond)
	}
	leases := mustLease(t, q, 10, time.Minute)
	if len(leases) != 4 {
		t.Fatalf("got %d leases, want 4", len(leases))
	}
	for i, l := range leases {
		if l.Message.ID != ids[i] {
			t.Fatalf("lease[%d].ID = %q, want %q (FIFO order)", i, l.Message.ID, ids[i])
		}
	}
}

func testLiveLeaseIsExclusive(t *testing.T, q queue.Queue, _ *Clock) {
	mustEnqueue(t, q, testEvent(0))
	if got := mustLease(t, q, 1, time.Minute); len(got) != 1 {
		t.Fatalf("got %d leases, want 1", len(got))
	}
	if got := mustLease(t, q, 10, time.Minute); len(got) != 0 {
		t.Fatal("message leasable while another lease is live")
	}
}

func testExpiredLeaseBecomesAvailable(t *testing.T, q queue.Queue, clk *Clock) {
	mustEnqueue(t, q, testEvent(0))

	first := mustLease(t, q, 1, time.Minute)
	if len(first) != 1 {
		t.Fatal("expected first lease")
	}

	clk.Advance(2 * time.Minute) // lease lapses

	second := mustLease(t, q, 1, time.Minute)
	if len(second) != 1 {
		t.Fatal("expired lease did not become available")
	}
	if second[0].Token <= first[0].Token {
		t.Errorf("re-lease token = %d, want > %d (tokens must increase)",
			second[0].Token, first[0].Token)
	}
	if second[0].Message.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2 after re-lease", second[0].Message.Attempts)
	}
}

func testAckRemovesMessage(t *testing.T, q queue.Queue, _ *Clock) {
	mustEnqueue(t, q, testEvent(0))
	leases := mustLease(t, q, 1, time.Minute)
	if err := q.Ack(context.Background(), leases[0].Message.ID, leases[0].Token); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if again := mustLease(t, q, 10, time.Minute); len(again) != 0 {
		t.Errorf("got %d leases after ack, want 0", len(again))
	}
	// Acked means gone: a second Ack reports the message as unknown.
	err := q.Ack(context.Background(), leases[0].Message.ID, leases[0].Token)
	if !errors.Is(err, queue.ErrNotFound) {
		t.Errorf("double Ack error = %v, want ErrNotFound", err)
	}
}

func testAckUnknownMessage(t *testing.T, q queue.Queue, _ *Clock) {
	if err := q.Ack(context.Background(), "does-not-exist", 1); !errors.Is(err, queue.ErrNotFound) {
		t.Errorf("Ack unknown error = %v, want ErrNotFound", err)
	}
}

func testNackUnknownMessage(t *testing.T, q queue.Queue, _ *Clock) {
	err := q.Nack(context.Background(), "does-not-exist", 1, time.Second)
	if !errors.Is(err, queue.ErrNotFound) {
		t.Errorf("Nack unknown error = %v, want ErrNotFound", err)
	}
}

func testNackReschedules(t *testing.T, q queue.Queue, clk *Clock) {
	mustEnqueue(t, q, testEvent(0))
	leases := mustLease(t, q, 1, time.Minute)
	if err := q.Nack(context.Background(), leases[0].Message.ID, leases[0].Token, 30*time.Second); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// Not ready until retryAfter elapses.
	if got := mustLease(t, q, 1, time.Minute); len(got) != 0 {
		t.Fatal("message leasable before retryAfter elapsed")
	}

	clk.Advance(30 * time.Second)
	got := mustLease(t, q, 1, time.Minute)
	if len(got) != 1 {
		t.Fatalf("got %d leases after retry delay, want 1", len(got))
	}
	if got[0].Message.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2", got[0].Message.Attempts)
	}
}

func testNackZeroDelay(t *testing.T, q queue.Queue, _ *Clock) {
	mustEnqueue(t, q, testEvent(0))
	leases := mustLease(t, q, 1, time.Minute)
	if err := q.Nack(context.Background(), leases[0].Message.ID, leases[0].Token, 0); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if got := mustLease(t, q, 1, time.Minute); len(got) != 1 {
		t.Fatal("message with zero retryAfter must be immediately leasable")
	}
}

func testStaleLeaseFencing(t *testing.T, q queue.Queue, clk *Clock) {
	mustEnqueue(t, q, testEvent(0))

	stale := mustLease(t, q, 1, time.Minute)
	clk.Advance(2 * time.Minute) // stale worker's lease expires
	fresh := mustLease(t, q, 1, time.Minute)
	if len(fresh) != 1 {
		t.Fatal("expected re-lease after expiry")
	}

	// The stale worker tries to finish its work — both paths must be rejected.
	ctx := context.Background()
	if err := q.Ack(ctx, stale[0].Message.ID, stale[0].Token); !errors.Is(err, queue.ErrStaleLease) {
		t.Errorf("stale Ack error = %v, want ErrStaleLease", err)
	}
	if err := q.Nack(ctx, stale[0].Message.ID, stale[0].Token, time.Second); !errors.Is(err, queue.ErrStaleLease) {
		t.Errorf("stale Nack error = %v, want ErrStaleLease", err)
	}

	// The fresh worker can still complete normally.
	if err := q.Ack(ctx, fresh[0].Message.ID, fresh[0].Token); err != nil {
		t.Errorf("fresh Ack error = %v, want nil", err)
	}
}

// testLateAckCurrentToken: a lease that expired but was NOT re-leased still
// holds the current token, so its Ack must succeed. The work was done; nobody
// else claimed it; rejecting would force a pointless redelivery.
func testLateAckCurrentToken(t *testing.T, q queue.Queue, clk *Clock) {
	mustEnqueue(t, q, testEvent(0))
	leases := mustLease(t, q, 1, time.Minute)
	clk.Advance(2 * time.Minute) // lease expired, but no one re-leased

	if err := q.Ack(context.Background(), leases[0].Message.ID, leases[0].Token); err != nil {
		t.Errorf("late Ack with current token error = %v, want nil", err)
	}
	if got := mustLease(t, q, 10, time.Minute); len(got) != 0 {
		t.Error("message still leasable after late Ack")
	}
}

// testConcurrentLeaseNoDoubleClaim hammers Lease from several goroutines and
// asserts no message is handed out twice while its lease is live. This is the
// race-detector workout for the claim path (FOR UPDATE SKIP LOCKED in
// Postgres, the mutex in memory).
func testConcurrentLeaseNoDoubleClaim(t *testing.T, q queue.Queue, _ *Clock) {
	const total = 100
	for i := 0; i < total; i++ {
		mustEnqueue(t, q, testEvent(i))
	}

	var (
		mu      sync.Mutex
		claimed []string
		wg      sync.WaitGroup
	)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				leases, err := q.Lease(context.Background(), 5, time.Hour)
				if err != nil {
					t.Errorf("Lease: %v", err)
					return
				}
				if len(leases) == 0 {
					return
				}
				mu.Lock()
				for _, l := range leases {
					claimed = append(claimed, l.Message.ID)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(claimed) != total {
		t.Fatalf("claimed %d messages, want %d", len(claimed), total)
	}
	sort.Strings(claimed)
	for i := 1; i < len(claimed); i++ {
		if claimed[i] == claimed[i-1] {
			t.Fatalf("message %q claimed by two live leases", claimed[i])
		}
	}
}
