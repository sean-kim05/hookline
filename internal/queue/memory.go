package queue

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/sean-kim05/hookline/internal/event"
	"github.com/sean-kim05/hookline/internal/id"
)

// memMessage is the internal bookkeeping record for a queued message.
type memMessage struct {
	msg        Message
	readyAt    time.Time // when the message becomes eligible for leasing
	leaseToken uint64    // current fencing token; 0 means never leased
	leased     bool
	leaseExp   time.Time
}

// MemoryQueue is an in-memory Queue implementation. It is the reference
// implementation used in tests and local development: correct, but not durable
// across restarts. The Postgres and WAL implementations must satisfy the same
// conformance tests this type does.
type MemoryQueue struct {
	mu   sync.Mutex
	msgs map[string]*memMessage
	// now is the clock, overridable in tests to make lease expiry and retry
	// scheduling deterministic (no real sleeps, no flaky timing).
	now func() time.Time
}

// Compile-time check that MemoryQueue satisfies the Queue interface.
var _ Queue = (*MemoryQueue)(nil)

// MemoryOption configures a MemoryQueue.
type MemoryOption func(*MemoryQueue)

// WithClock overrides the queue's clock. Tests use this to drive lease expiry
// and retry scheduling deterministically.
func WithClock(now func() time.Time) MemoryOption {
	return func(q *MemoryQueue) { q.now = now }
}

// NewMemoryQueue returns an empty in-memory queue.
func NewMemoryQueue(opts ...MemoryOption) *MemoryQueue {
	q := &MemoryQueue{
		msgs: make(map[string]*memMessage),
		now:  time.Now,
	}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// Enqueue accepts an event for delivery and returns its message ID.
func (q *MemoryQueue) Enqueue(_ context.Context, ev event.Event) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.now()
	if ev.ID == "" {
		ev.ID = id.New()
	}
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = now
	}

	mid := id.New()
	q.msgs[mid] = &memMessage{
		msg:     Message{ID: mid, Event: ev, EnqueuedAt: now},
		readyAt: now,
	}
	return mid, nil
}

// Lease claims up to n ready messages, granting each a fresh fencing token.
func (q *MemoryQueue) Lease(_ context.Context, n int, leaseFor time.Duration) ([]Lease, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.now()

	// A message is ready when it is not under a live lease and its readyAt has
	// arrived. A lapsed lease (leased but past expiry) counts as ready again.
	var ready []*memMessage
	for _, m := range q.msgs {
		if m.leased && now.Before(m.leaseExp) {
			continue
		}
		if !m.readyAt.After(now) {
			ready = append(ready, m)
		}
	}

	// Deterministic order: oldest readyAt first, ID as tiebreaker. This keeps
	// behaviour reproducible in tests and roughly FIFO in practice.
	sort.Slice(ready, func(i, j int) bool {
		if ready[i].readyAt.Equal(ready[j].readyAt) {
			return ready[i].msg.ID < ready[j].msg.ID
		}
		return ready[i].readyAt.Before(ready[j].readyAt)
	})

	if n >= 0 && n < len(ready) {
		ready = ready[:n]
	}

	leases := make([]Lease, 0, len(ready))
	for _, m := range ready {
		m.leaseToken++ // a new token immediately invalidates any prior lease
		m.leased = true
		m.leaseExp = now.Add(leaseFor)
		m.msg.Attempts++
		leases = append(leases, Lease{
			Message: m.msg,
			Token:   m.leaseToken,
			Expiry:  m.leaseExp,
		})
	}
	return leases, nil
}

// Ack marks the message delivered and removes it, if the token is current.
func (q *MemoryQueue) Ack(_ context.Context, mid string, token uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	m, ok := q.msgs[mid]
	if !ok {
		return ErrNotFound
	}
	if m.leaseToken != token {
		return ErrStaleLease
	}
	delete(q.msgs, mid)
	return nil
}

// Nack reschedules the message after retryAfter, if the token is current.
func (q *MemoryQueue) Nack(_ context.Context, mid string, token uint64, retryAfter time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	m, ok := q.msgs[mid]
	if !ok {
		return ErrNotFound
	}
	if m.leaseToken != token {
		return ErrStaleLease
	}
	m.leased = false
	m.readyAt = q.now().Add(retryAfter)
	return nil
}

// Close is a no-op for the in-memory queue.
func (q *MemoryQueue) Close() error { return nil }
