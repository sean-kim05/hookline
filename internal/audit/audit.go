// Package audit records the outcome of every delivery attempt Hookline makes.
//
// The audit log is the system's memory of what happened: it powers the
// operator dashboard (recent deliveries, the dead-letter queue, per-event
// attempt history) and is the source of truth for replay. It is deliberately
// separate from the delivery queue — the queue holds work still to do, the
// audit log holds a durable trail of what was already attempted, including
// messages that have since left the queue (acked or dead-lettered).
package audit

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Outcome classifies what happened to a message after a delivery attempt.
type Outcome string

const (
	// OutcomeDelivered means the endpoint accepted the delivery (2xx).
	OutcomeDelivered Outcome = "delivered"
	// OutcomeRetrying means the attempt failed and the message was rescheduled.
	OutcomeRetrying Outcome = "retrying"
	// OutcomeDeadLettered means attempts were exhausted and the message was
	// moved to the dead-letter queue.
	OutcomeDeadLettered Outcome = "dead_lettered"
)

// Attempt is a record of one delivery attempt.
type Attempt struct {
	ID         string        // unique attempt ID
	MessageID  string        // queue message ID
	EventID    string        // event ID (stable across retries of the same event)
	Endpoint   string        // destination URL
	Producer   string        // producer that submitted the event, if known
	Attempt    int           // attempt number (1-based)
	Outcome    Outcome       // delivered / retrying / dead_lettered
	StatusCode int           // HTTP status, or 0 if no response
	Duration   time.Duration // wall-clock time of the attempt
	Error      string        // transport/dead-letter error, if any
	At         time.Time     // when the attempt completed
}

// Log records and queries delivery attempts. Implementations must be safe for
// concurrent use.
type Log interface {
	// Record stores one attempt. Recording must never block delivery: callers
	// log and continue on error rather than failing the delivery.
	Record(ctx context.Context, a Attempt) error
	// List returns attempts matching the filter, newest first.
	List(ctx context.Context, f Filter) ([]Attempt, error)
}

// Filter narrows a List query. A zero Filter returns the most recent attempts
// across all events.
type Filter struct {
	EventID   string  // restrict to one event's attempt history
	MessageID string  // restrict to one queue message
	Outcome   Outcome // restrict to one outcome (e.g. dead_lettered for the DLQ view)
	Limit     int     // cap the number of rows (<= 0 means the default)
}

const defaultLimit = 100

// NopLog discards every record. It is the default when no audit log is wired,
// so the delivery worker runs unchanged without one.
type NopLog struct{}

// Record discards the attempt.
func (NopLog) Record(context.Context, Attempt) error { return nil }

// List always returns no attempts.
func (NopLog) List(context.Context, Filter) ([]Attempt, error) { return nil, nil }

// MemoryLog is an in-memory Log holding a bounded, newest-wins ring of
// attempts. It is the reference implementation: correct and fast, but not
// durable across restarts (the Postgres log is for that).
type MemoryLog struct {
	mu       sync.Mutex
	attempts []Attempt
	cap      int
}

var _ Log = (*MemoryLog)(nil)

// NewMemoryLog returns a MemoryLog retaining at most the most recent capacity
// attempts. A non-positive capacity uses a default of 10,000.
func NewMemoryLog(capacity int) *MemoryLog {
	if capacity <= 0 {
		capacity = 10_000
	}
	return &MemoryLog{cap: capacity}
}

// Record appends an attempt, evicting the oldest once capacity is exceeded.
func (l *MemoryLog) Record(_ context.Context, a Attempt) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.attempts = append(l.attempts, a)
	if over := len(l.attempts) - l.cap; over > 0 {
		// Drop the oldest entries. Copy down so the backing array can be reused
		// instead of growing without bound.
		l.attempts = append(l.attempts[:0], l.attempts[over:]...)
	}
	return nil
}

// List returns matching attempts, newest first.
func (l *MemoryLog) List(_ context.Context, f Filter) ([]Attempt, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	limit := f.Limit
	if limit <= 0 {
		limit = defaultLimit
	}

	out := make([]Attempt, 0, len(l.attempts))
	for _, a := range l.attempts {
		if f.EventID != "" && a.EventID != f.EventID {
			continue
		}
		if f.MessageID != "" && a.MessageID != f.MessageID {
			continue
		}
		if f.Outcome != "" && a.Outcome != f.Outcome {
			continue
		}
		out = append(out, a)
	}

	// Newest first, breaking ties by attempt ID for a stable order.
	sort.Slice(out, func(i, j int) bool {
		if out[i].At.Equal(out[j].At) {
			return out[i].ID > out[j].ID
		}
		return out[i].At.After(out[j].At)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
