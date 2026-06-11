package delivery

import (
	"context"
	"sync"
	"time"

	"github.com/sean-kim05/hookline/internal/queue"
)

// DeadLetter is a message that exhausted its delivery attempts, captured with
// the reason it gave up.
type DeadLetter struct {
	Message  queue.Message
	Reason   string
	FailedAt time.Time
}

// DeadLetterSink receives messages that have exhausted their retries. The
// worker records to the sink *before* removing the message from the queue, so a
// crash in between re-delivers rather than loses the event (at-least-once).
//
// Week 4 ships an in-memory sink; week 5 backs this with the Postgres delivery
// audit log that the dashboard reads.
type DeadLetterSink interface {
	DeadLetter(ctx context.Context, dl DeadLetter) error
}

// DeadLetterFunc adapts a function to a DeadLetterSink.
type DeadLetterFunc func(ctx context.Context, dl DeadLetter) error

func (f DeadLetterFunc) DeadLetter(ctx context.Context, dl DeadLetter) error { return f(ctx, dl) }

// MemoryDeadLetterSink collects dead-lettered messages in memory. It is for
// tests and local development; it is safe for concurrent use.
type MemoryDeadLetterSink struct {
	mu      sync.Mutex
	entries []DeadLetter
}

// DeadLetter records dl.
func (s *MemoryDeadLetterSink) DeadLetter(_ context.Context, dl DeadLetter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, dl)
	return nil
}

// Entries returns a snapshot of the recorded dead letters.
func (s *MemoryDeadLetterSink) Entries() []DeadLetter {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]DeadLetter, len(s.entries))
	copy(out, s.entries)
	return out
}

// Len returns the number of recorded dead letters.
func (s *MemoryDeadLetterSink) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}
