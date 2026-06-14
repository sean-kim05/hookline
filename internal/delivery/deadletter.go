package delivery

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/sean-kim05/hookline/internal/queue"
)

// ErrDeadLetterNotFound is returned by a DeadLetterStore when no dead letter
// exists for the given message ID.
var ErrDeadLetterNotFound = errors.New("delivery: dead letter not found")

// DeadLetter is a message that exhausted its delivery attempts, captured with
// the reason it gave up. It retains the full message — including the payload —
// so a dead letter can be replayed back onto the queue.
type DeadLetter struct {
	Message  queue.Message
	Reason   string
	FailedAt time.Time
}

// DeadLetterSink receives messages that have exhausted their retries. The
// worker records to the sink *before* removing the message from the queue, so a
// crash in between re-delivers rather than loses the event (at-least-once).
type DeadLetterSink interface {
	DeadLetter(ctx context.Context, dl DeadLetter) error
}

// DeadLetterStore is a DeadLetterSink that can also be read and pruned, which is
// what the dashboard's DLQ view and the replay endpoint need.
type DeadLetterStore interface {
	DeadLetterSink
	// ListDeadLetters returns dead letters newest first, capped at limit.
	ListDeadLetters(ctx context.Context, limit int) ([]DeadLetter, error)
	// GetDeadLetter returns the dead letter for a queue message ID, or
	// ErrDeadLetterNotFound.
	GetDeadLetter(ctx context.Context, messageID string) (DeadLetter, error)
	// RemoveDeadLetter deletes a dead letter (e.g. after a successful replay).
	RemoveDeadLetter(ctx context.Context, messageID string) error
}

// DeadLetterFunc adapts a function to a DeadLetterSink.
type DeadLetterFunc func(ctx context.Context, dl DeadLetter) error

func (f DeadLetterFunc) DeadLetter(ctx context.Context, dl DeadLetter) error { return f(ctx, dl) }

const deadLetterDefaultLimit = 100

// MemoryDeadLetterSink collects dead-lettered messages in memory. It is for
// tests and local development; it is safe for concurrent use and implements
// DeadLetterStore.
type MemoryDeadLetterSink struct {
	mu      sync.Mutex
	entries []DeadLetter
}

var _ DeadLetterStore = (*MemoryDeadLetterSink)(nil)

// DeadLetter records dl, replacing any existing entry with the same message ID.
func (s *MemoryDeadLetterSink) DeadLetter(_ context.Context, dl DeadLetter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.entries {
		if s.entries[i].Message.ID == dl.Message.ID {
			s.entries[i] = dl
			return nil
		}
	}
	s.entries = append(s.entries, dl)
	return nil
}

// Entries returns a snapshot of the recorded dead letters in insertion order.
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

// ListDeadLetters returns dead letters newest first, capped at limit.
func (s *MemoryDeadLetterSink) ListDeadLetters(_ context.Context, limit int) ([]DeadLetter, error) {
	if limit <= 0 {
		limit = deadLetterDefaultLimit
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]DeadLetter, len(s.entries))
	copy(out, s.entries)
	sort.SliceStable(out, func(i, j int) bool { return out[i].FailedAt.After(out[j].FailedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// GetDeadLetter returns the dead letter for messageID.
func (s *MemoryDeadLetterSink) GetDeadLetter(_ context.Context, messageID string) (DeadLetter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, dl := range s.entries {
		if dl.Message.ID == messageID {
			return dl, nil
		}
	}
	return DeadLetter{}, ErrDeadLetterNotFound
}

// RemoveDeadLetter deletes the dead letter for messageID.
func (s *MemoryDeadLetterSink) RemoveDeadLetter(_ context.Context, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, dl := range s.entries {
		if dl.Message.ID == messageID {
			s.entries = append(s.entries[:i], s.entries[i+1:]...)
			return nil
		}
	}
	return ErrDeadLetterNotFound
}
