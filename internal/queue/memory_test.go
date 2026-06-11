package queue_test

import (
	"testing"
	"time"

	"github.com/sean-kim05/hookline/internal/queue"
	"github.com/sean-kim05/hookline/internal/queue/queuetest"
)

// TestMemoryQueueConformance runs the shared conformance suite against the
// in-memory reference implementation. The suite — not this file — is where
// queue behavior is specified; every backend must pass it identically.
func TestMemoryQueueConformance(t *testing.T) {
	queuetest.Run(t, func(t *testing.T, now func() time.Time) queue.Queue {
		return queue.NewMemoryQueue(queue.WithClock(now))
	})
}
