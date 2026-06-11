package queuetest

import (
	"sync"
	"time"
)

// Clock is a manually-advanced clock used by the conformance suite so that
// lease expiry and retry scheduling are tested deterministically — no sleeps,
// no flaky timing. It is safe for concurrent use.
//
// The epoch starts at a fixed, non-zero instant so that backends storing
// timestamps (e.g. Postgres) handle realistic values. Advances are kept at
// millisecond granularity throughout the suite so backends with microsecond
// timestamp precision are not penalized.
type Clock struct {
	mu sync.Mutex
	t  time.Time
}

// NewClock returns a Clock fixed at a deterministic starting instant.
func NewClock() *Clock {
	return &Clock{t: time.Unix(1_700_000_000, 0).UTC()}
}

// Now returns the current fake time. Pass this method as the queue's clock.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// Advance moves the clock forward by d.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}
