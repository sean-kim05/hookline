package delivery

import (
	"math"
	"math/rand"
	"time"
)

// Backoff computes how long to wait before retrying a failed delivery, using
// exponential backoff with full jitter:
//
//	delay = uniform(0, min(Max, Base * 2^(attempt-1)))
//
// The exponential term spaces out retries to a struggling endpoint; the full
// jitter (a uniform sample over the whole interval, not a fixed delay) is what
// prevents a thundering herd — without it, every event that failed during an
// outage would retry in lockstep the moment the endpoint recovers and knock it
// over again. This is the AWS "full jitter" strategy.
type Backoff struct {
	Base time.Duration // delay scale for the first retry
	Max  time.Duration // ceiling on the pre-jitter interval

	// rand returns a float in [0,1). Injectable so tests are deterministic;
	// nil uses the package's shared source.
	rand func() float64
}

// NewBackoff returns a Backoff with the given base and max delays.
func NewBackoff(base, max time.Duration) Backoff {
	return Backoff{Base: base, Max: max}
}

// Delay returns the wait before the next attempt. attempt is the number of
// attempts already made (1 after the first delivery failed). The result is
// always in [0, Max].
func (b Backoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	// math.Pow keeps this overflow-safe: a huge exponent yields +Inf, which the
	// cap below clamps to Max rather than wrapping a shift.
	ceiling := float64(b.Base) * math.Pow(2, float64(attempt-1))
	if max := float64(b.Max); ceiling > max {
		ceiling = max
	}
	r := b.rand
	if r == nil {
		r = rand.Float64
	}
	return time.Duration(r() * ceiling)
}
