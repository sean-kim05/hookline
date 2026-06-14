package delivery

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sean-kim05/hookline/internal/queue"
)

// ErrRateLimited is the error on a Result when a delivery was held back by the
// endpoint's rate limiter. It is a retryable failure: the worker reschedules it
// with backoff, so the delivery is simply spread out over time.
var ErrRateLimited = errors.New("delivery: rate limited")

// RateLimitedDeliverer wraps a Deliverer with a per-endpoint token-bucket rate
// limiter, so Hookline never sends a consumer more than it has agreed to
// receive. Each endpoint host gets its own bucket that refills at ratePerSec up
// to burst tokens; a delivery with no token available fails fast as
// ErrRateLimited and is retried later.
type RateLimitedDeliverer struct {
	next       Deliverer
	ratePerSec float64
	burst      float64
	now        func() time.Time

	mu      sync.Mutex
	buckets map[string]*tokenBucket
}

var _ Deliverer = (*RateLimitedDeliverer)(nil)

type tokenBucket struct {
	tokens float64
	last   time.Time
}

// NewRateLimiter wraps next, allowing ratePerSec deliveries per endpoint host
// with a burst of burst. A non-positive ratePerSec disables limiting (every
// delivery passes through); a non-positive burst defaults to ratePerSec (at
// least 1).
func NewRateLimiter(next Deliverer, ratePerSec, burst float64) *RateLimitedDeliverer {
	if burst <= 0 {
		burst = ratePerSec
		if burst < 1 {
			burst = 1
		}
	}
	return &RateLimitedDeliverer{
		next:       next,
		ratePerSec: ratePerSec,
		burst:      burst,
		now:        time.Now,
		buckets:    make(map[string]*tokenBucket),
	}
}

// WithClock overrides the limiter clock (used in tests). Returns the receiver.
func (d *RateLimitedDeliverer) WithClock(now func() time.Time) *RateLimitedDeliverer {
	d.now = now
	return d
}

// Deliver passes through when a token is available, otherwise fails fast with
// ErrRateLimited.
func (d *RateLimitedDeliverer) Deliver(ctx context.Context, msg queue.Message) Result {
	if d.ratePerSec <= 0 {
		return d.next.Deliver(ctx, msg) // limiting disabled
	}
	if !d.allow(endpointKey(msg.Event.Endpoint)) {
		return Result{Success: false, Err: ErrRateLimited}
	}
	return d.next.Deliver(ctx, msg)
}

// allow refills the endpoint's bucket for elapsed time and consumes one token.
func (d *RateLimitedDeliverer) allow(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := d.now()
	b := d.buckets[key]
	if b == nil {
		// A new endpoint starts with a full burst so it isn't penalised on its
		// first delivery.
		d.buckets[key] = &tokenBucket{tokens: d.burst - 1, last: now}
		return true
	}

	// Refill proportionally to elapsed time, capped at burst.
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * d.ratePerSec
		if b.tokens > d.burst {
			b.tokens = d.burst
		}
		b.last = now
	}
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}
