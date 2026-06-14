package delivery

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"time"

	"github.com/sean-kim05/hookline/internal/queue"
)

// ErrCircuitOpen is the error on a Result when a delivery was short-circuited
// because the endpoint's circuit breaker is open.
var ErrCircuitOpen = errors.New("delivery: circuit open")

// circuitState is a per-endpoint breaker state.
type circuitState int

const (
	closed   circuitState = iota // requests flow; failures are counted
	open                         // requests are short-circuited until cooldown elapses
	halfOpen                     // a single trial request is allowed to test recovery
)

// CircuitBreakerDeliverer wraps a Deliverer with a per-endpoint circuit breaker.
//
// When an endpoint fails repeatedly, its breaker opens and further deliveries
// short-circuit (fail fast without an HTTP call) for a cooldown, so a down
// consumer is not hammered and worker capacity is not spent on requests that
// will fail. After the cooldown a single trial is allowed (half-open): success
// closes the breaker, failure reopens it.
type CircuitBreakerDeliverer struct {
	next      Deliverer
	threshold int           // consecutive failures that open the breaker
	cooldown  time.Duration // how long the breaker stays open
	now       func() time.Time

	mu       sync.Mutex
	breakers map[string]*breaker
}

var _ Deliverer = (*CircuitBreakerDeliverer)(nil)

type breaker struct {
	state        circuitState
	consecFails  int
	openedAt     time.Time
	trialInFlght bool // in half-open, guards to a single trial request
}

// NewCircuitBreaker wraps next. threshold consecutive failures open the breaker
// for cooldown. Sensible defaults are applied for non-positive values.
func NewCircuitBreaker(next Deliverer, threshold int, cooldown time.Duration) *CircuitBreakerDeliverer {
	if threshold <= 0 {
		threshold = 5
	}
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}
	return &CircuitBreakerDeliverer{
		next:      next,
		threshold: threshold,
		cooldown:  cooldown,
		now:       time.Now,
		breakers:  make(map[string]*breaker),
	}
}

// WithClock overrides the breaker clock (used in tests to drive cooldowns
// deterministically). It returns the receiver for chaining.
func (d *CircuitBreakerDeliverer) WithClock(now func() time.Time) *CircuitBreakerDeliverer {
	d.now = now
	return d
}

// Deliver short-circuits when the endpoint's breaker is open, otherwise delivers
// and records the result.
func (d *CircuitBreakerDeliverer) Deliver(ctx context.Context, msg queue.Message) Result {
	key := endpointKey(msg.Event.Endpoint)
	if !d.allow(key) {
		return Result{Success: false, Err: ErrCircuitOpen}
	}
	res := d.next.Deliver(ctx, msg)
	d.record(key, res.Success)
	return res
}

// allow reports whether a delivery to key may proceed, advancing the breaker
// state machine (open -> half-open after cooldown; one trial in half-open).
func (d *CircuitBreakerDeliverer) allow(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	b := d.breakers[key]
	if b == nil {
		b = &breaker{state: closed}
		d.breakers[key] = b
	}
	switch b.state {
	case closed:
		return true
	case open:
		if d.now().Sub(b.openedAt) >= d.cooldown {
			b.state = halfOpen
			b.trialInFlght = true
			return true
		}
		return false
	default: // halfOpen
		if b.trialInFlght {
			return false // a trial is already testing recovery
		}
		b.trialInFlght = true
		return true
	}
}

// record updates the breaker after a delivery completes.
func (d *CircuitBreakerDeliverer) record(key string, success bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	b := d.breakers[key]
	if b == nil {
		return
	}
	b.trialInFlght = false
	if success {
		b.state = closed
		b.consecFails = 0
		return
	}
	// Failure: a half-open trial reopens immediately; a closed breaker opens
	// once consecutive failures reach the threshold.
	if b.state == halfOpen {
		b.state = open
		b.openedAt = d.now()
		return
	}
	b.consecFails++
	if b.consecFails >= d.threshold {
		b.state = open
		b.openedAt = d.now()
	}
}

// endpointKey reduces an endpoint URL to the key the breaker and rate limiter
// share: its host, so all paths on one consumer host throttle together. It
// falls back to the raw string if the URL cannot be parsed.
func endpointKey(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return endpoint
	}
	return u.Host
}
