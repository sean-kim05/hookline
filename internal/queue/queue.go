// Package queue defines the durable delivery queue at the heart of Hookline.
//
// The Queue interface is deliberately the project's keystone abstraction. The
// headline goal is a from-scratch, WAL-backed persistent queue — but that is
// also the riskiest component to build. By coding the rest of Hookline against
// this interface, we can ship a working delivery platform on a simple, correct
// implementation first (in-memory for tests, Postgres for the MVP) and swap in
// the custom WAL engine behind the same contract later. A failed WAL then
// costs one component, not the whole product.
//
// Every implementation must pass the shared conformance tests, so correctness
// (fencing, at-least-once, retry scheduling) is guaranteed identically across
// the in-memory, Postgres, and WAL backends.
package queue

import (
	"context"
	"errors"
	"time"

	"github.com/sean-kim05/hookline/internal/event"
)

// ErrStaleLease is returned by Ack and Nack when the supplied fencing token no
// longer matches the message's current lease. This happens when a worker's
// lease expires, the message is re-leased to another worker, and the original
// worker then tries to complete it. Rejecting the stale token is what prevents
// the expired worker from double-acking or corrupting the retry schedule — the
// core defense against duplicate delivery under load.
var ErrStaleLease = errors.New("queue: stale lease token")

// ErrNotFound is returned when a message ID is unknown to the queue.
var ErrNotFound = errors.New("queue: message not found")

// Message is a unit of work in the queue: one pending delivery of an event.
type Message struct {
	ID         string
	Event      event.Event
	EnqueuedAt time.Time
	// Attempts counts delivery attempts made so far. It is 1 on the first
	// lease and increments on each subsequent lease (including re-leases after
	// a lapsed lease), so it drives retry backoff and dead-lettering.
	Attempts int
}

// Lease is an exclusive, time-bounded claim on a Message granted to a worker.
type Lease struct {
	Message Message
	// Token is a monotonically increasing fencing token. It must be presented
	// to Ack/Nack and must match the message's current lease, or the call is
	// rejected with ErrStaleLease.
	Token uint64
	// Expiry is when the lease lapses; after this the message becomes available
	// to other workers regardless of whether it was acked.
	Expiry time.Time
}

// Queue is a durable, at-least-once delivery queue. Implementations must be
// safe for concurrent use by multiple producers and workers.
type Queue interface {
	// Enqueue accepts an event for delivery and returns its message ID.
	Enqueue(ctx context.Context, ev event.Event) (id string, err error)

	// Lease atomically claims up to n messages that are ready for delivery,
	// granting each an exclusive lease for leaseFor. Messages whose previous
	// lease has expired become eligible again. Fewer than n leases (including
	// zero) may be returned if fewer messages are ready.
	Lease(ctx context.Context, n int, leaseFor time.Duration) ([]Lease, error)

	// Ack marks the message delivered and removes it from the queue. token must
	// match the message's current lease, or Ack returns ErrStaleLease.
	Ack(ctx context.Context, id string, token uint64) error

	// Nack reports a failed delivery attempt and reschedules the message to
	// become ready again after retryAfter. token must match the current lease,
	// or Nack returns ErrStaleLease.
	Nack(ctx context.Context, id string, token uint64, retryAfter time.Duration) error

	// Close releases any resources held by the queue.
	Close() error
}
