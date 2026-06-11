// Package event defines Hookline's core domain types: the events producers
// submit, and the metadata Hookline tracks while delivering them.
package event

import "time"

// Event is a single webhook delivery request submitted by a producer.
type Event struct {
	// ID uniquely identifies the event within Hookline. Enqueue assigns one if
	// it is empty.
	ID string
	// Endpoint is the consumer URL that should receive the payload.
	Endpoint string
	// Payload is the raw request body delivered to the endpoint.
	Payload []byte
	// ContentType is sent as the Content-Type header on delivery. Defaults to
	// application/json at the delivery layer when empty.
	ContentType string
	// IdempotencyKey, when set, is forwarded so a consumer can detect duplicate
	// deliveries. Hookline guarantees at-least-once delivery, so consumers are
	// responsible for deduplicating on this key.
	IdempotencyKey string
	// CreatedAt is when Hookline accepted the event. Enqueue sets it if zero.
	CreatedAt time.Time
}
