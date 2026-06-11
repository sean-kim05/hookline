// Package delivery turns queued messages into HTTP deliveries: it leases work,
// signs and POSTs each payload to its endpoint, and on the result either acks
// the message, reschedules it with backoff, or dead-letters it once attempts
// are exhausted.
package delivery

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"
)

// Delivery request headers. Consumers verify the signature and use the
// idempotency key to deduplicate redelivered events.
const (
	HeaderID             = "X-Hookline-Id"
	HeaderEventID        = "X-Hookline-Event-Id"
	HeaderTimestamp      = "X-Hookline-Timestamp"
	HeaderSignature      = "X-Hookline-Signature"
	HeaderAttempt        = "X-Hookline-Attempt"
	HeaderIdempotencyKey = "X-Hookline-Idempotency-Key"
)

// Signer produces HMAC-SHA256 signatures over deliveries.
//
// The signed value is `timestamp + "." + body`, the same scheme Stripe uses for
// webhook signatures. Binding the timestamp into the signature lets a consumer
// reject deliveries whose timestamp is outside an acceptable window, which
// defends against replay of a captured (still validly-signed) request.
type Signer struct {
	secret []byte
}

// NewSigner returns a Signer keyed with secret. In production the secret is
// per-endpoint (set when an endpoint is registered); week 4 wires a single
// signer and the per-endpoint key store arrives with the endpoint registry.
func NewSigner(secret string) *Signer {
	return &Signer{secret: []byte(secret)}
}

// Sign returns the signature header value for body at the given timestamp,
// formatted as `v1=<hex>`. The `v1=` scheme prefix leaves room to rotate the
// algorithm without breaking consumers that pin a version.
func (s *Signer) Sign(timestamp time.Time, body []byte) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(strconv.FormatInt(timestamp.Unix(), 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return "v1=" + hex.EncodeToString(mac.Sum(nil))
}

// Verify reports whether signature matches body at timestamp. It is the
// consumer-side check, exposed here so tests (and a future receiver SDK) share
// exactly one implementation. The comparison is constant-time.
func (s *Signer) Verify(timestamp time.Time, body []byte, signature string) bool {
	expected := s.Sign(timestamp, body)
	return hmac.Equal([]byte(expected), []byte(signature))
}
