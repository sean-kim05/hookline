package delivery

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/sean-kim05/hookline/internal/queue"
)

// Result is the outcome of one delivery attempt.
type Result struct {
	Success    bool          // true iff the endpoint returned a 2xx status
	StatusCode int           // HTTP status, or 0 if no response was received
	Duration   time.Duration // wall-clock time of the attempt
	Err        error         // transport error (DNS, connection, timeout), if any
}

// Deliverer attempts to deliver one message to its endpoint. Implementations
// must be safe for concurrent use; the worker calls Deliver from many
// goroutines.
type Deliverer interface {
	Deliver(ctx context.Context, msg queue.Message) Result
}

// SignerSource resolves the signing key to use for an endpoint. Per-endpoint
// secrets (from the endpoint registry) are what let each consumer verify
// deliveries with a key only it and Hookline share. A nil return means the
// delivery is sent unsigned.
type SignerSource interface {
	SignerFor(ctx context.Context, endpoint string) *Signer
}

// StaticSigner is a SignerSource that uses one signer for every endpoint. It is
// the single-secret configuration (and the zero value signs nothing).
type StaticSigner struct{ Signer *Signer }

// SignerFor returns the single configured signer regardless of endpoint.
func (s StaticSigner) SignerFor(context.Context, string) *Signer { return s.Signer }

// HTTPDeliverer delivers messages over HTTP, signing each request with the key
// its SignerSource resolves for the destination endpoint.
type HTTPDeliverer struct {
	client  *http.Client
	signers SignerSource
	now     func() time.Time
}

var _ Deliverer = (*HTTPDeliverer)(nil)

// NewHTTPDeliverer returns a deliverer that signs every delivery with signer. A
// nil client uses a default with a sane timeout; a nil signer sends unsigned
// requests (useful in tests). For per-endpoint signing keys, use
// NewSigningDeliverer with a registry-backed SignerSource.
func NewHTTPDeliverer(client *http.Client, signer *Signer) *HTTPDeliverer {
	return NewSigningDeliverer(client, StaticSigner{Signer: signer})
}

// NewSigningDeliverer returns a deliverer that resolves a per-endpoint signing
// key from signers for each delivery.
func NewSigningDeliverer(client *http.Client, signers SignerSource) *HTTPDeliverer {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	if signers == nil {
		signers = StaticSigner{}
	}
	return &HTTPDeliverer{client: client, signers: signers, now: time.Now}
}

// Deliver POSTs the message payload to its endpoint with signature and metadata
// headers. A 2xx response is success; any other status, or a transport error,
// is a retryable failure.
func (d *HTTPDeliverer) Deliver(ctx context.Context, msg queue.Message) Result {
	start := d.now()
	ev := msg.Event

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ev.Endpoint, bytes.NewReader(ev.Payload))
	if err != nil {
		return Result{Err: fmt.Errorf("delivery: build request: %w", err), Duration: d.now().Sub(start)}
	}

	contentType := ev.ContentType
	if contentType == "" {
		contentType = "application/json"
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", "Hookline/0.1")
	req.Header.Set(HeaderID, msg.ID)
	req.Header.Set(HeaderEventID, ev.ID)
	req.Header.Set(HeaderAttempt, strconv.Itoa(msg.Attempts))
	if ev.IdempotencyKey != "" {
		req.Header.Set(HeaderIdempotencyKey, ev.IdempotencyKey)
	}
	if signer := d.signers.SignerFor(ctx, ev.Endpoint); signer != nil {
		req.Header.Set(HeaderTimestamp, strconv.FormatInt(start.Unix(), 10))
		req.Header.Set(HeaderSignature, signer.Sign(start, ev.Payload))
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return Result{Err: fmt.Errorf("delivery: %w", err), Duration: d.now().Sub(start)}
	}
	defer resp.Body.Close()
	// Drain the body so the connection can be reused by keep-alive.
	_, _ = io.Copy(io.Discard, resp.Body)

	return Result{
		Success:    resp.StatusCode >= 200 && resp.StatusCode < 300,
		StatusCode: resp.StatusCode,
		Duration:   d.now().Sub(start),
	}
}
