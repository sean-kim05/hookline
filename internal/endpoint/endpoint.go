// Package endpoint is the registry of delivery destinations and their signing
// secrets.
//
// Registering an endpoint mints a per-endpoint signing secret. Each delivery to
// that endpoint is then signed with its own key, so a consumer verifies
// deliveries with a secret that only it and Hookline share — compromising one
// consumer's secret never lets an attacker forge deliveries to another. The
// secret is returned exactly once, at registration; the registry stores it to
// sign with but never exposes it again.
package endpoint

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sean-kim05/hookline/internal/id"
)

// ErrNotFound is returned when an endpoint ID or URL is unknown.
var ErrNotFound = errors.New("endpoint: not found")

// secretPrefix marks Hookline signing secrets, mirroring the convention used by
// Stripe ("whsec_") so consumers recognise them.
const secretPrefix = "whsec_"

// Endpoint is a registered delivery destination.
type Endpoint struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	Producer  string    `json:"producer,omitempty"`
	Secret    string    `json:"-"` // never serialised except in the one-time registration response
	Disabled  bool      `json:"disabled"`
	CreatedAt time.Time `json:"created_at"`
}

// Registry stores endpoints and their signing secrets. Implementations must be
// safe for concurrent use.
type Registry interface {
	// Register stores e, assigning an ID, a fresh signing secret (if e.Secret is
	// empty), and CreatedAt. The returned Endpoint includes the secret so the
	// caller can show it once.
	Register(ctx context.Context, e Endpoint) (Endpoint, error)
	// List returns all endpoints for a producer (or all endpoints when producer
	// is empty), newest first.
	List(ctx context.Context, producer string) ([]Endpoint, error)
	// Lookup finds the endpoint registered for a destination URL.
	Lookup(ctx context.Context, url string) (Endpoint, error)
}

// GenerateSecret returns a new random signing secret with the whsec_ prefix.
func GenerateSecret() string {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("endpoint: cannot read random bytes: " + err.Error())
	}
	return secretPrefix + hex.EncodeToString(b[:])
}

// normalizeURL trims surrounding whitespace so lookups match registration
// regardless of incidental padding. URLs are otherwise compared verbatim.
func normalizeURL(u string) string { return strings.TrimSpace(u) }

// MemoryRegistry is an in-memory Registry: the reference implementation for
// tests and single-node local runs. Endpoints do not survive a restart (the
// Postgres registry is for that).
type MemoryRegistry struct {
	mu      sync.RWMutex
	byID    map[string]Endpoint
	byURL   map[string]string // url -> id
	now     func() time.Time
	ordered []string // ids in registration order
}

var _ Registry = (*MemoryRegistry)(nil)

// MemoryOption configures a MemoryRegistry.
type MemoryOption func(*MemoryRegistry)

// WithClock overrides the registry clock (used in tests).
func WithClock(now func() time.Time) MemoryOption {
	return func(r *MemoryRegistry) { r.now = now }
}

// NewMemoryRegistry returns an empty in-memory registry.
func NewMemoryRegistry(opts ...MemoryOption) *MemoryRegistry {
	r := &MemoryRegistry{
		byID:  make(map[string]Endpoint),
		byURL: make(map[string]string),
		now:   time.Now,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Register stores e, returning it with its assigned ID and secret.
func (r *MemoryRegistry) Register(_ context.Context, e Endpoint) (Endpoint, error) {
	url := normalizeURL(e.URL)
	if url == "" {
		return Endpoint{}, errors.New("endpoint: URL is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	// Re-registering the same URL rotates its secret in place rather than
	// creating a duplicate, so a producer can recover from a lost secret.
	if existingID, ok := r.byURL[url]; ok {
		ep := r.byID[existingID]
		ep.Secret = GenerateSecret()
		ep.Disabled = e.Disabled
		if e.Producer != "" {
			ep.Producer = e.Producer
		}
		r.byID[existingID] = ep
		return ep, nil
	}

	ep := Endpoint{
		ID:        id.New(),
		URL:       url,
		Producer:  e.Producer,
		Secret:    e.Secret,
		Disabled:  e.Disabled,
		CreatedAt: r.now(),
	}
	if ep.Secret == "" {
		ep.Secret = GenerateSecret()
	}
	r.byID[ep.ID] = ep
	r.byURL[url] = ep.ID
	r.ordered = append(r.ordered, ep.ID)
	return ep, nil
}

// List returns endpoints for producer (all when empty), newest first.
func (r *MemoryRegistry) List(_ context.Context, producer string) ([]Endpoint, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]Endpoint, 0, len(r.ordered))
	for _, eid := range r.ordered {
		ep := r.byID[eid]
		if producer != "" && ep.Producer != producer {
			continue
		}
		out = append(out, ep)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

// Lookup returns the endpoint registered for url.
func (r *MemoryRegistry) Lookup(_ context.Context, url string) (Endpoint, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	eid, ok := r.byURL[normalizeURL(url)]
	if !ok {
		return Endpoint{}, ErrNotFound
	}
	return r.byID[eid], nil
}
