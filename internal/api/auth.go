package api

import (
	"crypto/sha256"
	"net/http"
	"strings"
)

// Authenticator validates a producer API key and reports the producer it
// identifies. Returning ok=false denies the request.
type Authenticator interface {
	Authenticate(apiKey string) (producer string, ok bool)
}

// StaticKeyAuth authenticates against a fixed set of API keys configured at
// startup. Keys are stored only as SHA-256 digests and looked up by the digest
// of the presented key, so neither the comparison nor the storage depends on
// secret bytes in a timing-observable way.
type StaticKeyAuth struct {
	// keys maps the digest of a valid key to its producer label.
	keys map[[32]byte]string
}

// NewStaticKeyAuth builds an authenticator from "producer:key" pairs. A key
// with no "producer:" prefix is labelled with its own ordinal.
func NewStaticKeyAuth(pairs map[string]string) *StaticKeyAuth {
	a := &StaticKeyAuth{keys: make(map[[32]byte]string, len(pairs))}
	for producer, key := range pairs {
		if key == "" {
			continue
		}
		a.keys[sha256.Sum256([]byte(key))] = producer
	}
	return a
}

// Authenticate reports the producer for apiKey, or ok=false if unknown.
func (a *StaticKeyAuth) Authenticate(apiKey string) (string, bool) {
	if apiKey == "" {
		return "", false
	}
	producer, ok := a.keys[sha256.Sum256([]byte(apiKey))]
	return producer, ok
}

// producerKey is the context key under which the authenticated producer is
// stored for handlers downstream of the auth middleware.
type producerKey struct{}

// authMiddleware rejects requests without a valid bearer API key and, on
// success, stores the producer label in the request context.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := bearerToken(r)
		producer, ok := s.auth.Authenticate(key)
		if !ok {
			writeError(w, http.StatusUnauthorized, "missing or invalid API key")
			return
		}
		ctx := contextWithProducer(r.Context(), producer)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// bearerToken extracts the token from an "Authorization: Bearer <token>"
// header. It returns "" if the header is absent or malformed.
func bearerToken(r *http.Request) string {
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(h[len(prefix):])
}
