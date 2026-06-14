package endpoint

import (
	"context"
	"errors"

	"github.com/sean-kim05/hookline/internal/delivery"
)

// SignerSource resolves a per-endpoint delivery.Signer from the registry,
// falling back to a default signer for endpoints that are not registered. It
// adapts a Registry to delivery.SignerSource so the HTTP deliverer can sign
// each delivery with the destination's own secret.
type SignerSource struct {
	reg      Registry
	fallback *delivery.Signer
}

var _ delivery.SignerSource = (*SignerSource)(nil)

// NewSignerSource returns a SignerSource over reg. fallback signs deliveries to
// endpoints that are not in the registry; pass nil to leave those unsigned.
func NewSignerSource(reg Registry, fallback *delivery.Signer) *SignerSource {
	return &SignerSource{reg: reg, fallback: fallback}
}

// SignerFor returns the signer for endpoint: the registered endpoint's own
// secret when one exists, otherwise the fallback. A registry error other than
// "not found" falls back too, so a transient registry blip degrades to the
// default secret rather than dropping the signature entirely.
func (s *SignerSource) SignerFor(ctx context.Context, endpoint string) *delivery.Signer {
	ep, err := s.reg.Lookup(ctx, endpoint)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			// Unexpected registry error: fall back rather than fail delivery.
			return s.fallback
		}
		return s.fallback
	}
	if ep.Secret == "" {
		return s.fallback
	}
	return delivery.NewSigner(ep.Secret)
}
