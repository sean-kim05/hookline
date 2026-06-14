package endpoint_test

import (
	"context"
	"testing"
	"time"

	"github.com/sean-kim05/hookline/internal/delivery"
	"github.com/sean-kim05/hookline/internal/endpoint"
)

func TestSignerSourceUsesRegisteredSecret(t *testing.T) {
	t.Parallel()
	reg := endpoint.NewMemoryRegistry()
	ctx := context.Background()
	ep, _ := reg.Register(ctx, endpoint.Endpoint{URL: "https://acme.test/hook"})

	src := endpoint.NewSignerSource(reg, delivery.NewSigner("fallback"))
	signer := src.SignerFor(ctx, "https://acme.test/hook")
	if signer == nil {
		t.Fatal("expected a signer for a registered endpoint")
	}

	// The resolved signer must match a signer built from the endpoint's own
	// secret, and must NOT match the fallback.
	ts := time.Unix(1_700_000_000, 0)
	body := []byte(`{"a":1}`)
	want := delivery.NewSigner(ep.Secret).Sign(ts, body)
	if got := signer.Sign(ts, body); got != want {
		t.Fatalf("signer did not use the endpoint secret\n got %s\nwant %s", got, want)
	}
	if signer.Sign(ts, body) == delivery.NewSigner("fallback").Sign(ts, body) {
		t.Fatal("resolved signer matched the fallback, not the endpoint secret")
	}
}

func TestSignerSourceFallsBackForUnregistered(t *testing.T) {
	t.Parallel()
	reg := endpoint.NewMemoryRegistry()
	fallback := delivery.NewSigner("fallback")
	src := endpoint.NewSignerSource(reg, fallback)

	signer := src.SignerFor(context.Background(), "https://unregistered.test")
	if signer == nil {
		t.Fatal("expected the fallback signer")
	}
	ts := time.Unix(1_700_000_000, 0)
	body := []byte(`{}`)
	if signer.Sign(ts, body) != fallback.Sign(ts, body) {
		t.Fatal("expected fallback signer to be used")
	}
}

func TestSignerSourceNilFallbackLeavesUnsigned(t *testing.T) {
	t.Parallel()
	src := endpoint.NewSignerSource(endpoint.NewMemoryRegistry(), nil)
	if src.SignerFor(context.Background(), "https://unregistered.test") != nil {
		t.Fatal("expected nil signer (unsigned) when no fallback and not registered")
	}
}
