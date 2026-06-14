package endpoint_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sean-kim05/hookline/internal/endpoint"
)

func fixedClock() func() time.Time {
	now := time.Unix(1_700_000_000, 0).UTC()
	return func() time.Time { return now }
}

func TestRegisterMintsSecretAndID(t *testing.T) {
	t.Parallel()
	reg := endpoint.NewMemoryRegistry(endpoint.WithClock(fixedClock()))
	ep, err := reg.Register(context.Background(), endpoint.Endpoint{URL: "https://acme.test/hook", Producer: "acme"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if ep.ID == "" {
		t.Fatal("expected an assigned ID")
	}
	if !strings.HasPrefix(ep.Secret, "whsec_") {
		t.Fatalf("expected whsec_ secret, got %q", ep.Secret)
	}
	if ep.CreatedAt.IsZero() {
		t.Fatal("expected CreatedAt set")
	}
}

func TestRegisterRequiresURL(t *testing.T) {
	t.Parallel()
	reg := endpoint.NewMemoryRegistry()
	if _, err := reg.Register(context.Background(), endpoint.Endpoint{URL: "  "}); err == nil {
		t.Fatal("expected error for empty URL")
	}
}

func TestLookupAndRotation(t *testing.T) {
	t.Parallel()
	reg := endpoint.NewMemoryRegistry()
	ctx := context.Background()

	first, _ := reg.Register(ctx, endpoint.Endpoint{URL: "https://acme.test/hook"})
	got, err := reg.Lookup(ctx, "https://acme.test/hook")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.ID != first.ID || got.Secret != first.Secret {
		t.Fatal("lookup returned a different endpoint than registered")
	}

	// Re-registering the same URL rotates the secret but keeps the ID.
	rotated, _ := reg.Register(ctx, endpoint.Endpoint{URL: "https://acme.test/hook"})
	if rotated.ID != first.ID {
		t.Fatalf("rotation changed ID: %s -> %s", first.ID, rotated.ID)
	}
	if rotated.Secret == first.Secret {
		t.Fatal("rotation did not change the secret")
	}

	all, _ := reg.List(ctx, "")
	if len(all) != 1 {
		t.Fatalf("rotation created a duplicate: %d endpoints", len(all))
	}
}

func TestLookupNotFound(t *testing.T) {
	t.Parallel()
	reg := endpoint.NewMemoryRegistry()
	_, err := reg.Lookup(context.Background(), "https://nope.test")
	if !errors.Is(err, endpoint.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestListFiltersByProducer(t *testing.T) {
	t.Parallel()
	reg := endpoint.NewMemoryRegistry()
	ctx := context.Background()
	_, _ = reg.Register(ctx, endpoint.Endpoint{URL: "https://a.test", Producer: "acme"})
	_, _ = reg.Register(ctx, endpoint.Endpoint{URL: "https://b.test", Producer: "beta"})

	acme, _ := reg.List(ctx, "acme")
	if len(acme) != 1 || acme[0].Producer != "acme" {
		t.Fatalf("producer filter failed: %+v", acme)
	}
	all, _ := reg.List(ctx, "")
	if len(all) != 2 {
		t.Fatalf("want 2 endpoints, got %d", len(all))
	}
}

func TestGenerateSecretUnique(t *testing.T) {
	t.Parallel()
	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		s := endpoint.GenerateSecret()
		if seen[s] {
			t.Fatal("duplicate secret generated")
		}
		seen[s] = true
	}
}
