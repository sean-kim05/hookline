package endpoint_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sean-kim05/hookline/internal/endpoint"
)

// TestPostgresRegistry exercises the Postgres registry against a real database.
// Skips without HOOKLINE_TEST_DATABASE_URL, matching the queue tests.
func TestPostgresRegistry(t *testing.T) {
	dsn := os.Getenv("HOOKLINE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("HOOKLINE_TEST_DATABASE_URL not set; see README")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	reg := endpoint.NewPostgresRegistry(pool)
	if err := reg.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE endpoints"); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	ep, err := reg.Register(ctx, endpoint.Endpoint{URL: "https://acme.test/hook", Producer: "acme"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if ep.ID == "" || ep.Secret == "" {
		t.Fatalf("register response incomplete: %+v", ep)
	}

	got, err := reg.Lookup(ctx, "https://acme.test/hook")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got.ID != ep.ID || got.Secret != ep.Secret {
		t.Fatalf("lookup mismatch: %+v vs %+v", got, ep)
	}

	// Re-register rotates the secret in place (ON CONFLICT), no duplicate.
	rotated, err := reg.Register(ctx, endpoint.Endpoint{URL: "https://acme.test/hook"})
	if err != nil {
		t.Fatalf("re-register: %v", err)
	}
	if rotated.ID != ep.ID {
		t.Fatalf("rotation changed ID: %s -> %s", ep.ID, rotated.ID)
	}
	if rotated.Secret == ep.Secret {
		t.Fatal("rotation did not change secret")
	}
	all, _ := reg.List(ctx, "")
	if len(all) != 1 {
		t.Fatalf("rotation duplicated: %d rows", len(all))
	}

	if _, err := reg.Lookup(ctx, "https://nope.test"); !errors.Is(err, endpoint.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}
