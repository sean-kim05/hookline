package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sean-kim05/hookline/internal/queue"
	"github.com/sean-kim05/hookline/internal/queue/postgres"
	"github.com/sean-kim05/hookline/internal/queue/queuetest"
)

// TestPostgresQueueConformance runs the shared conformance suite against a
// real PostgreSQL database. It needs HOOKLINE_TEST_DATABASE_URL and skips
// otherwise, so plain `go test ./...` works without a database while CI always
// runs it against a service container.
func TestPostgresQueueConformance(t *testing.T) {
	dsn := os.Getenv("HOOKLINE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("HOOKLINE_TEST_DATABASE_URL not set; start a disposable database with " +
			"`docker compose -f docker-compose.test.yml up -d` and set the URL " +
			"(see README)")
	}

	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(admin.Close)

	queuetest.Run(t, func(t *testing.T, now func() time.Time) queue.Queue {
		q, err := postgres.Open(ctx, dsn, postgres.WithClock(now))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { q.Close() })
		if err := q.Migrate(ctx); err != nil {
			t.Fatalf("Migrate: %v", err)
		}
		// Each conformance subtest expects a fresh, empty queue.
		if _, err := admin.Exec(ctx, "TRUNCATE queue_messages"); err != nil {
			t.Fatalf("truncate: %v", err)
		}
		return q
	})
}
