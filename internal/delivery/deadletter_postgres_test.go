package delivery_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sean-kim05/hookline/internal/delivery"
	"github.com/sean-kim05/hookline/internal/event"
	"github.com/sean-kim05/hookline/internal/queue"
)

// TestPostgresDeadLetterStore exercises the durable DLQ against a real
// database. Skips without HOOKLINE_TEST_DATABASE_URL.
func TestPostgresDeadLetterStore(t *testing.T) {
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

	store := delivery.NewPostgresDeadLetterSink(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE dead_letters"); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	dl := delivery.DeadLetter{
		Message: queue.Message{
			ID: "msg-1",
			Event: event.Event{
				ID: "evt-1", Endpoint: "https://acme.test/hook",
				Payload: []byte(`{"order":7}`), ContentType: "application/json",
				IdempotencyKey: "ord-7", CreatedAt: time.Unix(1_700_000_000, 0).UTC(),
			},
			Attempts: 12,
		},
		Reason:   "exhausted after 12 attempts: last status 500",
		FailedAt: time.Unix(1_700_000_100, 0).UTC(),
	}
	if err := store.DeadLetter(ctx, dl); err != nil {
		t.Fatalf("record: %v", err)
	}

	got, err := store.GetDeadLetter(ctx, "msg-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got.Message.Event.Payload) != `{"order":7}` || got.Message.Attempts != 12 {
		t.Fatalf("round-trip mismatch: %+v", got.Message)
	}

	list, err := store.ListDeadLetters(ctx, 10)
	if err != nil || len(list) != 1 {
		t.Fatalf("list: %v, len=%d", err, len(list))
	}

	if err := store.RemoveDeadLetter(ctx, "msg-1"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := store.GetDeadLetter(ctx, "msg-1"); !errors.Is(err, delivery.ErrDeadLetterNotFound) {
		t.Fatalf("want ErrDeadLetterNotFound after remove, got %v", err)
	}
	if err := store.RemoveDeadLetter(ctx, "msg-1"); !errors.Is(err, delivery.ErrDeadLetterNotFound) {
		t.Fatalf("double-remove should be ErrDeadLetterNotFound, got %v", err)
	}
}
