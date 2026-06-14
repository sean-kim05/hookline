// Command hookline runs the Hookline service: the ingestion + management API
// and the delivery worker, sharing one queue.
//
// By default it runs entirely in memory, so it is self-contained for local runs
// and demos. Set -database-url (or HOOKLINE_DATABASE_URL) to run on PostgreSQL,
// which makes the queue, audit log, endpoint registry, and dead-letter queue
// durable across restarts.
//
// Usage:
//
//	hookline -addr :8080 -api-key dev-key -secret whsec_dev
//	hookline -database-url postgres://hookline:hookline@localhost:5432/hookline
//
// Submit an event:
//
//	curl -X POST localhost:8080/v1/events \
//	  -H "Authorization: Bearer dev-key" \
//	  -H "Content-Type: application/json" \
//	  -d '{"endpoint":"http://localhost:9099/hook","payload":{"hello":"world"}}'
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sean-kim05/hookline/internal/api"
	"github.com/sean-kim05/hookline/internal/audit"
	"github.com/sean-kim05/hookline/internal/delivery"
	"github.com/sean-kim05/hookline/internal/endpoint"
	"github.com/sean-kim05/hookline/internal/metrics"
	"github.com/sean-kim05/hookline/internal/queue"
	"github.com/sean-kim05/hookline/internal/queue/postgres"
	"github.com/sean-kim05/hookline/internal/queue/wal"
)

// backend bundles the storage components, which differ between the in-memory
// and Postgres configurations but are wired into the API and worker identically.
type backend struct {
	queue    queue.Queue
	audit    audit.Log
	registry endpoint.Registry
	dlq      delivery.DeadLetterStore
	close    func()
}

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address for the API")
	apiKey := flag.String("api-key", env("HOOKLINE_API_KEY", "dev-key"), "producer API key")
	secret := flag.String("secret", env("HOOKLINE_SIGNING_SECRET", "whsec_dev"), "fallback HMAC signing secret for unregistered endpoints")
	databaseURL := flag.String("database-url", env("HOOKLINE_DATABASE_URL", ""), "PostgreSQL DSN; empty runs in memory")
	walDir := flag.String("wal-dir", env("HOOKLINE_WAL_DIR", ""), "directory for the WAL queue; durable queue with in-memory audit/registry/DLQ (ignored if -database-url is set)")
	concurrency := flag.Int("concurrency", 8, "concurrent deliveries")
	circuitThreshold := flag.Int("circuit-threshold", 5, "consecutive failures that open an endpoint's circuit breaker")
	circuitCooldown := flag.Duration("circuit-cooldown", 30*time.Second, "how long an open circuit breaker stays open")
	rateLimit := flag.Float64("rate-limit", 0, "max deliveries per second per endpoint host (0 disables)")
	rateBurst := flag.Float64("rate-burst", 0, "burst size for the per-endpoint rate limiter (0 = rate-limit)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	be, err := newBackend(ctx, *databaseURL, *walDir, log)
	if err != nil {
		log.Error("hookline: configure storage", "err", err)
		os.Exit(1)
	}
	defer be.close()

	// Metrics ride on the audit hook (delivery counters/latency) and an HTTP
	// middleware (request counters/latency); the queue depth is sampled below.
	m := metrics.New()
	be.audit = m.Instrument(be.audit)

	// Deliveries are signed with each endpoint's registered secret, falling back
	// to -secret for endpoints that were never registered.
	signers := endpoint.NewSignerSource(be.registry, delivery.NewSigner(*secret))

	// Delivery stack, innermost first: HTTP POST -> circuit breaker (stop
	// hammering a down endpoint) -> rate limiter (respect per-endpoint limits).
	var deliverer delivery.Deliverer = delivery.NewSigningDeliverer(&http.Client{Timeout: 10 * time.Second}, signers)
	deliverer = delivery.NewCircuitBreaker(deliverer, *circuitThreshold, *circuitCooldown)
	if *rateLimit > 0 {
		deliverer = delivery.NewRateLimiter(deliverer, *rateLimit, *rateBurst)
		log.Info("hookline: per-endpoint rate limit enabled", "per_sec", *rateLimit, "burst", *rateBurst)
	}

	worker, err := delivery.New(delivery.Config{
		Queue:       be.queue,
		Deliverer:   deliverer,
		Sink:        be.dlq,
		Audit:       be.audit,
		Concurrency: *concurrency,
		Logger:      log,
	})
	if err != nil {
		log.Error("hookline: configure worker", "err", err)
		os.Exit(1)
	}

	srv, err := api.NewServer(api.Config{
		Queue:    be.queue,
		Auth:     api.NewStaticKeyAuth(map[string]string{"default": *apiKey}),
		Registry: be.registry,
		Audit:    be.audit,
		DLQ:      be.dlq,
		Logger:   log,
	})
	if err != nil {
		log.Error("hookline: configure api", "err", err)
		os.Exit(1)
	}

	// Root mux: /metrics is unwrapped (so scrapes don't pollute request
	// metrics); everything else goes through the API behind the HTTP middleware.
	root := http.NewServeMux()
	root.Handle("/metrics", m.Handler())
	root.Handle("/", m.HTTPMiddleware(srv.Handler()))
	httpServer := &http.Server{Addr: *addr, Handler: root}

	// Sample queue depth in the background when the backend supports it.
	if src, ok := be.queue.(metrics.DepthSource); ok {
		go m.PollDepth(ctx, src, 5*time.Second)
	}

	go func() {
		log.Info("hookline: delivery worker started", "concurrency", *concurrency)
		if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
			log.Error("hookline: worker stopped", "err", err)
			stop()
		}
	}()

	go func() {
		log.Info("hookline: API listening", "addr", *addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("hookline: http server", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("hookline: shutting down")

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutCtx); err != nil {
		log.Error("hookline: shutdown", "err", err)
	}
}

// newBackend builds the storage layer. An empty databaseURL selects the
// in-memory backend; otherwise it connects to Postgres, sharing one pool across
// the queue, audit log, registry, and DLQ, and migrates every table.
func newBackend(ctx context.Context, databaseURL, walDir string, log *slog.Logger) (backend, error) {
	if databaseURL == "" {
		// WAL backs only the queue; audit/registry/DLQ stay in memory. This is
		// the from-scratch durable-queue configuration.
		if walDir != "" {
			q, err := wal.Open(walDir)
			if err != nil {
				return backend{}, fmt.Errorf("open wal: %w", err)
			}
			log.Info("hookline: using WAL queue (audit/registry/DLQ in memory)", "dir", walDir)
			return backend{
				queue:    q,
				audit:    audit.NewMemoryLog(0),
				registry: endpoint.NewMemoryRegistry(),
				dlq:      &delivery.MemoryDeadLetterSink{},
				close:    func() { _ = q.Close() },
			}, nil
		}

		log.Info("hookline: using in-memory storage (set -database-url or -wal-dir for durability)")
		return backend{
			queue:    queue.NewMemoryQueue(),
			audit:    audit.NewMemoryLog(0),
			registry: endpoint.NewMemoryRegistry(),
			dlq:      &delivery.MemoryDeadLetterSink{},
			close:    func() {},
		}, nil
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return backend{}, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return backend{}, fmt.Errorf("ping: %w", err)
	}

	q := postgres.NewQueue(pool)
	auditLog := audit.NewPostgresLog(pool)
	reg := endpoint.NewPostgresRegistry(pool)
	dlq := delivery.NewPostgresDeadLetterSink(pool)

	for name, migrate := range map[string]func(context.Context) error{
		"queue":    q.Migrate,
		"audit":    auditLog.Migrate,
		"registry": reg.Migrate,
		"dlq":      dlq.Migrate,
	} {
		if err := migrate(ctx); err != nil {
			pool.Close()
			return backend{}, fmt.Errorf("migrate %s: %w", name, err)
		}
	}

	log.Info("hookline: using PostgreSQL storage")
	return backend{
		queue:    q,
		audit:    auditLog,
		registry: reg,
		dlq:      dlq,
		close:    pool.Close,
	}, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
