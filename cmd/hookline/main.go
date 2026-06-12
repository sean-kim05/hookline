// Command hookline runs the Hookline service: the ingestion API and the
// delivery worker, sharing one queue. This wiring uses the in-memory queue, so
// it is self-contained for local runs and demos; the Postgres-backed
// deployment configuration arrives with the rest of week 5.
//
// Usage:
//
//	hookline -addr :8080 -api-key dev-key -secret whsec_dev
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
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sean-kim05/hookline/internal/api"
	"github.com/sean-kim05/hookline/internal/delivery"
	"github.com/sean-kim05/hookline/internal/queue"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address for the ingestion API")
	apiKey := flag.String("api-key", env("HOOKLINE_API_KEY", "dev-key"), "producer API key")
	secret := flag.String("secret", env("HOOKLINE_SIGNING_SECRET", "whsec_dev"), "HMAC signing secret for deliveries")
	concurrency := flag.Int("concurrency", 8, "concurrent deliveries")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	q := queue.NewMemoryQueue()
	defer q.Close()

	worker, err := delivery.New(delivery.Config{
		Queue:       q,
		Deliverer:   delivery.NewHTTPDeliverer(&http.Client{Timeout: 10 * time.Second}, delivery.NewSigner(*secret)),
		Concurrency: *concurrency,
		Logger:      log,
	})
	if err != nil {
		log.Error("hookline: configure worker", "err", err)
		os.Exit(1)
	}

	srv, err := api.NewServer(api.Config{
		Queue:  q,
		Auth:   api.NewStaticKeyAuth(map[string]string{"default": *apiKey}),
		Logger: log,
	})
	if err != nil {
		log.Error("hookline: configure api", "err", err)
		os.Exit(1)
	}

	httpServer := &http.Server{Addr: *addr, Handler: srv.Handler()}

	// One context cancels both the worker and the HTTP server on SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Info("hookline: delivery worker started", "concurrency", *concurrency)
		if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
			log.Error("hookline: worker stopped", "err", err)
			stop()
		}
	}()

	go func() {
		log.Info("hookline: ingestion API listening", "addr", *addr)
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

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
