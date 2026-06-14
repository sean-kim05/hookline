// Package api is Hookline's ingestion HTTP API: the front door where producers
// submit events. It authenticates the producer, validates the request, and
// enqueues the event for the delivery worker to pick up. Delivery itself is
// asynchronous, so a successful submit returns 202 Accepted with the assigned
// IDs, not a delivery result.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/sean-kim05/hookline/internal/audit"
	"github.com/sean-kim05/hookline/internal/delivery"
	"github.com/sean-kim05/hookline/internal/endpoint"
	"github.com/sean-kim05/hookline/internal/queue"
)

// Server is the ingestion API. It enqueues accepted events onto a queue.Queue
// for asynchronous delivery.
type Server struct {
	queue    queue.Queue
	auth     Authenticator
	registry endpoint.Registry        // optional; enables endpoint management
	audit    audit.Log                // optional; enables delivery read APIs
	dlq      delivery.DeadLetterStore // optional; enables the DLQ view + replay
	log      *slog.Logger
	// maxBodyBytes caps the request body size to bound memory per request.
	maxBodyBytes int64
}

// Config configures a Server. Queue and Auth are required. Registry and Audit
// are optional: supplying them enables the endpoint-management and
// delivery-inspection routes used by the dashboard.
type Config struct {
	Queue        queue.Queue
	Auth         Authenticator
	Registry     endpoint.Registry
	Audit        audit.Log
	DLQ          delivery.DeadLetterStore
	Logger       *slog.Logger
	MaxBodyBytes int64 // default 1 MiB
}

// NewServer validates cfg and returns a Server.
func NewServer(cfg Config) (*Server, error) {
	if cfg.Queue == nil {
		return nil, errMissing("Queue")
	}
	if cfg.Auth == nil {
		return nil, errMissing("Auth")
	}
	s := &Server{
		queue:        cfg.Queue,
		auth:         cfg.Auth,
		registry:     cfg.Registry,
		audit:        cfg.Audit,
		dlq:          cfg.DLQ,
		log:          cfg.Logger,
		maxBodyBytes: cfg.MaxBodyBytes,
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	if s.maxBodyBytes <= 0 {
		s.maxBodyBytes = 1 << 20 // 1 MiB
	}
	return s, nil
}

// Handler returns the API's HTTP handler. /healthz is unauthenticated; the
// /v1 routes require a valid API key.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.Handle("POST /v1/events", s.authMiddleware(http.HandlerFunc(s.handleCreateEvent)))

	// Endpoint management is available only when a registry is configured.
	if s.registry != nil {
		mux.Handle("POST /v1/endpoints", s.authMiddleware(http.HandlerFunc(s.handleRegisterEndpoint)))
		mux.Handle("GET /v1/endpoints", s.authMiddleware(http.HandlerFunc(s.handleListEndpoints)))
	}

	// Recent deliveries and per-event history come from the audit log.
	if s.audit != nil {
		mux.Handle("GET /v1/deliveries", s.authMiddleware(http.HandlerFunc(s.handleListDeliveries)))
	}

	// The DLQ view and replay need the dead-letter store (it retains payloads).
	if s.dlq != nil {
		mux.Handle("GET /v1/dlq", s.authMiddleware(http.HandlerFunc(s.handleListDLQ)))
		mux.Handle("POST /v1/dlq/{id}/replay", s.authMiddleware(http.HandlerFunc(s.handleReplay)))
	}

	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- helpers ---

func contextWithProducer(ctx context.Context, producer string) context.Context {
	return context.WithValue(ctx, producerKey{}, producer)
}

func producerFromContext(ctx context.Context) string {
	p, _ := ctx.Value(producerKey{}).(string)
	return p
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

type configError struct{ field string }

func (e configError) Error() string { return "api: Config." + e.field + " is required" }

func errMissing(field string) error { return configError{field: field} }
