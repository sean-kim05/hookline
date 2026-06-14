package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/sean-kim05/hookline/internal/audit"
	"github.com/sean-kim05/hookline/internal/delivery"
	"github.com/sean-kim05/hookline/internal/event"
)

// attemptResponse is the public view of one delivery attempt.
type attemptResponse struct {
	ID         string `json:"id"`
	MessageID  string `json:"message_id"`
	EventID    string `json:"event_id"`
	Endpoint   string `json:"endpoint"`
	Attempt    int    `json:"attempt"`
	Outcome    string `json:"outcome"`
	StatusCode int    `json:"status_code"`
	DurationMS int64  `json:"duration_ms"`
	Error      string `json:"error,omitempty"`
	At         string `json:"at"`
}

func toAttemptResponse(a audit.Attempt) attemptResponse {
	return attemptResponse{
		ID:         a.ID,
		MessageID:  a.MessageID,
		EventID:    a.EventID,
		Endpoint:   a.Endpoint,
		Attempt:    a.Attempt,
		Outcome:    string(a.Outcome),
		StatusCode: a.StatusCode,
		DurationMS: a.Duration.Milliseconds(),
		Error:      a.Error,
		At:         a.At.UTC().Format(time.RFC3339Nano),
	}
}

// handleListDeliveries returns recent delivery attempts, filterable by event,
// message, and outcome via query parameters.
func (s *Server) handleListDeliveries(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := audit.Filter{
		EventID:   q.Get("event_id"),
		MessageID: q.Get("message_id"),
		Outcome:   audit.Outcome(q.Get("outcome")),
		Limit:     parseLimit(q.Get("limit")),
	}
	attempts, err := s.audit.List(r.Context(), f)
	if err != nil {
		s.log.Error("hookline: list deliveries", "err", err)
		writeError(w, http.StatusInternalServerError, "could not list deliveries")
		return
	}
	out := make([]attemptResponse, 0, len(attempts))
	for _, a := range attempts {
		out = append(out, toAttemptResponse(a))
	}
	writeJSON(w, http.StatusOK, map[string]any{"deliveries": out})
}

// deadLetterResponse is the public view of a dead letter, including its payload
// so an operator can inspect what would be replayed.
type deadLetterResponse struct {
	MessageID string          `json:"message_id"`
	EventID   string          `json:"event_id"`
	Endpoint  string          `json:"endpoint"`
	Attempts  int             `json:"attempts"`
	Reason    string          `json:"reason"`
	Payload   json.RawMessage `json:"payload"`
	FailedAt  string          `json:"failed_at"`
}

func toDeadLetterResponse(dl delivery.DeadLetter) deadLetterResponse {
	payload := json.RawMessage(dl.Message.Event.Payload)
	if !json.Valid(payload) {
		// Non-JSON payloads are surfaced as a JSON string so the response stays
		// valid JSON.
		b, _ := json.Marshal(string(dl.Message.Event.Payload))
		payload = b
	}
	return deadLetterResponse{
		MessageID: dl.Message.ID,
		EventID:   dl.Message.Event.ID,
		Endpoint:  dl.Message.Event.Endpoint,
		Attempts:  dl.Message.Attempts,
		Reason:    dl.Reason,
		Payload:   payload,
		FailedAt:  dl.FailedAt.UTC().Format(time.RFC3339Nano),
	}
}

func (s *Server) handleListDLQ(w http.ResponseWriter, r *http.Request) {
	entries, err := s.dlq.ListDeadLetters(r.Context(), parseLimit(r.URL.Query().Get("limit")))
	if err != nil {
		s.log.Error("hookline: list dlq", "err", err)
		writeError(w, http.StatusInternalServerError, "could not list dead letters")
		return
	}
	out := make([]deadLetterResponse, 0, len(entries))
	for _, dl := range entries {
		out = append(out, toDeadLetterResponse(dl))
	}
	writeJSON(w, http.StatusOK, map[string]any{"dead_letters": out})
}

// handleReplay re-enqueues a dead-lettered event for delivery and removes it
// from the DLQ. The event keeps its original event ID (so consumer-side
// idempotency still recognises it) but gets a fresh queue message ID.
func (s *Server) handleReplay(w http.ResponseWriter, r *http.Request) {
	msgID := r.PathValue("id")
	dl, err := s.dlq.GetDeadLetter(r.Context(), msgID)
	if errors.Is(err, delivery.ErrDeadLetterNotFound) {
		writeError(w, http.StatusNotFound, "no dead letter for that message")
		return
	}
	if err != nil {
		s.log.Error("hookline: replay get", "err", err, "message", msgID)
		writeError(w, http.StatusInternalServerError, "could not load dead letter")
		return
	}

	// Re-enqueue a fresh copy of the original event. Attempts reset because the
	// queue assigns a new message; the event ID is preserved deliberately.
	ev := event.Event{
		ID:             dl.Message.Event.ID,
		Endpoint:       dl.Message.Event.Endpoint,
		Payload:        dl.Message.Event.Payload,
		ContentType:    dl.Message.Event.ContentType,
		IdempotencyKey: dl.Message.Event.IdempotencyKey,
		CreatedAt:      dl.Message.Event.CreatedAt,
	}
	newID, err := s.queue.Enqueue(r.Context(), ev)
	if err != nil {
		s.log.Error("hookline: replay enqueue", "err", err, "message", msgID)
		writeError(w, http.StatusInternalServerError, "could not re-enqueue event")
		return
	}

	// Remove from the DLQ only after a successful re-enqueue. If removal fails
	// the event is still queued for delivery; a stale DLQ row is acceptable.
	if err := s.dlq.RemoveDeadLetter(r.Context(), msgID); err != nil && !errors.Is(err, delivery.ErrDeadLetterNotFound) {
		s.log.Error("hookline: replay remove", "err", err, "message", msgID)
	}

	s.log.Info("hookline: dead letter replayed", "old_message", msgID, "new_message", newID, "event", ev.ID)
	writeJSON(w, http.StatusAccepted, map[string]string{"id": newID, "event_id": ev.ID})
}

// parseLimit parses a non-negative limit query parameter, returning 0 (meaning
// "use the default") when absent or invalid.
func parseLimit(s string) int {
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return 0
	}
	return n
}
