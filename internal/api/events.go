package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/sean-kim05/hookline/internal/event"
	"github.com/sean-kim05/hookline/internal/id"
)

// createEventRequest is the JSON body of POST /v1/events. Payload is kept as
// raw JSON so arbitrary event bodies pass through byte-for-byte (the exact
// bytes are what gets signed and delivered).
type createEventRequest struct {
	Endpoint       string          `json:"endpoint"`
	Payload        json.RawMessage `json:"payload"`
	ContentType    string          `json:"content_type,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

// createEventResponse is returned on a successful submit. Delivery is
// asynchronous, so this reports the IDs to track the event, not a delivery
// outcome.
type createEventResponse struct {
	ID      string `json:"id"`       // queue message ID
	EventID string `json:"event_id"` // event ID (echoed in delivery headers)
}

func (s *Server) handleCreateEvent(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}

	var req createEventRequest
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	if err := validateCreateEvent(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Assign the event ID at ingestion so the producer gets a stable handle
	// back immediately, before the event is even enqueued.
	ev := event.Event{
		ID:             id.New(),
		Endpoint:       req.Endpoint,
		Payload:        []byte(req.Payload),
		ContentType:    req.ContentType,
		IdempotencyKey: req.IdempotencyKey,
	}
	msgID, err := s.queue.Enqueue(r.Context(), ev)
	if err != nil {
		s.log.Error("hookline: enqueue", "err", err, "producer", producerFromContext(r.Context()))
		writeError(w, http.StatusInternalServerError, "could not accept event")
		return
	}

	s.log.Info("hookline: event accepted",
		"message", msgID, "event", ev.ID, "endpoint", req.Endpoint,
		"producer", producerFromContext(r.Context()))
	writeJSON(w, http.StatusAccepted, createEventResponse{ID: msgID, EventID: ev.ID})
}

func validateCreateEvent(req createEventRequest) error {
	if strings.TrimSpace(req.Endpoint) == "" {
		return errors.New("endpoint is required")
	}
	u, err := url.Parse(req.Endpoint)
	if err != nil {
		return errors.New("endpoint is not a valid URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("endpoint must be an http or https URL")
	}
	if u.Host == "" {
		return errors.New("endpoint must include a host")
	}
	if len(req.Payload) == 0 {
		return errors.New("payload is required")
	}
	return nil
}
