package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sean-kim05/hookline/internal/api"
	"github.com/sean-kim05/hookline/internal/queue"
)

const testKey = "whsec_producer_key"

func newTestServer(t *testing.T) (http.Handler, *queue.MemoryQueue) {
	t.Helper()
	q := queue.NewMemoryQueue()
	auth := api.NewStaticKeyAuth(map[string]string{"acme": testKey})
	s, err := api.NewServer(api.Config{
		Queue:  q,
		Auth:   auth,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return s.Handler(), q
}

func postEvent(t *testing.T, h http.Handler, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCreateEventAcceptedAndEnqueued(t *testing.T) {
	h, q := newTestServer(t)

	body := `{"endpoint":"https://consumer.test/hook","payload":{"order":42},"idempotency_key":"ord-42"}`
	rec := postEvent(t, h, testKey, body)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		ID      string `json:"id"`
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.ID == "" || resp.EventID == "" {
		t.Errorf("response missing ids: %+v", resp)
	}

	// The event must actually be on the queue, intact.
	leases, err := q.Lease(context.Background(), 10, time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("queued %d events, want 1", len(leases))
	}
	got := leases[0].Message.Event
	if got.Endpoint != "https://consumer.test/hook" {
		t.Errorf("endpoint = %q", got.Endpoint)
	}
	if got.IdempotencyKey != "ord-42" {
		t.Errorf("idempotency key = %q", got.IdempotencyKey)
	}
	if string(got.Payload) != `{"order":42}` {
		t.Errorf("payload = %q, want byte-for-byte passthrough", got.Payload)
	}
	if got.ID != resp.EventID {
		t.Errorf("queued event ID %q != response event_id %q", got.ID, resp.EventID)
	}
}

func TestCreateEventRequiresAuth(t *testing.T) {
	h, q := newTestServer(t)

	body := `{"endpoint":"https://consumer.test/hook","payload":{"x":1}}`

	if rec := postEvent(t, h, "", body); rec.Code != http.StatusUnauthorized {
		t.Errorf("no key: status = %d, want 401", rec.Code)
	}
	if rec := postEvent(t, h, "wrong-key", body); rec.Code != http.StatusUnauthorized {
		t.Errorf("bad key: status = %d, want 401", rec.Code)
	}

	// Nothing should have been enqueued by rejected requests.
	if leases, _ := q.Lease(context.Background(), 10, time.Minute); len(leases) != 0 {
		t.Errorf("rejected requests enqueued %d events, want 0", len(leases))
	}
}

func TestCreateEventValidation(t *testing.T) {
	h, _ := newTestServer(t)

	cases := []struct {
		name string
		body string
	}{
		{"missing endpoint", `{"payload":{"x":1}}`},
		{"empty endpoint", `{"endpoint":"","payload":{"x":1}}`},
		{"non-http scheme", `{"endpoint":"ftp://x.test/h","payload":{"x":1}}`},
		{"endpoint without host", `{"endpoint":"https://","payload":{"x":1}}`},
		{"missing payload", `{"endpoint":"https://x.test/h"}`},
		{"malformed json", `{"endpoint":`},
		{"unknown field", `{"endpoint":"https://x.test/h","payload":{"x":1},"bogus":true}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := postEvent(t, h, testKey, tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestHealthzNoAuth(t *testing.T) {
	h, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	h, _ := newTestServer(t)
	// GET on the events route should not match the "POST /v1/events" pattern.
	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req.Header.Set("Authorization", "Bearer "+testKey)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}
