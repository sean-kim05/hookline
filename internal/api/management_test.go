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
	"github.com/sean-kim05/hookline/internal/audit"
	"github.com/sean-kim05/hookline/internal/delivery"
	"github.com/sean-kim05/hookline/internal/endpoint"
	"github.com/sean-kim05/hookline/internal/event"
	"github.com/sean-kim05/hookline/internal/queue"
)

type fullServer struct {
	h     http.Handler
	q     *queue.MemoryQueue
	reg   *endpoint.MemoryRegistry
	audit *audit.MemoryLog
	dlq   *delivery.MemoryDeadLetterSink
}

func newFullServer(t *testing.T) fullServer {
	t.Helper()
	q := queue.NewMemoryQueue()
	reg := endpoint.NewMemoryRegistry()
	al := audit.NewMemoryLog(0)
	dlq := &delivery.MemoryDeadLetterSink{}
	s, err := api.NewServer(api.Config{
		Queue:    q,
		Auth:     api.NewStaticKeyAuth(map[string]string{"acme": testKey}),
		Registry: reg,
		Audit:    al,
		DLQ:      dlq,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return fullServer{h: s.Handler(), q: q, reg: reg, audit: al, dlq: dlq}
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = bytes.NewBufferString(body)
	}
	req := httptest.NewRequest(method, path, r)
	req.Header.Set("Authorization", "Bearer "+testKey)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestRegisterEndpointReturnsSecretOnce(t *testing.T) {
	fs := newFullServer(t)

	rec := do(t, fs.h, http.MethodPost, "/v1/endpoints", `{"url":"https://acme.test/hook"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	var reg struct {
		ID     string `json:"id"`
		URL    string `json:"url"`
		Secret string `json:"secret"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &reg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if reg.ID == "" || reg.Secret == "" {
		t.Fatalf("register response missing id/secret: %+v", reg)
	}

	// Listing must NOT leak the secret.
	rec = do(t, fs.h, http.MethodGet, "/v1/endpoints", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d", rec.Code)
	}
	if bytes.Contains(rec.Body.Bytes(), []byte(reg.Secret)) {
		t.Fatal("endpoint list leaked the signing secret")
	}
	if bytes.Contains(rec.Body.Bytes(), []byte(`"secret"`)) {
		t.Fatal("endpoint list response contains a secret field")
	}
}

func TestRegisterEndpointValidation(t *testing.T) {
	fs := newFullServer(t)
	for _, body := range []string{`{}`, `{"url":""}`, `{"url":"ftp://x.test"}`, `{"url":"https://"}`} {
		rec := do(t, fs.h, http.MethodPost, "/v1/endpoints", body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %s: status = %d, want 400", body, rec.Code)
		}
	}
}

func TestListDeliveriesFiltersByOutcome(t *testing.T) {
	fs := newFullServer(t)
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	_ = fs.audit.Record(ctx, audit.Attempt{ID: "1", EventID: "e1", Outcome: audit.OutcomeDelivered, At: base})
	_ = fs.audit.Record(ctx, audit.Attempt{ID: "2", EventID: "e2", Outcome: audit.OutcomeDeadLettered, At: base.Add(time.Second)})

	rec := do(t, fs.h, http.MethodGet, "/v1/deliveries?outcome=dead_lettered", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var resp struct {
		Deliveries []struct {
			ID      string `json:"id"`
			Outcome string `json:"outcome"`
		} `json:"deliveries"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Deliveries) != 1 || resp.Deliveries[0].ID != "2" {
		t.Fatalf("outcome filter failed: %+v", resp.Deliveries)
	}
}

func TestDLQListAndReplay(t *testing.T) {
	fs := newFullServer(t)
	ctx := context.Background()

	// Seed a dead letter with a real payload.
	dl := delivery.DeadLetter{
		Message: queue.Message{
			ID:       "msg-dead",
			Event:    event.Event{ID: "evt-1", Endpoint: "https://acme.test/hook", Payload: []byte(`{"order":7}`)},
			Attempts: 12,
		},
		Reason:   "exhausted after 12 attempts",
		FailedAt: time.Unix(1_700_000_000, 0).UTC(),
	}
	if err := fs.dlq.DeadLetter(ctx, dl); err != nil {
		t.Fatalf("seed dlq: %v", err)
	}

	// It shows up in the DLQ view with its payload.
	rec := do(t, fs.h, http.MethodGet, "/v1/dlq", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("dlq status = %d", rec.Code)
	}
	var list struct {
		DeadLetters []struct {
			MessageID string          `json:"message_id"`
			Payload   json.RawMessage `json:"payload"`
		} `json:"dead_letters"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode dlq: %v", err)
	}
	if len(list.DeadLetters) != 1 || string(list.DeadLetters[0].Payload) != `{"order":7}` {
		t.Fatalf("dlq view wrong: %+v", list.DeadLetters)
	}

	// Replay re-enqueues the event and clears the DLQ entry.
	rec = do(t, fs.h, http.MethodPost, "/v1/dlq/msg-dead/replay", "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("replay status = %d; body=%s", rec.Code, rec.Body.String())
	}
	var rep struct {
		ID      string `json:"id"`
		EventID string `json:"event_id"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &rep)
	if rep.EventID != "evt-1" {
		t.Fatalf("replay should preserve event ID, got %q", rep.EventID)
	}

	// The event is back on the queue, payload intact.
	leases, _ := fs.q.Lease(ctx, 10, time.Minute)
	if len(leases) != 1 || string(leases[0].Message.Event.Payload) != `{"order":7}` {
		t.Fatalf("replay did not re-enqueue intact: %+v", leases)
	}
	// And the DLQ entry is gone.
	if fs.dlq.Len() != 0 {
		t.Fatalf("replay left %d dlq entries", fs.dlq.Len())
	}

	// Replaying an unknown message is a 404.
	rec = do(t, fs.h, http.MethodPost, "/v1/dlq/nope/replay", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("replay unknown: status = %d, want 404", rec.Code)
	}
}

func TestManagementRoutesAbsentWithoutDeps(t *testing.T) {
	// A server with no registry/audit/dlq should not expose those routes.
	h, _ := newTestServer(t)
	for _, path := range []string{"/v1/endpoints", "/v1/deliveries", "/v1/dlq"} {
		rec := do(t, h, http.MethodGet, path, "")
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s: status = %d, want 404 when dependency absent", path, rec.Code)
		}
	}
}
