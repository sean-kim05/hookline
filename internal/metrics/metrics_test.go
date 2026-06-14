package metrics_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sean-kim05/hookline/internal/audit"
	"github.com/sean-kim05/hookline/internal/metrics"
)

// scrape returns the text exposition from the /metrics handler.
func scrape(t *testing.T, m *metrics.Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics scrape status = %d", rec.Code)
	}
	return rec.Body.String()
}

func TestInstrumentRecordsDeliveryMetrics(t *testing.T) {
	m := metrics.New()

	// The instrumented log must forward to the wrapped log AND count.
	inner := audit.NewMemoryLog(0)
	log := m.Instrument(inner)

	ctx := context.Background()
	_ = log.Record(ctx, audit.Attempt{Outcome: audit.OutcomeDelivered, StatusCode: 200, Duration: 5 * time.Millisecond})
	_ = log.Record(ctx, audit.Attempt{Outcome: audit.OutcomeRetrying, StatusCode: 500, Duration: 9 * time.Millisecond})

	// Forwarding: the inner log saw both attempts.
	got, _ := log.List(ctx, audit.Filter{})
	if len(got) != 2 {
		t.Fatalf("instrumented log did not forward to inner: %d records", len(got))
	}

	out := scrape(t, m)
	for _, want := range []string{
		`hookline_delivery_attempts_total{outcome="delivered",status_class="2xx"} 1`,
		`hookline_delivery_attempts_total{outcome="retrying",status_class="5xx"} 1`,
		`hookline_delivery_duration_seconds_count 2`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics missing %q\n--- output ---\n%s", want, out)
		}
	}
}

func TestHTTPMiddlewareRecordsRequests(t *testing.T) {
	m := metrics.New()
	h := m.HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/events", nil))

	out := scrape(t, m)
	if !strings.Contains(out, `hookline_http_requests_total{code="202",method="POST",path="/v1/events"} 1`) {
		t.Errorf("missing http request counter\n%s", out)
	}
}

func TestHTTPMiddlewareNormalizesIDPaths(t *testing.T) {
	m := metrics.New()
	h := m.HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/dlq/abc123def/replay", nil))

	out := scrape(t, m)
	if !strings.Contains(out, `path="/v1/dlq/{id}/replay"`) {
		t.Errorf("replay path not normalised\n%s", out)
	}
	if strings.Contains(out, "abc123def") {
		t.Errorf("raw message ID leaked into a metric label (cardinality risk)\n%s", out)
	}
}

type depthStub int

func (d depthStub) Depth(context.Context) (int, error) { return int(d), nil }

func TestPollDepthPublishesGauge(t *testing.T) {
	m := metrics.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go m.PollDepth(ctx, depthStub(7), 10*time.Millisecond)

	// Poll the scrape until the gauge appears (the first sample happens
	// immediately, but allow a beat for the goroutine to run).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(scrape(t, m), "hookline_queue_depth 7") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("queue_depth gauge never reached 7\n%s", scrape(t, m))
}
