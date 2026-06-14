// Package metrics exposes Hookline's Prometheus instrumentation.
//
// Instrumentation is wired without coupling the hot-path packages to
// Prometheus: delivery metrics ride on the existing audit.Log hook (Instrument
// wraps any Log and records to Prometheus as a side effect), HTTP metrics are a
// standard middleware, and queue depth is sampled by a background poller. So
// the api, delivery, and queue packages stay dependency-free and a build
// without metrics simply omits the wrappers.
package metrics

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sean-kim05/hookline/internal/audit"
)

// Metrics holds Hookline's collectors and the registry they are registered to.
type Metrics struct {
	reg *prometheus.Registry

	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec

	deliveryAttempts *prometheus.CounterVec
	deliveryDuration prometheus.Histogram

	queueDepth prometheus.Gauge
}

// New builds the metrics into a fresh registry. It also registers the standard
// Go runtime and process collectors so the dashboard gets memory/GC/goroutines
// for free.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg: reg,
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookline_http_requests_total",
			Help: "Total HTTP requests handled by the API, by method, path, and status code.",
		}, []string{"method", "path", "code"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "hookline_http_request_duration_seconds",
			Help:    "API request latency in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "path"}),
		deliveryAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookline_delivery_attempts_total",
			Help: "Total delivery attempts, by outcome and HTTP status class.",
		}, []string{"outcome", "status_class"}),
		deliveryDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "hookline_delivery_duration_seconds",
			Help:    "Delivery attempt latency in seconds (time to POST to the endpoint).",
			Buckets: prometheus.DefBuckets,
		}),
		queueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hookline_queue_depth",
			Help: "Number of messages currently in the delivery queue.",
		}),
	}
	reg.MustRegister(
		m.httpRequests, m.httpDuration,
		m.deliveryAttempts, m.deliveryDuration, m.queueDepth,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Handler returns the /metrics HTTP handler exposing this registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// HTTPMiddleware records request count and latency for each request. Paths are
// normalised to a fixed set of route templates so message/endpoint IDs do not
// explode label cardinality.
func (m *Metrics) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sr := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sr, r)

		path := normalizePath(r.URL.Path)
		m.httpRequests.WithLabelValues(r.Method, path, strconv.Itoa(sr.status)).Inc()
		m.httpDuration.WithLabelValues(r.Method, path).Observe(time.Since(start).Seconds())
	})
}

// Instrument wraps an audit.Log so every recorded attempt also updates the
// delivery metrics, then forwards to next. This is how delivery is instrumented
// without the delivery package importing Prometheus.
func (m *Metrics) Instrument(next audit.Log) audit.Log {
	if next == nil {
		next = audit.NopLog{}
	}
	return &instrumentedLog{m: m, next: next}
}

type instrumentedLog struct {
	m    *Metrics
	next audit.Log
}

func (l *instrumentedLog) Record(ctx context.Context, a audit.Attempt) error {
	l.m.deliveryAttempts.WithLabelValues(string(a.Outcome), statusClass(a.StatusCode)).Inc()
	l.m.deliveryDuration.Observe(a.Duration.Seconds())
	return l.next.Record(ctx, a)
}

func (l *instrumentedLog) List(ctx context.Context, f audit.Filter) ([]audit.Attempt, error) {
	return l.next.List(ctx, f)
}

// DepthSource is a queue that can report how many messages it holds. It is
// optional: a queue that does not implement it simply isn't polled.
type DepthSource interface {
	Depth(ctx context.Context) (int, error)
}

// PollDepth samples src every interval and publishes it as the queue_depth
// gauge until ctx is cancelled. Run it in a goroutine.
func (m *Metrics) PollDepth(ctx context.Context, src DepthSource, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if n, err := src.Depth(ctx); err == nil {
			m.queueDepth.Set(float64(n))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// statusRecorder captures the response status for the request counter.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	r.wroteHeader = true // an implicit 200 if WriteHeader was never called
	return r.ResponseWriter.Write(b)
}

func statusClass(code int) string {
	switch {
	case code == 0:
		return "none"
	case code >= 200 && code < 300:
		return "2xx"
	case code >= 300 && code < 400:
		return "3xx"
	case code >= 400 && code < 500:
		return "4xx"
	default:
		return "5xx"
	}
}

// normalizePath collapses request paths to a small, fixed set of route
// templates so per-request IDs do not create unbounded label cardinality.
func normalizePath(p string) string {
	switch {
	case p == "/healthz":
		return "/healthz"
	case p == "/metrics":
		return "/metrics"
	case p == "/v1/events":
		return "/v1/events"
	case p == "/v1/endpoints":
		return "/v1/endpoints"
	case p == "/v1/deliveries":
		return "/v1/deliveries"
	case p == "/v1/dlq":
		return "/v1/dlq"
	case strings.HasPrefix(p, "/v1/dlq/") && strings.HasSuffix(p, "/replay"):
		return "/v1/dlq/{id}/replay"
	default:
		return "other"
	}
}
