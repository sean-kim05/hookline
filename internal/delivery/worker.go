package delivery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sean-kim05/hookline/internal/audit"
	"github.com/sean-kim05/hookline/internal/id"
	"github.com/sean-kim05/hookline/internal/queue"
)

// Config configures a Worker. Queue and Deliverer are required; the rest have
// defaults applied by New.
type Config struct {
	Queue     queue.Queue
	Deliverer Deliverer
	Sink      DeadLetterSink // where exhausted messages go; defaults to an in-memory sink
	Audit     audit.Log      // records every attempt; defaults to audit.NopLog
	Backoff   Backoff        // retry schedule; defaults to 1s base, 1h max

	MaxAttempts  int           // dead-letter after this many failed attempts (default 12)
	BatchSize    int           // messages leased per poll (default 20)
	Concurrency  int           // concurrent deliveries within a batch (default = BatchSize)
	LeaseFor     time.Duration // lease duration; must exceed a delivery's worst case (default 30s)
	PollInterval time.Duration // wait between polls when the queue is empty (default 1s)

	Logger *slog.Logger
	Now    func() time.Time
}

// Worker drives the delivery loop: lease ready messages, deliver them
// concurrently, and ack / reschedule / dead-letter each by its result.
type Worker struct {
	q           queue.Queue
	deliverer   Deliverer
	sink        DeadLetterSink
	audit       audit.Log
	backoff     Backoff
	maxAttempts int
	batchSize   int
	concurrency int
	leaseFor    time.Duration
	poll        time.Duration
	log         *slog.Logger
	now         func() time.Time
}

// New validates cfg, applies defaults, and returns a Worker.
func New(cfg Config) (*Worker, error) {
	if cfg.Queue == nil {
		return nil, errors.New("delivery: Config.Queue is required")
	}
	if cfg.Deliverer == nil {
		return nil, errors.New("delivery: Config.Deliverer is required")
	}

	w := &Worker{
		q:           cfg.Queue,
		deliverer:   cfg.Deliverer,
		sink:        cfg.Sink,
		audit:       cfg.Audit,
		backoff:     cfg.Backoff,
		maxAttempts: cfg.MaxAttempts,
		batchSize:   cfg.BatchSize,
		concurrency: cfg.Concurrency,
		leaseFor:    cfg.LeaseFor,
		poll:        cfg.PollInterval,
		log:         cfg.Logger,
		now:         cfg.Now,
	}
	if w.sink == nil {
		w.sink = &MemoryDeadLetterSink{}
	}
	if w.audit == nil {
		w.audit = audit.NopLog{}
	}
	if w.backoff.Base <= 0 && w.backoff.Max <= 0 {
		w.backoff = NewBackoff(time.Second, time.Hour)
	}
	if w.maxAttempts <= 0 {
		w.maxAttempts = 12
	}
	if w.batchSize <= 0 {
		w.batchSize = 20
	}
	if w.concurrency <= 0 {
		w.concurrency = w.batchSize
	}
	if w.leaseFor <= 0 {
		w.leaseFor = 30 * time.Second
	}
	if w.poll <= 0 {
		w.poll = time.Second
	}
	if w.log == nil {
		w.log = slog.Default()
	}
	if w.now == nil {
		w.now = time.Now
	}
	return w, nil
}

// Run drives the delivery loop until ctx is cancelled. When a poll finds no
// ready work it waits PollInterval before trying again; otherwise it polls
// straight through so a backlog drains as fast as endpoints allow.
func (w *Worker) Run(ctx context.Context) error {
	for {
		n, err := w.ProcessBatch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// A queue-level error (e.g. database blip) is not fatal; log and
			// wait a poll interval before retrying so we don't busy-spin.
			w.log.Error("hookline: process batch", "err", err)
		}
		if n > 0 && err == nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(w.poll):
		}
	}
}

// ProcessBatch leases one batch, delivers every message in it concurrently, and
// returns how many were processed. It is exported so it can be driven directly
// in tests without real sleeps.
func (w *Worker) ProcessBatch(ctx context.Context) (int, error) {
	leases, err := w.q.Lease(ctx, w.batchSize, w.leaseFor)
	if err != nil {
		return 0, fmt.Errorf("delivery: lease: %w", err)
	}
	if len(leases) == 0 {
		return 0, nil
	}

	sem := make(chan struct{}, w.concurrency)
	var wg sync.WaitGroup
	for _, l := range leases {
		wg.Add(1)
		sem <- struct{}{}
		go func(l queue.Lease) {
			defer wg.Done()
			defer func() { <-sem }()
			w.handle(ctx, l)
		}(l)
	}
	wg.Wait()
	return len(leases), nil
}

// handle delivers one leased message and records the outcome.
func (w *Worker) handle(ctx context.Context, l queue.Lease) {
	res := w.deliverer.Deliver(ctx, l.Message)

	if res.Success {
		w.record(ctx, l, res, audit.OutcomeDelivered)
		if err := w.q.Ack(ctx, l.Message.ID, l.Token); err != nil {
			// ErrStaleLease here means our lease lapsed and someone else
			// re-leased the message; it may be delivered again. That is the
			// at-least-once contract, not a bug — log and move on.
			w.logAckErr("ack after success", l, err)
		}
		return
	}

	if l.Message.Attempts >= w.maxAttempts {
		w.deadLetter(ctx, l, res)
		return
	}

	w.record(ctx, l, res, audit.OutcomeRetrying)
	delay := w.backoff.Delay(l.Message.Attempts)
	if err := w.q.Nack(ctx, l.Message.ID, l.Token, delay); err != nil {
		w.logAckErr("nack after failure", l, err)
		return
	}
	w.log.Debug("hookline: delivery failed, rescheduled",
		"message", l.Message.ID, "attempt", l.Message.Attempts,
		"status", res.StatusCode, "retry_in", delay)
}

// record writes one attempt to the audit log. A failure to record must never
// fail the delivery itself, so it is only logged.
func (w *Worker) record(ctx context.Context, l queue.Lease, res Result, outcome audit.Outcome) {
	errMsg := ""
	if res.Err != nil {
		errMsg = res.Err.Error()
	}
	a := audit.Attempt{
		ID:         id.New(),
		MessageID:  l.Message.ID,
		EventID:    l.Message.Event.ID,
		Endpoint:   l.Message.Event.Endpoint,
		Attempt:    l.Message.Attempts,
		Outcome:    outcome,
		StatusCode: res.StatusCode,
		Duration:   res.Duration,
		Error:      errMsg,
		At:         w.now(),
	}
	if err := w.audit.Record(ctx, a); err != nil {
		w.log.Error("hookline: record attempt", "message", l.Message.ID, "err", err)
	}
}

// deadLetter records an exhausted message, then removes it from the queue.
// Order matters: recording first preserves at-least-once if we crash between
// the two steps.
func (w *Worker) deadLetter(ctx context.Context, l queue.Lease, res Result) {
	reason := fmt.Sprintf("exhausted after %d attempts", l.Message.Attempts)
	if res.Err != nil {
		reason += ": " + res.Err.Error()
	} else {
		reason += fmt.Sprintf(": last status %d", res.StatusCode)
	}

	w.record(ctx, l, res, audit.OutcomeDeadLettered)

	dl := DeadLetter{Message: l.Message, Reason: reason, FailedAt: w.now()}
	if err := w.sink.DeadLetter(ctx, dl); err != nil {
		// Could not record it — leave it in the queue. The lease will lapse and
		// it will be retried (and dead-lettered again) later. Better a
		// duplicate DLQ entry than a silently dropped event.
		w.log.Error("hookline: dead-letter sink failed; leaving message queued",
			"message", l.Message.ID, "err", err)
		return
	}
	if err := w.q.Ack(ctx, l.Message.ID, l.Token); err != nil {
		w.logAckErr("ack after dead-letter", l, err)
		return
	}
	w.log.Warn("hookline: message dead-lettered", "message", l.Message.ID, "reason", reason)
}

func (w *Worker) logAckErr(op string, l queue.Lease, err error) {
	if errors.Is(err, queue.ErrStaleLease) {
		w.log.Debug("hookline: stale lease, another worker owns this message",
			"op", op, "message", l.Message.ID)
		return
	}
	w.log.Error("hookline: "+op, "message", l.Message.ID, "err", err)
}
