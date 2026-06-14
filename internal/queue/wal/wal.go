// Package wal implements queue.Queue on a from-scratch, write-ahead segmented
// log — the headline component of Hookline.
//
// Every state change (enqueue, lease, ack, nack) is appended as a CRC-framed
// record to an append-only log before the in-memory index is updated, so the
// queue survives a crash: on open it replays the log to rebuild exactly the
// state it had, discarding any torn final write from an interrupted append.
//
// The log is split into fixed-size segments. Compaction rewrites the live set
// into a fresh segment as snapshot records and drops the obsolete ones, so the
// log does not grow without bound as messages are acked. The whole engine sits
// behind the same queue.Queue interface as the in-memory and Postgres backends
// and passes the same conformance suite — which is the point: a from-scratch
// durable queue, proven correct against the identical contract.
package wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sean-kim05/hookline/internal/event"
	"github.com/sean-kim05/hookline/internal/id"
	"github.com/sean-kim05/hookline/internal/queue"
)

// walMessage is the in-memory bookkeeping record for a queued message, mirroring
// the in-memory backend. The WAL is the durable source; this is the index.
type walMessage struct {
	msg        queue.Message
	readyAt    time.Time
	leaseToken uint64
	leased     bool
	leaseExp   time.Time
}

// Queue is a durable, WAL-backed queue.Queue.
type Queue struct {
	mu   sync.Mutex
	msgs map[string]*walMessage
	now  func() time.Time

	dir          string
	sync         bool
	maxSegment   int64
	compactEvery int

	active       *os.File
	activeSeq    int
	segSize      int64
	segSeqs      []int
	totalRecords int
}

var _ queue.Queue = (*Queue)(nil)

// Option configures a Queue.
type Option func(*Queue)

// WithClock overrides the clock (used by the conformance suite).
func WithClock(now func() time.Time) Option {
	return func(q *Queue) { q.now = now }
}

// WithSync controls whether each append is fsync'd. Default true (durable).
// Tests disable it for speed; production should leave it on.
func WithSync(on bool) Option {
	return func(q *Queue) { q.sync = on }
}

// WithMaxSegmentBytes sets the segment roll threshold (default 8 MiB).
func WithMaxSegmentBytes(n int64) Option {
	return func(q *Queue) {
		if n > 0 {
			q.maxSegment = n
		}
	}
}

// WithCompactEvery sets how many appended records trigger a compaction check
// (default 10,000). A check compacts only when tombstones dominate.
func WithCompactEvery(n int) Option {
	return func(q *Queue) {
		if n > 0 {
			q.compactEvery = n
		}
	}
}

// Open opens (or creates) a WAL queue rooted at dir, replaying any existing log
// to recover state.
func Open(dir string, opts ...Option) (*Queue, error) {
	q := &Queue{
		msgs:         make(map[string]*walMessage),
		now:          time.Now,
		dir:          dir,
		sync:         true,
		maxSegment:   8 << 20,
		compactEvery: 10_000,
	}
	for _, opt := range opts {
		opt(q)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: create dir: %w", err)
	}
	if err := q.recover(); err != nil {
		return nil, err
	}
	return q, nil
}

// recover replays all segments in order and opens the newest for appending.
func (q *Queue) recover() error {
	matches, err := filepath.Glob(filepath.Join(q.dir, "wal-*.log"))
	if err != nil {
		return fmt.Errorf("wal: list segments: %w", err)
	}
	seqs := make([]int, 0, len(matches))
	for _, m := range matches {
		if seq, ok := parseSeq(filepath.Base(m)); ok {
			seqs = append(seqs, seq)
		}
	}
	sort.Ints(seqs)

	if len(seqs) == 0 {
		// Fresh queue: start the first segment.
		return q.startSegment(1)
	}

	q.segSeqs = seqs
	for i, seq := range seqs {
		isLast := i == len(seqs)-1
		good, torn, serr := q.replaySegment(seq)
		if serr != nil {
			return serr
		}
		if torn {
			if !isLast {
				// A torn frame anywhere but the final segment means real
				// corruption, not an interrupted append.
				return fmt.Errorf("wal: corrupt frame in non-final segment %s", segName(seq))
			}
			// Truncate the interrupted final append so future writes are clean.
			if terr := os.Truncate(filepath.Join(q.dir, segName(seq)), good); terr != nil {
				return fmt.Errorf("wal: truncate torn tail: %w", terr)
			}
		}
	}

	// Append to the newest segment.
	last := seqs[len(seqs)-1]
	f, err := os.OpenFile(filepath.Join(q.dir, segName(last)), os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: open active segment: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("wal: stat active segment: %w", err)
	}
	q.active = f
	q.activeSeq = last
	q.segSize = info.Size()
	return nil
}

// replaySegment applies every intact record in one segment.
func (q *Queue) replaySegment(seq int) (good int64, torn bool, err error) {
	f, err := os.Open(filepath.Join(q.dir, segName(seq)))
	if err != nil {
		return 0, false, fmt.Errorf("wal: open segment %s: %w", segName(seq), err)
	}
	defer f.Close()
	return scanSegment(f, func(rec record) error {
		q.apply(rec)
		q.totalRecords++
		return nil
	})
}

// startSegment creates a fresh, empty segment and makes it active.
func (q *Queue) startSegment(seq int) error {
	f, err := os.OpenFile(filepath.Join(q.dir, segName(seq)), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: create segment %s: %w", segName(seq), err)
	}
	q.active = f
	q.activeSeq = seq
	q.segSize = 0
	q.segSeqs = append(q.segSeqs, seq)
	return nil
}

// apply mutates the in-memory index for one record. It is the single definition
// of how a record changes state, used by both recovery and live writes.
func (q *Queue) apply(rec record) {
	switch rec.Op {
	case opEnqueue:
		q.msgs[rec.MsgID] = &walMessage{
			msg:     queue.Message{ID: rec.MsgID, Event: *rec.Event, EnqueuedAt: fromNanos(rec.EnqueuedAt)},
			readyAt: fromNanos(rec.ReadyAt),
		}
	case opLease:
		if m := q.msgs[rec.MsgID]; m != nil {
			m.leaseToken = rec.Token
			m.leased = true
			m.leaseExp = fromNanos(rec.LeaseExp)
			m.msg.Attempts = rec.Attempts
		}
	case opAck:
		delete(q.msgs, rec.MsgID)
	case opNack:
		if m := q.msgs[rec.MsgID]; m != nil {
			m.leased = false
			m.readyAt = fromNanos(rec.ReadyAt)
		}
	case opSnapshot:
		q.msgs[rec.MsgID] = &walMessage{
			msg:        queue.Message{ID: rec.MsgID, Event: *rec.Event, EnqueuedAt: fromNanos(rec.EnqueuedAt), Attempts: rec.Attempts},
			readyAt:    fromNanos(rec.ReadyAt),
			leaseToken: rec.Token,
			leased:     rec.Leased,
			leaseExp:   fromNanos(rec.LeaseExp),
		}
	}
}

// persist frames and writes recs to the active segment, rolling to a new
// segment when the current one is full, then fsyncs if configured.
func (q *Queue) persist(recs []record) error {
	for _, rec := range recs {
		frame, err := encodeRecord(rec)
		if err != nil {
			return err
		}
		if q.segSize > 0 && q.segSize+int64(len(frame)) > q.maxSegment {
			if err := q.roll(); err != nil {
				return err
			}
		}
		if _, err := q.active.Write(frame); err != nil {
			return fmt.Errorf("wal: append: %w", err)
		}
		q.segSize += int64(len(frame))
		q.totalRecords++
	}
	if q.sync {
		if err := q.active.Sync(); err != nil {
			return fmt.Errorf("wal: fsync: %w", err)
		}
	}
	return nil
}

// roll closes the active segment and starts the next one.
func (q *Queue) roll() error {
	if err := q.active.Close(); err != nil {
		return fmt.Errorf("wal: close segment: %w", err)
	}
	return q.startSegment(q.activeSeq + 1)
}

// write persists recs, applies them, and runs a compaction check. The caller
// holds the lock.
func (q *Queue) write(recs ...record) error {
	if err := q.persist(recs); err != nil {
		return err
	}
	for _, rec := range recs {
		q.apply(rec)
	}
	return q.maybeCompact()
}

// Enqueue appends an event and returns its message ID.
func (q *Queue) Enqueue(_ context.Context, ev event.Event) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.now()
	if ev.ID == "" {
		ev.ID = id.New()
	}
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = now
	}
	mid := id.New()
	rec := record{
		Op:         opEnqueue,
		MsgID:      mid,
		Event:      &ev,
		EnqueuedAt: toNanos(now),
		ReadyAt:    toNanos(now),
	}
	if err := q.write(rec); err != nil {
		return "", err
	}
	return mid, nil
}

// Lease claims up to n ready messages, journaling each lease before returning.
func (q *Queue) Lease(_ context.Context, n int, leaseFor time.Duration) ([]queue.Lease, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.now()

	var ready []*walMessage
	for _, m := range q.msgs {
		if m.leased && now.Before(m.leaseExp) {
			continue
		}
		if !m.readyAt.After(now) {
			ready = append(ready, m)
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		if ready[i].readyAt.Equal(ready[j].readyAt) {
			return ready[i].msg.ID < ready[j].msg.ID
		}
		return ready[i].readyAt.Before(ready[j].readyAt)
	})
	if n >= 0 && n < len(ready) {
		ready = ready[:n]
	}
	if len(ready) == 0 {
		return nil, nil
	}

	exp := now.Add(leaseFor)
	recs := make([]record, len(ready))
	leases := make([]queue.Lease, len(ready))
	for i, m := range ready {
		// Compute the post-lease state, journal it, then apply via write below.
		token := m.leaseToken + 1
		attempts := m.msg.Attempts + 1
		recs[i] = record{Op: opLease, MsgID: m.msg.ID, Token: token, LeaseExp: toNanos(exp), Attempts: attempts}
		// The returned lease reflects the new state.
		leased := m.msg
		leased.Attempts = attempts
		leases[i] = queue.Lease{Message: leased, Token: token, Expiry: exp}
	}
	if err := q.write(recs...); err != nil {
		return nil, err
	}
	return leases, nil
}

// Ack removes the message if the token is current.
func (q *Queue) Ack(_ context.Context, mid string, token uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	m, ok := q.msgs[mid]
	if !ok {
		return queue.ErrNotFound
	}
	if m.leaseToken != token {
		return queue.ErrStaleLease
	}
	return q.write(record{Op: opAck, MsgID: mid})
}

// Nack reschedules the message if the token is current.
func (q *Queue) Nack(_ context.Context, mid string, token uint64, retryAfter time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	m, ok := q.msgs[mid]
	if !ok {
		return queue.ErrNotFound
	}
	if m.leaseToken != token {
		return queue.ErrStaleLease
	}
	readyAt := q.now().Add(retryAfter)
	return q.write(record{Op: opNack, MsgID: mid, ReadyAt: toNanos(readyAt)})
}

// Depth reports how many messages are in the queue.
func (q *Queue) Depth(_ context.Context) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.msgs), nil
}

// Close flushes and closes the active segment.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.active == nil {
		return nil
	}
	if q.sync {
		_ = q.active.Sync()
	}
	err := q.active.Close()
	q.active = nil
	return err
}

// maybeCompact rewrites the log when tombstones dominate it. The caller holds
// the lock.
func (q *Queue) maybeCompact() error {
	if q.totalRecords < q.compactEvery {
		return nil
	}
	// Compact only when fewer than half the records describe live messages —
	// i.e. acks/nacks have accumulated enough to be worth collapsing.
	if len(q.msgs)*2 >= q.totalRecords {
		// Reset the counter window so we don't re-check on every write.
		q.totalRecords = len(q.msgs)
		return nil
	}
	return q.compact()
}

// compact writes the live set into a new segment as snapshot records, then
// removes the now-obsolete older segments. It is crash-safe: the new segment
// has a higher sequence number than every old one, so even if the process dies
// before the old segments are deleted, recovery applies the snapshot last and
// reaches the same state.
func (q *Queue) compact() error {
	newSeq := q.activeSeq + 1
	path := filepath.Join(q.dir, segName(newSeq))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: compact create: %w", err)
	}

	var size int64
	for _, m := range q.msgs {
		ev := m.msg.Event
		rec := record{
			Op:         opSnapshot,
			MsgID:      m.msg.ID,
			Event:      &ev,
			EnqueuedAt: toNanos(m.msg.EnqueuedAt),
			ReadyAt:    toNanos(m.readyAt),
			Token:      m.leaseToken,
			Leased:     m.leased,
			LeaseExp:   toNanos(m.leaseExp),
			Attempts:   m.msg.Attempts,
		}
		frame, encErr := encodeRecord(rec)
		if encErr != nil {
			f.Close()
			return encErr
		}
		if _, werr := f.Write(frame); werr != nil {
			f.Close()
			return fmt.Errorf("wal: compact write: %w", werr)
		}
		size += int64(len(frame))
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("wal: compact fsync: %w", err)
	}

	// Switch over: the snapshot segment becomes active before old segments are
	// removed, so a crash mid-delete still recovers correctly.
	old := q.segSeqs
	if q.active != nil {
		_ = q.active.Close()
	}
	q.active = f
	q.activeSeq = newSeq
	q.segSize = size
	q.segSeqs = []int{newSeq}
	q.totalRecords = len(q.msgs)

	for _, seq := range old {
		if err := os.Remove(filepath.Join(q.dir, segName(seq))); err != nil && !os.IsNotExist(err) {
			// A leftover old segment is harmless (snapshot wins on replay); log
			// nothing here to keep the queue dependency-free, just continue.
			continue
		}
	}
	return nil
}

// --- helpers ---

func segName(seq int) string { return fmt.Sprintf("wal-%010d.log", seq) }

func parseSeq(base string) (int, bool) {
	s := strings.TrimSuffix(strings.TrimPrefix(base, "wal-"), ".log")
	if s == base {
		return 0, false
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}
	return n, true
}

func toNanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func fromNanos(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}
