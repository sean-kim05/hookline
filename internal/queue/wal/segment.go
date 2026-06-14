package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/sean-kim05/hookline/internal/event"
)

// opType identifies what a WAL record does to the queue state.
type opType uint8

const (
	opEnqueue  opType = 1 // a new message entered the queue
	opLease    opType = 2 // a message was leased (token bumped, attempts++)
	opAck      opType = 3 // a message was acked and removed
	opNack     opType = 4 // a message was rescheduled
	opSnapshot opType = 5 // full current state of a live message (written by compaction)
)

// record is one entry in the write-ahead log. Every state change appends one
// record; the queue's in-memory index is rebuilt by replaying them in order.
// Only the fields an op needs are populated, so JSON omitempty keeps records
// compact.
type record struct {
	Op         opType       `json:"op"`
	MsgID      string       `json:"id"`
	Event      *event.Event `json:"ev,omitempty"`
	EnqueuedAt int64        `json:"enq,omitempty"`   // unix nanos
	ReadyAt    int64        `json:"ready,omitempty"` // unix nanos
	Token      uint64       `json:"tok,omitempty"`
	Leased     bool         `json:"leased,omitempty"`
	LeaseExp   int64        `json:"exp,omitempty"` // unix nanos
	Attempts   int          `json:"att,omitempty"`
}

// Frame format on disk, repeated per record:
//
//	[ uint32 payload length ][ uint32 CRC32(payload) ][ payload bytes ]
//
// The length lets recovery find record boundaries; the CRC detects a torn
// final write from a crash (a partially-flushed frame) so it can be discarded
// rather than misparsed.
const (
	frameHeaderSize = 8
	// maxRecordSize bounds the allocation recovery will make for one record, so
	// a garbage length field from a torn write can't trigger a huge alloc.
	maxRecordSize = 64 << 20
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// encodeRecord marshals r and frames it with a length and CRC.
func encodeRecord(r record) ([]byte, error) {
	payload, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("wal: marshal record: %w", err)
	}
	buf := make([]byte, frameHeaderSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], crc32.Checksum(payload, crcTable))
	copy(buf[frameHeaderSize:], payload)
	return buf, nil
}

// scanSegment replays every intact frame from r, calling apply for each. It
// returns the number of bytes consumed by intact frames and whether the segment
// ended in a torn (partial or corrupt) frame — the signature of a crash
// mid-append. A torn tail is expected and recoverable; the caller truncates the
// file to goodBytes. apply errors abort the scan.
func scanSegment(r io.Reader, apply func(record) error) (goodBytes int64, torn bool, err error) {
	br := bufio.NewReader(r)
	var hdr [frameHeaderSize]byte

	for {
		n, herr := io.ReadFull(br, hdr[:])
		if herr == io.EOF {
			return goodBytes, false, nil // clean end of segment
		}
		if herr == io.ErrUnexpectedEOF || (herr == nil && n < frameHeaderSize) {
			return goodBytes, true, nil // partial header: torn tail
		}
		if herr != nil {
			return goodBytes, false, fmt.Errorf("wal: read frame header: %w", herr)
		}

		length := binary.BigEndian.Uint32(hdr[0:4])
		wantCRC := binary.BigEndian.Uint32(hdr[4:8])
		if length == 0 || length > maxRecordSize {
			return goodBytes, true, nil // implausible length: torn/garbage tail
		}

		payload := make([]byte, length)
		if _, perr := io.ReadFull(br, payload); perr != nil {
			return goodBytes, true, nil // payload short: torn tail
		}
		if crc32.Checksum(payload, crcTable) != wantCRC {
			return goodBytes, true, nil // CRC mismatch: torn/corrupt tail
		}

		var rec record
		if jerr := json.Unmarshal(payload, &rec); jerr != nil {
			return goodBytes, true, nil // undecodable: treat as torn tail
		}
		if aerr := apply(rec); aerr != nil {
			return goodBytes, false, aerr
		}
		goodBytes += int64(frameHeaderSize + len(payload))
	}
}
