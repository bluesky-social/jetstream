package oracle

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"sync"

	"github.com/bluesky-social/jetstream/segment"
)

// EventLogRow is a normalized, JSON-serializable representation of one durable
// event, with payloads reduced to a length and SHA-256 prefix so logs can be
// compared without carrying raw bytes.
type EventLogRow struct {
	Seq              uint64 `json:"seq"`
	Kind             string `json:"kind"`
	DID              string `json:"did,omitempty"`
	Collection       string `json:"collection,omitempty"`
	Rkey             string `json:"rkey,omitempty"`
	Rev              string `json:"rev,omitempty"`
	PayloadLen       int    `json:"payload_len,omitempty"`
	PayloadSHA256_64 string `json:"payload_sha256_64,omitempty"`
	AccountDeleted   bool   `json:"account_deleted,omitempty"`
}

type eventLogRecorder struct {
	mu     sync.Mutex
	events []segment.Event
}

func newEventLogRecorder() *eventLogRecorder {
	return &eventLogRecorder{}
}

func (r *eventLogRecorder) Observe(ev *segment.Event) {
	if r == nil || ev == nil {
		return
	}
	r.mu.Lock()
	r.events = append(r.events, cloneSegmentEvent(*ev))
	r.mu.Unlock()
}

func (r *eventLogRecorder) RowsByUpstreamCursor(after, through int64) []EventLogRow {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	events := append([]segment.Event(nil), r.events...)
	r.mu.Unlock()

	var observed []ObservedEvent
	for _, ev := range events {
		if ev.UpstreamRelayCursor <= after || ev.UpstreamRelayCursor > through {
			continue
		}
		ev.Seq = uint64(ev.UpstreamRelayCursor)
		observed = append(observed, observedEventFromSegment(ev))
	}
	return NormalizeEventLog(observed)
}

func cloneSegmentEvent(ev segment.Event) segment.Event {
	ev.Payload = append([]byte(nil), ev.Payload...)
	return ev
}

// NormalizeEventLog converts observed events into EventLogRows, hashing
// payloads and decoding the account-deleted flag for account events.
func NormalizeEventLog(events []ObservedEvent) []EventLogRow {
	out := make([]EventLogRow, 0, len(events))
	for _, ev := range events {
		row := EventLogRow{
			Seq:        ev.Seq,
			Kind:       eventLogKind(ev.Kind),
			DID:        ev.DID,
			Collection: ev.Collection,
			Rkey:       ev.Rkey,
			Rev:        ev.Rev,
		}
		if ev.Payload != nil {
			sum := sha256.Sum256(ev.Payload)
			row.PayloadLen = len(ev.Payload)
			row.PayloadSHA256_64 = hex.EncodeToString(sum[:8])
		}
		if ev.Kind == segment.KindAccount {
			row.AccountDeleted, _ = oracleAccountDeleted(ev.Payload)
		}
		out = append(out, row)
	}
	return out
}

// CompareEventLogs reports the first positional mismatch between two event
// logs, distinguishing missing, extra, reordered, and field-level differences.
func CompareEventLogs(want, got []EventLogRow) error {
	for i := 0; i < len(want) && i < len(got); i++ {
		if want[i] == got[i] {
			continue
		}
		wantContainsGot := eventLogContains(want[i+1:], got[i])
		gotContainsWant := eventLogContains(got[i+1:], want[i])
		if wantContainsGot && gotContainsWant {
			return fmt.Errorf("oracle: event order mismatch at index %d: want %s got %s",
				i, want[i].describe(), got[i].describe())
		}
		if wantContainsGot {
			return fmt.Errorf("oracle: missing expected event at index %d: %s", i, want[i].describe())
		}
		if gotContainsWant {
			return fmt.Errorf("oracle: extra observed event at index %d: %s", i, got[i].describe())
		}
		return fmt.Errorf("oracle: event mismatch at index %d: want %s got %s%s",
			i, want[i].describe(), got[i].describe(), eventLogMismatchFields(want[i], got[i]))
	}
	if len(want) > len(got) {
		return fmt.Errorf("oracle: missing expected event at index %d: %s", len(got), want[len(got)].describe())
	}
	if len(got) > len(want) {
		return fmt.Errorf("oracle: extra observed event at index %d: %s", len(want), got[len(want)].describe())
	}
	return nil
}

// CompareEventLogMultiset compares two event logs ignoring order by sorting
// both sides into a canonical order before a positional comparison.
func CompareEventLogMultiset(want, got []EventLogRow) error {
	wantSorted := eventLogRowsSorted(want)
	gotSorted := eventLogRowsSorted(got)
	return CompareEventLogs(wantSorted, gotSorted)
}

// CompareEventLogCoverage asserts at-least-once coverage: every row in want
// appears at least once in got, ignoring order. Extra rows in got (including
// duplicates) are tolerated — this honors jetstream's at-least-once delivery
// contract (docs/README.md:156), under which the durable stream may legitimately
// re-deliver an event (e.g. a merge re-run across a crash boundary re-stamps and
// re-appends rows). It is sensitive to LOSS (a missing expected row) and blind to
// duplication; spurious-duplication classes are covered elsewhere (final-state
// Compare, CheckInvariants' unique-seq guard). Coverage matches on the full row
// key (kind+did+collection+rkey+rev+payload-hash, but NOT seq), so it is
// seq-space-agnostic: callers should zero the Seq field on both sides (the on-disk
// jetstream-seq is not comparable to a model-derived expectation). Returns an error
// naming the first uncovered expected row.
func CompareEventLogCoverage(want, got []EventLogRow) error {
	present := make(map[EventLogRow]struct{}, len(got))
	for _, row := range got {
		present[row] = struct{}{}
	}
	for _, row := range want {
		if _, ok := present[row]; ok {
			continue
		}
		return fmt.Errorf("oracle: event-log coverage gap: expected durable row not found on disk: %s", row.describe())
	}
	return nil
}

// CompareEventLogsCompacted compares against got after dropping the expected
// rows that compaction would have removed at or below watermark, so the
// expected log matches a compacted segment stream.
func CompareEventLogsCompacted(want, got []EventLogRow, watermark uint64) error {
	return CompareEventLogs(filterCompactedExpectedRows(want, watermark), got)
}

// CompareEventLogsCompactedMultiset is CompareEventLogsCompacted ignoring order:
// it drops the expected rows compaction would have removed at or below
// watermark, then compares want and got as multisets. Use this when want and
// got are two physical scans of the same stream (e.g. pre- and post-compaction
// disk snapshots) whose block/segment ordering need not match.
func CompareEventLogsCompactedMultiset(want, got []EventLogRow, watermark uint64) error {
	return CompareEventLogMultiset(filterCompactedExpectedRows(want, watermark), got)
}

func filterCompactedExpectedRows(rows []EventLogRow, watermark uint64) []EventLogRow {
	recordTombstones := make(map[RecordKey]uint64)
	didTombstones := make(map[string]uint64)

	for _, row := range rows {
		if row.Seq > watermark {
			continue
		}
		switch row.Kind {
		case "delete", "update":
			key := RecordKey{DID: row.DID, Collection: row.Collection, Rkey: row.Rkey}
			if row.Seq > recordTombstones[key] {
				recordTombstones[key] = row.Seq
			}
		case "sync":
			if row.Seq > didTombstones[row.DID] {
				didTombstones[row.DID] = row.Seq
			}
		case "account":
			if !row.AccountDeleted {
				continue
			}
			if row.Seq > didTombstones[row.DID] {
				didTombstones[row.DID] = row.Seq
			}
		}
	}

	out := make([]EventLogRow, 0, len(rows))
	for _, row := range rows {
		if row.Seq <= watermark && (row.Kind == "create" || row.Kind == "update" || row.Kind == "create_resync") {
			key := RecordKey{DID: row.DID, Collection: row.Collection, Rkey: row.Rkey}
			if recordTombstones[key] > row.Seq || didTombstones[row.DID] > row.Seq {
				continue
			}
		}
		out = append(out, row)
	}
	return out
}

func eventLogContains(rows []EventLogRow, target EventLogRow) bool {
	return slices.Contains(rows, target)
}

func eventLogRowsSorted(rows []EventLogRow) []EventLogRow {
	out := append([]EventLogRow(nil), rows...)
	slices.SortFunc(out, compareEventLogRows)
	return out
}

func compareEventLogRows(a, b EventLogRow) int {
	if a.Seq != b.Seq {
		return compareOrdered(a.Seq, b.Seq)
	}
	if a.Kind != b.Kind {
		return compareOrdered(a.Kind, b.Kind)
	}
	if a.DID != b.DID {
		return compareOrdered(a.DID, b.DID)
	}
	if a.Collection != b.Collection {
		return compareOrdered(a.Collection, b.Collection)
	}
	if a.Rkey != b.Rkey {
		return compareOrdered(a.Rkey, b.Rkey)
	}
	if a.Rev != b.Rev {
		return compareOrdered(a.Rev, b.Rev)
	}
	if a.PayloadLen != b.PayloadLen {
		return compareOrdered(a.PayloadLen, b.PayloadLen)
	}
	if a.PayloadSHA256_64 != b.PayloadSHA256_64 {
		return compareOrdered(a.PayloadSHA256_64, b.PayloadSHA256_64)
	}
	return compareBool(a.AccountDeleted, b.AccountDeleted)
}

func compareOrdered[T ~int | ~uint64 | ~string](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func compareBool(a, b bool) int {
	switch {
	case !a && b:
		return -1
	case a && !b:
		return 1
	default:
		return 0
	}
}

func eventLogMismatchFields(want, got EventLogRow) string {
	var out string
	if want.Seq != got.Seq {
		out += " seq"
	}
	if want.Kind != got.Kind {
		out += " kind"
	}
	if want.DID != got.DID {
		out += " did"
	}
	if want.Collection != got.Collection || want.Rkey != got.Rkey {
		out += " key"
	}
	if want.Rev != got.Rev {
		out += " rev"
	}
	if want.PayloadLen != got.PayloadLen || want.PayloadSHA256_64 != got.PayloadSHA256_64 {
		out += " payload"
	}
	if want.AccountDeleted != got.AccountDeleted {
		out += " account_deleted"
	}
	if out == "" {
		return ""
	}
	return " fields:" + out
}

func (r EventLogRow) describe() string {
	return fmt.Sprintf("seq=%d kind=%s did=%s key=%s/%s rev=%s payload_len=%d payload_sha256_64=%s account_deleted=%t",
		r.Seq, r.Kind, r.DID, r.Collection, r.Rkey, r.Rev, r.PayloadLen, r.PayloadSHA256_64, r.AccountDeleted)
}

func eventLogKind(kind segment.Kind) string {
	switch kind {
	case segment.KindCreate:
		return "create"
	case segment.KindCreateResync:
		return "create_resync"
	case segment.KindUpdate:
		return "update"
	case segment.KindDelete:
		return "delete"
	case segment.KindIdentity:
		return "identity"
	case segment.KindAccount:
		return "account"
	case segment.KindSync:
		return "sync"
	default:
		return fmt.Sprintf("unknown-%d", kind)
	}
}
