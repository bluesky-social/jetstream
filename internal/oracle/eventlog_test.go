package oracle

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestCompareEventLogsRejectsMissingIntermediateUpdateDespiteConvergedFinalState(t *testing.T) {
	t.Parallel()

	wantEvents := []ObservedEvent{
		{
			Seq:        1,
			Kind:       segment.KindCreate,
			DID:        "did:plc:a",
			Collection: "app.bsky.feed.post",
			Rkey:       "one",
			Rev:        "rev1",
			Payload:    []byte("old"),
		},
		{
			Seq:        2,
			Kind:       segment.KindUpdate,
			DID:        "did:plc:a",
			Collection: "app.bsky.feed.post",
			Rkey:       "one",
			Rev:        "rev2",
			Payload:    []byte("new"),
		},
	}
	gotEvents := []ObservedEvent{wantEvents[1]}

	wantModel, err := Reconstruct(wantEvents)
	require.NoError(t, err)
	gotModel, err := Reconstruct(gotEvents)
	require.NoError(t, err)
	require.NoError(t, Compare(wantModel, gotModel), "final-state comparison should converge")

	err = CompareEventLogs(NormalizeEventLog(wantEvents), NormalizeEventLog(gotEvents))
	require.ErrorContains(t, err, "missing expected event")
	require.ErrorContains(t, err, "seq=1")
	require.ErrorContains(t, err, "kind=create")
	require.ErrorContains(t, err, "did=did:plc:a")
	require.ErrorContains(t, err, "app.bsky.feed.post/one")
}

func TestNormalizeEventLogUsesBoundedPayloadFingerprint(t *testing.T) {
	t.Parallel()

	rows := NormalizeEventLog([]ObservedEvent{{
		Seq:        7,
		Kind:       segment.KindCreate,
		DID:        "did:plc:a",
		Collection: "app.bsky.feed.post",
		Rkey:       "one",
		Rev:        "rev1",
		Payload:    []byte("full payload bytes must not appear"),
	}})

	require.Len(t, rows, 1)
	require.Equal(t, uint64(7), rows[0].Seq)
	require.Equal(t, "create", rows[0].Kind)
	require.Equal(t, "did:plc:a", rows[0].DID)
	require.Equal(t, "app.bsky.feed.post", rows[0].Collection)
	require.Equal(t, "one", rows[0].Rkey)
	require.Equal(t, "rev1", rows[0].Rev)
	require.Equal(t, 34, rows[0].PayloadLen)
	require.Equal(t, "b767a06ad616b7ff", rows[0].PayloadSHA256_64)

	encoded, err := json.Marshal(rows[0])
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "full payload bytes")
	require.Contains(t, string(encoded), "payload_sha256_64")
}

func TestCompareEventLogsRejectsPayloadMismatch(t *testing.T) {
	t.Parallel()

	want := NormalizeEventLog([]ObservedEvent{{
		Seq:        1,
		Kind:       segment.KindCreate,
		DID:        "did:plc:a",
		Collection: "c",
		Rkey:       "r",
		Rev:        "rev1",
		Payload:    []byte("want"),
	}})
	got := NormalizeEventLog([]ObservedEvent{{
		Seq:        1,
		Kind:       segment.KindCreate,
		DID:        "did:plc:a",
		Collection: "c",
		Rkey:       "r",
		Rev:        "rev1",
		Payload:    []byte("got"),
	}})

	err := CompareEventLogs(want, got)
	require.ErrorContains(t, err, "event mismatch at index 0")
	require.ErrorContains(t, err, "payload")
	require.ErrorContains(t, err, "seq=1")
}

func TestCompareEventLogsRejectsExtraEvent(t *testing.T) {
	t.Parallel()

	got := NormalizeEventLog([]ObservedEvent{{
		Seq:  9,
		Kind: segment.KindAccount,
		DID:  "did:plc:a",
	}})

	err := CompareEventLogs(nil, got)
	require.ErrorContains(t, err, "extra observed event")
	require.ErrorContains(t, err, "seq=9")
	require.ErrorContains(t, err, "kind=account")
}

func TestCompareEventLogsRejectsOrderMismatch(t *testing.T) {
	t.Parallel()

	first := ObservedEvent{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "rev1"}
	second := ObservedEvent{Seq: 2, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "rev2"}

	err := CompareEventLogs(
		NormalizeEventLog([]ObservedEvent{first, second}),
		NormalizeEventLog([]ObservedEvent{second, first}),
	)
	require.ErrorContains(t, err, "event order mismatch at index 0")
	require.ErrorContains(t, err, "want seq=1")
	require.ErrorContains(t, err, "got seq=2")
}

func TestCompareEventLogMultisetAllowsInterDIDReordering(t *testing.T) {
	t.Parallel()

	first := ObservedEvent{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r1", Rev: "rev1"}
	second := ObservedEvent{Seq: 2, Kind: segment.KindCreate, DID: "did:plc:b", Collection: "c", Rkey: "r2", Rev: "rev1"}

	require.NoError(t, CompareEventLogMultiset(
		NormalizeEventLog([]ObservedEvent{first, second}),
		NormalizeEventLog([]ObservedEvent{second, first}),
	))
}

func TestCompareEventLogMultisetRejectsMissingDuplicate(t *testing.T) {
	t.Parallel()

	row := ObservedEvent{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "rev1"}

	err := CompareEventLogMultiset(
		NormalizeEventLog([]ObservedEvent{row, row}),
		NormalizeEventLog([]ObservedEvent{row}),
	)
	require.ErrorContains(t, err, "missing expected event")
	require.ErrorContains(t, err, "seq=1")
}

func TestCompareEventLogsCompactedAllowsSupersededCreateBelowWatermark(t *testing.T) {
	t.Parallel()

	old := ObservedEvent{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "rev1", Payload: []byte("old")}
	update := ObservedEvent{Seq: 2, Kind: segment.KindUpdate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "rev2", Payload: []byte("new")}

	require.Error(t, CompareEventLogs(NormalizeEventLog([]ObservedEvent{old, update}), NormalizeEventLog([]ObservedEvent{update})),
		"strict comparison must still reject missing rows")
	require.NoError(t, CompareEventLogsCompacted(
		NormalizeEventLog([]ObservedEvent{old, update}),
		NormalizeEventLog([]ObservedEvent{update}),
		2,
	))
}

func TestCompareEventLogsCompactedAllowsSupersededCreateResyncBelowWatermark(t *testing.T) {
	t.Parallel()

	// Production compaction drops superseded create_resync rows the same as
	// plain creates (tombstone.ShouldDrop -> Kind.IsMaterialization), so the
	// expected-side filter must drop them too or the oracle reports a spurious
	// "missing expected event" once a resync replacement row is compacted away.
	resync := ObservedEvent{Seq: 1, Kind: segment.KindCreateResync, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "rev1", Payload: []byte("old")}
	deleted := ObservedEvent{Seq: 2, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "rev2"}

	require.NoError(t, CompareEventLogsCompacted(
		NormalizeEventLog([]ObservedEvent{resync, deleted}),
		NormalizeEventLog([]ObservedEvent{deleted}),
		2,
	))
}

func TestCompareEventLogsCompactedAllowsRowsSupersededByDIDTombstoneBelowWatermark(t *testing.T) {
	t.Parallel()

	old := ObservedEvent{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "rev1", Payload: []byte("old")}
	sync := ObservedEvent{Seq: 2, Kind: segment.KindSync, DID: "did:plc:a", Rev: "rev2"}
	other := ObservedEvent{Seq: 3, Kind: segment.KindCreate, DID: "did:plc:b", Collection: "c", Rkey: "r", Rev: "rev1", Payload: []byte("other")}
	account := ObservedEvent{Seq: 4, Kind: segment.KindAccount, DID: "did:plc:b", Payload: oracleAccountPayload(t, false, "deleted")}

	require.NoError(t, CompareEventLogsCompacted(
		NormalizeEventLog([]ObservedEvent{old, sync, other, account}),
		NormalizeEventLog([]ObservedEvent{sync, account}),
		4,
	))
}

func TestCompareEventLogsCompactedRejectsMissingRowAboveWatermark(t *testing.T) {
	t.Parallel()

	old := ObservedEvent{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "rev1", Payload: []byte("old")}
	update := ObservedEvent{Seq: 2, Kind: segment.KindUpdate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "rev2", Payload: []byte("new")}

	err := CompareEventLogsCompacted(
		NormalizeEventLog([]ObservedEvent{old, update}),
		NormalizeEventLog([]ObservedEvent{update}),
		1,
	)
	require.ErrorContains(t, err, "missing expected event")
	require.ErrorContains(t, err, "seq=1")
}

func TestCompareEventLogsCompactedRejectsMissingTombstone(t *testing.T) {
	t.Parallel()

	old := ObservedEvent{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "rev1", Payload: []byte("old")}
	deleted := ObservedEvent{Seq: 2, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "rev2"}

	err := CompareEventLogsCompacted(
		NormalizeEventLog([]ObservedEvent{old, deleted}),
		NormalizeEventLog([]ObservedEvent{}),
		2,
	)
	require.ErrorContains(t, err, "missing expected event")
	require.ErrorContains(t, err, "seq=2")
	require.ErrorContains(t, err, "kind=delete")
}

func TestCompareEventLogsCompactedRejectsMissingRowBeforeNonDeletedAccountEvent(t *testing.T) {
	t.Parallel()

	old := ObservedEvent{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "rev1", Payload: []byte("old")}
	account := ObservedEvent{Seq: 2, Kind: segment.KindAccount, DID: "did:plc:a", Payload: oracleAccountPayload(t, false, "suspended")}

	err := CompareEventLogsCompacted(
		NormalizeEventLog([]ObservedEvent{old, account}),
		NormalizeEventLog([]ObservedEvent{account}),
		2,
	)
	require.ErrorContains(t, err, "missing expected event")
	require.ErrorContains(t, err, "seq=1")
}

func TestCompareEventLogsCompactedMultisetToleratesReorderingButCatchesOverDrop(t *testing.T) {
	t.Parallel()

	// Two records that both survive at the watermark (no superseding
	// tombstone), plus one superseded create that compaction legitimately
	// drops. pre is the pre-compaction stream; post is what a correct pass
	// leaves behind, in a different block order.
	createA := ObservedEvent{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "ra", Rev: "rev1", Payload: []byte("a")}
	superseded := ObservedEvent{Seq: 2, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "rb", Rev: "rev1", Payload: []byte("b-old")}
	updateB := ObservedEvent{Seq: 3, Kind: segment.KindUpdate, DID: "did:plc:a", Collection: "c", Rkey: "rb", Rev: "rev2", Payload: []byte("b-new")}

	pre := NormalizeEventLog([]ObservedEvent{createA, superseded, updateB})
	// Correct post: superseded create (seq 2) dropped; survivors reordered.
	goodPost := NormalizeEventLog([]ObservedEvent{updateB, createA})
	require.NoError(t, CompareEventLogsCompactedMultiset(pre, goodPost, 3),
		"reordered survivors with the superseded row dropped must pass")

	// Over-drop: the pass also wrongly dropped surviving createA (seq 1).
	overDropped := NormalizeEventLog([]ObservedEvent{updateB})
	err := CompareEventLogsCompactedMultiset(pre, overDropped, 3)
	require.Error(t, err, "an over-dropped surviving row must be caught")
	require.ErrorContains(t, err, "ra")
}

func TestCompareEventLogsCompactedRejectsUnjustifiedMissingCreate(t *testing.T) {
	t.Parallel()

	old := ObservedEvent{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "rev1", Payload: []byte("old")}

	err := CompareEventLogsCompacted(
		NormalizeEventLog([]ObservedEvent{old}),
		nil,
		1,
	)
	require.ErrorContains(t, err, "missing expected event")
	require.ErrorContains(t, err, "seq=1")
}

func TestEventLogRecorderNormalizesByUpstreamCursor(t *testing.T) {
	t.Parallel()

	rec := newEventLogRecorder()
	rec.Observe(&segment.Event{
		Seq:                 100,
		UpstreamRelayCursor: 7,
		Kind:                segment.KindCreate,
		DID:                 "did:plc:a",
		Collection:          "c",
		Rkey:                "r",
		Rev:                 "rev1",
		Payload:             []byte("payload"),
	})
	rec.Observe(&segment.Event{
		Seq:                 101,
		UpstreamRelayCursor: 8,
		Kind:                segment.KindDelete,
		DID:                 "did:plc:a",
		Collection:          "c",
		Rkey:                "r",
		Rev:                 "rev2",
	})

	rows := rec.RowsByUpstreamCursor(0, 7)
	require.Len(t, rows, 1)
	require.Equal(t, uint64(7), rows[0].Seq)
	require.Equal(t, "create", rows[0].Kind)
	require.Equal(t, "did:plc:a", rows[0].DID)
	require.NotEmpty(t, rows[0].PayloadSHA256_64)
}

func TestEventLogRecorderIgnoresSyntheticEventsWithoutUpstreamCursor(t *testing.T) {
	t.Parallel()

	rec := newEventLogRecorder()
	rec.Observe(&segment.Event{Seq: 1, UpstreamRelayCursor: 0, Kind: segment.KindSync, DID: "did:plc:a"})

	require.Empty(t, rec.RowsByUpstreamCursor(0, 10))
}

// observeCursor records one create row at the given upstream cursor, the shape
// waitForRowCount counts in its (after, through] window.
func observeCursor(rec *eventLogRecorder, cursor int64, rkey string) {
	rec.Observe(&segment.Event{
		Seq:                 uint64(cursor),
		UpstreamRelayCursor: cursor,
		Kind:                segment.KindCreate,
		DID:                 "did:plc:a",
		Collection:          "c",
		Rkey:                rkey,
		Rev:                 "rev",
		Payload:             []byte("p"),
	})
}

// TestWaitForRowCount is the direct regression guard for the cond-var deadlock
// guard (otherwise exercised only through the 5-minute live harness). It pins:
// the count-reached path returns true; an unreachable count returns false on
// the deadline WITHOUT hanging; and the nil-recorder fast path. Run under
// `go test -race` it also catches a missed Broadcast or a leaked watcher
// goroutine in the cond logic. Run after Observe-from-another-goroutine so the
// wait actually parks and is woken by the broadcast, not by an already-met
// predicate.
func TestWaitForRowCount(t *testing.T) {
	t.Parallel()

	t.Run("reaches count via a concurrent observe", func(t *testing.T) {
		t.Parallel()
		rec := newEventLogRecorder()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		go func() {
			observeCursor(rec, 1, "r1")
			observeCursor(rec, 2, "r2")
		}()
		require.True(t, rec.waitForRowCount(ctx, 0, 10, 2),
			"waitForRowCount must return true once the window holds the wanted rows")
	})

	t.Run("unreachable count returns false on deadline without hanging", func(t *testing.T) {
		t.Parallel()
		rec := newEventLogRecorder()
		observeCursor(rec, 1, "r1") // only one row; we ask for two
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		done := make(chan bool, 1)
		go func() { done <- rec.waitForRowCount(ctx, 0, 10, 2) }()
		select {
		case got := <-done:
			require.False(t, got, "an unreachable count must return false when the ctx deadline fires")
		case <-time.After(5 * time.Second):
			t.Fatal("waitForRowCount did not return after its ctx deadline (missed wakeup / hang)")
		}
	})

	t.Run("nil recorder", func(t *testing.T) {
		t.Parallel()
		var rec *eventLogRecorder
		require.True(t, rec.waitForRowCount(context.Background(), 0, 10, 0),
			"nil recorder with want<=0 is trivially satisfied")
		require.False(t, rec.waitForRowCount(context.Background(), 0, 10, 1),
			"nil recorder can never reach a positive want")
	})
}
