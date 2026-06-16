package oracle

import (
	"encoding/json"
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
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
