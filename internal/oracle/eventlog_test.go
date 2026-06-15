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
