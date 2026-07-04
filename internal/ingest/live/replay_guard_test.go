package live

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest/syncstate"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/streaming"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/gt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func archivedAccountSeq(t *testing.T, payload []byte) int64 {
	t.Helper()
	var acc comatproto.SyncSubscribeRepos_Account
	require.NoError(t, acc.UnmarshalCBOR(payload))
	return acc.Seq
}

func accountEvent(did string, seq int64, active bool) streaming.Event {
	acc := &comatproto.SyncSubscribeRepos_Account{
		DID:    did,
		Active: active,
		Seq:    seq,
		Time:   "2026-05-21T00:00:00Z",
	}
	if !active {
		acc.Status = gt.Some("deleted")
	}
	return streaming.Event{Seq: seq, Account: acc}
}

// TestProcessBatch_ReplayedAccountEventIsDroppedNotReArchived pins the
// #231 guard: an #account event whose upstream seq is at or below the
// DID's APPLIED hosting-state seq is a relay seq replay whose row is
// already archived. Re-archiving it would put a stale account-delete
// above newer rows and every fold (reconstruct, tombstones, compaction)
// would erase live records. Events above the applied seq must still
// archive, and a DID with no hosting state must pass through untouched.
func TestProcessBatch_ReplayedAccountEventIsDroppedNotReArchived(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")
	metrics := NewMetrics(prometheus.NewRegistry())
	stateStore := syncstate.New(st)

	const did = "did:plc:replayed"

	// Arrange: the DID's hosting state at seq 5 is APPLIED (promoted) —
	// the row for seq 5 has been appended and promoted, exactly the
	// state a relay replay would arrive into.
	require.NoError(t, stateStore.SaveHosting(t.Context(), atmos.DID(did),
		atmossync.HostingState{Active: false, Status: "deleted", Seq: 5}))
	stateStore.PromoteHosting(atmos.DID(did), 5)

	c, err := Open(Config{
		SegmentsDir:    dir,
		Store:          st,
		SeqKey:         "live_segments/seq/next",
		CursorKey:      "relay/cursor",
		RelayURL:       "https://example.invalid",
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:       newTestVerifier(t),
		SyncStateStore: stateStore,
		Metrics:        metrics,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	// Replays: at and below the applied seq. Both must be dropped.
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{
		accountEvent(did, 5, false),
		accountEvent(did, 4, false),
	}))
	// New data: above the applied seq. Must archive.
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{
		accountEvent(did, 6, true),
	}))
	// A DID with no hosting state at all: must archive (first sighting).
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{
		accountEvent("did:plc:fresh", 7, true),
	}))
	require.NoError(t, c.Close())

	got := readAllSegmentEvents(t, dir)
	require.Len(t, got, 2, "only the post-applied-seq and first-sighting account rows may archive")
	// UpstreamRelayCursor is not persisted in the segment format; the
	// upstream seq is recovered from the archived #account payload.
	require.Equal(t, int64(6), archivedAccountSeq(t, got[0].Payload))
	require.Equal(t, did, got[0].DID)
	require.Equal(t, int64(7), archivedAccountSeq(t, got[1].Payload))
	require.Equal(t, "did:plc:fresh", got[1].DID)

	require.InDelta(t, 2.0, testutil.ToFloat64(metrics.ReplayedAccountsDrop), 0,
		"both replayed account events must be counted")
	require.Equal(t, int64(7), c.LastUpstreamSeq(),
		"replay drops must still advance the in-memory upstream watermark")
}
