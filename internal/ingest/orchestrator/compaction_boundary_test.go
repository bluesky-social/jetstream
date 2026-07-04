package orchestrator

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// TestMerge_FirstInitWatermarkFloor_BoundarySeqCompacts pins the
// watermark-boundary data-loss mode of mutant m002 deterministically (#199):
// a superseding tombstone landing EXACTLY at the first merged seq.
//
// Shape: the destination tree already holds a bootstrap create at seq 1
// (seq/next = 2), and the live source carries a delete of that same record
// which the merge appends at seq 2 — the boundary seq.
//
//   - Correct floor (nextSeq-1 = 1): the merge-tail pass folds window (1, 2],
//     sees the delete, and drops the superseded create.
//   - m002 floor (nextSeq = 2): the pass no-ops (targetWatermark 2 <= floor 2)
//     and the miss is PERMANENT — every later pass's fold window (W, target]
//     is exclusive below, so the delete at seq 2 is never folded again and
//     the superseded create survives forever.
//
// The stress tier catches this only when a seed happens to place a tombstone
// at the boundary (4/5 seeds historically); this scenario places it there by
// construction, so the mutation campaign's `compaction` tier kills m002 on
// every run.
func TestMerge_FirstInitWatermarkFloor_BoundarySeqCompacts(t *testing.T) {
	t.Parallel()

	// Live source: a delete of the bootstrapped record, rev above the
	// backfill rev so the merge filter keeps it.
	del := ev("did:plc:a", "3l6", segment.KindDelete, 2000)
	del.Rkey = "rkey-boundary"
	fix := newMergeFixture(t, [][]segment.Event{{del}}, map[string]string{"did:plc:a": "3l5"})
	fix.cfg.CompactionInterval = time.Hour // enable the merge-tail pass

	// Destination tree: one sealed bootstrap segment with the create at
	// seq 1, leaving seq/next = 2 so the merged delete lands at the
	// watermark-floor boundary.
	bw, err := ingest.Open(ingest.Config{
		SegmentsDir: filepath.Join(fix.dataDir, "segments"),
		Store:       fix.store,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	create := ev("did:plc:a", "3l5", segment.KindCreate, 1000)
	create.Rkey = "rkey-boundary"
	require.NoError(t, bw.Append(t.Context(), &create))
	require.NoError(t, bw.SealActiveAndClose())
	require.Equal(t, uint64(1), create.Seq, "precondition: bootstrap create at seq 1")

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, o.runMerge(t.Context()))

	// The merge-tail pass must have committed the boundary seq.
	w, ok, err := loadCompactionWatermark(fix.store)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(2), w, "merge-tail pass must commit the boundary watermark")

	// Anti-vacuity + contract: the delete merged at seq 2 (the boundary) and
	// its superseded create at seq 1 was physically dropped.
	got := readDestEvents(t, fix.dataDir)
	var sawDelete bool
	for _, e := range got {
		if e.Kind == segment.KindDelete && e.Rkey == "rkey-boundary" {
			sawDelete = true
			require.Equal(t, uint64(2), e.Seq, "delete must land exactly at the boundary seq")
		}
		require.False(t, e.Kind.IsMaterialization() && e.Rkey == "rkey-boundary" && e.Seq <= w,
			"oracle: superseded record row survived at/below the first-init watermark: seq=%d watermark=%d (m002 boundary miss is permanent: later fold windows are (W, target], exclusive below)", e.Seq, w)
	}
	require.True(t, sawDelete, "anti-vacuity: the boundary delete must survive the merge filter")
}
