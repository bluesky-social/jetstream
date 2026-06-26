package orchestrator

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// TestCompaction_StoreFaultOnWatermarkSave_FailsLoudNoAdvance is the kill for
// mutation m028 (compaction_watermark_save_error_swallowed), a new
// swallowed-persistence-error mutant on the compaction watermark write — a
// distinct high-risk fault point named in issue #30, and a different store op
// from m006 (a plain Set on compaction/seq, not a batch commit), so it
// exercises the fault seam's Set path.
//
// m028 inverts the error check on saveCompactionWatermark in
// runDeleteCompaction so a FAILED watermark write is swallowed and the pass
// proceeds: the in-memory watermark gauge and finalWatermark advance, tombstone
// eviction runs, and the pass returns success — all while the DURABLE
// compaction/seq is stale. On the next process the persisted watermark is below
// what was already rewritten, so a resumed compaction can re-drop or mis-handle
// rows relative to a watermark the rest of the system believed had advanced.
//
// We force the watermark Set to fail and assert the phase contract (#30): the
// pass fails LOUD and the durable watermark does NOT advance, so a restart
// re-runs the pass from the true persisted point. Under the mutant the error is
// swallowed and runDeleteCompaction returns nil — the kill.
func TestCompaction_StoreFaultOnWatermarkSave_FailsLoudNoAdvance(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	segmentsDir := filepath.Join(dataDir, "segments")
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))

	injected := errors.New("injected: compaction watermark save failed")
	fault := &store.KeyPrefixFault{
		Prefix:  []byte(compactionWatermarkKey),
		Op:      store.WriteOpSet,
		Ordinal: 1,
		Err:     injected,
	}
	st, err := store.Open(dataDir, nil, store.WithFaultInjector(fault))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	// A create superseded by a delete: the merge-tail pass rewrites the segment
	// (dropping the create) and then advances the watermark to seq 2 — the Set
	// our fault aborts.
	path := writeCompactionSegment(t, segmentsDir, 0, []segment.Event{
		{Seq: 1, IndexedAt: 10, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "1", Payload: []byte("old")},
		{Seq: 2, IndexedAt: 20, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r", Rev: "2"},
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	o := &Orchestrator{cfg: Config{
		DataDir:            dataDir,
		Store:              st,
		Logger:             logger,
		CompactionInterval: time.Hour,
	}, logger: logger}

	// Correct code propagates the watermark-save failure; the mutant swallows it
	// and returns nil. This is the kill.
	err = o.runDeleteCompaction(t.Context(), compactionMergeTail, nil)
	require.Error(t, err, "compaction must fail loud when the watermark save fails")
	require.ErrorIs(t, err, injected)

	// No silent advance: the durable watermark must remain unset/zero, so a
	// restart re-runs the pass from the true persisted point rather than
	// trusting a watermark that was never durably written.
	watermark, ok, lerr := loadCompactionWatermark(st)
	require.NoError(t, lerr)
	require.False(t, ok && watermark > 0,
		"durable watermark must not advance when its save failed (got ok=%v watermark=%d)", ok, watermark)

	// Anti-vacuity: the rewrite actually ran (the create was dropped), so the
	// failed write was a real watermark advance, not a no-op pass.
	events := readCompactionSegment(t, path)
	require.Len(t, events, 1, "rewrite should have dropped the superseded create before the watermark save")
	require.Equal(t, segment.KindDelete, events[0].Kind)
}
