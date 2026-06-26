package ingest

import (
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestWriter_DurableBatchFailsLoudOnStoreFault pins the fail-loud contract for
// the seq/next durable-commit boundary (issue #30 fault point: "seq/next
// writes"). commitDurableBatchLocked stages seq/next (plus any OnDurableBatch
// metadata) and commits it with Sync after the block's segment bytes are
// fsynced. A failed commit must surface as an error so the flush — and the
// owning consumer/backfill loop — tears down, rather than reporting an
// advanced in-memory durableNextSeq that never reached disk. A swallowed
// failure here would leave seq/next behind the durable segment data: across a
// restart the writer would re-allocate already-used seqs, duplicating or
// colliding archived events.
//
// A full block flush drives commitDurableBatchLocked; the fault aborts the
// seq/next batch commit. We assert Append surfaces the injected error and that
// seq/next is not durable (no silent advance).
func TestWriter_DurableBatchFailsLoudOnStoreFault(t *testing.T) {
	t.Parallel()

	injected := errors.New("injected: seq/next durable commit failed")
	fault := &store.KeyPrefixFault{
		Prefix:  []byte(seqNextKey),
		Op:      store.WriteOpBatchCommit,
		Ordinal: 1,
		Err:     injected,
	}
	st, err := store.Open(t.TempDir(), nil, store.WithFaultInjector(fault))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	const blockSize = 4
	w, err := Open(Config{
		SegmentsDir:       filepath.Join(t.TempDir(), "segments"),
		Store:             st,
		SeqKey:            seqNextKey,
		MaxEventsPerBlock: blockSize,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           NewMetrics(prometheus.NewRegistry()),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	// The first blockSize-1 appends buffer; the blockSize-th fills the block,
	// flushes the segment bytes, and commits the seq/next batch — which the
	// fault aborts. The failure must surface out of Append.
	var appendErr error
	for range blockSize {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a"}
		if appendErr = w.Append(t.Context(), &ev); appendErr != nil {
			break
		}
	}
	require.Error(t, appendErr, "block flush must fail loud when the seq/next commit fails")
	require.ErrorIs(t, appendErr, injected)

	// No silent advance: seq/next never became durable.
	_, _, getErr := st.Get([]byte(seqNextKey))
	require.ErrorIs(t, getErr, pebble.ErrNotFound,
		"seq/next must not be durable when its commit failed")
}
