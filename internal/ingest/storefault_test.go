package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/segment"
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
	fault := &metastore.KeyPrefixFault{
		Prefix:  []byte(seqNextKey),
		Op:      metastore.WriteOpBatchCommit,
		Ordinal: 1,
		Err:     injected,
	}
	stRaw, err := pebblestore.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stRaw.Close() })
	st := metastore.WithFaults(stRaw, fault)

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
	_, getErr := st.Get(context.Background(), []byte(seqNextKey))
	require.ErrorIs(t, getErr, metastore.ErrNotFound,
		"seq/next must not be durable when its commit failed")
}

// TestWriter_DurableBatchENOSPCReturnsFatalOperatorMessage pins that a
// disk-full failure on the seq/next durable commit — the pebble half of the
// ingest persistence boundary, not just the segment file — carries the same
// fatal operator guidance as segment write/fsync ENOSPC (issue #201).
func TestWriter_DurableBatchENOSPCReturnsFatalOperatorMessage(t *testing.T) {
	t.Parallel()

	fault := &metastore.KeyPrefixFault{
		Prefix:  []byte(seqNextKey),
		Op:      metastore.WriteOpBatchCommit,
		Ordinal: 1,
		Err:     syscall.ENOSPC,
	}
	dataDir := t.TempDir()
	stRaw, err := pebblestore.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stRaw.Close() })
	st := metastore.WithFaults(stRaw, fault)

	const blockSize = 4
	w, err := Open(Config{
		DataDir:           dataDir,
		SegmentsDir:       filepath.Join(dataDir, "segments"),
		Store:             st,
		SeqKey:            seqNextKey,
		MaxEventsPerBlock: blockSize,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           NewMetrics(prometheus.NewRegistry()),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	var appendErr error
	for range blockSize {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a"}
		if appendErr = w.Append(t.Context(), &ev); appendErr != nil {
			break
		}
	}
	require.ErrorIs(t, appendErr, syscall.ENOSPC)
	require.ErrorContains(t, appendErr, "fatal persistence error")
	require.ErrorContains(t, appendErr, "disk full")
	require.ErrorContains(t, appendErr, dataDir)
	require.ErrorContains(t, appendErr, "restart jetstream")
}
