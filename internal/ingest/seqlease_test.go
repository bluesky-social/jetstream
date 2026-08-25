package ingest

import (
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/seqspace"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

var errSeqLeaseInjected = errors.New("injected seq lease commit failure")

func leaseTestConfig(dir string, st *store.Store, blockSize int) Config {
	return Config{
		SegmentsDir:              filepath.Join(dir, "segments"),
		Store:                    st,
		Logger:                   slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxEventsPerBlock:        blockSize,
		MaxSegmentBytes:          1 << 30,
		ReserveClientVisibleSeqs: true,
	}
}

func TestSeqLeaseFreshCleanAndCrashRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	st := newTestStore(t)
	cfg := leaseTestConfig(dir, st, 4)

	w1, err := Open(cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(1), w1.NextSeq())
	require.Empty(t, w1.SeqGaps().Ranges())
	require.Equal(t, uint64(5), mustLoadSeq(t, st, seqReservedKey))

	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:lost"}
	require.NoError(t, w1.Append(t.Context(), &ev))
	require.Equal(t, uint64(1), ev.Seq)

	// Model an unclean process death by abandoning w1 without Close. Its event
	// exists only in memory; the durable lease survives in Pebble.
	w2, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })
	require.Equal(t, uint64(5), w2.NextSeq())
	require.Equal(t, uint64(5), mustLoadSeq(t, st, seqNextKey), "startup atomically advances durable coverage across the registered vacancy")
	require.Equal(t, []seqspace.Gap{{Start: 1, End: 5}}, w2.SeqGaps().Ranges())

	replayed := segment.Event{Kind: segment.KindCreate, DID: "did:plc:other"}
	require.NoError(t, w2.Append(t.Context(), &replayed))
	require.Equal(t, uint64(5), replayed.Seq, "the observed seq from the abandoned writer must never be reused")
	require.NoError(t, w2.Close())
	require.Equal(t, uint64(6), mustLoadSeq(t, st, seqReservedKey), "clean close collapses the lease")

	w3, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w3.Close() })
	require.Equal(t, uint64(6), w3.NextSeq(), "clean reopen creates no additional gap")
	require.Equal(t, []seqspace.Gap{{Start: 1, End: 5}}, w3.SeqGaps().Ranges(), "registered vacancies survive clean restarts")
}

func TestSeqLeaseRepeatedCrashWithoutProgressCoalesces(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	st := newTestStore(t)
	cfg := leaseTestConfig(dir, st, 4)

	w1, err := Open(cfg)
	require.NoError(t, err)
	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:lost"}
	require.NoError(t, w1.Append(t.Context(), &ev))

	w2, err := Open(cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(5), w2.NextSeq())

	w3, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w3.Close() })
	require.Equal(t, uint64(9), w3.NextSeq())
	require.Equal(t, []seqspace.Gap{{Start: 1, End: 9}}, w3.SeqGaps().Ranges())
}

func TestSeqLeasePartialFlushRetainsFullBlockHeadroom(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	st := newTestStore(t)
	cfg := leaseTestConfig(dir, st, 4)
	w, err := Open(cfg)
	require.NoError(t, err)

	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a"}
	require.NoError(t, w.Append(t.Context(), &ev))
	require.NoError(t, w.Flush(t.Context()))
	require.Equal(t, uint64(6), mustLoadSeq(t, st, seqReservedKey))

	for range 4 {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:b"}
		require.NoError(t, w.Append(t.Context(), &ev))
	}
	require.Equal(t, uint64(6), w.NextSeq())
	require.Equal(t, uint64(10), mustLoadSeq(t, st, seqReservedKey))
	require.NoError(t, w.Close())
}

func TestSeqLeaseLegacyMigrationAndPreServingAttestation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		trust bool
		want  uint64
	}{
		{name: "upgrade burns one block", want: 6},
		{name: "pre-serving merge may attest unobservable", trust: true, want: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			st := newTestStore(t)
			legacy := leaseTestConfig(dir, st, 4)
			legacy.ReserveClientVisibleSeqs = false
			w1, err := Open(legacy)
			require.NoError(t, err)
			ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:durable"}
			require.NoError(t, w1.Append(t.Context(), &ev))
			require.NoError(t, w1.Close())

			upgraded := leaseTestConfig(dir, st, 4)
			upgraded.UnreservedSeqsUnobservable = tc.trust
			w2, err := Open(upgraded)
			require.NoError(t, err)
			t.Cleanup(func() { _ = w2.Close() })
			require.Equal(t, tc.want, w2.NextSeq())
		})
	}
}

func TestSeqLeaseStartupCommitFailureFailsOpen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fault := &store.KeyPrefixFault{
		Prefix: []byte("seq/"), Op: store.WriteOpBatchCommit, Ordinal: 1, Err: errSeqLeaseInjected,
	}
	st, err := store.Open(dir, nil, store.WithFaultInjector(fault))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	_, err = Open(leaseTestConfig(dir, st, 4))
	require.ErrorIs(t, err, errSeqLeaseInjected)
	_, found, loadErr := loadNextSeqFound(st, seqReservedKey)
	require.NoError(t, loadErr)
	require.False(t, found, "a failed startup transaction must not publish a reservation")
}

func TestSeqLeaseRenewalFailureExhaustsOldLease(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fault := &store.KeyPrefixFault{
		Prefix: []byte("seq/"), Op: store.WriteOpBatchCommit, Ordinal: 2, Err: errSeqLeaseInjected,
	}
	st, err := store.Open(dir, nil, store.WithFaultInjector(fault))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	w, err := Open(leaseTestConfig(dir, st, 4))
	require.NoError(t, err)

	for i := range 4 {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:renewal"}
		err = w.Append(t.Context(), &ev)
		if i < 3 {
			require.NoError(t, err)
		}
	}
	require.ErrorIs(t, err, errSeqLeaseInjected)
	require.Equal(t, uint64(5), w.NextSeq())
	require.Equal(t, uint64(5), mustLoadSeq(t, st, seqReservedKey), "failed renewal must leave the old durable lease intact")

	extra := segment.Event{Kind: segment.KindCreate, DID: "did:plc:must-not-allocate"}
	err = w.Append(t.Context(), &extra)
	require.ErrorContains(t, err, "sequence lease exhausted")
	require.Zero(t, extra.Seq)
}

func TestSeqLeaseRejectsMalformedAndOverlappingGapRecords(t *testing.T) {
	t.Parallel()
	t.Run("malformed", func(t *testing.T) {
		dir := t.TempDir()
		st := newTestStore(t)
		require.NoError(t, st.Set([]byte(seqGapPrefix+"bad"), []byte{gapVersion}, store.SyncWrites))
		_, err := Open(leaseTestConfig(dir, st, 4))
		require.ErrorContains(t, err, "malformed seq gap record")
	})

	t.Run("overlap durable block", func(t *testing.T) {
		dir := t.TempDir()
		st := newTestStore(t)
		legacy := leaseTestConfig(dir, st, 4)
		legacy.ReserveClientVisibleSeqs = false
		w, err := Open(legacy)
		require.NoError(t, err)
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:durable"}
		require.NoError(t, w.Append(t.Context(), &ev))
		require.NoError(t, w.Close())

		gaps, err := seqspace.NewGaps([]seqspace.Gap{{Start: 1, End: 2}})
		require.NoError(t, err)
		b := st.NewBatch()
		require.NoError(t, stageSeqGaps(b, gaps))
		require.NoError(t, stageNextSeq(b, seqReservedKey, 2))
		require.NoError(t, st.Commit(b, store.SyncWrites))
		require.NoError(t, b.Close())

		_, err = Open(leaseTestConfig(dir, st, 4))
		require.ErrorContains(t, err, "overlaps durable block")
	})

	t.Run("future gap beyond coverage frontier", func(t *testing.T) {
		dir := t.TempDir()
		st := newTestStore(t)
		gaps, err := seqspace.NewGaps([]seqspace.Gap{{Start: 10, End: 12}})
		require.NoError(t, err)
		b := st.NewBatch()
		require.NoError(t, stageSeqGaps(b, gaps))
		require.NoError(t, st.Commit(b, store.SyncWrites))
		require.NoError(t, b.Close())

		_, err = Open(leaseTestConfig(dir, st, 4))
		require.ErrorContains(t, err, "exceeds durable coverage frontier")
	})

	t.Run("reservation behind durable frontier", func(t *testing.T) {
		dir := t.TempDir()
		st := newTestStore(t)
		require.NoError(t, saveNextSeq(st, seqNextKey, 10))
		require.NoError(t, saveNextSeq(st, seqReservedKey, 9))

		_, err := Open(leaseTestConfig(dir, st, 4))
		require.ErrorContains(t, err, "trails durable coverage frontier")
	})
}

func TestSeqLeaseChangedBlockSizeUsesCurrentConfiguration(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	st := newTestStore(t)
	w1, err := Open(leaseTestConfig(dir, st, 4))
	require.NoError(t, err)
	lost := segment.Event{Kind: segment.KindCreate, DID: "did:plc:lost"}
	require.NoError(t, w1.Append(t.Context(), &lost))

	w2, err := Open(leaseTestConfig(dir, st, 7))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })
	require.Equal(t, uint64(5), w2.NextSeq(), "recovery consumes the old reservation exactly")
	require.Equal(t, uint64(12), mustLoadSeq(t, st, seqReservedKey), "the new lease uses the current block size")
}

func TestSeqLeaseRejectsRecoveredSeqOutsideCursorNamespace(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	require.NoError(t, mkdirAllFS(nil, segDir, 0o755))

	sw, err := segment.New(segment.Config{
		Path:              filepath.Join(segDir, SegmentFilename(0)),
		MaxEventsPerBlock: 1,
	})
	require.NoError(t, err)
	_, err = sw.Append(segment.Event{
		Seq:  ^uint64(0),
		Kind: segment.KindCreate,
		DID:  "did:plc:impossible",
	})
	require.NoError(t, err)
	_, err = sw.Seal()
	require.NoError(t, err)

	st := newTestStore(t)
	_, err = Open(leaseTestConfig(dir, st, 4))
	require.ErrorContains(t, err, "invalid recovered max seq")
	require.ErrorContains(t, err, "reservation overflow")
}

func TestSeqLeaseDrainRenewsRatherThanCollapses(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	st := newTestStore(t)
	w, err := Open(leaseTestConfig(dir, st, 4))
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	first := segment.Event{Kind: segment.KindCreate, DID: "did:plc:first"}
	require.NoError(t, w.Append(t.Context(), &first))
	require.NoError(t, w.DrainDurability(t.Context()))
	require.Equal(t, uint64(6), mustLoadSeq(t, st, seqReservedKey))

	second := segment.Event{Kind: segment.KindCreate, DID: "did:plc:after-drain"}
	require.NoError(t, w.Append(t.Context(), &second), "DrainDurability is non-terminal; allocation must remain leased")
	require.Equal(t, uint64(2), second.Seq)
}

func TestSeqLeaseDrainImmediatelyAfterGapRecoveryPreservesAllocationFrontier(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	st := newTestStore(t)
	cfg := leaseTestConfig(dir, st, 4)
	w1, err := Open(cfg)
	require.NoError(t, err)
	lost := segment.Event{Kind: segment.KindCreate, DID: "did:plc:lost"}
	require.NoError(t, w1.Append(t.Context(), &lost))

	w2, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })
	require.Equal(t, uint64(5), w2.NextSeq())
	require.NotPanics(t, func() {
		require.NoError(t, w2.DrainDurability(t.Context()))
	})
	require.Equal(t, uint64(9), mustLoadSeq(t, st, seqReservedKey), "metadata-only drain renews from allocation next, not the lower segment tip")

	afterDrain := segment.Event{Kind: segment.KindCreate, DID: "did:plc:after-gap-drain"}
	require.NoError(t, w2.Append(t.Context(), &afterDrain))
	require.Equal(t, uint64(5), afterDrain.Seq)
}

func TestSeqLeaseCrashSequenceModelNeverReusesOrLeavesUnregisteredVacancies(t *testing.T) {
	t.Parallel()
	const blockSize = 4
	dir := t.TempDir()
	st := newTestStore(t)
	cfg := leaseTestConfig(dir, st, blockSize)
	w, err := Open(cfg)
	require.NoError(t, err)

	allocated := make(map[uint64]struct{})
	durable := make(map[uint64]struct{})
	var pending []uint64
	state := uint64(0x345)
	for cycle := range 64 {
		// A deterministic LCG gives varied sub-block crash windows while keeping
		// this a reproducible model test rather than a probabilistic flake.
		state = state*6364136223846793005 + 1442695040888963407
		count := int(state % blockSize)
		for range count {
			ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:model"}
			require.NoError(t, w.Append(t.Context(), &ev))
			_, reused := allocated[ev.Seq]
			require.Falsef(t, reused, "cycle %d reused allocated seq %d", cycle, ev.Seq)
			allocated[ev.Seq] = struct{}{}
			pending = append(pending, ev.Seq)
		}
		if cycle%3 == 0 {
			require.NoError(t, w.Flush(t.Context()))
			for _, seq := range pending {
				durable[seq] = struct{}{}
			}
			pending = pending[:0]
		}

		lost := append([]uint64(nil), pending...)
		pending = nil
		w, err = Open(cfg) // abandon the old writer: deterministic crash model
		require.NoError(t, err)
		gaps := w.SeqGaps()
		for _, seq := range lost {
			_, ok := gaps.EndContaining(seq)
			require.Truef(t, ok, "cycle %d lost allocated seq %d is not registered vacant", cycle, seq)
		}
		for seq := uint64(1); seq < w.NextSeq(); seq++ {
			_, wasDurable := durable[seq]
			_, inGap := gaps.EndContaining(seq)
			require.Truef(t, wasDurable || inGap,
				"cycle %d seq %d below next=%d is neither durable nor registered vacant", cycle, seq, w.NextSeq())
		}
	}
	require.NoError(t, w.Close())
}

func mustLoadSeq(t *testing.T, st *store.Store, key string) uint64 {
	t.Helper()
	v, err := loadNextSeq(st, key)
	require.NoError(t, err)
	return v
}
