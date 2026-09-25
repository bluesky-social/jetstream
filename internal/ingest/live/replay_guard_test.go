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

// TestProcessBatch_ReplayedAccountEventDropsWhenHostingPromotionBlocked
// reproduces the CI failure mode from #254: atmos can stage newer same-DID
// hosting state before Jetstream appends an older #account row. The older row
// must not promote that newer pending state, but its own append still has to
// arm archive-level replay dedupe so a duplicate older frame does not append.
func TestProcessBatch_ReplayedAccountEventDropsWhenHostingPromotionBlocked(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")
	metrics := NewMetrics(prometheus.NewRegistry())
	stateStore := syncstate.New(st)

	const did = "did:plc:blockedpromote"
	atmosDID := atmos.DID(did)
	require.NoError(t, stateStore.SaveHosting(t.Context(), atmosDID,
		atmossync.HostingState{Active: true, Seq: 6}))

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

	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{
		accountEvent(did, 5, false),
	}))
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{
		accountEvent(did, 5, false),
	}))
	require.NoError(t, c.Close())

	got := readAllSegmentEvents(t, dir)
	require.Len(t, got, 1, "blocked hosting promotion must not let account replay re-archive")
	require.Equal(t, int64(5), archivedAccountSeq(t, got[0].Payload))
	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.ReplayedAccountsDrop), 0)

	hosting, err := stateStore.LoadAppliedHosting(t.Context(), atmosDID)
	require.NoError(t, err)
	require.Nil(t, hosting, "older row must not promote newer pending hosting state")
}

// TestProcessBatch_AccountRatchetDurableAtBlockBoundary mirrors the identity
// crash-window guard for #account rows: a full-block append commits the cursor
// batch inside Append, so the account replay ratchet must be recorded by
// OnAppend before that flush.
func TestProcessBatch_AccountRatchetDurableAtBlockBoundary(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")
	stateStore := syncstate.New(st)

	const did = "did:plc:acctblockedge"

	c, err := Open(Config{
		SegmentsDir:       dir,
		Store:             st,
		SeqKey:            "live_segments/seq/next",
		CursorKey:         "relay/cursor",
		RelayURL:          "https://example.invalid",
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:          newTestVerifier(t),
		SyncStateStore:    stateStore,
		MaxEventsPerBlock: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{
		accountEvent(did, 5, false),
	}))

	applied, err := syncstate.New(st).LoadAppliedAccountSeq(t.Context(), atmos.DID(did))
	require.NoError(t, err)
	require.Equal(t, int64(5), applied,
		"account ratchet must be durable once the row's block has flushed")
}

func archivedIdentitySeq(t *testing.T, payload []byte) int64 {
	t.Helper()
	var ident comatproto.SyncSubscribeRepos_Identity
	require.NoError(t, ident.UnmarshalCBOR(payload))
	return ident.Seq
}

func identityEvent(did string, seq int64) streaming.Event {
	return streaming.Event{Seq: seq, Identity: &comatproto.SyncSubscribeRepos_Identity{
		DID:  did,
		Seq:  seq,
		Time: "2026-07-04T00:00:00Z",
	}}
}

// TestProcessBatch_ReplayedIdentityEventIsDroppedNotReArchived pins the
// #234 guard: #identity events have no replay protection at any other
// layer (atmos does not process them), so a relay seq replay after a
// reconnect would re-archive the row as a permanent duplicate. The
// guard's ratchet is recorded by the consumer itself at append time —
// so unlike the #account arrangement above, archiving through
// processBatch IS the arrangement: a second delivery of the same seq
// must drop, a higher seq must archive, and a fresh DID must pass.
func TestProcessBatch_ReplayedIdentityEventIsDroppedNotReArchived(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")
	metrics := NewMetrics(prometheus.NewRegistry())
	stateStore := syncstate.New(st)

	const did = "did:plc:identreplay"

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

	// First delivery archives and records the applied-seq ratchet.
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{
		identityEvent(did, 5),
	}))
	// Relay replays: at and below the applied seq. Both must drop.
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{
		identityEvent(did, 5),
		identityEvent(did, 4),
	}))
	// New data above the ratchet must archive; a fresh DID must pass.
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{
		identityEvent(did, 6),
		identityEvent("did:plc:identfresh", 7),
	}))
	require.NoError(t, c.Close())

	got := readAllSegmentEvents(t, dir)
	require.Len(t, got, 3, "replayed identity events must not re-archive")
	require.Equal(t, int64(5), archivedIdentitySeq(t, got[0].Payload))
	require.Equal(t, did, got[0].DID)
	require.Equal(t, int64(6), archivedIdentitySeq(t, got[1].Payload))
	require.Equal(t, did, got[1].DID)
	require.Equal(t, int64(7), archivedIdentitySeq(t, got[2].Payload))
	require.Equal(t, "did:plc:identfresh", got[2].DID)

	require.InDelta(t, 2.0, testutil.ToFloat64(metrics.ReplayedIdentityDrop), 0,
		"both replayed identity events must be counted")
	require.Equal(t, int64(7), c.LastUpstreamSeq(),
		"replay drops must still advance the in-memory upstream watermark")
}

// TestProcessBatch_IdentityRatchetDurableAtBlockBoundary pins the
// crash-window ordering of #234: Append can synchronously flush a full
// block, and that flush commits the cursor+syncstate batch. The ratchet
// must therefore be recorded by the writer's OnAppend hook (before the
// flush), NOT after Append returns — otherwise a crash right after the
// in-Append flush leaves the identity row durable with no durable
// ratchet, and a restart redelivery re-archives it. Asserted by reading
// the ratchet back through a FRESH syncstate store (empty in-memory
// maps, so only pebble answers) without closing the consumer.
func TestProcessBatch_IdentityRatchetDurableAtBlockBoundary(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")
	stateStore := syncstate.New(st)

	const did = "did:plc:identblockedge"

	c, err := Open(Config{
		SegmentsDir:       dir,
		Store:             st,
		SeqKey:            "live_segments/seq/next",
		CursorKey:         "relay/cursor",
		RelayURL:          "https://example.invalid",
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:          newTestVerifier(t),
		SyncStateStore:    stateStore,
		MaxEventsPerBlock: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	// MaxEventsPerBlock=1: this append fills the block, so Append itself
	// flushes it and commits the durable cursor batch before returning.
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{
		identityEvent(did, 5),
	}))

	// Deliberately no Close: simulate the crash window. A fresh store
	// over the same pebble db sees only what is durable.
	applied, err := syncstate.New(st).LoadAppliedIdentitySeq(t.Context(), atmos.DID(did))
	require.NoError(t, err)
	require.Equal(t, int64(5), applied,
		"identity ratchet must be durable once the row's block has flushed")
}

// TestProcessBatch_IdentityReplayGuardSurvivesRestart pins the durable
// half of #234: the ratchet persists via the syncstate flush, so a
// replay delivered to a FRESH consumer over the same store (the
// restart + relay-regression window) still drops.
func TestProcessBatch_IdentityReplayGuardSurvivesRestart(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")
	stateStore := syncstate.New(st)

	const did = "did:plc:identrestart"

	open := func(m *Metrics) *Consumer {
		c, err := Open(Config{
			SegmentsDir:    dir,
			Store:          st,
			SeqKey:         "live_segments/seq/next",
			CursorKey:      "relay/cursor",
			RelayURL:       "https://example.invalid",
			Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
			Verifier:       newTestVerifier(t),
			SyncStateStore: stateStore,
			Metrics:        m,
		})
		require.NoError(t, err)
		return c
	}

	c1 := open(nil)
	require.NoError(t, c1.processBatch(t.Context(), []streaming.Event{
		identityEvent(did, 9),
	}))
	// Close flushes promoted syncstate (the ratchet) to pebble.
	require.NoError(t, c1.Close())

	metrics := NewMetrics(prometheus.NewRegistry())
	c2 := open(metrics)
	t.Cleanup(func() { _ = c2.Close() })
	require.NoError(t, c2.processBatch(t.Context(), []streaming.Event{
		identityEvent(did, 9),
	}))
	require.NoError(t, c2.Close())

	got := readAllSegmentEvents(t, dir)
	require.Len(t, got, 1, "cross-restart identity replay must not re-archive")
	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.ReplayedIdentityDrop), 0)
}
