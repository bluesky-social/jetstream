package orchestrator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// ev is a brief constructor for synthetic source events. seq is left
// at 0 — the source ingest.Writer will assign one when the event is
// appended.
func ev(did, rev string, kind segment.Kind, indexedAtMicros int64) segment.Event {
	return segment.Event{
		IndexedAt:  indexedAtMicros,
		Kind:       kind,
		DID:        did,
		Collection: "app.bsky.feed.post",
		Rkey:       "rkey-" + rev,
		Rev:        rev,
		Payload:    []byte("payload-" + rev),
	}
}

// pointErrorInjector simulates a crash at a single checkpoint by
// RETURNING an error there. The error unwinds the call stack, so all
// deferred cleanup (dst.Close, writer Close, etc.) still runs — this is
// a "recoverable error" model, NOT a faithful process death. It is
// sufficient for the checkpoints used here because each fires AFTER the
// durable fsync it names, so the on-disk/in-store state at the crash
// boundary is identical whether the process unwinds gracefully or is
// SIGKILLed. The oracle restart harness (oracleCrashInjector) provides
// the faithful SIGKILL model where cleanup-on-crash behavior matters.
type pointErrorInjector struct {
	point crashpoint.Point
	err   error
}

func (i pointErrorInjector) SimulateCrash(_ context.Context, point crashpoint.Point) error {
	if point == i.point {
		return i.err
	}
	return nil
}

// readDestEvents returns every event currently present in
// data/segments/, in seq-ascending order. Reader.Open insists on
// fully-sealed segments, so the test must call dst.SealActiveAndClose
// indirectly via runMerge before invoking this.
func readDestEvents(t *testing.T, dataDir string) []segment.Event {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dataDir, "segments", "seg_*.jss"))
	require.NoError(t, err)
	var out []segment.Event
	for _, m := range matches {
		rd, err := segment.Open(segment.ReaderConfig{Path: m})
		require.NoError(t, err)
		for i := range int(rd.Header().BlockCount) {
			blk, err := rd.DecodeBlock(i)
			require.NoError(t, err)
			out = append(out, blk...)
		}
		_ = rd.Close()
	}
	return out
}

func TestMerge_DropsCoveredCommits_KeepsOthers(t *testing.T) {
	t.Parallel()
	// did:plc:a backfilled at rev=3l5; did:plc:b never backfilled.
	srcEvs := []segment.Event{
		ev("did:plc:a", "3l3", segment.KindCreate, 1000),                // drop (covered)
		ev("did:plc:a", "3l5", segment.KindCreate, 1001),                // drop (== BackfillRev)
		ev("did:plc:a", "3l6", segment.KindCreate, 1002),                // keep
		ev("did:plc:b", "3l4", segment.KindCreate, 1003),                // keep (no backfill)
		{Kind: segment.KindIdentity, DID: "did:plc:a", IndexedAt: 1004}, // keep (non-commit)
	}
	fix := newMergeFixture(t, [][]segment.Event{srcEvs}, map[string]string{"did:plc:a": "3l5"})

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, o.runMerge(t.Context()))

	got := readDestEvents(t, fix.dataDir)
	require.Len(t, got, 3, "want 3 survivors (a@3l6, b@3l4, identity)")

	// IndexedAt re-stamping: every survivor > max(source IndexedAt).
	const maxSrc = int64(1004)
	for _, e := range got {
		require.Greater(t, e.IndexedAt, maxSrc, "survivor IndexedAt must be re-stamped to merge time")
	}
}

func TestMerge_PublishesTerminalSealToManifest(t *testing.T) {
	t.Parallel()
	srcEvs := []segment.Event{ev("did:plc:a", "3l6", segment.KindCreate, 1000)}
	fix := newMergeFixture(t, [][]segment.Event{srcEvs}, map[string]string{"did:plc:a": "3l5"})

	segmentsDir := filepath.Join(fix.dataDir, "segments")
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
	mft, err := manifest.Open(manifest.Options{
		SegmentsDir: segmentsDir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	require.Equal(t, 0, mft.SegmentCount(), "manifest starts before merge seals any destination segment")

	fix.cfg.IngestOnAfterSeal = mft.OnSegmentSealed
	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, o.runMerge(t.Context()))

	require.Equal(t, 1, mft.SegmentCount(), "merge terminal seal must publish without process restart")
	entries, nextIdx, more := mft.ListFrom(0, 10)
	require.False(t, more)
	require.Zero(t, nextIdx)
	require.Len(t, entries, 1)
	require.Equal(t, uint64(0), entries[0].Idx)
	require.Equal(t, uint32(1), entries[0].EventCount)
	require.Equal(t, uint64(1), entries[0].MinSeq)
	require.Equal(t, uint64(1), entries[0].MaxSeq)
}

func TestMerge_RefreshesRepoRev_PreservesBackfillRev(t *testing.T) {
	t.Parallel()
	srcEvs := []segment.Event{
		ev("did:plc:a", "3l6", segment.KindCreate, 1000),
		ev("did:plc:a", "3l7", segment.KindUpdate, 1001),
	}
	fix := newMergeFixture(t, [][]segment.Event{srcEvs}, map[string]string{"did:plc:a": "3l5"})

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, o.runMerge(t.Context()))

	val, closer, err := fix.store.Get(backfill.RepoKey("did:plc:a"))
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()
	rs, err := backfill.DecodeRepoStatus(val)
	require.NoError(t, err)
	require.Equal(t, "3l7", rs.Rev, "top-level Rev advances to last surviving rev")
	require.Equal(t, "3l5", rs.Backfill.Rev, "Backfill.Rev is immutable post-merge")
}

func TestMerge_MultiSourceContiguousCommit(t *testing.T) {
	t.Parallel()
	src1 := []segment.Event{ev("did:plc:a", "3l6", segment.KindCreate, 1000)}
	src2 := []segment.Event{ev("did:plc:a", "3l7", segment.KindCreate, 1001)}
	fix := newMergeFixture(t, [][]segment.Event{src1, src2}, map[string]string{"did:plc:a": "3l5"})

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, o.runMerge(t.Context()))

	// Cursor key absent (terminal cleanup ran).
	got, err := loadMergeCursor(fix.store)
	require.NoError(t, err)
	require.Equal(t, uint64(0), got)

	// data/backfill removed.
	_, err = os.Stat(filepath.Join(fix.dataDir, "backfill"))
	require.True(t, os.IsNotExist(err))
}

func TestMerge_EmptyLiveSegmentsDir(t *testing.T) {
	t.Parallel()
	fix := newMergeFixture(t, nil, nil) // creates empty live_segments dir

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, o.runMerge(t.Context()))
}

func TestMerge_RestartAfterCleanup_NoLiveSegmentsDir(t *testing.T) {
	t.Parallel()
	fix := newMergeFixture(t, nil, nil)
	require.NoError(t, os.RemoveAll(filepath.Join(fix.dataDir, "backfill")))

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, o.runMerge(t.Context()))
}

func TestMerge_TopLevelRunAdvancesPhase(t *testing.T) {
	t.Parallel()
	srcEvs := []segment.Event{ev("did:plc:a", "3l6", segment.KindCreate, 1000)}
	fix := newMergeFixture(t, [][]segment.Event{srcEvs}, map[string]string{"did:plc:a": "3l5"})

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	o, err := New(fix.cfg)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()

	require.Eventually(t, func() bool {
		p, err := lifecycle.ReadPhase(fix.store)
		return err == nil && p == lifecycle.PhaseSteadyState
	}, 5*time.Second, 20*time.Millisecond, "phase did not advance to steady_state")
	cancel()
	runErr := <-done
	require.True(t, runErr == nil || errors.Is(runErr, context.Canceled),
		"unexpected Run error: %v", runErr)

	// At least one sealed segment should exist from the merge phase.
	// The steady-state consumer may have created a new active (unsealed)
	// segment, which is expected.
	matches, err := filepath.Glob(filepath.Join(fix.dataDir, "segments", "seg_*.jss"))
	require.NoError(t, err)
	require.NotEmpty(t, matches)
	var sealedCount int
	for _, m := range matches {
		if isSealed(t, m) {
			sealedCount++
		}
	}
	require.GreaterOrEqual(t, sealedCount, 1, "at least one sealed segment should exist from merge")
}

func TestMerge_CrashAfterFlushBeforeCommit_ProducesDuplicates(t *testing.T) {
	t.Parallel()

	srcEvs := []segment.Event{
		ev("did:plc:a", "3l6", segment.KindCreate, 1000),
		ev("did:plc:a", "3l7", segment.KindCreate, 1001),
	}
	fix := newMergeFixture(t, [][]segment.Event{srcEvs}, map[string]string{"did:plc:a": "3l5"})

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	sentinel := errors.New("kill point: flush-before-commit")
	fix.cfg.CrashInjector = pointErrorInjector{
		point: crashpoint.AfterMergeDstFlushBeforeSourceCommit,
		err:   sentinel,
	}

	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.ErrorIs(t, o.runMerge(t.Context()), sentinel)

	// Cursor unchanged (commit never ran).
	cur, err := loadMergeCursor(fix.store)
	require.NoError(t, err)
	require.Equal(t, uint64(0), cur)

	// Restart: clear the hook and re-run.
	fix.cfg.CrashInjector = nil
	o2, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, o2.runMerge(t.Context()))

	got := readDestEvents(t, fix.dataDir)

	// Both source events appear at least once. They MAY appear twice
	// (pre-crash flushed copy + post-recovery copy). Seqs strictly
	// monotonic across all destination events.
	revs := map[string]int{}
	for _, e := range got {
		revs[e.Rev]++
	}
	require.GreaterOrEqual(t, revs["3l6"], 1, "rev 3l6 must survive at least once")
	require.GreaterOrEqual(t, revs["3l7"], 1, "rev 3l7 must survive at least once")
	for i := 1; i < len(got); i++ {
		require.Greater(t, got[i].Seq, got[i-1].Seq,
			"destination seqs must be strictly monotonic (got %d, prev %d)",
			got[i].Seq, got[i-1].Seq)
	}
}

func TestMerge_CrashAfterSealBeforeDiscovery_RestartCleansUp(t *testing.T) {
	t.Parallel()

	srcEvs := []segment.Event{ev("did:plc:a", "3l6", segment.KindCreate, 1000)}
	fix := newMergeFixture(t, [][]segment.Event{srcEvs}, map[string]string{"did:plc:a": "3l5"})

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	sentinel := errors.New("kill point: seal-before-removeall")
	fix.cfg.CrashInjector = pointErrorInjector{
		point: crashpoint.AfterMergeDstSealBeforeDiscovery,
		err:   sentinel,
	}

	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.ErrorIs(t, o.runMerge(t.Context()), sentinel)

	// Sealed dst still on disk; live_segments still on disk.
	_, err = os.Stat(filepath.Join(fix.dataDir, "backfill", "live_segments"))
	require.NoError(t, err)

	// Restart: clear the hook and re-run.
	fix.cfg.CrashInjector = nil
	o2, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, o2.runMerge(t.Context()))

	// data/backfill should now be gone.
	_, err = os.Stat(filepath.Join(fix.dataDir, "backfill"))
	require.True(t, os.IsNotExist(err), "backfill dir should be removed after restart")

	// Both cursor keys should be gone.
	cur, err := loadMergeCursor(fix.store)
	require.NoError(t, err)
	require.Equal(t, uint64(0), cur)
	bcur, err := backfill.LoadBootstrapLastListReposCursor(fix.store)
	require.NoError(t, err)
	require.Equal(t, "", bcur)
}

func TestMerge_CrashAfterDiscoveryBeforeCleanup_RestartIsIdempotent(t *testing.T) {
	t.Parallel()

	srcEvs := []segment.Event{ev("did:plc:a", "3l6", segment.KindCreate, 1000)}
	fix := newMergeFixture(t, [][]segment.Event{srcEvs}, map[string]string{"did:plc:a": "3l5"})
	fix.seedBootstrapLastCursor(t, "page2")
	fix.relay.pages = map[string]listReposPage{
		"page2": {
			Cursor: "",
			Repos: []listReposEntry{
				{DID: "did:plc:new", Active: true},
			},
		},
	}

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	sentinel := errors.New("kill point: discovery-before-cleanup")
	fix.cfg.CrashInjector = pointErrorInjector{
		point: crashpoint.AfterMergeDiscoveryBeforeCleanup,
		err:   sentinel,
	}

	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.ErrorIs(t, o.runMerge(t.Context()), sentinel)

	val, closer, err := fix.store.Get(backfill.RepoKey("did:plc:new"))
	require.NoError(t, err, "discovered DID row must be durable before cleanup")
	rs, err := backfill.DecodeRepoStatus(val)
	require.NoError(t, err)
	_ = closer.Close()
	require.Equal(t, backfill.StatusFailed, rs.Backfill.Status)
	require.True(t, rs.Active)

	_, err = os.Stat(filepath.Join(fix.dataDir, "backfill", "live_segments"))
	require.NoError(t, err, "backfill tree should still exist when cleanup has not run")

	fix.cfg.CrashInjector = nil
	o2, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, o2.runMerge(t.Context()))

	val, closer, err = fix.store.Get(backfill.RepoKey("did:plc:new"))
	require.NoError(t, err, "discovered DID row must survive idempotent discovery replay")
	rs, err = backfill.DecodeRepoStatus(val)
	require.NoError(t, err)
	_ = closer.Close()
	require.Equal(t, backfill.StatusFailed, rs.Backfill.Status)
	require.True(t, rs.Active)

	_, err = os.Stat(filepath.Join(fix.dataDir, "backfill"))
	require.True(t, os.IsNotExist(err), "backfill dir should be removed after restart")
	bcur, err := backfill.LoadBootstrapLastListReposCursor(fix.store)
	require.NoError(t, err)
	require.Equal(t, "", bcur)
}

// TestMerge_SealsActiveSourceSegmentBeforeDrain exercises
// sealActiveMergeSource directly. It reproduces the on-disk state a
// crash at AfterBootstrapLiveCloseBeforeSeal leaves behind: the
// bootstrap-live source tree has a trailing ACTIVE (unsealed) segment
// because the process died after Close() flushed data but before the
// seal-via-reopen ran.
//
// Without sealActiveMergeSource, the merge runner's
// processSourceSegment would call segment.Open on the active file and
// get ErrActiveSegment, crash-looping merge forever. This test pins
// that the seal guard makes the source readable and the survivor is
// drained exactly once.
func TestMerge_SealsActiveSourceSegmentBeforeDrain(t *testing.T) {
	t.Parallel()

	// No sealed sources from the fixture; we build the unsealed one by hand.
	fix := newMergeFixture(t, nil, map[string]string{"did:plc:active": "3l5"})
	liveDir := filepath.Join(fix.dataDir, "backfill", "live_segments")

	// Write one surviving event (rev > cutoff) and Close() WITHOUT
	// sealing — mirroring bootstrapLive.Close() in the crash window.
	srcW, err := ingest.Open(ingest.Config{
		SegmentsDir: liveDir,
		Store:       fix.store,
		SeqKey:      live.BootstrapSeqKey,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	survivor := ev("did:plc:active", "3l9", segment.KindCreate, 1000)
	require.NoError(t, srcW.Append(t.Context(), &survivor))
	require.NoError(t, srcW.Close()) // Close, not SealActiveAndClose.

	// Precondition: the trailing source segment is genuinely unsealed,
	// otherwise this test would pass for the wrong reason.
	srcFiles, err := readSegFiles(liveDir)
	require.NoError(t, err)
	require.Len(t, srcFiles, 1)
	require.False(t, isSealed(t, srcFiles[0]),
		"precondition: trailing source segment must be active/unsealed")

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))

	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, o.runMerge(t.Context()),
		"merge must seal the active source segment and drain it")

	// The survivor reached the destination exactly once.
	got := readDestEvents(t, fix.dataDir)
	survivors := map[string]int{}
	for _, e := range got {
		survivors[e.Rev]++
	}
	require.Equal(t, 1, survivors["3l9"], "survivor must be drained exactly once")

	// Full merge completed: backfill tree removed, cursors clean.
	_, err = os.Stat(filepath.Join(fix.dataDir, "backfill"))
	require.True(t, os.IsNotExist(err), "backfill dir should be removed after a clean merge")
	cur, err := loadMergeCursor(fix.store)
	require.NoError(t, err)
	require.Equal(t, uint64(0), cur)
}

func TestMerge_DiscoversNewDIDsViaListReposResume(t *testing.T) {
	t.Parallel()

	srcEvs := []segment.Event{ev("did:plc:a", "3l6", segment.KindCreate, 1000)}
	fix := newMergeFixture(t, [][]segment.Event{srcEvs}, map[string]string{"did:plc:a": "3l5"})

	// Pre-seed the bootstrap-last cursor so runDiscovery is exercised.
	fix.seedBootstrapLastCursor(t, "page2")

	// Wire the fake relay to return a page when queried at cursor=page2.
	// Two entries: did:plc:a is already known (idempotency check); did:plc:new
	// is novel (discovery should write a StatusFailed row for it).
	fix.relay.pages = map[string]listReposPage{
		"page2": {
			Cursor: "", // signal end-of-pagination
			Repos: []listReposEntry{
				{DID: "did:plc:a", Active: true},
				{DID: "did:plc:new", Active: true},
			},
		},
	}

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, o.runMerge(t.Context()))

	// Pre-existing did:plc:a row: top-level Rev was advanced by the
	// merge's commitSourceComplete (rev 3l6), but Backfill is unchanged.
	val, closer, err := fix.store.Get(backfill.RepoKey("did:plc:a"))
	require.NoError(t, err)
	rsA, err := backfill.DecodeRepoStatus(val)
	require.NoError(t, err)
	_ = closer.Close()
	require.Equal(t, "3l6", rsA.Rev)
	require.Equal(t, backfill.StatusComplete, rsA.Backfill.Status)
	require.Equal(t, "3l5", rsA.Backfill.Rev)

	// Newly discovered did:plc:new gets a StatusFailed row with the
	// synthetic LastError so the steady-state retry path picks it up.
	val2, closer2, err := fix.store.Get(backfill.RepoKey("did:plc:new"))
	require.NoError(t, err)
	rsNew, err := backfill.DecodeRepoStatus(val2)
	require.NoError(t, err)
	_ = closer2.Close()
	require.Equal(t, backfill.StatusFailed, rsNew.Backfill.Status)
	require.Equal(t, "discovered post-bootstrap; queued for retry", rsNew.Backfill.LastError)
	require.True(t, rsNew.Active)

	// Bootstrap-last cursor is gone (terminal cleanup ran).
	got, err := backfill.LoadBootstrapLastListReposCursor(fix.store)
	require.NoError(t, err)
	require.Equal(t, "", got)
}

// TestMerge_DiscoveryWalksMultiplePages exercises the page-loop in
// runDiscovery: bootstrap-last cursor points at page A, the relay
// returns A → cursor=B, then B → cursor="" (end). Both pages contain
// new DIDs; both must end up with StatusFailed-discovered rows.
func TestMerge_DiscoveryWalksMultiplePages(t *testing.T) {
	t.Parallel()

	fix := newMergeFixture(t, nil, nil)

	fix.seedBootstrapLastCursor(t, "pageA")

	fix.relay.pages = map[string]listReposPage{
		"pageA": {
			Cursor: "pageB",
			Repos: []listReposEntry{
				{DID: "did:plc:new1", Active: true},
			},
		},
		"pageB": {
			Cursor: "", // end
			Repos: []listReposEntry{
				{DID: "did:plc:new2", Active: false},
			},
		},
	}

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	o, err := New(fix.cfg)
	require.NoError(t, err)
	require.NoError(t, o.runMerge(t.Context()))

	for _, did := range []string{"did:plc:new1", "did:plc:new2"} {
		val, closer, err := fix.store.Get(backfill.RepoKey(did))
		require.NoError(t, err, "expected discovered row for %s", did)
		rs, err := backfill.DecodeRepoStatus(val)
		require.NoError(t, err)
		_ = closer.Close()
		require.Equal(t, backfill.StatusFailed, rs.Backfill.Status)
	}

	// Active flag round-trip: matches the entry the relay returned.
	val1, closer1, err := fix.store.Get(backfill.RepoKey("did:plc:new1"))
	require.NoError(t, err)
	rs1, err := backfill.DecodeRepoStatus(val1)
	require.NoError(t, err)
	_ = closer1.Close()
	require.True(t, rs1.Active)

	val2, closer2, err := fix.store.Get(backfill.RepoKey("did:plc:new2"))
	require.NoError(t, err)
	rs2, err := backfill.DecodeRepoStatus(val2)
	require.NoError(t, err)
	_ = closer2.Close()
	require.False(t, rs2.Active)
}

// TestMerge_DiscoveryRelayErrorAborts confirms that a relay-side
// failure during the discovery walk is surfaced as a runMerge error
// rather than silently swallowed. The drained source data is durable
// (live_segments is still on disk and merge cursor reflects what
// drained), so a restart re-runs the empty source loop and retries
// discovery.
func TestMerge_DiscoveryRelayErrorAborts(t *testing.T) {
	t.Parallel()

	fix := newMergeFixture(t, nil, nil)
	fix.seedBootstrapLastCursor(t, "discovery-fails")

	// Override the relay handler to 500 on listRepos so atmos surfaces a
	// transport error from the iterator.
	fix.relay.srv.Close()
	fix.relay.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/com.atproto.sync.listRepos") {
			http.Error(w, "transient relay outage", http.StatusInternalServerError)
			return
		}
		fix.relay.handle(w, r)
	}))
	t.Cleanup(fix.relay.srv.Close)
	fix.cfg.RelayURL = fix.relay.srv.URL

	require.NoError(t, lifecycle.WritePhase(fix.store, lifecycle.PhaseMerging, time.Now().UTC()))
	o, err := New(fix.cfg)
	require.NoError(t, err)
	err = o.runMerge(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), "discovery")

	// live_segments must still exist (RemoveAll never ran), and the
	// bootstrap-last cursor must still be set so a future restart can
	// retry the discovery walk from the same point.
	_, err = os.Stat(filepath.Join(fix.dataDir, "backfill", "live_segments"))
	require.NoError(t, err)
	bcur, err := backfill.LoadBootstrapLastListReposCursor(fix.store)
	require.NoError(t, err)
	require.Equal(t, "discovery-fails", bcur)
}
