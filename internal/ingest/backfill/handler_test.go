package backfill

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// newTestIngest builds a *ingest.Writer rooted at t.TempDir for handler tests.
func newTestIngest(t *testing.T) *ingest.Writer {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       filepath.Join(dir, "segments"),
		Store:             st,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func buildSingleRecordRepo(t *testing.T, did atmos.DID, collection, rkey string, record map[string]any) (*atmosrepo.Repo, *atmosrepo.Commit) {
	t.Helper()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	mstore := mst.NewMemBlockStore()
	r := &atmosrepo.Repo{
		DID:   did,
		Clock: atmos.NewTIDClock(0),
		Store: mstore,
		Tree:  mst.NewTree(mstore),
	}
	require.NoError(t, r.Create(collection, rkey, record))
	commit, err := r.Commit(key)
	require.NoError(t, err)
	return r, commit
}

func buildMultiRecordRepo(t *testing.T, did atmos.DID, collection string, n int) (*atmosrepo.Repo, *atmosrepo.Commit) {
	t.Helper()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	mstore := mst.NewMemBlockStore()
	r := &atmosrepo.Repo{
		DID:   did,
		Clock: atmos.NewTIDClock(0),
		Store: mstore,
		Tree:  mst.NewTree(mstore),
	}
	for i := range n {
		rkey := fmt.Sprintf("rkey%03d", i)
		require.NoError(t, r.Create(collection, rkey, map[string]any{"text": rkey}))
	}
	commit, err := r.Commit(key)
	require.NoError(t, err)
	return r, commit
}

func collectActiveEvents(t *testing.T, path string) []segment.Event {
	t.Helper()
	var events []segment.Event
	require.NoError(t, segment.WalkActive(path, func(block []segment.Event) error {
		events = append(events, block...)
		return nil
	}))
	return events
}

// TestSegmentHandler_EmitsOneEventPerRecord pins the contract: a
// repo with K records lands K Create rows in the segment with the
// expected (DID, Collection, Rkey, Rev) coordinates.
func TestSegmentHandler_EmitsOneEventPerRecord(t *testing.T) {
	t.Parallel()
	w := newTestIngest(t)

	frozen := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)
	h := NewSegmentHandler(w, nil, nil)
	h.now = func() time.Time { return frozen }

	r, commit := buildSingleRecordRepo(t,
		"did:plc:test", "app.bsky.feed.post", "rkey1",
		map[string]any{"text": "hello"})

	require.NoError(t, h.HandleRepo(context.Background(), "did:plc:test", r, commit))

	require.Equal(t, uint64(2), w.NextSeq(),
		"one record yields exactly one event")
}

func TestSegmentHandler_HandleRepoQueuesCompletionWithoutFlush(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	bs := NewStore(st, nil)

	segmentsDir := filepath.Join(dir, "segments")
	cb := NewCompletionBatcher(bs, nil)
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       segmentsDir,
		Store:             st,
		MaxEventsPerBlock: 4096,
		OnDurableBatch:    cb.StageDurable,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	did := atmos.DID("did:plc:no-flush-before-complete")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))
	bs.SetCompletionBatcher(cb)

	r, commit := buildSingleRecordRepo(t,
		did, "app.bsky.feed.post", "rkey1",
		map[string]any{"text": "completion is tied to durable block metadata"})
	h := NewSegmentHandler(w, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	h.SetCompletionBatcher(cb)

	require.NoError(t, h.HandleRepo(t.Context(), did, r, commit))

	events := collectActiveEvents(t, filepath.Join(segmentsDir, ingest.SegmentFilename(0)))
	require.Empty(t, events, "HandleRepo must not force a per-repo segment flush")
	require.Equal(t, completionWatermark{lastSeq: 1, appended: true}, cb.watermarks[did])

	require.NoError(t, bs.OnComplete(t.Context(), did, "", commit))
	requireLookupState(t, bs, did, atmosbackfill.StateDiscovered)
	require.Len(t, cb.queued, 1)

	require.NoError(t, w.DrainDurability(t.Context()))
	requireLookupState(t, bs, did, atmosbackfill.StateComplete)
	require.Empty(t, cb.queued)
}

// TestSegmentHandler_MultiBlockRepoCompletesOnlyWithFinalBlock covers the spec
// testing checklist item "a repo spanning multiple blocks completes only with
// the final block" — the core durability gate this change introduces. A repo
// whose events span several blocks must stay StateDiscovered while earlier
// blocks become durable (advancing seq/next), and flip StateComplete only once
// the block containing its FINAL event is fsynced. We use a small
// MaxEventsPerBlock and the sync flush path so block boundaries are
// deterministic and intermediate durable commits are observable.
func TestSegmentHandler_MultiBlockRepoCompletesOnlyWithFinalBlock(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	bs := NewStore(st, nil)
	cb := NewCompletionBatcher(bs, nil)

	const perBlock = 2
	const records = 5 // seqs 1..5 across 3 blocks: [1,2] [3,4] [5]
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       filepath.Join(dir, "segments"),
		Store:             st,
		MaxEventsPerBlock: perBlock,
		MaxSegmentBytes:   1 << 30,
		OnDurableBatch:    cb.StageDurable,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	did := atmos.DID("did:plc:multi-block-repo")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))
	bs.SetCompletionBatcher(cb)

	r, commit := buildMultiRecordRepo(t, did, "app.bsky.feed.post", records)
	h := NewSegmentHandler(w, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	h.SetCompletionBatcher(cb)

	require.NoError(t, h.HandleRepo(t.Context(), did, r, commit))

	// HandleRepo appended all 5 events; the two full blocks [1,2] and [3,4]
	// auto-flushed during AppendBatch and committed durably (seq/next advanced
	// to 5), but the final event seq 5 sits in an un-fsynced pending block. The
	// watermark records the repo's final event seq before OnComplete consumes
	// it (QueueComplete deletes the watermark entry).
	require.Equal(t, completionWatermark{lastSeq: uint64(records), appended: true}, cb.watermarks[did],
		"watermark must record the repo's final event seq")
	require.Equal(t, uint64(records+1), w.NextSeq())

	require.NoError(t, bs.OnComplete(t.Context(), did, "", commit))
	// The completion must NOT be durable yet: its watermark lastSeq=5 is not
	// below the durable seq/next=5 (final event's block not fsynced).
	requireLookupState(t, bs, did, atmosbackfill.StateDiscovered)
	require.Len(t, cb.queued, 1, "completion stays queued until the final block is durable")

	// Draining flushes the trailing block (seq 5), fsyncs it, then commits the
	// completion in the same durable batch. Only now may it be complete.
	require.NoError(t, w.DrainDurability(t.Context()))
	requireLookupState(t, bs, did, atmosbackfill.StateComplete)
	require.Empty(t, cb.queued)

	rs, err := bs.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, commit.Rev, rs.Backfill.Rev)
}

func TestSegmentHandler_HandleEmptyRepoRecordsEmptyWatermark(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	bs := NewStore(st, nil)
	cb := NewCompletionBatcher(bs, nil)

	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       filepath.Join(dir, "segments"),
		Store:             st,
		MaxEventsPerBlock: 4096,
		OnDurableBatch:    cb.StageDurable,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	did := atmos.DID("did:plc:empty-repo")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))
	bs.SetCompletionBatcher(cb)

	blockStore := mst.NewMemBlockStore()
	r := &atmosrepo.Repo{
		DID:   did,
		Store: blockStore,
		Tree:  mst.NewTree(blockStore),
	}
	commit := &atmosrepo.Commit{DID: string(did), Rev: "3l3qo2vutsw2b"}
	h := NewSegmentHandler(w, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	h.SetCompletionBatcher(cb)

	require.NoError(t, h.HandleRepo(t.Context(), did, r, commit))
	require.Equal(t, completionWatermark{lastSeq: 0, appended: false}, cb.watermarks[did])

	require.NoError(t, bs.OnComplete(t.Context(), did, "", commit))
	requireLookupState(t, bs, did, atmosbackfill.StateDiscovered)
	require.NoError(t, w.DrainDurability(t.Context()))
	requireLookupState(t, bs, did, atmosbackfill.StateComplete)
}

func TestSegmentHandler_QueuedCompletionBecomesDurableOnWriterClose(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	st, err := store.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	bs := NewStore(st, nil)
	cb := NewCompletionBatcher(bs, nil)

	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       filepath.Join(dir, "segments"),
		Store:             st,
		MaxEventsPerBlock: 4096,
		OnDurableBatch:    cb.StageDurable,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	did := atmos.DID("did:plc:complete-on-close")
	require.NoError(t, bs.OnDiscover(t.Context(), testListReposEntry(did)))
	bs.SetCompletionBatcher(cb)

	r, commit := buildSingleRecordRepo(t,
		did, "app.bsky.feed.post", "rkey1",
		map[string]any{"text": "completion should be durable on close"})
	h := NewSegmentHandler(w, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	h.SetCompletionBatcher(cb)

	require.NoError(t, h.HandleRepo(t.Context(), did, r, commit))
	require.NoError(t, bs.OnComplete(t.Context(), did, "", commit))
	requireLookupState(t, bs, did, atmosbackfill.StateDiscovered)

	require.NoError(t, w.Close())
	requireLookupState(t, bs, did, atmosbackfill.StateComplete)
	require.Empty(t, cb.queued)
}

func TestSegmentHandler_DropsRecordThatExceedsSegmentColumnWidth(t *testing.T) {
	t.Parallel()

	w := newTestIngest(t)
	dropMetrics := ingest.NewDropMetrics(prometheus.NewRegistry())
	h := NewSegmentHandler(w, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	h.dropMetrics = dropMetrics

	var writerErr error
	h.onWriterError = func(err error) { writerErr = err }

	longRkey := strings.Repeat("x", 256)
	r, commit := buildSingleRecordRepo(t,
		"did:plc:widefield", "app.bsky.feed.post", longRkey,
		map[string]any{"text": "this rkey cannot fit in the segment column"})

	require.NoError(t, h.HandleRepo(t.Context(), "did:plc:widefield", r, commit))
	require.NoError(t, writerErr, "invalid upstream record data must not abort the local writer")
	require.Equal(t, uint64(1), w.NextSeq(), "skipped records must not allocate seqs (NextSeq stays at the fresh-dir seed)")
	require.InDelta(t, 1.0, testutil.ToFloat64(dropMetrics.Counter(ingest.DropSourceBackfill, ingest.DropReasonFieldTooLong)), 0,
		"the skipped record must be visible under field_too_long — a spec-valid rkey we chose not to represent")
}

// TestSegmentHandler_InvalidRevFailsRepo pins the #197 backfill rev
// gate: a commit whose rev is not a spec-valid TID fails the whole
// repo (visible in failed-repo diagnostics, retried by the retry
// loop) rather than silently archiving records under a rev that
// merge/compaction ordering cannot reason about. atmos's repo loader
// already rejects invalid NON-empty revs before HandleRepo runs, so
// the empty rev is the reachable case — but the gate rejects both.
func TestSegmentHandler_InvalidRevFailsRepo(t *testing.T) {
	t.Parallel()

	for _, rev := range []string{"", "not-a-tid"} {
		w := newTestIngest(t)
		dropMetrics := ingest.NewDropMetrics(prometheus.NewRegistry())
		h := NewSegmentHandler(w, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
		h.dropMetrics = dropMetrics

		r, commit := buildSingleRecordRepo(t,
			"did:plc:badrev", "app.bsky.feed.post", "rkey1",
			map[string]any{"text": "x"})
		commit.Rev = rev

		err := h.HandleRepo(t.Context(), "did:plc:badrev", r, commit)
		require.Error(t, err, "rev=%q must fail the repo", rev)
		require.Equal(t, uint64(1), w.NextSeq(),
			"rev=%q: no record may be archived under an invalid rev", rev)
		require.InDelta(t, 1.0, testutil.ToFloat64(dropMetrics.Counter(ingest.DropSourceBackfill, ingest.DropReasonInvalidRev)), 0)
	}
}

// buildHostileKeyRepo builds a repo whose MST contains records
// inserted under raw keys, bypassing Repo.Create's path validation —
// the shape a hostile or buggy PDS's CAR produces (mst.LoadTree
// decodes keys from CBOR without spec validation).
func buildHostileKeyRepo(t *testing.T, did atmos.DID, keys ...string) (*atmosrepo.Repo, *atmosrepo.Commit) {
	t.Helper()
	mstore := mst.NewMemBlockStore()
	tree := mst.NewTree(mstore)
	for i, key := range keys {
		blk, err := cbor.Marshal(map[string]any{"v": i})
		require.NoError(t, err)
		cid := cbor.ComputeCID(cbor.CodecDagCBOR, blk)
		require.NoError(t, mstore.PutBlock(cid, blk))
		require.NoError(t, tree.Insert(key, cid))
	}
	r := &atmosrepo.Repo{
		DID:   did,
		Clock: atmos.NewTIDClock(0),
		Store: mstore,
		Tree:  tree,
	}
	return r, &atmosrepo.Commit{DID: string(did), Rev: "3l3qo2vutsw2b"}
}

// TestSegmentHandler_SpecInvalidPathDropsRecordKeepsSiblings pins the
// per-record half of the gate: a record whose MST key fails atproto
// path validation (spec-invalid NSID or record key — including keys
// the MST charset allows but the specs don't) is dropped and counted
// while well-formed siblings archive normally. Pre-#197 a malformed
// key failed the whole repo.
func TestSegmentHandler_SpecInvalidPathDropsRecordKeepsSiblings(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		key    string
		reason ingest.DropReason
	}{
		{"nodots/rkey1", ingest.DropReasonInvalidCollection},
		{"two.segments/rkey1", ingest.DropReasonInvalidCollection},
		{"justonepart", ingest.DropReasonInvalidCollection},
		{"app.bsky.feed.post/..", ingest.DropReasonInvalidRkey},
		{"app.bsky.feed.post/bad/extra", ingest.DropReasonInvalidRkey},
	} {
		w := newTestIngest(t)
		dropMetrics := ingest.NewDropMetrics(prometheus.NewRegistry())
		h := NewSegmentHandler(w, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
		h.dropMetrics = dropMetrics

		r, commit := buildHostileKeyRepo(t, "did:plc:hostilekeys",
			"app.bsky.feed.post/good", tc.key)

		require.NoError(t, h.HandleRepo(t.Context(), "did:plc:hostilekeys", r, commit),
			"key=%q: a droppable record must not fail the repo", tc.key)
		require.Equal(t, uint64(2), w.NextSeq(),
			"key=%q: exactly the well-formed sibling must be archived", tc.key)
		require.InDelta(t, 1.0, testutil.ToFloat64(dropMetrics.Counter(ingest.DropSourceBackfill, tc.reason)), 0,
			"key=%q must count under %s", tc.key, tc.reason)
	}
}

// TestSegmentHandler_ResyncDropsInvalidPathOnceKeepsSiblings pins the
// resync path's consistency: the pre-validation walk must classify a
// spec-invalid key as droppable (not fail the repo before the
// tombstone is appended) and the drop is counted exactly once — by
// the appending walk, not the pre-check.
func TestSegmentHandler_ResyncDropsInvalidPathOnceKeepsSiblings(t *testing.T) {
	t.Parallel()

	w := newTestIngest(t)
	dropMetrics := ingest.NewDropMetrics(prometheus.NewRegistry())
	h := NewSegmentHandler(w, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	h.dropMetrics = dropMetrics

	r, commit := buildHostileKeyRepo(t, "did:plc:resynchostile",
		"app.bsky.feed.post/good", "nodots/evil")

	require.NoError(t, h.HandleRepoResync(t.Context(), "did:plc:resynchostile", r, commit))
	require.Equal(t, uint64(3), w.NextSeq(),
		"the KindSync tombstone plus the surviving record must be archived")
	require.InDelta(t, 1.0, testutil.ToFloat64(dropMetrics.Counter(ingest.DropSourceBackfill, ingest.DropReasonInvalidCollection)), 0,
		"the drop must be counted exactly once across pre-check and append walks")
}

// TestSegmentHandler_MissingDownloadedRecordBlockSurfacesError pins the
// handler's post-fix contract for a missing record block. Completeness of a
// downloaded full repo is now verified UPSTREAM, before HandleRepo runs:
//   - the atmos backfill engine calls repo.(*Repo).CheckComplete in download()
//     (covered by atmos backfill/truncation_test.go), and
//   - jetstream's own direct loaders (retry.go, selected.go) use
//     repo.LoadCompleteFromCAR.
//
// Both classify a block-boundary-truncated CAR as a transient
// io.ErrUnexpectedEOF and retry it. The handler therefore no longer needs to
// re-tag a missing block as transient (the previous bandaid that only covered
// leaf record blocks, never interior MST nodes). It now surfaces the store's
// error plainly; we assert it is matchable as mst.ErrBlockNotFound so a
// genuinely-missing block still fails loudly rather than silently dropping a
// record.
func TestSegmentHandler_MissingDownloadedRecordBlockSurfacesError(t *testing.T) {
	t.Parallel()

	w := newTestIngest(t)
	h := NewSegmentHandler(w, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	store := mst.NewMemBlockStore()
	tree := mst.NewTree(store)
	missingCID := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("missing-record-block"))
	require.NoError(t, tree.Insert("app.bsky.feed.post/rkey1", missingCID))
	r := &atmosrepo.Repo{
		DID:   "did:plc:truncatedcar",
		Store: store,
		Tree:  tree,
	}
	commit := &atmosrepo.Commit{Rev: "3l3qo2vutsw2b"}

	err := h.HandleRepo(t.Context(), "did:plc:truncatedcar", r, commit)
	require.Error(t, err)
	require.ErrorIs(t, err, mst.ErrBlockNotFound, "a missing record block must surface loudly")
}

// TestSegmentHandler_NilWriterPanics pins the constructor's
// fast-fail invariant.
func TestSegmentHandler_NilWriterPanics(t *testing.T) {
	t.Parallel()
	require.Panics(t, func() { _ = NewSegmentHandler(nil, nil, nil) })
}

// TestSplitRecordPath rounds the gate helper through happy and
// unhappy cases, pinning which half of the path each reason blames.
func TestSplitRecordPath(t *testing.T) {
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		c, k, reason := splitRecordPath("app.bsky.feed.post/rkey1")
		require.Equal(t, ingest.DropReason(""), reason)
		require.Equal(t, "app.bsky.feed.post", c)
		require.Equal(t, "rkey1", k)
	})

	for _, tc := range []struct {
		in     string
		reason ingest.DropReason
	}{
		{"", ingest.DropReasonInvalidCollection},
		{"justonepart", ingest.DropReasonInvalidCollection},
		{"/leading-slash", ingest.DropReasonInvalidCollection},
		{"nodots/rkey", ingest.DropReasonInvalidCollection},
		{"$account/rkey", ingest.DropReasonInvalidCollection},
		{"app.bsky.feed.post/", ingest.DropReasonInvalidRkey},
		{"app.bsky.feed.post/.", ingest.DropReasonInvalidRkey},
		{"app.bsky.feed.post/..", ingest.DropReasonInvalidRkey},
		{"app.bsky.feed.post/too/many", ingest.DropReasonInvalidRkey},
		{"app.bsky.feed.post/" + strings.Repeat("x", 513), ingest.DropReasonInvalidRkey},
	} {
		_, _, reason := splitRecordPath(tc.in)
		require.Equal(t, tc.reason, reason, "input %q", tc.in)
	}
}
