package repoexport

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/manifest"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
	"github.com/stretchr/testify/require"
)

const testDID = "did:plc:repoexport"

// manifestSelector adapts a real *manifest.Manifest to repoexport.Selector,
// mirroring the production adapter in jetstreamd. Tests use a real manifest
// (not a hand-rolled fake) so reconstruction is exercised end-to-end:
// real resident blooms -> real segment decode -> real MST root.
type manifestSelector struct{ m *manifest.Manifest }

func (s manifestSelector) SelectBlocksForDID(did string) ([]BlockSelection, error) {
	sel, err := s.m.SelectBlocksForDID(did)
	if err != nil {
		return nil, err
	}
	out := make([]BlockSelection, len(sel))
	for i := range sel {
		out[i] = BlockSelection{Path: sel[i].Path, Blocks: sel[i].Blocks}
	}
	return out, nil
}

func (s manifestSelector) ActiveSegmentPaths() ([]string, error) {
	return s.m.ActiveSegmentPaths()
}

// openSelector opens a manifest over dataDir/segments and returns it adapted
// to repoexport.Selector. The manifest is the single in-memory cache the
// production /status path uses; reconstruction never re-scans segments/.
func openSelector(t *testing.T, dataDir string) Selector {
	t.Helper()
	segmentsDir := filepath.Join(dataDir, "segments")
	// Production MkdirAll's the segments dir before opening the manifest
	// (jetstreamd runtime); mirror that so an account with no on-disk
	// segments still yields a usable (empty) selector.
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
	m, err := manifest.Open(manifest.Options{
		SegmentsDir: segmentsDir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	return manifestSelector{m: m}
}

// TestReconstruct_CoversSealedActiveAndPending is the central correctness
// test for the manifest-backed path. A DID's records are spread across all
// three regions that exist at steady state:
//   - r1: in a SEALED segment (resident in the manifest, selected by bloom)
//   - r2: in the ACTIVE segment's flushed-but-unsealed block (NOT in the
//     manifest and NOT in SnapshotPending -- the gap a manifest-only
//     implementation would silently drop)
//   - r3: in the live writer's in-memory pending block (PendingEvents)
//
// All three must appear in the reconstructed root, or /status would report
// a spurious mismatch.
func TestReconstruct_CoversSealedActiveAndPending(t *testing.T) {
	t.Parallel()

	dataDir, st := newTestDataDir(t)
	segmentsDir := filepath.Join(dataDir, "segments")

	r1 := createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1"))
	r2 := createEvent(testDID, "app.bsky.feed.like", "r2", "rev2", payload("2"))
	r3 := createEvent(testDID, "app.bsky.feed.post", "r3", "rev3", payload("3"))

	// Seal r1 into segment 0, then leave r2 in segment 1 as a flushed but
	// unsealed (active) block.
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       segmentsDir,
		Store:             st,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)
	ev1 := r1
	require.NoError(t, w.Append(t.Context(), &ev1))
	require.NoError(t, w.SealActiveAndClose())

	w2, err := ingest.Open(ingest.Config{
		SegmentsDir:       segmentsDir,
		Store:             st,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)
	ev2 := r2
	require.NoError(t, w2.Append(t.Context(), &ev2))
	require.NoError(t, w2.Flush(t.Context())) // flush block to active seg, do NOT seal
	require.NoError(t, w2.Close())

	wantRoot, wantCount := expectedRoot(t, []segment.Event{r1, r2, r3})
	got, err := Reconstruct(context.Background(), Config{
		DataDir:       dataDir,
		DID:           testDID,
		Selector:      openSelector(t, dataDir),
		PendingEvents: []segment.Event{r3},
	})
	require.NoError(t, err)
	require.Equal(t, "rev3", got.LatestRev)
	require.Equal(t, wantRoot, got.Root)
	require.Equal(t, wantCount, got.RecordCount)
}

func TestReconstruct_RequiresSelector(t *testing.T) {
	t.Parallel()

	_, err := Reconstruct(context.Background(), Config{
		DataDir: t.TempDir(),
		DID:     testDID,
	})
	require.ErrorContains(t, err, "Selector is required")
}

func TestReconstruct_CreateOnlyBuildsExpectedRoot(t *testing.T) {
	t.Parallel()

	dataDir, st := newTestDataDir(t)
	archive := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
	}
	live := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r2", "rev2", payload("2")),
	}
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), archive)
	writeSegmentTree(t, st, filepath.Join(dataDir, "backfill", "live_segments"), live)

	wantRoot, wantCount := expectedRoot(t, append(archive, live...))
	got, err := Reconstruct(context.Background(), Config{
		DataDir:  dataDir,
		DID:      testDID,
		Selector: openSelector(t, dataDir),
	})
	require.NoError(t, err)
	require.Equal(t, testDID, got.DID)
	require.Equal(t, "rev2", got.LatestRev)
	require.Equal(t, wantRoot, got.Root)
	require.Equal(t, wantCount, got.RecordCount)
}

func TestReconstruct_LiveSegmentsSkipEventsAtOrBeforePrimaryWatermark(t *testing.T) {
	t.Parallel()

	dataDir, st := newTestDataDir(t)
	primary := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "3l5", payload("1")),
	}
	live := []segment.Event{
		updateEvent(testDID, "app.bsky.feed.post", "r1", "3l4", payload("2")),
		createEvent(testDID, "app.bsky.feed.post", "r2", "3l6", payload("3")),
	}
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), primary)
	writeSegmentTree(t, st, filepath.Join(dataDir, "backfill", "live_segments"), live)

	wantEvents := []segment.Event{
		primary[0],
		live[1],
	}
	wantRoot, wantCount := expectedRoot(t, wantEvents)
	got, err := Reconstruct(context.Background(), Config{
		DataDir:  dataDir,
		DID:      testDID,
		Selector: openSelector(t, dataDir),
	})
	require.NoError(t, err)
	require.Equal(t, "3l6", got.LatestRev)
	require.Equal(t, wantRoot, got.Root)
	require.Equal(t, wantCount, got.RecordCount)

	tree := mst.LoadTree(got.Blocks, got.Root)
	r1CID, err := tree.Get("app.bsky.feed.post/r1")
	require.NoError(t, err)
	require.NotNil(t, r1CID)
	require.Equal(t, cbor.ComputeCID(cbor.CodecDagCBOR, payload("1")), *r1CID)

	r2CID, err := tree.Get("app.bsky.feed.post/r2")
	require.NoError(t, err)
	require.NotNil(t, r2CID)
	require.Equal(t, cbor.ComputeCID(cbor.CodecDagCBOR, payload("3")), *r2CID)
}

func TestReconstruct_UpdateReplacesRecordCID(t *testing.T) {
	t.Parallel()

	dataDir, st := newTestDataDir(t)
	events := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
		updateEvent(testDID, "app.bsky.feed.post", "r1", "rev2", payload("2")),
	}
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), events)

	wantRoot, wantCount := expectedRoot(t, events)
	got, err := Reconstruct(context.Background(), Config{
		DataDir:  dataDir,
		DID:      testDID,
		Selector: openSelector(t, dataDir),
	})
	require.NoError(t, err)
	require.Equal(t, "rev2", got.LatestRev)
	require.Equal(t, wantRoot, got.Root)
	require.Equal(t, wantCount, got.RecordCount)

	newCID := cbor.ComputeCID(cbor.CodecDagCBOR, payload("2"))
	stored, err := got.Blocks.GetBlock(newCID)
	require.NoError(t, err)
	require.Equal(t, payload("2"), stored)

	tree := mst.LoadTree(got.Blocks, got.Root)
	gotCID, err := tree.Get("app.bsky.feed.post/r1")
	require.NoError(t, err)
	require.NotNil(t, gotCID)
	require.Equal(t, newCID, *gotCID)
}

func TestReconstruct_BlocksExcludeStaleRecordPayloads(t *testing.T) {
	t.Parallel()

	dataDir, st := newTestDataDir(t)
	events := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "updated", "rev1", payload("1")),
		updateEvent(testDID, "app.bsky.feed.post", "updated", "rev2", payload("2")),
		createEvent(testDID, "app.bsky.feed.post", "deleted", "rev3", payload("3")),
		deleteEvent(testDID, "app.bsky.feed.post", "deleted", "rev4"),
	}
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), events)

	got, err := Reconstruct(context.Background(), Config{
		DataDir:  dataDir,
		DID:      testDID,
		Selector: openSelector(t, dataDir),
	})
	require.NoError(t, err)

	staleUpdatedCID := cbor.ComputeCID(cbor.CodecDagCBOR, payload("1"))
	_, err = got.Blocks.GetBlock(staleUpdatedCID)
	require.Error(t, err)

	deletedCID := cbor.ComputeCID(cbor.CodecDagCBOR, payload("3"))
	_, err = got.Blocks.GetBlock(deletedCID)
	require.Error(t, err)

	currentCID := cbor.ComputeCID(cbor.CodecDagCBOR, payload("2"))
	stored, err := got.Blocks.GetBlock(currentCID)
	require.NoError(t, err)
	require.Equal(t, payload("2"), stored)
}

func TestReconstruct_DeleteRemovesRecord(t *testing.T) {
	t.Parallel()

	dataDir, st := newTestDataDir(t)
	events := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
		createEvent(testDID, "app.bsky.feed.post", "r2", "rev2", payload("2")),
		deleteEvent(testDID, "app.bsky.feed.post", "r1", "rev3"),
	}
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), events)

	wantRoot, wantCount := expectedRoot(t, events)
	got, err := Reconstruct(context.Background(), Config{
		DataDir:  dataDir,
		DID:      testDID,
		Selector: openSelector(t, dataDir),
	})
	require.NoError(t, err)
	require.Equal(t, "rev3", got.LatestRev)
	require.Equal(t, wantRoot, got.Root)
	require.Equal(t, wantCount, got.RecordCount)
}

func TestReconstruct_IgnoresOtherDIDsAndNonCommitEvents(t *testing.T) {
	t.Parallel()

	dataDir, st := newTestDataDir(t)
	matching := createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1"))
	events := []segment.Event{
		createEvent("did:plc:other", "app.bsky.feed.post", "r1", "other-rev1", payload("2")),
		{Kind: segment.KindIdentity, DID: testDID, Rev: "identity-rev"},
		matching,
		{Kind: segment.KindAccount, DID: testDID, Rev: "account-rev"},
		{Kind: segment.KindSync, DID: testDID, Rev: "sync-rev"},
		updateEvent("did:plc:other", "app.bsky.feed.post", "r1", "other-rev2", payload("3")),
	}
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), events)

	wantRoot, wantCount := expectedRoot(t, []segment.Event{matching})
	got, err := Reconstruct(context.Background(), Config{
		DataDir:  dataDir,
		DID:      testDID,
		Selector: openSelector(t, dataDir),
	})
	require.NoError(t, err)
	require.Equal(t, "rev1", got.LatestRev)
	require.Equal(t, wantRoot, got.Root)
	require.Equal(t, wantCount, got.RecordCount)
}

func TestReconstruct_MissingDIDReturnsErrNoLocalRepo(t *testing.T) {
	t.Parallel()

	dataDir, st := newTestDataDir(t)
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), []segment.Event{
		createEvent("did:plc:other", "app.bsky.feed.post", "r1", "rev1", payload("1")),
	})

	_, err := Reconstruct(context.Background(), Config{
		DataDir:  dataDir,
		DID:      testDID,
		Selector: openSelector(t, dataDir),
	})
	require.ErrorIs(t, err, ErrNoLocalRepo)

	emptyDir := t.TempDir()
	_, err = Reconstruct(context.Background(), Config{
		DataDir:  emptyDir,
		DID:      testDID,
		Selector: openSelector(t, emptyDir),
	})
	require.ErrorIs(t, err, ErrNoLocalRepo)
}

func TestReconstruct_ActiveSegmentUsesWalkActive(t *testing.T) {
	t.Parallel()

	dataDir, st := newTestDataDir(t)
	events := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
	}
	writeActiveSegmentTree(t, st, filepath.Join(dataDir, "segments"), events)

	wantRoot, wantCount := expectedRoot(t, events)
	got, err := Reconstruct(context.Background(), Config{
		DataDir:  dataDir,
		DID:      testDID,
		Selector: openSelector(t, dataDir),
	})
	require.NoError(t, err)
	require.Equal(t, "rev1", got.LatestRev)
	require.Equal(t, wantRoot, got.Root)
	require.Equal(t, wantCount, got.RecordCount)
}

func TestReconstruct_IncludesPendingWriterEvents(t *testing.T) {
	t.Parallel()

	dataDir, st := newTestDataDir(t)
	// On disk: one record already flushed to a sealed segment.
	disk := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
	}
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), disk)

	// In the live writer's in-memory pending block but NOT yet flushed to
	// disk: e.g. a like the user just created. Reconstruct must see it so
	// the status page's MST verification matches immediately instead of
	// waiting for the next compaction-driven flush.
	pending := []segment.Event{
		createEvent(testDID, "app.bsky.feed.like", "r2", "rev2", payload("2")),
	}

	wantRoot, wantCount := expectedRoot(t, append(append([]segment.Event(nil), disk...), pending...))
	got, err := Reconstruct(context.Background(), Config{
		DataDir:       dataDir,
		DID:           testDID,
		Selector:      openSelector(t, dataDir),
		PendingEvents: pending,
	})
	require.NoError(t, err)
	require.Equal(t, "rev2", got.LatestRev)
	require.Equal(t, wantRoot, got.Root)
	require.Equal(t, wantCount, got.RecordCount)
}

func TestReconstruct_PendingEventsFilterByDIDAndKind(t *testing.T) {
	t.Parallel()

	dataDir, st := newTestDataDir(t)
	disk := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
	}
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), disk)

	// SnapshotPending returns every pending event in the active block,
	// across all DIDs and kinds. Reconstruct must apply the same filtering
	// it applies to on-disk events: only this DID's commit events count.
	pending := []segment.Event{
		createEvent("did:plc:other", "app.bsky.feed.post", "x1", "rev9", payload("3")),
		{Kind: segment.KindIdentity, DID: testDID, Rev: "identity-rev"},
		updateEvent(testDID, "app.bsky.feed.post", "r1", "rev2", payload("2")),
	}

	wantEvents := []segment.Event{
		disk[0],
		updateEvent(testDID, "app.bsky.feed.post", "r1", "rev2", payload("2")),
	}
	wantRoot, wantCount := expectedRoot(t, wantEvents)
	got, err := Reconstruct(context.Background(), Config{
		DataDir:       dataDir,
		DID:           testDID,
		Selector:      openSelector(t, dataDir),
		PendingEvents: pending,
	})
	require.NoError(t, err)
	require.Equal(t, "rev2", got.LatestRev)
	require.Equal(t, wantRoot, got.Root)
	require.Equal(t, wantCount, got.RecordCount)
}

func TestReconstruct_ValidatesConfig(t *testing.T) {
	t.Parallel()

	_, err := Reconstruct(context.Background(), Config{DID: testDID})
	require.ErrorContains(t, err, "DataDir is required")

	_, err = Reconstruct(context.Background(), Config{DataDir: t.TempDir()})
	require.ErrorContains(t, err, "DID is required")

	_, err = Reconstruct(context.Background(), Config{DataDir: t.TempDir(), DID: testDID})
	require.ErrorContains(t, err, "Selector is required")
}

func newTestDataDir(t *testing.T) (string, *store.Store) {
	t.Helper()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return dataDir, st
}

func writeSegmentTree(t *testing.T, st *store.Store, segmentsDir string, events []segment.Event) {
	t.Helper()
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       segmentsDir,
		Store:             st,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	for i := range events {
		ev := events[i]
		require.NoError(t, w.Append(t.Context(), &ev))
	}
	require.NoError(t, w.SealActiveAndClose())
}

func writeActiveSegmentTree(t *testing.T, st *store.Store, segmentsDir string, events []segment.Event) {
	t.Helper()
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       segmentsDir,
		Store:             st,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)
	for i := range events {
		ev := events[i]
		require.NoError(t, w.Append(t.Context(), &ev))
	}
	require.NoError(t, w.Flush(t.Context()))
	require.NoError(t, w.Close())
}

func expectedRoot(t *testing.T, events []segment.Event) (cbor.CID, int) {
	t.Helper()
	store := mst.NewMemBlockStore()
	tree := mst.NewTree(store)
	records := make(map[string]struct{})
	for _, ev := range events {
		key := ev.Collection + "/" + ev.Rkey
		switch ev.Kind {
		case segment.KindCreate, segment.KindUpdate:
			cid := cbor.ComputeCID(cbor.CodecDagCBOR, ev.Payload)
			require.NoError(t, store.PutBlock(cid, ev.Payload))
			require.NoError(t, tree.Insert(key, cid))
			records[key] = struct{}{}
		case segment.KindDelete:
			require.NoError(t, tree.Remove(key))
			delete(records, key)
		}
	}
	root, err := tree.WriteBlocks(store)
	require.NoError(t, err)
	return root, len(records)
}

func createEvent(did, collection, rkey, rev string, p []byte) segment.Event {
	return segment.Event{
		Kind:       segment.KindCreate,
		DID:        did,
		Collection: collection,
		Rkey:       rkey,
		Rev:        rev,
		Payload:    p,
	}
}

func updateEvent(did, collection, rkey, rev string, p []byte) segment.Event {
	ev := createEvent(did, collection, rkey, rev, p)
	ev.Kind = segment.KindUpdate
	return ev
}

func deleteEvent(did, collection, rkey, rev string) segment.Event {
	return segment.Event{
		Kind:       segment.KindDelete,
		DID:        did,
		Collection: collection,
		Rkey:       rkey,
		Rev:        rev,
	}
}

func payload(value string) []byte {
	if value == "1" {
		return []byte{0xa1, 0x61, 0x61, 0x61, 0x31}
	}
	if value == "2" {
		return []byte{0xa1, 0x61, 0x61, 0x61, 0x32}
	}
	return []byte{0xa1, 0x61, 0x61, 0x61, 0x33}
}
