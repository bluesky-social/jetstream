package repoexport

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
	"github.com/stretchr/testify/require"
)

const testDID = "did:plc:repoexport"

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
		DataDir: dataDir,
		DID:     testDID,
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
		DataDir: dataDir,
		DID:     testDID,
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
		DataDir: dataDir,
		DID:     testDID,
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
		DataDir: dataDir,
		DID:     testDID,
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
		DataDir: dataDir,
		DID:     testDID,
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
		DataDir: dataDir,
		DID:     testDID,
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
		DataDir: dataDir,
		DID:     testDID,
	})
	require.ErrorIs(t, err, ErrNoLocalRepo)

	_, err = Reconstruct(context.Background(), Config{
		DataDir: t.TempDir(),
		DID:     testDID,
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
		DataDir: dataDir,
		DID:     testDID,
	})
	require.NoError(t, err)
	require.Equal(t, "rev1", got.LatestRev)
	require.Equal(t, wantRoot, got.Root)
	require.Equal(t, wantCount, got.RecordCount)
}

func TestReconstruct_ValidatesConfig(t *testing.T) {
	t.Parallel()

	_, err := Reconstruct(context.Background(), Config{DID: testDID})
	require.ErrorContains(t, err, "DataDir is required")

	_, err = Reconstruct(context.Background(), Config{DataDir: t.TempDir()})
	require.ErrorContains(t, err, "DID is required")
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
