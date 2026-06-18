package manifest_test

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func ev(seq uint64, did string) segment.Event {
	return segment.Event{
		Seq: seq, Kind: segment.KindCreate, DID: did,
		Collection: "app.bsky.feed.post", Rkey: "r", Rev: "rev", Payload: []byte{0xa0},
	}
}

func openManifestDir(t *testing.T, dir string) *manifest.Manifest {
	t.Helper()
	m, err := manifest.Open(manifest.Options{
		SegmentsDir: dir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	return m
}

func TestSelectBlocksForDID_PrunesAcrossSegments(t *testing.T) {
	t.Parallel()

	const target = "did:plc:needle"
	dir := t.TempDir()

	// Segment 0: target absent entirely -> must be skipped via segment bloom.
	mustWriteSealedSegmentWithEvents(t, filepath.Join(dir, "seg_0000000000.jss"), 1, []segment.Event{
		ev(0, "did:plc:a"),
		ev(1, "did:plc:b"),
	})
	// Segment 1: target present in block 1 only (one event per block).
	mustWriteSealedSegmentWithEvents(t, filepath.Join(dir, "seg_0000000001.jss"), 1, []segment.Event{
		ev(2, "did:plc:c"),
		ev(3, target),
		ev(4, "did:plc:d"),
	})

	m := openManifestDir(t, dir)

	sel, err := m.SelectBlocksForDID(target)
	require.NoError(t, err)

	// Segment 0 must not appear at all (segment-level bloom miss).
	for _, s := range sel {
		require.NotEqual(t, uint64(0), s.Idx, "segment 0 has no target events and must be pruned")
	}

	// Segment 1 must appear, and its selection must include block 1
	// (no false negative). Selections are ascending by Idx.
	var seg1 *manifest.SegmentBlockSelection
	for i := range sel {
		if sel[i].Idx == 1 {
			seg1 = &sel[i]
		}
	}
	require.NotNil(t, seg1, "segment 1 holds the target and must be selected")
	require.Contains(t, seg1.Blocks, 1)
	require.Equal(t, seg1.Path, filepath.Join(dir, "seg_0000000001.jss"))
	require.Less(t, len(seg1.Blocks), 3, "must prune at least one decoy block")
}

func TestSelectBlocksForDID_AbsentDIDSelectsNothing(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	mustWriteSealedSegmentWithEvents(t, filepath.Join(dir, "seg_0000000000.jss"), 2, []segment.Event{
		ev(0, "did:plc:a"),
		ev(1, "did:plc:b"),
	})
	m := openManifestDir(t, dir)

	sel, err := m.SelectBlocksForDID("did:plc:not-present-anywhere")
	require.NoError(t, err)
	require.Empty(t, sel)
}

func TestSelectBlocksForDID_EmptyManifest(t *testing.T) {
	t.Parallel()

	m := openManifestDir(t, t.TempDir())
	sel, err := m.SelectBlocksForDID("did:plc:anything")
	require.NoError(t, err)
	require.Empty(t, sel)
}

func TestActiveSegmentPaths(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// One sealed segment (idx 0) and one active/unsealed segment (idx 1).
	mustWriteSealedSegmentWithEvents(t, filepath.Join(dir, "seg_0000000000.jss"), 4, []segment.Event{
		ev(0, "did:plc:a"),
	})
	mustWriteEmptyActiveSegment(t, filepath.Join(dir, "seg_0000000001.jss"))

	m := openManifestDir(t, dir)
	require.Equal(t, 1, m.SegmentCount(), "only the sealed segment is resident")

	active, err := m.ActiveSegmentPaths()
	require.NoError(t, err)
	require.Equal(t, []string{filepath.Join(dir, "seg_0000000001.jss")}, active,
		"the unsealed segment is not in the manifest and must be reported for direct scan")
}

func TestActiveSegmentPaths_NoneWhenAllSealed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	mustWriteSealedSegmentWithEvents(t, filepath.Join(dir, "seg_0000000000.jss"), 4, []segment.Event{
		ev(0, "did:plc:a"),
	})
	m := openManifestDir(t, dir)

	active, err := m.ActiveSegmentPaths()
	require.NoError(t, err)
	require.Empty(t, active)
}

func TestActiveSegmentPaths_EmptyDir(t *testing.T) {
	t.Parallel()

	m := openManifestDir(t, t.TempDir())
	active, err := m.ActiveSegmentPaths()
	require.NoError(t, err)
	require.Empty(t, active)
}
