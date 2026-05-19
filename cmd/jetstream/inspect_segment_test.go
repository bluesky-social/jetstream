package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

// makeSealedFixture builds a minimal sealed segment for the CLI tests.
func makeSealedFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for _, seq := range []uint64{1, 2, 3, 4} {
		full, err := w.Append(segment.Event{
			Seq:        seq,
			IndexedAt:  int64(1_700_000_000_000_000) + int64(seq),
			Kind:       segment.KindCreate,
			DID:        "did:plc:demo",
			Collection: "app.bsky.feed.post",
			Rkey:       "r",
			Rev:        "v",
			Payload:    []byte("p"),
		})
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
	return path
}

func TestRenderInspection_SealedTableMode(t *testing.T) {
	t.Parallel()

	path := makeSealedFixture(t)
	ins, err := segment.Inspect(path)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, renderInspection(&buf, ins, "table", 100))

	out := buf.String()
	require.Contains(t, out, "state: sealed")
	require.Contains(t, out, "checksum:")
	require.Contains(t, out, "(valid)")
	require.Contains(t, out, "block_count:")
	require.Contains(t, out, "footer layout")
	require.Contains(t, out, "blocks (2 total)")
	require.Contains(t, out, "app.bsky.feed.post")
}

func TestRenderInspection_SummaryModeOmitsBlockTable(t *testing.T) {
	t.Parallel()

	path := makeSealedFixture(t)
	ins, err := segment.Inspect(path)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, renderInspection(&buf, ins, "summary", 100))
	out := buf.String()
	require.Contains(t, out, "state: sealed")
	require.NotContains(t, out, "blocks (")
}

func TestRenderInspection_FullModeListsPerBlockCollections(t *testing.T) {
	t.Parallel()

	path := makeSealedFixture(t)
	ins, err := segment.Inspect(path)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, renderInspection(&buf, ins, "full", 100))
	out := buf.String()
	require.Contains(t, out, "collections:")
	require.Contains(t, out, "app.bsky.feed.post")
}

func TestRenderInspection_CorruptChecksumLabelled(t *testing.T) {
	t.Parallel()

	path := makeSealedFixture(t)

	// Corrupt the per-block-blooms body so Reader.Open succeeds but
	// the xxh3 fails. (The segment-level bloom is tighter — corrupting
	// it can break gloom.UnmarshalBinary.)
	ins0, err := segment.Inspect(path)
	require.NoError(t, err)
	regionStart := int64(ins0.Header.BlockDIDBloomOffset) + 8
	regionEnd := int64(ins0.Header.CollectionIndexOffset)
	require.Greater(t, regionEnd, regionStart)
	off := regionStart + (regionEnd-regionStart)/2

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	var b [1]byte
	_, err = f.ReadAt(b[:], off)
	require.NoError(t, err)
	b[0] ^= 0xff
	_, err = f.WriteAt(b[:], off)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	ins, err := segment.Inspect(path)
	require.NoError(t, err)
	require.False(t, ins.ChecksumValid)

	var buf bytes.Buffer
	require.NoError(t, renderInspection(&buf, ins, "table", 100))
	require.Contains(t, buf.String(), "(invalid)")
}

func TestRenderInspection_ActiveFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "active.jss")
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for _, seq := range []uint64{1, 2} {
		_, err := w.Append(segment.Event{Seq: seq, Kind: segment.KindCreate, DID: "d", Collection: "c", Rkey: "r", Rev: "v"})
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())

	ins, err := segment.Inspect(path)
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, renderInspection(&buf, ins, "table", 100))

	out := buf.String()
	require.Contains(t, out, "state: active")
	require.Contains(t, out, "footer layout: not present (active file)")
}

func TestRenderInspection_BlockTruncation(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "many.jss")
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	for seq := uint64(1); seq <= 20; seq++ {
		full, err := w.Append(segment.Event{Seq: seq, Kind: segment.KindCreate, DID: "d", Collection: "c", Rkey: "r", Rev: "v"})
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)

	ins, err := segment.Inspect(path)
	require.NoError(t, err)
	require.EqualValues(t, 20, ins.Header.BlockCount)

	var buf bytes.Buffer
	require.NoError(t, renderInspection(&buf, ins, "table", 6))
	out := buf.String()
	require.Contains(t, out, "rows omitted")
}

func TestInspectSegmentCommand_EndToEndAgainstRealFile(t *testing.T) {
	t.Parallel()

	path := makeSealedFixture(t)

	var buf bytes.Buffer
	app := newApp()
	app.Writer = &buf

	err := app.Run(t.Context(), []string{
		"jetstream", "inspect-segment",
		"--blocks=full",
		"--blocks-truncate=0",
		path,
	})
	require.NoError(t, err)

	out := buf.String()
	require.Contains(t, out, "state: sealed")
	require.Contains(t, out, "(valid)")
	require.Contains(t, out, "blocks (")
	require.Contains(t, out, "collections:")
}

func TestInspectSegmentCommand_RejectsBadFlag(t *testing.T) {
	t.Parallel()
	path := makeSealedFixture(t)

	app := newApp()
	app.Writer = new(bytes.Buffer)
	err := app.Run(t.Context(), []string{
		"jetstream", "inspect-segment", "--blocks=foo", path,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "--blocks")
}

func TestInspectSegmentCommand_RejectsMissingArg(t *testing.T) {
	t.Parallel()
	app := newApp()
	app.Writer = new(bytes.Buffer)
	err := app.Run(t.Context(), []string{"jetstream", "inspect-segment"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected exactly one path argument")
}
