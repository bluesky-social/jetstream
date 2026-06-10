package segment

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRewriteDropsRowsAndKeepsEmptyBlocks(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for _, ev := range []Event{
		{Seq: 1, IndexedAt: 10, Kind: KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r1"},
		{Seq: 2, IndexedAt: 20, Kind: KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r2"},
		{Seq: 3, IndexedAt: 30, Kind: KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r1"},
	} {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)

	res, err := Rewrite(path, func(ev *Event) RowDecision {
		if ev.Kind == KindCreate {
			return RowDrop
		}
		return RowKeep
	}, RewriteOptions{})
	require.NoError(t, err)
	require.True(t, res.Rewritten)
	require.EqualValues(t, 2, res.RowsDropped)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	require.EqualValues(t, 2, r.Header().BlockCount)
	require.EqualValues(t, 1, r.Header().EventCount)
	require.EqualValues(t, 0, r.Blocks()[0].EventCount)
	require.EqualValues(t, 1, r.Blocks()[1].EventCount)

	b0, err := r.DecodeBlock(0)
	require.NoError(t, err)
	require.Empty(t, b0)
	b1, err := r.DecodeBlock(1)
	require.NoError(t, err)
	require.Len(t, b1, 1)
	require.Equal(t, KindDelete, b1[0].Kind)
}

func TestRewriteZeroDropLeavesFileUntouched(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir, []Event{{Seq: 1, IndexedAt: 10, Kind: KindCreate, DID: "did:plc:a"}}, 2)
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	res, err := Rewrite(path, func(ev *Event) RowDecision { return RowKeep }, RewriteOptions{})
	require.NoError(t, err)
	require.False(t, res.Rewritten)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
