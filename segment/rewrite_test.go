package segment

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/crashpoint"
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

func TestRewriteCandidateDIDsSkipDisjointSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir, []Event{{Seq: 1, IndexedAt: 10, Kind: KindCreate, DID: "did:plc:a"}}, 2)
	res, err := Rewrite(path, func(*Event) RowDecision {
		t.Fatal("decider must not run when candidate DIDs miss the segment bloom")
		return RowKeep
	}, RewriteOptions{CandidateDIDs: []string{"did:plc:not-present"}})
	require.NoError(t, err)
	require.False(t, res.Rewritten)
}

func TestRewriteCrashSeams(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name              string
		point             crashpoint.Point
		wantRewrittenFile bool
	}{
		{name: "temp written", point: crashpoint.AfterSegmentRewriteTempWritten, wantRewrittenFile: false},
		{name: "temp synced", point: crashpoint.AfterSegmentRewriteTempSynced, wantRewrittenFile: false},
		{name: "renamed", point: crashpoint.AfterSegmentRewriteRenamed, wantRewrittenFile: true},
		{name: "dir synced", point: crashpoint.AfterSegmentRewriteDirSynced, wantRewrittenFile: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := sealedSegmentForReader(t, t.TempDir(), []Event{
				{Seq: 1, IndexedAt: 10, Kind: KindCreate, DID: "did:plc:a"},
				{Seq: 2, IndexedAt: 20, Kind: KindDelete, DID: "did:plc:a"},
			}, 2)

			crashErr := errors.New("simulated segment rewrite crash")
			_, err := Rewrite(path, func(ev *Event) RowDecision {
				if ev.Kind == KindCreate {
					return RowDrop
				}
				return RowKeep
			}, RewriteOptions{CrashInjector: rewritePointInjector{point: tc.point, err: crashErr}})
			require.ErrorIs(t, err, crashErr)

			r, err := Open(ReaderConfig{Path: path})
			require.NoError(t, err)
			t.Cleanup(func() { _ = r.Close() })
			require.EqualValues(t, 1, r.Header().BlockCount)
			if tc.wantRewrittenFile {
				require.EqualValues(t, 1, r.Header().EventCount)
				block, err := r.DecodeBlock(0)
				require.NoError(t, err)
				require.Len(t, block, 1)
				require.Equal(t, KindDelete, block[0].Kind)
			} else {
				require.EqualValues(t, 2, r.Header().EventCount)
				block, err := r.DecodeBlock(0)
				require.NoError(t, err)
				require.Len(t, block, 2)
			}
		})
	}
}

type rewritePointInjector struct {
	point crashpoint.Point
	err   error
}

func (i rewritePointInjector) SimulateCrash(_ context.Context, point crashpoint.Point) error {
	if point == i.point {
		return i.err
	}
	return nil
}
