package segment

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
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
		{Seq: 1, WitnessedAt: 10, Kind: KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r1"},
		{Seq: 2, WitnessedAt: 20, Kind: KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r2"},
		{Seq: 3, WitnessedAt: 30, Kind: KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r1"},
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
	path := sealedSegmentForReader(t, dir, []Event{{Seq: 1, WitnessedAt: 10, Kind: KindCreate, DID: "did:plc:a"}}, 2)
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	res, err := Rewrite(path, func(ev *Event) RowDecision { return RowKeep }, RewriteOptions{})
	require.NoError(t, err)
	require.False(t, res.Rewritten)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestRewritePreservesSourcePerBlockBloomParams(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir, []Event{
		{Seq: 1, WitnessedAt: 10, Kind: KindCreate, DID: "did:plc:a"},
		{Seq: 2, WitnessedAt: 20, Kind: KindCreate, DID: "did:plc:b"},
	}, 2)

	before, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	sourceBloom, err := before.BlockBloom(0)
	require.NoError(t, err)
	sourceBlocks := sourceBloom.NumBlocks()
	sourceK := sourceBloom.K()
	require.NoError(t, before.Close())

	res, err := Rewrite(path, func(ev *Event) RowDecision {
		if ev.Seq == 2 {
			return RowDrop
		}
		return RowKeep
	}, RewriteOptions{})
	require.NoError(t, err)
	require.True(t, res.Rewritten)

	after, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = after.Close() })
	rewrittenBloom, err := after.BlockBloom(0)
	require.NoError(t, err)
	require.Equal(t, sourceBlocks, rewrittenBloom.NumBlocks())
	require.Equal(t, sourceK, rewrittenBloom.K())
	require.True(t, rewrittenBloom.TestString("did:plc:a"))
}

func TestRewriteCandidateDIDsSkipDisjointSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir, []Event{{Seq: 1, WitnessedAt: 10, Kind: KindCreate, DID: "did:plc:a"}}, 2)
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
		point             string
		wantRewrittenFile bool
	}{
		{name: "temp written", point: CrashPointRewriteTempWritten, wantRewrittenFile: false},
		{name: "temp synced", point: CrashPointRewriteTempSynced, wantRewrittenFile: false},
		{name: "renamed", point: CrashPointRewriteRenamed, wantRewrittenFile: true},
		{name: "dir synced", point: CrashPointRewriteDirSynced, wantRewrittenFile: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := sealedSegmentForReader(t, t.TempDir(), []Event{
				{Seq: 1, WitnessedAt: 10, Kind: KindCreate, DID: "did:plc:a"},
				{Seq: 2, WitnessedAt: 20, Kind: KindDelete, DID: "did:plc:a"},
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
	point string
	err   error
}

func (i rewritePointInjector) SimulateCrash(_ context.Context, point string) error {
	if point == i.point {
		return i.err
	}
	return nil
}

func TestRewriteAllDropProducesValidSealedSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	events := []Event{
		{Seq: 5, WitnessedAt: 10, Kind: KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r1"},
		{Seq: 6, WitnessedAt: 20, Kind: KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r2"},
		{Seq: 7, WitnessedAt: 30, Kind: KindUpdate, DID: "did:plc:b", Collection: "c", Rkey: "r3"},
	}
	for _, ev := range events {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)

	res, err := Rewrite(path, func(*Event) RowDecision { return RowDrop }, RewriteOptions{})
	require.NoError(t, err)
	require.True(t, res.Rewritten)
	require.EqualValues(t, 3, res.RowsDropped)

	// The all-dropped file stays a valid, reopenable sealed segment
	// with its historical seq envelope intact (spec §6).
	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	require.EqualValues(t, 0, r.Header().EventCount)
	require.EqualValues(t, 0, r.Header().UniqueDIDCount)
	require.EqualValues(t, 5, r.Header().MinSeq)
	require.EqualValues(t, 7, r.Header().MaxSeq)
	require.EqualValues(t, 2, r.Header().BlockCount)
	for i := range int(r.Header().BlockCount) {
		evs, err := r.DecodeBlock(i)
		require.NoError(t, err)
		require.Empty(t, evs)
	}

	// Idempotency: a second rewrite has nothing to drop and must not touch the file.
	res2, err := Rewrite(path, func(*Event) RowDecision { return RowDrop }, RewriteOptions{})
	require.NoError(t, err)
	require.False(t, res2.Rewritten)
}

func TestRewritePreservesSeqEnvelopeWhenEdgeRowsDrop(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir, []Event{
		{Seq: 10, WitnessedAt: 10, Kind: KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r1"},
		{Seq: 11, WitnessedAt: 20, Kind: KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r2"},
	}, 4)

	res, err := Rewrite(path, func(ev *Event) RowDecision {
		if ev.Seq == 11 {
			return RowDrop
		}
		return RowKeep
	}, RewriteOptions{})
	require.NoError(t, err)
	require.True(t, res.Rewritten)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	require.EqualValues(t, 10, r.Header().MinSeq)
	require.EqualValues(t, 11, r.Header().MaxSeq, "historical envelope must survive edge-row drops")
	require.EqualValues(t, 11, r.Blocks()[0].MaxSeq)
	require.EqualValues(t, 1, r.Header().EventCount, "counts describe contents and must be true")
}

func TestRewriteZeroDropKeepsInodeAndMtime(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir, []Event{{Seq: 1, WitnessedAt: 10, Kind: KindCreate, DID: "did:plc:a"}}, 2)
	before, err := os.Stat(path)
	require.NoError(t, err)

	res, err := Rewrite(path, func(*Event) RowDecision { return RowKeep }, RewriteOptions{})
	require.NoError(t, err)
	require.False(t, res.Rewritten)

	after, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, before.ModTime(), after.ModTime(), "zero-drop rewrite must not touch the file")
	beforeSys, ok := before.Sys().(*syscall.Stat_t)
	require.True(t, ok)
	afterSys, ok := after.Sys().(*syscall.Stat_t)
	require.True(t, ok)
	require.Equal(t, beforeSys.Ino, afterSys.Ino, "zero-drop rewrite must not replace the inode")
}

// FuzzRewrite feeds arbitrary bytes to Rewrite: it must error (never
// panic) on corrupt input and leave the source file untouched.
func FuzzRewrite(f *testing.F) {
	dir := f.TempDir()
	seedPath := sealedSegmentForReader(f, dir, []Event{
		{Seq: 1, WitnessedAt: 10, Kind: KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r1"},
		{Seq: 2, WitnessedAt: 20, Kind: KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r1"},
	}, 2)
	seed, err := os.ReadFile(seedPath)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add(seed[:len(seed)/2])
	f.Add([]byte{})
	corrupted := append([]byte(nil), seed...)
	corrupted[300] ^= 0xff
	f.Add(corrupted)

	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "seg.jss")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		res, err := Rewrite(path, func(*Event) RowDecision { return RowDrop }, RewriteOptions{})
		after, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatalf("source file unreadable after rewrite attempt: %v", readErr)
		}
		if err != nil {
			if !bytes.Equal(data, after) {
				t.Fatalf("failed rewrite mutated the source file")
			}
			return
		}
		if res.Rewritten {
			// A successful rewrite of fuzz input must still produce a
			// file the reader accepts.
			r, openErr := Open(ReaderConfig{Path: path})
			if openErr != nil {
				t.Fatalf("rewritten file does not reopen: %v", openErr)
			}
			_ = r.Close()
		}
	})
}
