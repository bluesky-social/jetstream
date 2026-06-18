package segment

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// buildMultiBlockSegment writes a sealed segment with blockCount blocks of
// perBlock events each and returns its path.
func buildMultiBlockSegment(t *testing.T, perBlock, blockCount int) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg_test.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: perBlock})
	require.NoError(t, err)
	var seq uint64
	for b := range blockCount {
		for range perBlock {
			seq++
			_, err = w.Append(Event{
				Seq:        seq,
				IndexedAt:  int64(1_730_000_000_000_000 + seq*1_000),
				Kind:       KindCreate,
				DID:        "did:plc:test",
				Collection: "app.bsky.feed.post",
				Rkey:       "rkey",
				Rev:        "rev",
				Payload:    []byte{0xa0},
			})
			require.NoError(t, err)
		}
		if b < blockCount-1 {
			err = w.Flush()
			require.NoError(t, err)
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
	return path
}

func TestReadBlockFrame_MatchesOnDiskAndDecodes(t *testing.T) {
	t.Parallel()

	path := buildMultiBlockSegment(t, 2, 3) // 3 blocks, 2 events each

	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	hdr, err := ReadSealedHeader(f)
	require.NoError(t, err)
	require.Equal(t, uint32(3), hdr.BlockCount)

	// Reader for cross-checking decoded events + raw offsets.
	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	infos := r.Blocks()

	for idx := 0; idx < int(hdr.BlockCount); idx++ {
		frame, err := ReadBlockFrame(f, hdr, idx)
		require.NoError(t, err)

		// Byte-identical to an independent slice read of [Offset+8, +CompressedSize).
		want := make([]byte, infos[idx].CompressedSize)
		_, err = f.ReadAt(want, int64(infos[idx].Offset)+8)
		require.NoError(t, err)
		require.Equal(t, want, frame, "block %d frame bytes", idx)

		// Decodes to the same events as DecodeBlock.
		gotEvents, _, err := decodeBlockCompressedSized(frame)
		require.NoError(t, err)
		wantEvents, err := r.DecodeBlock(idx)
		require.NoError(t, err)
		require.Equal(t, wantEvents, gotEvents, "block %d events", idx)
	}
}

func TestDecodeBlockFrame_RoundTrip(t *testing.T) {
	t.Parallel()

	path := buildMultiBlockSegment(t, 4, 2)
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	hdr, err := ReadSealedHeader(f)
	require.NoError(t, err)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	defer func() { _ = r.Close() }()

	for idx := 0; idx < int(hdr.BlockCount); idx++ {
		frame, err := ReadBlockFrame(f, hdr, idx)
		require.NoError(t, err)

		got, err := DecodeBlockFrame(frame)
		require.NoError(t, err)
		want, err := r.DecodeBlock(idx)
		require.NoError(t, err)
		require.Equal(t, want, got, "block %d events via DecodeBlockFrame", idx)
	}
}

func TestDecodeBlockFrame_RejectsGarbage(t *testing.T) {
	t.Parallel()
	_, err := DecodeBlockFrame([]byte("not a zstd frame"))
	require.Error(t, err)
}

func TestReadBlockFrame_OutOfRange(t *testing.T) {
	t.Parallel()

	path := buildMultiBlockSegment(t, 2, 2)
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	hdr, err := ReadSealedHeader(f)
	require.NoError(t, err)

	_, err = ReadBlockFrame(f, hdr, -1)
	require.ErrorIs(t, err, ErrBlockOutOfRange)
	_, err = ReadBlockFrame(f, hdr, int(hdr.BlockCount))
	require.ErrorIs(t, err, ErrBlockOutOfRange)
}

func TestReadBlockFrame_RejectsOverflowingBlockRange(t *testing.T) {
	t.Parallel()

	path := buildMultiBlockSegment(t, 2, 1)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	hdr, err := ReadSealedHeader(f)
	require.NoError(t, err)

	entry := make([]byte, blockIndexEntrySize)
	binary.LittleEndian.PutUint64(entry[0:8], math.MaxUint64-4)
	binary.LittleEndian.PutUint32(entry[8:12], 0)
	_, err = f.WriteAt(entry, int64(hdr.BlockIndexOffset))
	require.NoError(t, err)

	_, err = ReadBlockFrame(f, hdr, 0)
	require.ErrorIs(t, err, ErrInvalidBlockIndex)
}

func TestReadBlockFrame_RejectsInvalidHeaderOffsets(t *testing.T) {
	t.Parallel()

	path := buildMultiBlockSegment(t, 2, 1)
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	hdr, err := ReadSealedHeader(f)
	require.NoError(t, err)

	badBlockIndex := hdr
	badBlockIndex.BlockIndexOffset = uint64(ReservedHeaderBytes)
	_, err = ReadBlockFrame(f, badBlockIndex, 0)
	require.ErrorIs(t, err, ErrInvalidFooter)

	hugeEntryOffset := hdr
	hugeEntryOffset.BlockIndexOffset = math.MaxUint64
	hugeEntryOffset.FooterOffset = math.MaxUint64
	_, err = ReadBlockFrame(strings.NewReader(""), hugeEntryOffset, 0)
	require.ErrorIs(t, err, ErrInvalidFooter)
}

func TestReadBlockFrame_EmptyCompactedBlockDecodesToZeroEvents(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg_empty_block.jss")
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

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	require.EqualValues(t, 0, r.Blocks()[0].EventCount)

	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	hdr, err := ReadSealedHeader(f)
	require.NoError(t, err)
	frame, err := ReadBlockFrame(f, hdr, 0)
	require.NoError(t, err)
	events, _, err := decodeBlockCompressedSized(frame)
	require.NoError(t, err)
	require.Empty(t, events)
}
