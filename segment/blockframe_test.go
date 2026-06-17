package segment

import (
	"os"
	"path/filepath"
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
	for b := 0; b < blockCount; b++ {
		for i := 0; i < perBlock; i++ {
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

func TestReadBlockFrame_OutOfRange(t *testing.T) {
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
