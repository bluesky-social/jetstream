package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// sealedParts splits a sealed segment file into its header, its block
// frames (length prefixes stripped, in block order), and its footer.
func sealedParts(t testing.TB, data []byte) (header []byte, frames [][]byte, footer []byte) {
	t.Helper()
	h, err := decodeHeader(data[:ReservedHeaderBytes])
	require.NoError(t, err)
	off := uint64(ReservedHeaderBytes)
	for off < h.FooterOffset {
		n := binary.LittleEndian.Uint64(data[off : off+8])
		frames = append(frames, data[off+8:off+8+n])
		off += 8 + n
	}
	require.Equal(t, h.FooterOffset, off)
	return data[:ReservedHeaderBytes], frames, data[h.FooterOffset:]
}

// assembleSegment lays header, frames, and footer out as the segment
// file they describe.
func assembleSegment(header []byte, frames [][]byte, footer []byte) []byte {
	out := append([]byte(nil), header...)
	for _, fr := range frames {
		out = binary.LittleEndian.AppendUint64(out, uint64(len(fr)))
		out = append(out, fr...)
	}
	return append(out, footer...)
}

// encodeFrames builds block frames from events with a BlockBuilder,
// cutting a block every maxPerBlock events, the way a Writer does.
func encodeFrames(t testing.TB, events []Event, maxPerBlock int) [][]byte {
	t.Helper()
	b, err := NewBlockBuilder(maxPerBlock)
	require.NoError(t, err)
	var frames [][]byte
	for _, ev := range events {
		full, err := b.Append(ev)
		require.NoError(t, err)
		if full {
			fr, _ := b.Encode()
			frames = append(frames, fr)
		}
	}
	if fr, _ := b.Encode(); fr != nil {
		frames = append(frames, fr)
	}
	return frames
}

// frameFetcher serves frames as a BlockFetcher.
func frameFetcher(frames [][]byte) BlockFetcher {
	return func(i int) ([]byte, error) {
		if i < 0 || i >= len(frames) {
			return nil, fmt.Errorf("no block %d", i)
		}
		return frames[i], nil
	}
}

// openAllReaders opens the sealed segment in data through all three
// constructors: the file at path, a bytes.Reader, and header+footer
// with a frame fetcher.
func openAllReaders(t testing.TB, path string, data []byte) map[string]*Reader {
	t.Helper()
	header, frames, footer := sealedParts(t, data)
	file, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	at, err := OpenReaderAt(bytes.NewReader(data), int64(len(data)), ReaderOptions{Name: "bytes"})
	require.NoError(t, err)
	parts, err := OpenReaderParts(header, footer, frameFetcher(frames), ReaderOptions{Name: "parts"})
	require.NoError(t, err)
	readers := map[string]*Reader{"file": file, "readerat": at, "parts": parts}
	t.Cleanup(func() {
		for _, r := range readers {
			_ = r.Close()
		}
	})
	return readers
}

// requireReadersAgree checks that every reader answers every query the
// same way as the file reader.
func requireReadersAgree(t testing.TB, readers map[string]*Reader, dids []string) {
	t.Helper()
	ref := readers["file"]
	for name, r := range readers {
		require.Equalf(t, ref.Header(), r.Header(), "%s header", name)
		require.Equalf(t, ref.Blocks(), r.Blocks(), "%s blocks", name)
		require.Equalf(t, ref.Collections(), r.Collections(), "%s collections", name)
		require.Equalf(t, ref.CollectionEventCounts(), r.CollectionEventCounts(), "%s collection counts", name)
		require.NoErrorf(t, VerifySealedMetadata(r), "%s verify", name)

		for i := range ref.Blocks() {
			want, err := ref.DecodeBlock(i)
			require.NoError(t, err)
			got, err := r.DecodeBlock(i)
			require.NoErrorf(t, err, "%s decode block %d", name, i)
			require.Lenf(t, got, len(want), "%s block %d", name, i)
			for j := range want {
				require.Truef(t, eventsEqual(want[j], got[j]), "%s block %d event %d", name, i, j)
			}

			wantBloom, err := ref.BlockBloom(i)
			require.NoError(t, err)
			gotBloom, err := r.BlockBloom(i)
			require.NoErrorf(t, err, "%s bloom %d", name, i)
			wb, err := wantBloom.MarshalBinary()
			require.NoError(t, err)
			gb, err := gotBloom.MarshalBinary()
			require.NoError(t, err)
			require.Equalf(t, wb, gb, "%s bloom %d", name, i)

			wantCols, err := ref.BlockCollections(i)
			require.NoError(t, err)
			gotCols, err := r.BlockCollections(i)
			require.NoError(t, err)
			require.Equalf(t, wantCols, gotCols, "%s block %d collections", name, i)
		}
		for _, did := range append(dids, "did:plc:absent") {
			want, err := ref.BlocksContainingDID(did)
			require.NoError(t, err)
			got, err := r.BlocksContainingDID(did)
			require.NoError(t, err)
			require.Equalf(t, want, got, "%s BlocksContainingDID(%s)", name, did)
		}
	}
}

// TestBuildSealedMatchesFileSeal is the property that makes BuildSealed
// safe to use in place of the file seal: over swarm-generated segments,
// its header and footer equal the file seal's byte for byte, frames a
// BlockBuilder encodes equal the Writer's, and all three Reader
// constructors agree on the result.
func TestBuildSealedMatchesFileSeal(t *testing.T) {
	t.Parallel()

	iters := 30
	if !testing.Short() {
		iters = 500
	}
	r := rand.New(rand.NewPCG(17, 23))
	for it := range iters {
		axes, events, maxPerBlock := genSwarmSegment(r)
		path := filepath.Join(t.TempDir(), "seg.jss")
		sealRes := writeSwarmSegment(t, path, events, maxPerBlock)
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		fileHeader, fileFrames, fileFooter := sealedParts(t, data)

		frames := encodeFrames(t, events, maxPerBlock)
		require.Equalf(t, fileFrames, frames, "iter %d: BlockBuilder frames differ from the Writer's (axes=%+v)", it, axes)

		header, footer, h, err := BuildSealed(SliceFrameSource(frames))
		require.NoErrorf(t, err, "iter %d", it)
		require.Equalf(t, fileHeader, header, "iter %d header (axes=%+v)", it, axes)
		require.Equalf(t, fileFooter, footer, "iter %d footer (axes=%+v)", it, axes)
		require.Equal(t, sealRes.Checksum, h.Checksum)
		require.Equal(t, sealRes.FooterOffset, h.FooterOffset)
		require.Equal(t, data, assembleSegment(header, frames, footer))

		dids := make([]string, 0, len(events))
		for _, ev := range events {
			dids = append(dids, ev.DID)
		}
		requireReadersAgree(t, openAllReaders(t, path, data), dids)
	}
}

// TestBuildSealedEmptyBlockEndsIndex pins the empty-block break: a
// zero-event frame ends the block index, for BuildSealed exactly as for
// the file seal, while every frame's bytes still count toward the
// footer offset.
func TestBuildSealedEmptyBlockEndsIndex(t *testing.T) {
	t.Parallel()

	a := encodeFrames(t, []Event{
		{Seq: 1, WitnessedAt: 10, Kind: KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r1"},
		{Seq: 2, WitnessedAt: 20, Kind: KindCreate, DID: "did:plc:b", Collection: "c", Rkey: "r2"},
	}, 2)
	require.Len(t, a, 1)
	b := encodeFrames(t, []Event{
		{Seq: 3, WitnessedAt: 30, Kind: KindCreate, DID: "did:plc:c", Collection: "d", Rkey: "r3"},
	}, 2)
	frames := [][]byte{a[0], encodeEmptyBlockCompressed(), b[0]}

	header, footer, h, err := BuildSealed(SliceFrameSource(frames))
	require.NoError(t, err)
	require.EqualValues(t, 1, h.BlockCount)
	require.EqualValues(t, 2, h.EventCount)
	require.EqualValues(t, 2, h.MaxSeq)
	wantFooterOffset := uint64(ReservedHeaderBytes)
	for _, fr := range frames {
		wantFooterOffset += 8 + uint64(len(fr))
	}
	require.Equal(t, wantFooterOffset, h.FooterOffset)

	// The file seal of an active file holding the same frames. Writers
	// never flush an empty block, so the tail is written by hand.
	path := filepath.Join(t.TempDir(), "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	tail := assembleSegment(nil, frames, nil)
	_, err = w.file.WriteAt(tail, ReservedHeaderBytes)
	require.NoError(t, err)
	_, err = w.Seal()
	require.NoError(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, header, data[:ReservedHeaderBytes])
	require.Equal(t, footer, data[h.FooterOffset:])
}

func TestBuildSealedEmpty(t *testing.T) {
	t.Parallel()
	header, footer, h, err := BuildSealed(SliceFrameSource(nil))
	require.NoError(t, err)
	require.EqualValues(t, 0, h.BlockCount)
	require.EqualValues(t, ReservedHeaderBytes, h.FooterOffset)

	path := filepath.Join(t.TempDir(), "seg.jss")
	writeSwarmSegment(t, path, nil, 4)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, data, assembleSegment(header, nil, footer))

	r, err := OpenReaderParts(header, footer, frameFetcher(nil), ReaderOptions{})
	require.NoError(t, err)
	require.NoError(t, VerifySealedMetadata(r))
	got, err := r.BlocksContainingDID("did:plc:a")
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestBuildSealedRejectsCorruptFrames(t *testing.T) {
	t.Parallel()
	_, _, _, err := BuildSealed(SliceFrameSource([][]byte{[]byte("not zstd")}))
	require.Error(t, err)

	srcErr := errors.New("source failed")
	_, _, _, err = BuildSealed(errFrameSource{srcErr})
	require.ErrorIs(t, err, srcErr)
}

type errFrameSource struct{ err error }

func (s errFrameSource) NextFrame() ([]byte, error) { return nil, s.err }

func TestReaderPartsAndReaderAtErrors(t *testing.T) {
	t.Parallel()
	events := []Event{
		{Seq: 1, WitnessedAt: 10, Kind: KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r1"},
		{Seq: 2, WitnessedAt: 20, Kind: KindCreate, DID: "did:plc:b", Collection: "c", Rkey: "r2"},
	}
	frames := encodeFrames(t, events, 1)
	header, footer, _, err := BuildSealed(SliceFrameSource(frames))
	require.NoError(t, err)

	_, err = OpenReaderAt(nil, 0, ReaderOptions{})
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = OpenReaderParts(header, footer, nil, ReaderOptions{})
	require.ErrorIs(t, err, ErrInvalidConfig)

	// A truncated footer changes the virtual size and fails validation.
	_, err = OpenReaderParts(header, footer[:len(footer)-1], frameFetcher(frames), ReaderOptions{})
	require.Error(t, err)

	// The checksum covers the footer for every constructor.
	badFooter := append([]byte(nil), footer...)
	badFooter[len(badFooter)-1] ^= 0xff
	_, err = OpenReaderParts(header, badFooter, frameFetcher(frames), ReaderOptions{})
	require.ErrorIs(t, err, ErrChecksumMismatch)

	// A fetched frame whose length disagrees with the block index is
	// corruption, not a decode attempt.
	r, err := OpenReaderParts(header, footer, func(i int) ([]byte, error) {
		return append(append([]byte(nil), frames[i]...), 0), nil
	}, ReaderOptions{})
	require.NoError(t, err)
	_, err = r.DecodeBlock(0)
	require.ErrorIs(t, err, ErrCorruptSegment)

	// Fetch errors surface from DecodeBlock.
	fetchErr := errors.New("fetch failed")
	r, err = OpenReaderParts(header, footer, func(int) ([]byte, error) { return nil, fetchErr }, ReaderOptions{})
	require.NoError(t, err)
	_, err = r.DecodeBlock(1)
	require.ErrorIs(t, err, fetchErr)
	_, err = r.DecodeBlock(2)
	require.ErrorIs(t, err, ErrBlockOutOfRange)
}

// TestComputeRewriteOverBytesMatchesFileRewrite checks that the pure
// rewrite step, run over a byte-backed Reader, lays out exactly the file
// the file-backed Rewrite produces.
func TestComputeRewriteOverBytesMatchesFileRewrite(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(5, 9))
	for it := range 10 {
		_, events, maxPerBlock := genSwarmSegment(r)
		path := filepath.Join(t.TempDir(), "seg.jss")
		writeSwarmSegment(t, path, events, maxPerBlock)
		orig, err := os.ReadFile(path)
		require.NoError(t, err)

		decide := func(ev *Event) RowDecision {
			if ev.DID == "did:plc:a" || ev.Seq%5 == 0 {
				return RowDrop
			}
			return RowKeep
		}
		header, frames, footer := sealedParts(t, orig)
		src, err := OpenReaderParts(header, footer, frameFetcher(frames), ReaderOptions{})
		require.NoError(t, err)
		plan, err := computeRewrite(src, decide)
		require.NoError(t, err)

		res, err := Rewrite(path, decide, RewriteOptions{})
		require.NoError(t, err)
		require.Equal(t, res.Rewritten, plan.rowsDropped > 0)
		if !res.Rewritten {
			continue
		}
		require.Equal(t, res.RowsDropped, plan.rowsDropped)
		require.Equal(t, res.BlocksTouched, plan.blocksTouched)
		require.Equal(t, res.Header, plan.header)
		rewritten, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equalf(t, rewritten, assembleSegment(plan.headerBytes, plan.frames, plan.footerBytes), "iter %d", it)
	}
}
