package segment

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestScanMaxSeq_RejectsSealed pins the contract: callers use
// segment.Reader for sealed files; ScanMaxSeq is for active-segment
// crash recovery only.
func TestScanMaxSeq_RejectsSealed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	_, err = w.Append(Event{Seq: 0, Kind: KindCreate, DID: "did:plc:a"})
	require.NoError(t, err)
	_, err = w.Append(Event{Seq: 1, Kind: KindCreate, DID: "did:plc:b"})
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	_, err = w.Seal()
	require.NoError(t, err)

	_, _, err = ScanMaxSeq(path)
	require.True(t, errors.Is(err, ErrSegmentSealed),
		"expected ErrSegmentSealed for sealed file, got %v", err)
}

// TestScanMaxSeq_Empty pins behavior for an active segment that
// contains zero blocks.
func TestScanMaxSeq_Empty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	maxSeq, found, err := ScanMaxSeq(path)
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, uint64(0), maxSeq)
}

// TestScanMaxSeq_SingleBlock confirms ScanMaxSeq finds the max seq in
// a single fully-flushed block.
func TestScanMaxSeq_SingleBlock(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	for i := range uint64(4) {
		_, err := w.Append(Event{Seq: i, Kind: KindCreate, DID: "did:plc:a"})
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	maxSeq, found, err := ScanMaxSeq(path)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(3), maxSeq)
}

// TestScanMaxSeq_MultipleBlocks confirms the running max across
// multiple flushed blocks.
func TestScanMaxSeq_MultipleBlocks(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for i := range uint64(6) {
		full, err := w.Append(Event{Seq: i, Kind: KindCreate, DID: "did:plc:a"})
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
	}
	require.NoError(t, w.Close())

	maxSeq, found, err := ScanMaxSeq(path)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(5), maxSeq)
}

// TestScanMaxSeq_IgnoresTornTail confirms that bytes past the last
// fully-written frame are skipped — same recovery semantics as
// lastGoodOffset/resumeExistingSegment.
func TestWalkActiveRange_MiddleSuffixAndExactEnd(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for seq := uint64(1); seq <= 6; seq++ {
		full, err := w.Append(Event{Seq: seq, Kind: KindCreate, DID: "did:plc:range"})
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
	}
	blocks := w.Blocks()
	require.Len(t, blocks, 3)

	var got []uint64
	err = WalkActiveRange(path, blocks[1].Offset, blocks[2].Offset, func(events []Event) error {
		for i := range events {
			got = append(got, events[i].Seq)
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 4}, got)
	require.NoError(t, w.Close())
}

func TestWalkActiveRange_ConcurrentSealAfterOpenUsesSnapshottedEnd(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for seq := uint64(1); seq <= 6; seq++ {
		full, err := w.Append(Event{Seq: seq, Kind: KindCreate, DID: "did:plc:sealrange"})
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
	}
	blocks := w.Blocks()
	end := w.nextBlockOffset
	var got []uint64
	calls := 0
	err = WalkActiveRange(path, blocks[0].Offset, end, func(events []Event) error {
		calls++
		for i := range events {
			got = append(got, events[i].Seq)
		}
		if calls == 1 {
			_, sealErr := w.Seal()
			require.NoError(t, sealErr)
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6}, got)
}

func TestWalkActiveRange_RejectsMisalignedAndSealed(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	_, err = w.Append(Event{Seq: 1, Kind: KindCreate, DID: "did:plc:range"})
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	blocks := w.Blocks()
	require.Len(t, blocks, 1)

	err = WalkActiveRange(path, blocks[0].Offset+1, w.nextBlockOffset, func([]Event) error { return nil })
	require.ErrorIs(t, err, ErrCorruptSegment)

	_, err = w.Seal()
	require.NoError(t, err)
	err = WalkActiveRange(path, blocks[0].Offset, blocks[0].Offset+8+uint64(blocks[0].CompressedSize), func([]Event) error { return nil })
	require.ErrorIs(t, err, ErrSegmentSealed)
}

func TestScanMaxSeq_IgnoresTornTail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for i := range uint64(2) {
		_, err := w.Append(Event{Seq: i, Kind: KindCreate, DID: "did:plc:a"})
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	// Append a torn tail: a length prefix that promises more bytes than
	// the file holds. The frame body is intentionally truncated.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], 1<<20) // 1 MiB promised
	_, err = f.Write(lenBuf[:])
	require.NoError(t, err)
	_, err = f.Write([]byte{0xff, 0xff, 0xff})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	maxSeq, found, err := ScanMaxSeq(path)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(1), maxSeq)
}
