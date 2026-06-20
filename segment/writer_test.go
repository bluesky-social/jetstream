package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewCreatesEmpty256ByteFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.EqualValues(t, ReservedHeaderBytes, info.Size())

	contents, err := os.ReadFile(path)
	require.NoError(t, err)

	// Layout: segmentMagic + zero padding to ReservedHeaderBytes.
	require.Equal(t, segmentMagic, contents[:len(segmentMagic)],
		"first %d bytes must be segmentMagic", len(segmentMagic))
	for i, b := range contents[len(segmentMagic):] {
		require.Zerof(t, b, "padding byte %d should be zero", i+len(segmentMagic))
	}
}

func TestNewRejectsTooSmallFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")
	require.NoError(t, os.WriteFile(path, []byte{0, 0, 0}, 0o644))

	_, err := New(Config{Path: path})
	require.True(t, errors.Is(err, ErrCorruptSegment))
}

// TestNewRejectsBadMagic covers the magic-validation path in
// resumeExistingSegment: a header-sized file whose first 4 bytes
// are not segmentMagic must be rejected as corrupt.
func TestNewRejectsBadMagic(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")
	header := make([]byte, ReservedHeaderBytes)
	copy(header, []byte("XXXX"))
	require.NoError(t, os.WriteFile(path, header, 0o644))

	_, err := New(Config{Path: path})
	require.True(t, errors.Is(err, ErrCorruptSegment))
}

// TestNewRejectsSealedFile verifies that the sealed-vs-active
// detection logic in resumeExistingSegment returns ErrSegmentSealed
// for a file whose checksum bytes (offset 4..11) are non-zero. We
// build the fixture by hand here rather than via the public Seal API
// because Seal lives in a sibling package file and we want this test
// to cover detection in isolation; an end-to-end test that round-
// trips through Seal lives in seal_test.go.
func TestNewRejectsSealedFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	header := make([]byte, ReservedHeaderBytes)
	copy(header, segmentMagic)
	// Any non-zero value at offset 4..11 trips the detection.
	binary.LittleEndian.PutUint64(header[4:12], 0xCAFEBABE)
	require.NoError(t, os.WriteFile(path, header, 0o644))

	_, err := New(Config{Path: path})
	require.True(t, errors.Is(err, ErrSegmentSealed))
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	_, err := New(Config{}) // empty Path
	require.True(t, errors.Is(err, ErrInvalidConfig))

	_, err = New(Config{Path: "/dev/null/whatever", MaxEventsPerBlock: -1})
	require.True(t, errors.Is(err, ErrInvalidConfig))
}

// A MaxEventsPerBlock setting larger than the decoder's hard cap
// would silently produce blocks unreadable by the same package's
// decoder, so the writer must refuse it up front.
func TestNewRejectsMaxEventsPerBlockAboveDecoderCap(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	_, err := New(Config{Path: path, MaxEventsPerBlock: maxBlockEventsLimit + 1})
	require.True(t, errors.Is(err, ErrInvalidConfig),
		"writer cap above decoder cap must be rejected")
}

func TestAppendBuffersEvents(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	require.Equal(t, 0, w.Pending())
	require.Equal(t, DefaultMaxEventsPerBlock, w.Cap())

	for i := range 3 {
		full, err := w.Append(Event{
			Seq: uint64(i + 1), Kind: KindCreate, DID: "did:plc:a",
		})
		require.NoError(t, err)
		require.False(t, full)
	}
	require.Equal(t, 3, w.Pending())
}

func TestAppendSignalsFullAtCap(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	full, err := w.Append(Event{Seq: 1, Kind: KindCreate, DID: "d"})
	require.NoError(t, err)
	require.False(t, full)

	full, err = w.Append(Event{Seq: 2, Kind: KindCreate, DID: "d"})
	require.NoError(t, err)
	require.True(t, full, "second append should signal full")
}

func TestAppendRejectsAtCap(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	_, err = w.Append(Event{Seq: 1, Kind: KindCreate, DID: "d"})
	require.NoError(t, err)

	_, err = w.Append(Event{Seq: 2, Kind: KindCreate, DID: "d"})
	require.True(t, errors.Is(err, ErrBufferFull))
	require.Equal(t, 1, w.Pending(), "buffer must be unchanged after ErrBufferFull")
}

func TestAppendValidatesEvent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	_, err = w.Append(Event{Kind: 0, DID: "d"})
	require.True(t, errors.Is(err, ErrInvalidKind))
	require.Equal(t, 0, w.Pending())
}

func TestAppendAfterCloseReturnsErrClosed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	_, err = w.Append(Event{Seq: 1, Kind: KindCreate, DID: "d"})
	require.True(t, errors.Is(err, ErrClosed))
}

func TestFlushEmptyIsNoOp(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	require.NoError(t, w.Flush())

	// File should still be exactly 256 zero bytes.
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.EqualValues(t, ReservedHeaderBytes, info.Size())
}

func TestFlushWritesFramedBlockAndFsyncs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)

	events := []Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:a", Payload: []byte("p1")},
		{Seq: 2, Kind: KindCreate, DID: "did:plc:b", Payload: []byte("p2")},
	}
	for _, ev := range events {
		_, err := w.Append(ev)
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	require.Equal(t, 0, w.Pending(), "Flush must reset pending buffer")
	require.NoError(t, w.Close())

	// Read the file back and walk the framing.
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, len(contents), ReservedHeaderBytes+8,
		"expected header + framed block")

	body := contents[ReservedHeaderBytes:]
	require.GreaterOrEqual(t, len(body), 8, "need at least the length prefix")

	frameLen := binary.LittleEndian.Uint64(body[:8])
	require.EqualValues(t, len(body)-8, frameLen, "frame length must match remaining bytes")

	frame := body[8:]
	decoded, err := decodeBlockCompressed(frame)
	require.NoError(t, err)
	require.Equal(t, len(events), len(decoded))
	for i := range events {
		require.True(t, eventsEqual(events[i], decoded[i]))
	}
}

func TestFlushMultipleBlocks(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)

	allEvents := []Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:a"},
		{Seq: 2, Kind: KindCreate, DID: "did:plc:b"},
		{Seq: 3, Kind: KindCreate, DID: "did:plc:c"},
	}
	for _, ev := range allEvents[:2] {
		_, err := w.Append(ev)
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	_, err = w.Append(allEvents[2])
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	// Walk both blocks back.
	contents, err := os.ReadFile(path)
	require.NoError(t, err)

	walked := walkFramedBlocks(t, contents[ReservedHeaderBytes:])
	require.Len(t, walked, 2, "expected two framed blocks")
	require.Len(t, walked[0], 2)
	require.Len(t, walked[1], 1)
	require.True(t, eventsEqual(allEvents[0], walked[0][0]))
	require.True(t, eventsEqual(allEvents[1], walked[0][1]))
	require.True(t, eventsEqual(allEvents[2], walked[1][0]))
}

func TestPrepareAndCommitPreparedFlushAllowsAppendWhileBlockIsCompressed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)

	allEvents := []Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:a"},
		{Seq: 2, Kind: KindCreate, DID: "did:plc:b"},
		{Seq: 3, Kind: KindCreate, DID: "did:plc:c"},
		{Seq: 4, Kind: KindCreate, DID: "did:plc:d"},
	}
	for _, ev := range allEvents[:2] {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if ev.Seq == 2 {
			require.True(t, full)
		}
	}

	first, err := w.PrepareFlush()
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, 0, w.Pending(), "PrepareFlush must swap in an empty pending block")

	for _, ev := range allEvents[2:] {
		_, err := w.Append(ev)
		require.NoError(t, err)
	}
	second, err := w.PrepareFlush()
	require.NoError(t, err)
	require.NotNil(t, second)

	firstFrame := CompressPreparedBlock(first)
	secondFrame := CompressPreparedBlock(second)
	require.NoError(t, w.CommitPreparedFlush(first, firstFrame))
	require.NoError(t, w.CommitPreparedFlush(second, secondFrame))
	require.NoError(t, w.Close())

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	walked := walkFramedBlocks(t, contents[ReservedHeaderBytes:])
	require.Len(t, walked, 2)
	require.Len(t, walked[0], 2)
	require.Len(t, walked[1], 2)
	for i, ev := range allEvents {
		require.True(t, eventsEqual(ev, walked[i/2][i%2]))
	}
}

func TestCommitPreparedFlushRejectsOutOfOrderPreparedBlocks(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	_, err = w.Append(Event{Seq: 1, Kind: KindCreate, DID: "did:plc:a"})
	require.NoError(t, err)
	first, err := w.PrepareFlush()
	require.NoError(t, err)
	require.NotNil(t, first)

	_, err = w.Append(Event{Seq: 2, Kind: KindCreate, DID: "did:plc:b"})
	require.NoError(t, err)
	second, err := w.PrepareFlush()
	require.NoError(t, err)
	require.NotNil(t, second)

	err = w.CommitPreparedFlush(second, CompressPreparedBlock(second))
	require.ErrorContains(t, err, "prepared block order")
	require.NoError(t, w.CommitPreparedFlush(first, CompressPreparedBlock(first)))
	require.NoError(t, w.CommitPreparedFlush(second, CompressPreparedBlock(second)))
}

func TestCommitPreparedFlushRejectsPreparedBlockFromDifferentWriter(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	pathA := filepath.Join(dir, "a.jss")
	pathB := filepath.Join(dir, "b.jss")

	wA, err := New(Config{Path: pathA, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	defer func() { _ = wA.Close() }()
	wB, err := New(Config{Path: pathB, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	defer func() { _ = wB.Close() }()

	_, err = wA.Append(Event{Seq: 1, Kind: KindCreate, DID: "did:plc:a"})
	require.NoError(t, err)
	prepared, err := wA.PrepareFlush()
	require.NoError(t, err)
	require.NotNil(t, prepared)

	err = wB.CommitPreparedFlush(prepared, CompressPreparedBlock(prepared))
	require.ErrorContains(t, err, "different writer")
}

func TestCommitPreparedFlushRejectsAlreadyCommittedBlock(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	_, err = w.Append(Event{Seq: 1, Kind: KindCreate, DID: "did:plc:a"})
	require.NoError(t, err)
	prepared, err := w.PrepareFlush()
	require.NoError(t, err)
	require.NotNil(t, prepared)

	frame := CompressPreparedBlock(prepared)
	require.NoError(t, w.CommitPreparedFlush(prepared, frame))
	err = w.CommitPreparedFlush(prepared, frame)
	require.ErrorContains(t, err, "already committed")
}

func TestCloseRejectsUncommittedPreparedBlock(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	_, err = w.Append(Event{Seq: 1, Kind: KindCreate, DID: "did:plc:a"})
	require.NoError(t, err)
	prepared, err := w.PrepareFlush()
	require.NoError(t, err)
	require.NotNil(t, prepared)

	err = w.Close()
	require.ErrorContains(t, err, "uncommitted prepared block")

	require.NoError(t, w.CommitPreparedFlush(prepared, CompressPreparedBlock(prepared)))
	require.NoError(t, w.Close())
}

func TestSealRejectsUncommittedPreparedBlock(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	_, err = w.Append(Event{Seq: 1, Kind: KindCreate, DID: "did:plc:a"})
	require.NoError(t, err)
	prepared, err := w.PrepareFlush()
	require.NoError(t, err)
	require.NotNil(t, prepared)

	_, err = w.Seal()
	require.ErrorContains(t, err, "uncommitted prepared block")

	require.NoError(t, w.CommitPreparedFlush(prepared, CompressPreparedBlock(prepared)))
	_, err = w.Seal()
	require.NoError(t, err)
}

func TestFlushAfterCloseReturnsErrClosed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	require.True(t, errors.Is(w.Flush(), ErrClosed))
}

// walkFramedBlocks reads [uint64 LE len][zstd frame] pairs from
// the given body until exhausted. It's a test-only helper because
// the public Reader doesn't ship in this slice.
func walkFramedBlocks(t *testing.T, body []byte) [][]Event {
	t.Helper()
	var out [][]Event
	for len(body) > 0 {
		require.GreaterOrEqual(t, len(body), 8, "truncated framing")
		frameLen := binary.LittleEndian.Uint64(body[:8])
		body = body[8:]
		require.LessOrEqual(t, frameLen, uint64(len(body)), "frame length overruns body")
		frame := body[:frameLen]
		body = body[frameLen:]
		evs, err := decodeBlockCompressed(frame)
		require.NoError(t, err)
		out = append(out, evs)
	}
	return out
}

func TestCloseFlushesPending(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)

	_, err = w.Append(Event{Seq: 1, Kind: KindCreate, DID: "d", Payload: []byte("x")})
	require.NoError(t, err)

	require.NoError(t, w.Close())

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, len(contents), ReservedHeaderBytes+8,
		"Close must flush pending events before closing the file")
}

func TestCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, w.Close(), "second Close should be a no-op")
}

// TestNewResumeAppendsAfterExistingBlocks is the regression test
// for a torn `f.Seek(0, end)` replacement: reopening a writer
// against a file that already holds a real block must continue to
// append after that block, not overwrite it.
func TestNewResumeAppendsAfterExistingBlocks(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	first := []Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:a"},
		{Seq: 2, Kind: KindCreate, DID: "did:plc:b"},
	}
	w1, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	for _, ev := range first {
		_, err := w1.Append(ev)
		require.NoError(t, err)
	}
	require.NoError(t, w1.Flush())
	require.NoError(t, w1.Close())

	second := []Event{
		{Seq: 3, Kind: KindUpdate, DID: "did:plc:c"},
	}
	w2, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	for _, ev := range second {
		_, err := w2.Append(ev)
		require.NoError(t, err)
	}
	require.NoError(t, w2.Flush())
	require.NoError(t, w2.Close())

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	walked := walkFramedBlocks(t, contents[ReservedHeaderBytes:])
	require.Len(t, walked, 2, "expected one block from each writer")
	require.Len(t, walked[0], 2)
	require.Len(t, walked[1], 1)
	require.True(t, eventsEqual(first[0], walked[0][0]))
	require.True(t, eventsEqual(first[1], walked[0][1]))
	require.True(t, eventsEqual(second[0], walked[1][0]))
}

// TestNewTruncatesTornFrameTail simulates a crash mid-Write: a
// length prefix is on disk but only some of the frame body
// landed. The next New() must truncate the torn tail before
// allowing further appends, otherwise a subsequent Reader walking
// length prefixes would mis-interpret the next real frame as
// being inside the torn one.
func TestNewTruncatesTornFrameTail(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	good := []Event{{Seq: 1, Kind: KindCreate, DID: "did:plc:a"}}
	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	_, err = w.Append(good[0])
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	// Append a torn frame: an 8-byte length prefix claiming 1024
	// bytes of frame body, but only 100 actual bytes follow.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
	require.NoError(t, err)
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], 1024)
	_, err = f.Write(lenBuf[:])
	require.NoError(t, err)
	_, err = f.Write(make([]byte, 100))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	preInfo, err := os.Stat(path)
	require.NoError(t, err)
	tornSize := preInfo.Size()

	// Reopen — torn tail must be truncated.
	w2, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w2.Close())

	postInfo, err := os.Stat(path)
	require.NoError(t, err)
	require.Less(t, postInfo.Size(), tornSize,
		"reopen must shrink the file by the torn-tail length")

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	walked := walkFramedBlocks(t, contents[ReservedHeaderBytes:])
	require.Len(t, walked, 1, "only the original good block should remain")
	require.True(t, eventsEqual(good[0], walked[0][0]))
}

// TestStickyErrorIsLatchedAcrossFlushAndClose pins the durability
// invariant violated by an earlier flushLocked: once a Write fails
// and stickyErr is set, the buffered events MUST NOT be re-encoded
// and re-written — Close (which also calls flushLocked) would
// otherwise append a second copy of the same frame after a torn
// partial Write on disk. This test forces the failure by closing
// the underlying *os.File behind the writer's back, then asserts
// that Flush and Close both report the latched stickyErr.
func TestStickyErrorIsLatchedAcrossFlushAndClose(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)

	_, err = w.Append(Event{Seq: 1, Kind: KindCreate, DID: "d"})
	require.NoError(t, err)

	// Closing the underlying *os.File behind the writer's back makes
	// the next Write fail with os.ErrClosed, which the writer wraps
	// as "segment: write block: ...". A subsequent Close must surface
	// the same wrapped sentinel — not a fresh wrap from a second
	// write attempt — proving flushLocked short-circuited.
	require.NoError(t, w.file.Close())

	flushErr := w.Flush()
	require.Error(t, flushErr)
	require.ErrorIs(t, flushErr, os.ErrClosed)

	closeErr := w.Close()
	require.ErrorIs(t, closeErr, flushErr,
		"Close after a failed Flush must surface the latched stickyErr,"+
			" not re-attempt the write")

	// The latched-error invariant is a durability property: after a
	// failed flush, the writer must not have written more bytes than
	// before. Capture file size up to the failed flush via a separate
	// reopen — the writer's *os.File is closed.
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.EqualValues(t, ReservedHeaderBytes, info.Size(),
		"failed flush must not have grown the file past its initial header")
}

// TestNewTruncatesTornLengthPrefix is the edge case where the
// crash interrupted the 8-byte length prefix itself (e.g. only
// 3 bytes of it landed). lastGoodOffset must treat any
// non-8-byte trailer as a torn prefix and truncate.
func TestNewTruncatesTornLengthPrefix(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	_, err = w.Append(Event{Seq: 1, Kind: KindCreate, DID: "did:plc:a"})
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xAA, 0xBB, 0xCC}) // partial uint64 length prefix
	require.NoError(t, err)
	require.NoError(t, f.Close())

	w2, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w2.Close())

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	walked := walkFramedBlocks(t, contents[ReservedHeaderBytes:])
	require.Len(t, walked, 1)
}

// TestWriterBlocks_EmptyWhenNoFlush — a brand-new writer has no
// flushed blocks to report.
func TestWriterBlocks_EmptyWhenNoFlush(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.Empty(t, w.Blocks())
}

// TestWriterBlocks_PendingNotIncluded — events buffered but not yet
// flushed must NOT show up in Blocks(). The pending block has no
// on-disk offset, so reporting it would be a lie.
func TestWriterBlocks_PendingNotIncluded(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	_, err = w.Append(Event{
		Seq: 1, IndexedAt: 100, Kind: KindCreate, DID: "did:plc:a",
	})
	require.NoError(t, err)
	require.Empty(t, w.Blocks(), "Pending events must not appear in Blocks()")

	require.NoError(t, w.Flush())
	require.Len(t, w.Blocks(), 1, "After Flush, the block becomes visible")
}

// TestWriterBlocks_AppendsOnFlush — two flushes produce two blocks
// with correct offsets, sizes, event counts, seq bounds, and
// indexed_at bounds.
func TestWriterBlocks_AppendsOnFlush(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	// Block 0: seq 1..2, indexed_at 100..250.
	for _, ev := range []Event{
		{Seq: 1, IndexedAt: 100, Kind: KindCreate, DID: "did:plc:a"},
		{Seq: 2, IndexedAt: 250, Kind: KindCreate, DID: "did:plc:b"},
	} {
		_, err := w.Append(ev)
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())

	// Block 1: seq 3..4, indexed_at 50..1000 (out-of-order to prove
	// min/max really track min and max, not first and last).
	for _, ev := range []Event{
		{Seq: 3, IndexedAt: 1000, Kind: KindCreate, DID: "did:plc:c"},
		{Seq: 4, IndexedAt: 50, Kind: KindUpdate, DID: "did:plc:d"},
	} {
		_, err := w.Append(ev)
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())

	got := w.Blocks()
	require.Len(t, got, 2)

	require.EqualValues(t, ReservedHeaderBytes, got[0].Offset)
	require.EqualValues(t, 2, got[0].EventCount)
	require.EqualValues(t, 1, got[0].MinSeq)
	require.EqualValues(t, 2, got[0].MaxSeq)
	require.EqualValues(t, 100, got[0].MinIndexedAt)
	require.EqualValues(t, 250, got[0].MaxIndexedAt)
	require.NotZero(t, got[0].CompressedSize)
	require.NotZero(t, got[0].UncompressedSize)

	// Block 1's offset is block 0's offset + 8 + comp size.
	expectedOffset := uint64(ReservedHeaderBytes) + 8 + uint64(got[0].CompressedSize)
	require.Equal(t, expectedOffset, got[1].Offset)
	require.EqualValues(t, 2, got[1].EventCount)
	require.EqualValues(t, 3, got[1].MinSeq)
	require.EqualValues(t, 4, got[1].MaxSeq)
	require.EqualValues(t, 50, got[1].MinIndexedAt)
	require.EqualValues(t, 1000, got[1].MaxIndexedAt)
}

// TestWriterBlocks_ReturnsCopy — mutating the returned slice must
// not affect a subsequent Blocks() call.
func TestWriterBlocks_ReturnsCopy(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	_, err = w.Append(Event{Seq: 1, IndexedAt: 100, Kind: KindCreate, DID: "d"})
	require.NoError(t, err)
	require.NoError(t, w.Flush())

	first := w.Blocks()
	require.Len(t, first, 1)
	first[0].MinSeq = 0xDEADBEEF // mutation must not propagate

	second := w.Blocks()
	require.EqualValues(t, 1, second[0].MinSeq)
}

// TestWriterBlocks_RebuiltOnResume — write a block, close (without
// sealing), reopen via New() on the same path, assert Blocks()
// reflects what's already on disk.
func TestWriterBlocks_RebuiltOnResume(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w1, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for _, ev := range []Event{
		{Seq: 1, IndexedAt: 100, Kind: KindCreate, DID: "did:plc:a"},
		{Seq: 2, IndexedAt: 200, Kind: KindCreate, DID: "did:plc:b"},
	} {
		_, err := w1.Append(ev)
		require.NoError(t, err)
	}
	require.NoError(t, w1.Flush())
	require.NoError(t, w1.Close())

	w2, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })

	got := w2.Blocks()
	require.Len(t, got, 1)
	require.EqualValues(t, ReservedHeaderBytes, got[0].Offset)
	require.EqualValues(t, 2, got[0].EventCount)
	require.EqualValues(t, 1, got[0].MinSeq)
	require.EqualValues(t, 2, got[0].MaxSeq)
	require.EqualValues(t, 100, got[0].MinIndexedAt)
	require.EqualValues(t, 200, got[0].MaxIndexedAt)
}

// TestWriterBlocks_NilAfterSeal — Seal closes the writer; Blocks()
// returns nil. Mirrors Reader.Blocks() always returning a slice and
// avoids forcing callers into a nil-check just because the writer
// hit end-of-life.
func TestWriterBlocks_NilAfterSeal(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	_, err = w.Append(Event{Seq: 1, IndexedAt: 100, Kind: KindCreate, DID: "d"})
	require.NoError(t, err)

	_, err = w.Seal()
	require.NoError(t, err)

	require.Nil(t, w.Blocks())
}

func TestWriter_SnapshotPending_Empty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := New(Config{
		Path:              filepath.Join(dir, "seg.jss"),
		MaxEventsPerBlock: 4096,
	})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	got := w.SnapshotPending()
	require.Empty(t, got)
}

func TestWriter_SnapshotPending_ReturnsAllPending(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := New(Config{
		Path:              filepath.Join(dir, "seg.jss"),
		MaxEventsPerBlock: 4096,
	})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	events := []Event{
		{Seq: 1, IndexedAt: 1000, RenderedAt: 9001, Kind: KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "abc", Rev: "rev1", Payload: []byte{0xa1, 0x61, 0x78, 0x01}},
		{Seq: 2, IndexedAt: 2000, RenderedAt: 9002, Kind: KindUpdate, DID: "did:plc:b", Collection: "app.bsky.feed.like", Rkey: "def", Rev: "rev2", Payload: []byte{0xa1, 0x61, 0x79, 0x02}},
	}
	for _, ev := range events {
		full, err := w.Append(ev)
		require.NoError(t, err)
		require.False(t, full)
	}

	got := w.SnapshotPending()
	require.Len(t, got, 2)
	for i, ev := range events {
		require.Equal(t, ev.Seq, got[i].Seq)
		require.Equal(t, ev.IndexedAt, got[i].IndexedAt)
		require.Equal(t, ev.RenderedAt, got[i].RenderedAt)
		require.Equal(t, ev.Kind, got[i].Kind)
		require.Equal(t, ev.DID, got[i].DID)
		require.Equal(t, ev.Collection, got[i].Collection)
		require.Equal(t, ev.Rkey, got[i].Rkey)
		require.Equal(t, ev.Rev, got[i].Rev)
		require.Equal(t, ev.Payload, got[i].Payload)
	}
}

func TestWriter_SnapshotPending_AfterFlushIsEmpty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := New(Config{
		Path:              filepath.Join(dir, "seg.jss"),
		MaxEventsPerBlock: 4096,
	})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	_, err = w.Append(Event{Seq: 1, IndexedAt: 1, Kind: KindCreate, DID: "did:plc:a", Payload: []byte{0xa0}})
	require.NoError(t, err)

	require.NoError(t, w.Flush())
	require.Empty(t, w.SnapshotPending())
}

func TestWriter_SnapshotPending_PayloadIsSafeAgainstFurtherAppend(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w, err := New(Config{
		Path: filepath.Join(dir, "seg.jss"),
		// Small block cap forces the writer's payloads buffer to its
		// initial low capacity (16 * 512 = 8KB), so the 14 × 1KB appends
		// below DO trigger multiple growth events. Without this, a default
		// MaxEventsPerBlock=4096 preallocates ~2MB and the appends fit
		// without ever resizing — making the safety check a false positive.
		MaxEventsPerBlock: 16,
	})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	original := []byte{0xa1, 0x61, 0x78, 0xff}
	_, err = w.Append(Event{Seq: 1, IndexedAt: 1, Kind: KindCreate, DID: "did:plc:a", Payload: original})
	require.NoError(t, err)

	got := w.SnapshotPending()
	require.Len(t, got, 1)
	snapshotPayload := got[0].Payload

	// 14 more 1KB-payload appends after the snapshot. With initial
	// payload capacity of 8KB (16 events × 512 bytes), this forces the
	// payloads buffer to grow at least once. Stay under MaxEventsPerBlock
	// (16 with our seq=1 baseline) so we don't trigger a flush mid-test.
	for i := 2; i <= 15; i++ {
		_, err := w.Append(Event{
			Seq: uint64(i), IndexedAt: int64(i), Kind: KindCreate,
			DID: "did:plc:a", Payload: bytes.Repeat([]byte{0xff}, 1024),
		})
		require.NoError(t, err)
	}

	require.Equal(t, original, snapshotPayload, "snapshot must not be aliased into the writer's growing buffer")
}
