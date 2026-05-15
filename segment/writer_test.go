package segment

import (
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
	require.EqualValues(t, reservedHeaderBytes, info.Size())

	contents, err := os.ReadFile(path)
	require.NoError(t, err)

	// Layout: segmentMagic + zero padding to reservedHeaderBytes.
	require.Equal(t, segmentMagic, contents[:len(segmentMagic)],
		"first %d bytes must be segmentMagic", len(segmentMagic))
	for i, b := range contents[len(segmentMagic):] {
		require.Zerof(t, b, "padding byte %d should be zero", i+len(segmentMagic))
	}
}

func TestNewResumesActiveFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	// Create with first writer, close.
	w1, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w1.Close())

	// Reopen.
	w2, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w2.Close())
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
//
// kaizen: when the seal/unseal trailer format lands, add a
// dedicated TestNewRejectsSealedFile that builds a real sealed
// segment (via the future Seal API) and expects ErrSegmentSealed.
// Until then ErrSegmentSealed has no producer in this slice.
func TestNewRejectsBadMagic(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")
	header := make([]byte, reservedHeaderBytes)
	copy(header, []byte("XXXX"))
	require.NoError(t, os.WriteFile(path, header, 0o644))

	_, err := New(Config{Path: path})
	require.True(t, errors.Is(err, ErrCorruptSegment))
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
	require.EqualValues(t, reservedHeaderBytes, info.Size())
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
	require.Greater(t, len(contents), reservedHeaderBytes+8,
		"expected header + framed block")

	body := contents[reservedHeaderBytes:]
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

	walked := walkFramedBlocks(t, contents[reservedHeaderBytes:])
	require.Len(t, walked, 2, "expected two framed blocks")
	require.Len(t, walked[0], 2)
	require.Len(t, walked[1], 1)
	require.True(t, eventsEqual(allEvents[0], walked[0][0]))
	require.True(t, eventsEqual(allEvents[1], walked[0][1]))
	require.True(t, eventsEqual(allEvents[2], walked[1][0]))
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
	require.Greater(t, len(contents), reservedHeaderBytes+8,
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
	walked := walkFramedBlocks(t, contents[reservedHeaderBytes:])
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
	walked := walkFramedBlocks(t, contents[reservedHeaderBytes:])
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
	require.EqualValues(t, reservedHeaderBytes, info.Size(),
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
	walked := walkFramedBlocks(t, contents[reservedHeaderBytes:])
	require.Len(t, walked, 1)
}
