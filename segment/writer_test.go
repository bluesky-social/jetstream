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
	require.EqualValues(t, 256, info.Size())

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	for i, b := range contents {
		require.Zerof(t, b, "byte %d should be zero", i)
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

func TestNewRejectsSealedFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")
	header := make([]byte, 256)
	copy(header, []byte("jss0"))
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

func TestAppendBuffersEvents(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	require.Equal(t, 0, w.Pending())
	require.Equal(t, DefaultMaxEventsPerBlock, w.Cap())

	for i := 0; i < 3; i++ {
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
