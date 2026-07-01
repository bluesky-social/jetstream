package segment

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// makeSealedFixture builds a deterministic sealed segment file in dir
// and returns the path and the SealResult so tests can cross-check.
func makeSealedFixture(t *testing.T, dir string) (string, SealResult) {
	t.Helper()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	events := []Event{
		{Seq: 1, WitnessedAt: 1_000_000, Kind: KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "a", Rev: "r1", Payload: []byte("p1")},
		{Seq: 2, WitnessedAt: 1_000_001, Kind: KindCreate, DID: "did:plc:bob", Collection: "app.bsky.feed.post", Rkey: "b", Rev: "r2", Payload: []byte("p2")},
		{Seq: 3, WitnessedAt: 1_000_002, Kind: KindCreate, DID: "did:plc:carol", Collection: "app.bsky.feed.like", Rkey: "c", Rev: "r3", Payload: []byte("p3")},
		{Seq: 4, WitnessedAt: 1_000_003, Kind: KindUpdate, DID: "did:plc:alice", Collection: "app.bsky.feed.like", Rkey: "d", Rev: "r4", Payload: []byte("p4")},
		{Seq: 5, WitnessedAt: 1_000_004, Kind: KindCreate, DID: "did:plc:dan", Collection: "app.bsky.graph.follow", Rkey: "e", Rev: "r5", Payload: []byte("p5")},
	}
	for i, ev := range events {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
		// Also flush after the last event if it wasn't already full
		if i == len(events)-1 && !full {
			require.NoError(t, w.Flush())
		}
	}
	res, err := w.Seal()
	require.NoError(t, err)
	return path, res
}

func TestInspect_SealedRoundtrip(t *testing.T) {
	t.Parallel()

	path, sealRes := makeSealedFixture(t, t.TempDir())

	ins, err := Inspect(path)
	require.NoError(t, err)
	require.NotNil(t, ins)

	require.Equal(t, path, ins.Path)
	require.True(t, ins.Sealed)
	require.True(t, ins.ChecksumValid)
	require.Equal(t, sealRes.BlockCount, ins.Header.BlockCount)
	require.Equal(t, sealRes.EventCount, ins.Header.EventCount)
	require.Equal(t, sealRes.UniqueDIDCount, ins.Header.UniqueDIDCount)
	require.Equal(t, sealRes.MinSeq, ins.Header.MinSeq)
	require.Equal(t, sealRes.MaxSeq, ins.Header.MaxSeq)
	require.Equal(t, sealRes.Checksum, ins.Header.Checksum)
	require.Equal(t, sealRes.FileSize, ins.FileSize)

	require.EqualValues(t, sealRes.BlockCount, len(ins.Blocks))
	require.ElementsMatch(t,
		[]string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.graph.follow"},
		ins.Collections)
	require.Len(t, ins.BlockCollections, len(ins.Blocks))

	require.Len(t, ins.CollectionEventCounts, len(ins.Collections))
	byName := map[string]uint32{}
	for i, n := range ins.Collections {
		byName[n] = ins.CollectionEventCounts[i]
	}
	// Fixture: 2 posts, 2 likes, 1 follow.
	require.Equal(t, uint32(2), byName["app.bsky.feed.post"])
	require.Equal(t, uint32(2), byName["app.bsky.feed.like"])
	require.Equal(t, uint32(1), byName["app.bsky.graph.follow"])

	// Per-block witnessed_at bounds round-trip through seal.
	for i, b := range ins.Blocks {
		require.LessOrEqual(t, b.MinWitnessedAt, b.MaxWitnessedAt,
			"block %d has inverted witnessed_at bounds", i)
		require.GreaterOrEqual(t, b.MinWitnessedAt, int64(1_000_000),
			"block %d min_witnessed_at below fixture floor", i)
		require.LessOrEqual(t, b.MaxWitnessedAt, int64(1_000_004),
			"block %d max_witnessed_at above fixture ceiling", i)
	}

	require.EqualValues(t, sealRes.EventCount, ins.TotalEvents)
	require.NotZero(t, ins.BlockIndexBytes)
	require.NotZero(t, ins.SegmentBloomBytes)
	require.NotZero(t, ins.BlockBloomsBytes)
	require.NotZero(t, ins.CollectionIndexBytes)
	require.NotZero(t, ins.PerBlockBloomBytes)

	totalFooter := ins.BlockIndexBytes + ins.SegmentBloomBytes +
		ins.BlockBloomsBytes + ins.CollectionIndexBytes
	require.EqualValues(t, uint64(ins.FileSize)-ins.Header.FooterOffset, totalFooter)
}

func TestInspect_CorruptSealedReportsInvalidChecksum(t *testing.T) {
	t.Parallel()

	path, _ := makeSealedFixture(t, t.TempDir())

	// Read a first inspection so we have the offsets we need.
	ins0, err := Inspect(path)
	require.NoError(t, err)
	require.True(t, ins0.ChecksumValid)

	// Corrupt a byte halfway through the per-block-blooms body.
	// Reader.Open only reads the 8-byte region header at Open time;
	// per-block bloom bytes are lazily fetched. So flipping a byte
	// here invalidates the file's xxh3 checksum but lets Reader.Open
	// (and our metadata parse) succeed.
	regionStart := int64(ins0.Header.BlockDIDBloomOffset) + int64(blockBloomsRegionHeaderSize)
	regionEnd := int64(ins0.Header.CollectionIndexOffset)
	require.Greater(t, regionEnd, regionStart, "fixture must have at least one per-block bloom")
	corruptOff := regionStart + (regionEnd-regionStart)/2

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	var b [1]byte
	_, err = f.ReadAt(b[:], corruptOff)
	require.NoError(t, err)
	b[0] ^= 0xff
	_, err = f.WriteAt(b[:], corruptOff)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	ins, err := Inspect(path)
	require.NoError(t, err)
	require.True(t, ins.Sealed)
	require.False(t, ins.ChecksumValid)
	require.NotZero(t, ins.Header.BlockCount)
	require.Equal(t, len(ins.Blocks), int(ins.Header.BlockCount))
	// Collections should still be present since the collection index
	// is intact.
	require.NotEmpty(t, ins.Collections)
}

func TestInspect_NotASegmentFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "garbage.jss")
	require.NoError(t, os.WriteFile(path, []byte("not-a-segment-file"), 0o644))
	_, err := Inspect(path)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrCorruptSegment))
}

func TestInspect_ActiveFileWithBlocks(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "active.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	events := []Event{
		{Seq: 10, WitnessedAt: 2_000_000, Kind: KindCreate, DID: "did:plc:x", Collection: "app.bsky.feed.post", Rkey: "1", Rev: "r"},
		{Seq: 11, WitnessedAt: 2_000_001, Kind: KindCreate, DID: "did:plc:y", Collection: "app.bsky.feed.post", Rkey: "2", Rev: "r"},
		{Seq: 12, WitnessedAt: 2_000_002, Kind: KindUpdate, DID: "did:plc:x", Collection: "app.bsky.feed.like", Rkey: "3", Rev: "r"},
		{Seq: 13, WitnessedAt: 2_000_003, Kind: KindCreate, DID: "did:plc:z", Collection: "app.bsky.feed.like", Rkey: "4", Rev: "r"},
	}
	for _, ev := range events {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
	}
	require.NoError(t, w.Flush())
	// Deliberately do NOT seal: the file is now an active segment with
	// two flushed blocks of size 2 each.

	ins, err := Inspect(path)
	require.NoError(t, err)
	require.False(t, ins.Sealed)
	require.False(t, ins.ChecksumValid) // active = no checksum
	require.Nil(t, ins.PartialError)
	require.EqualValues(t, 4, ins.TotalEvents)
	require.Len(t, ins.Blocks, 2)
	require.Equal(t, uint64(10), ins.MinSeq)
	require.Equal(t, uint64(13), ins.MaxSeq)
	require.ElementsMatch(t,
		[]string{"app.bsky.feed.post", "app.bsky.feed.like"},
		ins.Collections)
	require.Len(t, ins.BlockCollections, 2)

	require.Len(t, ins.CollectionEventCounts, len(ins.Collections))
	byName := map[string]uint32{}
	for i, n := range ins.Collections {
		byName[n] = ins.CollectionEventCounts[i]
	}
	// Fixture: 2 posts, 2 likes.
	require.Equal(t, uint32(2), byName["app.bsky.feed.post"])
	require.Equal(t, uint32(2), byName["app.bsky.feed.like"])

	for i, b := range ins.Blocks {
		require.LessOrEqual(t, b.MinWitnessedAt, b.MaxWitnessedAt,
			"active block %d has inverted witnessed_at bounds", i)
		require.GreaterOrEqual(t, b.MinWitnessedAt, int64(2_000_000),
			"active block %d min below fixture floor", i)
		require.LessOrEqual(t, b.MaxWitnessedAt, int64(2_000_003),
			"active block %d max above fixture ceiling", i)
	}
}

func TestInspect_ActiveFileEmpty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w.Close()) // no events appended; file is just the 256B header

	ins, err := Inspect(path)
	require.NoError(t, err)
	require.False(t, ins.Sealed)
	require.Empty(t, ins.Blocks)
	require.EqualValues(t, 0, ins.TotalEvents)
}

func TestInspect_ActiveFileTornTailReportsPartialError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "torn.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for _, seq := range []uint64{1, 2} {
		_, err := w.Append(Event{Seq: seq, Kind: KindCreate, DID: "d", Collection: "c", Rkey: "r", Rev: "rv"})
		require.NoError(t, err)
	}
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())

	// Append 4 garbage bytes that look like the start of a length
	// prefix but truncate before its 8 bytes are complete. The frame
	// walker should see an unread length prefix and surface it as a
	// torn tail.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xff, 0xff, 0xff, 0xff})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	ins, err := Inspect(path)
	require.NoError(t, err) // partial errors are surfaced via PartialError, not return
	require.False(t, ins.Sealed)
	require.NotNil(t, ins.PartialError)
	// The first (clean) block must still be visible.
	require.GreaterOrEqual(t, len(ins.Blocks), 1)
}
