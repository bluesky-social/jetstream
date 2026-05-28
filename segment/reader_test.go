package segment

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func sealedSegmentForReader(t *testing.T, dir string, events []Event, maxPerBlock int) string {
	t.Helper()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path, MaxEventsPerBlock: maxPerBlock})
	require.NoError(t, err)
	for _, ev := range events {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if full {
			err := w.Flush()
			require.NoError(t, err)
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
	return path
}

func TestReaderOpenSucceeds(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	events := []Event{
		{Seq: 1, IndexedAt: 100, Kind: KindCreate, DID: "did:plc:a",
			Collection: "app.bsky.feed.post", Rkey: "k1", Rev: "v1"},
		{Seq: 2, IndexedAt: 200, Kind: KindCreate, DID: "did:plc:b",
			Collection: "app.bsky.feed.like", Rkey: "k2", Rev: "v2"},
	}
	path := sealedSegmentForReader(t, dir, events, 2)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	h := r.Header()
	require.EqualValues(t, 1, h.BlockCount)
	require.EqualValues(t, 2, h.EventCount)
	require.EqualValues(t, 2, h.UniqueDIDCount)

	infos := r.Blocks()
	require.Len(t, infos, 1)
	require.EqualValues(t, 2, infos[0].EventCount)

	bloom := r.SegmentBloom()
	require.NotNil(t, bloom)
	require.True(t, bloom.TestString("did:plc:a"))
	require.True(t, bloom.TestString("did:plc:b"))

	cols := r.Collections()
	require.ElementsMatch(t,
		[]string{"app.bsky.feed.post", "app.bsky.feed.like"}, cols)

	got0, err := r.BlockCollections(0)
	require.NoError(t, err)
	require.Len(t, got0, 2)
}

func TestReaderOpenChecksumMismatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir,
		[]Event{{Seq: 1, Kind: KindCreate, DID: "did:plc:a"}}, 4)

	// Corrupt a byte inside the reserved padding region of the fixed
	// header (bytes 98..255 are zero-filled but inside the xxh3-
	// checksummed range, so flipping one breaks the checksum without
	// breaking any parser). This proves checksum validation rejects
	// corrupted files reliably without depending on which region
	// happens to fail-loudly.
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	var b [1]byte
	const corruptOffset = 100
	_, err = f.ReadAt(b[:], corruptOffset)
	require.NoError(t, err)
	b[0] ^= 0xFF
	_, err = f.WriteAt(b[:], corruptOffset)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	_, err = Open(ReaderConfig{Path: path})
	require.True(t, errors.Is(err, ErrChecksumMismatch))
}

func TestReaderOpenSkipChecksum(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir,
		[]Event{{Seq: 1, Kind: KindCreate, DID: "did:plc:a"}}, 4)

	// Corrupt a byte inside the reserved padding region of the fixed
	// header (bytes 98..255 are zero-filled but inside the xxh3-
	// checksummed range, so flipping one breaks the checksum without
	// breaking any parser). This proves SkipChecksum bypasses the
	// integrity check without giving us a "tolerate corruption"
	// behavior in production.
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	var b [1]byte
	const corruptOffset = 100
	_, err = f.ReadAt(b[:], corruptOffset)
	require.NoError(t, err)
	b[0] ^= 0xFF
	_, err = f.WriteAt(b[:], corruptOffset)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	r, err := Open(ReaderConfig{Path: path, SkipChecksum: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
}

func TestReaderOpenRejectsActiveFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	// Create an active segment but don't seal it.
	w, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	_, err = Open(ReaderConfig{Path: path})
	require.True(t, errors.Is(err, ErrCorruptSegment))
}

func TestReaderOpenRejectsMissingPath(t *testing.T) {
	t.Parallel()

	_, err := Open(ReaderConfig{})
	require.True(t, errors.Is(err, ErrInvalidConfig))
}

func TestReaderCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir,
		[]Event{{Seq: 1, Kind: KindCreate, DID: "did:plc:a"}}, 4)
	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.NoError(t, r.Close())
}

func TestReaderDecodeBlockReturnsEvents(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	events := []Event{
		{Seq: 1, IndexedAt: 100, Kind: KindCreate, DID: "did:plc:a",
			Collection: "app.bsky.feed.post", Rkey: "k1", Rev: "v1",
			Payload: []byte("p1")},
		{Seq: 2, IndexedAt: 200, Kind: KindCreate, DID: "did:plc:b",
			Collection: "app.bsky.feed.like", Rkey: "k2", Rev: "v2",
			Payload: []byte("p2")},
		{Seq: 3, IndexedAt: 300, Kind: KindUpdate, DID: "did:plc:a",
			Collection: "app.bsky.feed.post", Rkey: "k1", Rev: "v3",
			Payload: []byte("p3")},
	}
	path := sealedSegmentForReader(t, dir, events, 2)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	got0, err := r.DecodeBlock(0)
	require.NoError(t, err)
	require.Len(t, got0, 2)
	require.True(t, eventsEqual(events[0], got0[0]))
	require.True(t, eventsEqual(events[1], got0[1]))

	got1, err := r.DecodeBlock(1)
	require.NoError(t, err)
	require.Len(t, got1, 1)
	require.True(t, eventsEqual(events[2], got1[0]))

	_, err = r.DecodeBlock(2)
	require.True(t, errors.Is(err, ErrBlockOutOfRange))

	_, err = r.DecodeBlock(-1)
	require.True(t, errors.Is(err, ErrBlockOutOfRange))
}

func TestReaderBlockBloom(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	events := []Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:alice"},
		{Seq: 2, Kind: KindCreate, DID: "did:plc:bob"},
		{Seq: 3, Kind: KindCreate, DID: "did:plc:carol"},
		{Seq: 4, Kind: KindCreate, DID: "did:plc:dave"},
	}
	path := sealedSegmentForReader(t, dir, events, 2)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	b0, err := r.BlockBloom(0)
	require.NoError(t, err)
	require.True(t, b0.TestString("did:plc:alice"))
	require.True(t, b0.TestString("did:plc:bob"))

	b1, err := r.BlockBloom(1)
	require.NoError(t, err)
	require.True(t, b1.TestString("did:plc:carol"))
	require.True(t, b1.TestString("did:plc:dave"))

	_, err = r.BlockBloom(2)
	require.True(t, errors.Is(err, ErrBlockOutOfRange))

	_, err = r.BlockBloom(-1)
	require.True(t, errors.Is(err, ErrBlockOutOfRange))
}

func TestReaderLoadAllBlockBlooms(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	events := []Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:a"},
		{Seq: 2, Kind: KindCreate, DID: "did:plc:b"},
		{Seq: 3, Kind: KindCreate, DID: "did:plc:c"},
	}
	path := sealedSegmentForReader(t, dir, events, 1)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	blooms, err := r.LoadAllBlockBlooms()
	require.NoError(t, err)
	require.Len(t, blooms, 3)

	require.True(t, blooms[0].TestString("did:plc:a"))
	require.True(t, blooms[1].TestString("did:plc:b"))
	require.True(t, blooms[2].TestString("did:plc:c"))

	// Cross-check: each filter equals the one BlockBloom returns.
	for i := range blooms {
		single, err := r.BlockBloom(i)
		require.NoError(t, err)
		require.Equal(t,
			blooms[i].TestString("did:plc:a"),
			single.TestString("did:plc:a"))
	}
}

// TestReaderOpenEmptySegment exercises the corner case where Seal
// runs on a Writer that never received any Append. The on-disk
// metadata is still well-formed (zero-block index, empty per-block
// blooms region, empty collection table) and Reader.Open must
// surface it cleanly.
func TestReaderOpenEmptySegment(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")
	w, err := New(Config{Path: path})
	require.NoError(t, err)
	_, err = w.Seal()
	require.NoError(t, err)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	require.EqualValues(t, 0, r.Header().BlockCount)
	require.EqualValues(t, 0, r.Header().EventCount)
	require.Empty(t, r.Blocks())
	require.Empty(t, r.Collections())

	// SegmentBloom is non-nil even for an empty segment per gloom.New
	// allocating a single block when expectedItems is zero. The
	// Reader doc reflects this contract.
	require.NotNil(t, r.SegmentBloom())

	// All idx-keyed accessors out-of-range against zero blocks.
	_, err = r.DecodeBlock(0)
	require.True(t, errors.Is(err, ErrBlockOutOfRange))
	_, err = r.BlockBloom(0)
	require.True(t, errors.Is(err, ErrBlockOutOfRange))
	_, err = r.BlockCollections(0)
	require.True(t, errors.Is(err, ErrBlockOutOfRange))
}

// TestReaderOpenRejectsOversizedBlockCount asserts that a corrupt
// header.BlockCount past the safety cap is rejected before any
// allocation keyed off it. This is the primary DoS mitigation: a
// hostile or bit-flipped 4-byte field at offset 14 must not drive a
// gigabyte-scale make() call.
func TestReaderOpenRejectsOversizedBlockCount(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir,
		[]Event{{Seq: 1, Kind: KindCreate, DID: "did:plc:a"}}, 4)

	// Header offset 14 is block_count (uint32 LE). Overwrite with a
	// value past maxBlockCountLimit by writing 4 bytes via a uint32
	// patch: WriteAt of a buf where only the first 4 bytes matter.
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], maxBlockCountLimit+1)
	_, err = f.WriteAt(buf[:], 14)
	require.NoError(t, err)
	require.NoError(t, f.Sync())

	// SkipChecksum so we hit the block-count cap rather than the
	// (also-tripped) checksum-mismatch path.
	_, err = Open(ReaderConfig{Path: path, SkipChecksum: true})
	require.True(t, errors.Is(err, ErrInvalidFooter))
}

// TestReaderOpenRejectsBlockCountMismatch ensures the cross-check
// between header.BlockCount and the per-block-blooms region's
// embedded count fires when they disagree. We force the disagreement
// by patching header.BlockCount to a smaller-but-positive value; the
// per-block-blooms region still records the original count.
func TestReaderOpenRejectsBlockCountMismatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir, []Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:a"},
		{Seq: 2, Kind: KindCreate, DID: "did:plc:b"},
	}, 1) // 2 blocks total

	// Corrupt block_count to 1 (stays under the cap, but disagrees
	// with the bloom region's recorded count of 2).
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], 1)
	_, err = f.WriteAt(buf[:], 14)
	require.NoError(t, err)
	require.NoError(t, f.Sync())

	_, err = Open(ReaderConfig{Path: path, SkipChecksum: true})
	require.True(t, errors.Is(err, ErrInvalidFooter))
}

// TestReaderOpenRejectsOverlappingBlockIndex confirms that overlap
// detection in validateBlockOffsets rejects a hand-corrupted block
// index whose second entry's offset is inside the first entry's
// range. Without this check, a malformed index would survive Open
// and produce confusing decode errors at DecodeBlock time.
func TestReaderOpenRejectsOverlappingBlockIndex(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir, []Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:a"},
		{Seq: 2, Kind: KindCreate, DID: "did:plc:b"},
	}, 1) // 2 blocks => 2 block-index entries at footer_offset

	// Pull the header off so we know the block index location.
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	headerBytes := make([]byte, ReservedHeaderBytes)
	_, err = f.ReadAt(headerBytes, 0)
	require.NoError(t, err)
	hdr, err := decodeHeader(headerBytes)
	require.NoError(t, err)

	// Patch the SECOND block-index entry's offset to point inside the
	// first block. The offset field is at entry-relative bytes 0..8;
	// we step over the first entry by adding blockIndexEntrySize.
	var bad [8]byte
	binary.LittleEndian.PutUint64(bad[:], uint64(ReservedHeaderBytes))
	secondOffsetField := int64(hdr.BlockIndexOffset) + int64(blockIndexEntrySize)
	_, err = f.WriteAt(bad[:], secondOffsetField)
	require.NoError(t, err)
	require.NoError(t, f.Sync())

	_, err = Open(ReaderConfig{Path: path, SkipChecksum: true})
	require.True(t, errors.Is(err, ErrInvalidBlockIndex))
}

func TestReaderConcurrentDecodeBlock(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	var events []Event
	for i := 1; i <= 64; i++ {
		events = append(events, Event{
			Seq: uint64(i), Kind: KindCreate,
			DID: "did:plc:" + string(rune('a'+(i%26))),
		})
	}
	path := sealedSegmentForReader(t, dir, events, 4)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	const goroutines = 10
	const itersPerGoroutine = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	errs := make(chan error, goroutines*itersPerGoroutine)

	for g := range goroutines {
		go func(g int) {
			defer wg.Done()
			for i := range itersPerGoroutine {
				idx := (g + i) % len(r.Blocks())
				if _, err := r.DecodeBlock(idx); err != nil {
					errs <- err
					return
				}
				if _, err := r.BlockBloom(idx); err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// TestReaderOpenRejectsInvertedIndexedAtBounds asserts the
// validateBlockOffsets pass refuses a file whose block-index entry
// has max_indexed_at < min_indexed_at. Mirrors the existing seq
// invariant test pattern.
func TestReaderOpenRejectsInvertedIndexedAtBounds(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir, []Event{
		{Seq: 1, IndexedAt: 100, Kind: KindCreate, DID: "did:plc:a"},
		{Seq: 2, IndexedAt: 200, Kind: KindCreate, DID: "did:plc:b"},
	}, 2)

	// Patch the FIRST block-index entry's max_indexed_at to a value
	// less than its min_indexed_at. The two indexed_at fields live at
	// entry-relative offsets [36:44] and [44:52].
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	headerBytes := make([]byte, ReservedHeaderBytes)
	_, err = f.ReadAt(headerBytes, 0)
	require.NoError(t, err)
	hdr, err := decodeHeader(headerBytes)
	require.NoError(t, err)

	// First entry's min is currently 100, max is 200. Patch max to 50.
	maxField := int64(hdr.BlockIndexOffset) + 44
	var bad [8]byte
	binary.LittleEndian.PutUint64(bad[:], uint64(50))
	_, err = f.WriteAt(bad[:], maxField)
	require.NoError(t, err)
	require.NoError(t, f.Sync())

	_, err = Open(ReaderConfig{Path: path, SkipChecksum: true})
	require.True(t, errors.Is(err, ErrInvalidBlockIndex))
}
