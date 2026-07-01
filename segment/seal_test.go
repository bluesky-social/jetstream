package segment

import (
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSealAfterCloseReturnsErrClosed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	_, err = w.Seal()
	require.True(t, errors.Is(err, ErrClosed))
}

func TestSealOnEmptyWriterProducesValidFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path})
	require.NoError(t, err)

	res, err := w.Seal()
	require.NoError(t, err)
	require.EqualValues(t, 0, res.BlockCount)
	require.EqualValues(t, 0, res.EventCount)
	require.NotZero(t, res.Checksum)
	require.Greater(t, res.FileSize, int64(ReservedHeaderBytes))

	// Subsequent calls report ErrClosed.
	_, err = w.Seal()
	require.True(t, errors.Is(err, ErrClosed))

	// Close after Seal is a no-op.
	require.NoError(t, w.Close())
}

func TestSealAfterStickyErrIsRejected(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 4})
	require.NoError(t, err)
	_, err = w.Append(Event{Seq: 1, Kind: KindCreate, DID: "d"})
	require.NoError(t, err)

	// Closing the underlying file behind the writer's back makes the
	// next Write fail; flushLocked latches stickyErr. Seal must surface
	// the same latched error rather than partially succeed.
	require.NoError(t, w.file.Close())

	flushErr := w.Flush()
	require.Error(t, flushErr)

	_, err = w.Seal()
	require.ErrorIs(t, err, flushErr)
}

func TestSealRoundtripSmallStream(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)

	events := []Event{
		{Seq: 1, WitnessedAt: 100, Kind: KindCreate, DID: "did:plc:a",
			Collection: "app.bsky.feed.post", Rkey: "k1", Rev: "v1",
			Payload: []byte("p1")},
		{Seq: 2, WitnessedAt: 200, Kind: KindCreate, DID: "did:plc:b",
			Collection: "app.bsky.feed.like", Rkey: "k2", Rev: "v2",
			Payload: []byte("p2")},
		{Seq: 3, WitnessedAt: 300, Kind: KindUpdate, DID: "did:plc:a",
			Collection: "app.bsky.feed.post", Rkey: "k1", Rev: "v3",
			Payload: []byte("p3")},
	}
	for _, ev := range events {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
	}

	res, err := w.Seal()
	require.NoError(t, err)
	require.EqualValues(t, 2, res.BlockCount)
	require.EqualValues(t, 3, res.EventCount)
	require.EqualValues(t, 2, res.UniqueDIDCount)
	require.EqualValues(t, 1, res.MinSeq)
	require.EqualValues(t, 3, res.MaxSeq)
	require.EqualValues(t, 100, res.MinWitnessedAt)
	require.EqualValues(t, 300, res.MaxWitnessedAt)
	require.NotZero(t, res.Checksum)

	// Verify the on-disk header reflects the same values.
	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	headerBytes := make([]byte, ReservedHeaderBytes)
	_, err = f.ReadAt(headerBytes, 0)
	require.NoError(t, err)
	require.Equal(t, segmentMagic, headerBytes[0:4])
	require.NotZero(t, binary.LittleEndian.Uint64(headerBytes[4:12]))
	require.EqualValues(t, 1,
		binary.LittleEndian.Uint16(headerBytes[12:14]))
	require.EqualValues(t, 2,
		binary.LittleEndian.Uint32(headerBytes[14:18]))
}

func TestNewRejectsRealSealedFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	_, err = w.Append(Event{Seq: 1, Kind: KindCreate, DID: "did:plc:a"})
	require.NoError(t, err)
	_, err = w.Seal()
	require.NoError(t, err)

	_, err = New(Config{Path: path})
	require.True(t, errors.Is(err, ErrSegmentSealed))
}

func TestSealReaderRoundtripProperty(t *testing.T) {
	t.Parallel()

	iters := 30
	if !testing.Short() {
		iters = 500
	}

	r := rand.New(rand.NewPCG(42, 99))
	for it := range iters {
		dir := t.TempDir()
		path := filepath.Join(dir, "seg.jss")

		nEvents := 1 + r.IntN(50)
		maxPerBlock := 1 + r.IntN(8)

		w, err := New(Config{Path: path, MaxEventsPerBlock: maxPerBlock})
		require.NoErrorf(t, err, "iter %d", it)
		var events []Event
		for i := range nEvents {
			ev := Event{
				Seq:         uint64(i + 1),
				WitnessedAt: int64(it*1000 + i),
				Kind:        Kind(1 + r.IntN(7)),
				DID:         "did:plc:" + string(rune('a'+r.IntN(26))),
				Collection:  "app.bsky.feed." + string(rune('a'+r.IntN(5))),
				Rkey:        "k" + string(rune('a'+r.IntN(26))),
				Rev:         "rev" + string(rune('a'+r.IntN(26))),
				Payload:     []byte{byte(r.IntN(256))},
			}
			events = append(events, ev)
			full, err := w.Append(ev)
			require.NoErrorf(t, err, "iter %d append %d", it, i)
			if full {
				require.NoError(t, w.Flush())
			}
		}

		res, err := w.Seal()
		require.NoErrorf(t, err, "iter %d seal", it)
		require.EqualValues(t, len(events), res.EventCount)

		rdr, err := Open(ReaderConfig{Path: path})
		require.NoErrorf(t, err, "iter %d open", it)

		// Verify segment-level bloom: every DID we put in must come back
		// true. (False positives are allowed; we don't assert on them.)
		for _, ev := range events {
			require.Truef(t, rdr.SegmentBloom().TestString(ev.DID),
				"iter %d DID %q missing from segment bloom", it, ev.DID)
		}

		// Walk every block via DecodeBlock and reassemble the stream.
		var got []Event
		for i := range rdr.Blocks() {
			evs, err := rdr.DecodeBlock(i)
			require.NoErrorf(t, err, "iter %d block %d", it, i)
			got = append(got, evs...)
		}
		require.Lenf(t, got, len(events), "iter %d", it)
		for i := range events {
			require.Truef(t, eventsEqual(events[i], got[i]),
				"iter %d event %d mismatch", it, i)
		}

		require.NoError(t, rdr.Close())
	}
}

// TestSealPopulatesPerBlockWitnessedAtBounds asserts the seal walk
// records min/max witnessed_at on every block in the on-disk block
// index, mirroring how it already records min/max seq.
func TestSealPopulatesPerBlockWitnessedAtBounds(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)

	// Two flushes -> two blocks.
	// Block 0: witnessed_at in [100, 250]
	// Block 1: witnessed_at in [400, 1000]
	events := []Event{
		{Seq: 1, WitnessedAt: 100, Kind: KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "v", Payload: []byte("p")},
		{Seq: 2, WitnessedAt: 250, Kind: KindCreate, DID: "did:plc:b", Collection: "c", Rkey: "r", Rev: "v", Payload: []byte("p")},
		{Seq: 3, WitnessedAt: 400, Kind: KindCreate, DID: "did:plc:c", Collection: "c", Rkey: "r", Rev: "v", Payload: []byte("p")},
		{Seq: 4, WitnessedAt: 1000, Kind: KindCreate, DID: "did:plc:d", Collection: "c", Rkey: "r", Rev: "v", Payload: []byte("p")},
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

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })

	blocks := r.Blocks()
	require.Len(t, blocks, 2)

	require.EqualValues(t, 100, blocks[0].MinWitnessedAt)
	require.EqualValues(t, 250, blocks[0].MaxWitnessedAt)
	require.EqualValues(t, 400, blocks[1].MinWitnessedAt)
	require.EqualValues(t, 1000, blocks[1].MaxWitnessedAt)
}

// TestSealGolden pins the byte-exact output of a deterministic seal.
// Any accidental layout change breaks this test loudly. Regenerate
// the fixture with: go test -run TestSealGolden -update ./segment
func TestSealGolden(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "seg.jss")

	w, err := New(Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)

	events := []Event{
		{Seq: 1, WitnessedAt: 100, IndexedAt: 0, Kind: KindCreate,
			DID: "did:plc:a", Collection: "app.bsky.feed.post",
			Rkey: "k1", Rev: "v1", Payload: []byte("p1")},
		{Seq: 2, WitnessedAt: 200, IndexedAt: 250, Kind: KindCreate,
			DID: "did:plc:b", Collection: "app.bsky.feed.like",
			Rkey: "k2", Rev: "v2", Payload: []byte("p2")},
	}
	for _, ev := range events {
		_, err := w.Append(ev)
		require.NoError(t, err)
	}
	_, err = w.Seal()
	require.NoError(t, err)

	got, err := os.ReadFile(path)
	require.NoError(t, err)

	goldenPath := filepath.Join("testdata", "golden_seal.bin")
	if *updateGolden {
		require.NoError(t, os.MkdirAll(filepath.Dir(goldenPath), 0o755))
		require.NoError(t, os.WriteFile(goldenPath, got, 0o644))
		t.Logf("updated %s (%d bytes)", goldenPath, len(got))
		return
	}
	want, err := os.ReadFile(goldenPath)
	require.NoErrorf(t, err, "missing golden file; run with -update")
	require.Equal(t, want, got)
}
