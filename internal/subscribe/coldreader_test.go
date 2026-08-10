package subscribe_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/stretchr/testify/require"
)

func TestColdReadBatch_BoundedAndResumes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 99, minWitnessedAt: 1_000, maxWitnessedAt: 100_000, eventCount: 100,
	})
	m := mustOpenManifest(t, segDir)
	st, w := openWriterAtTip(t, dir, 100)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	var writerPtr atomic.Pointer[ingest.Writer]
	writerPtr.Store(w)
	rd := subscribe.NewColdReader(subscribe.ColdReaderConfig{
		Manifest: m, WriterRef: &writerPtr, BlockCacheBytes: 1 << 20,
	})

	// First batch of 10 starting at seq 5.
	batch, next, err := rd.Read(context.Background(), 5, 10)
	require.NoError(t, err)
	require.Len(t, batch, 10)
	require.Equal(t, uint64(5), batch[0].Event.Seq)
	require.Equal(t, uint64(14), batch[9].Event.Seq)
	require.Equal(t, uint64(15), next)

	// Resume from next; verify contiguity.
	batch2, _, err := rd.Read(context.Background(), next, 10)
	require.NoError(t, err)
	require.Equal(t, uint64(15), batch2[0].Event.Seq)
}

func TestColdReadActiveMultiBatchAdvancesRangeWithoutPrefixRescan(t *testing.T) {
	t.Parallel()
	const blocks, perBlock = 12, 4
	_, w, rd, rec := openActiveColdReader(t, blocks, perBlock)

	cursor := uint64(1)
	var priorStart uint64
	var got []uint64
	for range blocks {
		rng, ok := w.ActiveFlushedRange(cursor)
		require.True(t, ok)
		if priorStart != 0 {
			require.Greater(t, rng.StartOffset, priorStart,
				"each resumed cold batch must seek to a later active block")
		}
		priorStart = rng.StartOffset

		rec.reset()
		batch, next, err := rd.Read(context.Background(), cursor, perBlock)
		require.NoError(t, err)
		require.Len(t, batch, perBlock)
		// The load-bearing assertion for issue #300: the batch's disk reads in
		// the framed region must start at (or after) the snapshot's block
		// offset. The quadratic WalkActiveFS path rescans from byte
		// ReservedHeaderBytes every batch and trips this on batch 2.
		for _, off := range rec.framedReads() {
			require.GreaterOrEqual(t, off, int64(rng.StartOffset),
				"cold batch at cursor %d rescanned the active prefix (read at %d, snapshot start %d)",
				cursor, off, rng.StartOffset)
		}
		for _, e := range batch {
			got = append(got, e.Event.Seq)
		}
		require.Greater(t, next, cursor)
		cursor = next
	}
	want := make([]uint64, blocks*perBlock)
	for i := range want {
		want[i] = uint64(i + 1)
	}
	require.Equal(t, want, got)
}

// TestColdReadActiveNilManifestSealedFileFailsLoud pins the no-manifest error
// contract: when the snapshotted active generation turns out to be sealed at
// open (rotation seam), a walker WITHOUT a manifest has no second source to
// converge on, so the read must fail loud with the cursor unchanged — never
// return an empty success that lets the caller jump the cursor to the floor
// past unread durable events.
func TestColdReadActiveNilManifestSealedFileFailsLoud(t *testing.T) {
	t.Parallel()
	const blocks, perBlock = 3, 4
	_, w, rd, _ := openActiveColdReader(t, blocks, perBlock)

	// Finalize the active file's header checksum out-of-band, simulating a
	// seal landing between the writer snapshot and the range walk's open.
	path := filepath.Join(w.SegmentsDir(), ingest.SegmentFilename(w.ActiveIndex()))
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	var checksum [8]byte
	binary.LittleEndian.PutUint64(checksum[:], 0xdeadbeef)
	_, err = f.WriteAt(checksum[:], 4)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	batch, next, err := rd.Read(context.Background(), 1, perBlock)
	require.ErrorIs(t, err, segment.ErrSegmentSealed,
		"nil-manifest cold read of a sealed active file must fail loud")
	require.Empty(t, batch)
	require.Equal(t, uint64(1), next, "cursor must not advance past unread events")
}

// recordingFS wraps a vfs.FS and records the offset of every ReadAt issued
// through files it opened, so tests can assert on the byte ranges a cold
// read actually touched.
type recordingFS struct {
	vfs.FS
	mu      sync.Mutex
	offsets []int64
}

func (r *recordingFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := r.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	return &recordingFile{File: f, rec: r}, nil
}

func (r *recordingFS) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.offsets = r.offsets[:0]
}

// framedReads returns the recorded offsets that landed in the framed-block
// region (at or past the reserved header). Header/checksum probes below
// ReservedHeaderBytes are expected on every open and excluded.
func (r *recordingFS) framedReads() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []int64
	for _, off := range r.offsets {
		if off >= int64(segment.ReservedHeaderBytes) {
			out = append(out, off)
		}
	}
	return out
}

type recordingFile struct {
	vfs.File
	rec *recordingFS
}

func (f *recordingFile) ReadAt(p []byte, off int64) (int, error) {
	f.rec.mu.Lock()
	f.rec.offsets = append(f.rec.offsets, off)
	f.rec.mu.Unlock()
	return f.File.ReadAt(p, off)
}

func BenchmarkColdReadActiveRange(b *testing.B) {
	for _, blocks := range []int{16, 64, 256} {
		b.Run(fmt.Sprintf("blocks=%d", blocks), func(b *testing.B) {
			_, _, rd, _ := openActiveColdReader(b, blocks, 16)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				cursor := uint64(1)
				for cursor <= uint64(blocks*16) {
					batch, next, err := rd.Read(context.Background(), cursor, 64)
					if err != nil {
						b.Fatal(err)
					}
					if len(batch) == 0 || next <= cursor {
						b.Fatalf("non-advancing active replay at %d", cursor)
					}
					cursor = next
				}
			}
			b.ReportMetric(float64(blocks*16), "events/op")
		})
	}
}

func openActiveColdReader(tb testing.TB, blocks, perBlock int) (*store.Store, *ingest.Writer, *subscribe.ColdReader, *recordingFS) {
	tb.Helper()
	dir := tb.TempDir()
	st, err := store.Open(dir, store.NewMetrics(prometheus.NewRegistry()))
	require.NoError(tb, err)
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:           filepath.Join(dir, "segments"),
		Store:                 st,
		Logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:               ingest.NewMetrics(prometheus.NewRegistry()),
		MaxEventsPerBlock:     perBlock,
		MaxSegmentBytes:       1 << 30,
		ReadLogRetentionBytes: 0,
	})
	require.NoError(tb, err)
	for range blocks {
		events := make([]segment.Event, perBlock)
		for i := range events {
			events[i] = segment.Event{Kind: segment.KindCreate, DID: "did:plc:active", Collection: "app.bsky.feed.post", Payload: []byte{0xa0}}
		}
		require.NoError(tb, w.AppendBatch(context.Background(), events))
	}
	require.NoError(tb, w.Flush(context.Background()))

	// The writer writes through the host OS filesystem; the recording FS wraps
	// vfs.Default so the cold reader sees the same files while every ReadAt
	// offset is captured for prefix-rescan assertions.
	rec := &recordingFS{FS: vfs.Default}
	var writerPtr atomic.Pointer[ingest.Writer]
	writerPtr.Store(w)
	rd := subscribe.NewColdReader(subscribe.ColdReaderConfig{WriterRef: &writerPtr, FS: rec, BlockCacheBytes: 1 << 20})
	tb.Cleanup(func() { _ = w.Close(); _ = st.Close() })
	return st, w, rd, rec
}

func TestColdReadBatch_ExhaustsBeforeMax(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 9, minWitnessedAt: 1_000, maxWitnessedAt: 9_999, eventCount: 10,
	})
	m := mustOpenManifest(t, segDir)
	st, w := openWriterAtTip(t, dir, 10)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	var writerPtr atomic.Pointer[ingest.Writer]
	writerPtr.Store(w)
	rd := subscribe.NewColdReader(subscribe.ColdReaderConfig{
		Manifest: m, WriterRef: &writerPtr, BlockCacheBytes: 1 << 20,
	})
	batch, next, err := rd.Read(context.Background(), 8, 100) // only 8,9 remain
	require.NoError(t, err)
	require.Len(t, batch, 2)
	require.Equal(t, uint64(10), next, "next is one past the last available seq")
}
