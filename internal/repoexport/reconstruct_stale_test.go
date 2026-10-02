package repoexport

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// staleFetcher fails chosen fetches with catalog.ErrStaleRef, the way a
// compaction rewrite landing mid-reconstruction does, and records which
// blocks were fetched successfully.
type staleFetcher struct {
	inner catalog.Fetcher
	// stale reports whether the n'th fetch (from 0) fails.
	stale func(n int) bool

	mu      sync.Mutex
	calls   int
	fetched map[[2]uint64]int
}

func (f *staleFetcher) Fetch(ctx context.Context, ref catalog.BlockRef) ([]byte, error) {
	f.mu.Lock()
	n := f.calls
	f.calls++
	f.mu.Unlock()
	if f.stale(n) {
		return nil, fmt.Errorf("%w: injected", catalog.ErrStaleRef)
	}
	frame, err := f.inner.Fetch(ctx, ref)
	if err == nil {
		f.mu.Lock()
		f.fetched[[2]uint64{ref.Segment, uint64(ref.Block)}]++
		f.mu.Unlock()
	}
	return frame, err
}

func TestReconstruct_StaleRefResumesWithoutReplayingTwice(t *testing.T) {
	t.Parallel()

	dataDir, st := newTestDataDir(t)
	var events []segment.Event
	for i := range 10 {
		events = append(events, createEvent(testDID, "app.bsky.feed.post", fmt.Sprintf("r%d", i), fmt.Sprintf("rev%02d", i), payload(fmt.Sprint(i))))
	}
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), events)

	archive := openArchive(t, dataDir)
	f := &staleFetcher{inner: archive.Fetcher, stale: func(n int) bool { return n == 1 }, fetched: map[[2]uint64]int{}}
	archive.Fetcher = f

	got, err := Reconstruct(t.Context(), Config{Archive: archive, DID: testDID})
	require.NoError(t, err)
	wantRoot, wantCount := expectedRoot(t, events)
	require.Equal(t, wantRoot, got.Root)
	require.Equal(t, wantCount, got.RecordCount)
	require.Equal(t, "rev09", got.LatestRev)

	require.Len(t, f.fetched, 3, "10 events at 4 per block")
	for blk, n := range f.fetched {
		require.Equal(t, 1, n, "block %v replayed %d times", blk, n)
	}
}

func TestReconstruct_StaleRefRetriesAreBounded(t *testing.T) {
	t.Parallel()

	dataDir, st := newTestDataDir(t)
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
	})

	archive := openArchive(t, dataDir)
	f := &staleFetcher{inner: archive.Fetcher, stale: func(int) bool { return true }, fetched: map[[2]uint64]int{}}
	archive.Fetcher = f

	_, err := Reconstruct(t.Context(), Config{Archive: archive, DID: testDID})
	require.ErrorIs(t, err, catalog.ErrStaleRef)
	require.Equal(t, maxStaleRetries+1, f.calls)
}
