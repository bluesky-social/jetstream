package oracle

import (
	"fmt"
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// writeSegment writes events [first, first+n) to segment idx of dir, sealing
// it if seal is set and otherwise leaving it active with every block flushed.
func writeSegment(t *testing.T, fs vfs.FS, dir string, idx uint64, first uint64, n int, seal bool) {
	t.Helper()
	require.NoError(t, fs.MkdirAll(dir, 0o755))
	w, err := segment.New(segment.Config{Path: fs.PathJoin(dir, ingest.SegmentFilename(idx)), FS: fs, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for i := range n {
		seq := first + uint64(i)
		_, err := w.Append(segment.Event{
			Seq: seq, WitnessedAt: int64(seq), Kind: segment.KindCreate,
			DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: fmt.Sprint(seq), Rev: fmt.Sprintf("%06d", seq),
			Payload: []byte{byte(seq)},
		})
		require.NoError(t, err)
		if i%2 == 1 {
			require.NoError(t, w.Flush())
		}
	}
	if seal {
		_, err = w.Seal()
		require.NoError(t, err)
		return
	}
	require.NoError(t, w.Flush())
	require.NoError(t, w.Close())
}

func TestObserveSegments_CatalogAndPathsAgree(t *testing.T) {
	t.Parallel()

	fs := vfs.NewMem()
	dir := namespaceDir(fs, "/data", catalog.Main)
	writeSegment(t, fs, dir, 0, 1, 5, true)
	writeSegment(t, fs, dir, 1, 6, 3, true)
	writeSegment(t, fs, dir, 2, 9, 3, false)

	all, err := ObserveSegmentsFS(fs, "/data")
	require.NoError(t, err)
	require.Len(t, all, 11)
	for i, ev := range all {
		require.Equal(t, uint64(i+1), ev.Seq)
	}

	sealed, err := ObserveSealedSegmentsFS(fs, "/data")
	require.NoError(t, err)
	require.Equal(t, all[:8], sealed)

	boot, err := ObserveBootstrapSegmentsFS(fs, "/data")
	require.NoError(t, err, "a missing bootstrap namespace is empty to both observers")
	require.Equal(t, all, boot)

	writeSegment(t, fs, namespaceDir(fs, "/data", catalog.BootstrapLive), 0, 12, 2, false)
	boot, err = ObserveBootstrapSegmentsFS(fs, "/data")
	require.NoError(t, err)
	require.Len(t, boot, 13)
}

// TestObserveSegments_CrossCheckCatchesStrandedActive pins the cross-check's
// point: the local catalog only reads the newest unsealed file, so rows in an
// older unsealed file are invisible to catalog readers while the path walk
// still sees them. The observer must fail rather than pick one.
func TestObserveSegments_CrossCheckCatchesStrandedActive(t *testing.T) {
	t.Parallel()

	fs := vfs.NewMem()
	dir := namespaceDir(fs, "/data", catalog.Main)
	writeSegment(t, fs, dir, 0, 1, 3, false)
	writeSegment(t, fs, dir, 1, 4, 3, false)

	_, err := ObserveSegmentsFS(fs, "/data")
	require.ErrorContains(t, err, "catalog observer disagrees with path observer")
}

func TestCheckLiveSegment(t *testing.T) {
	t.Parallel()

	evs := func(seqs ...uint64) []ObservedEvent {
		out := make([]ObservedEvent, len(seqs))
		for i, s := range seqs {
			out[i] = ObservedEvent{Seq: s, DID: "did:plc:a", Rev: fmt.Sprint(s)}
		}
		return out
	}
	seg := func(sealed bool, seqs ...uint64) observedSegment {
		return observedSegment{Sealed: sealed, Events: evs(seqs...)}
	}

	cases := []struct {
		name       string
		c, p       observedSegment
		compaction bool
		ok         bool
	}{
		{"sealed unchanged", seg(true, 1, 2, 3), seg(true, 1, 2, 3), false, true},
		{"sealed lost a row", seg(true, 1, 2, 3), seg(true, 1, 3), false, false},
		{"active grew and sealed", seg(false, 1, 2), seg(true, 1, 2, 3), false, true},
		{"active shrank", seg(false, 1, 2), seg(false, 1), false, false},
		{"active rewrote a row", seg(false, 1, 2), seg(false, 1, 7, 8), false, false},
		{"compaction dropped a row", seg(true, 1, 2, 3), seg(true, 1, 3), true, true},
		{"compaction invented a row", seg(true, 1, 3), seg(true, 1, 2, 3), true, false},
		{"sealed gained rows", seg(true, 1, 2), seg(true, 1, 2, 3), true, false},
		{"active grew under compaction", seg(false, 1, 2), seg(true, 2, 3), true, true},
	}
	for _, tc := range cases {
		err := checkLiveSegment(tc.c, tc.p, tc.compaction)
		if tc.ok {
			require.NoError(t, err, tc.name)
		} else {
			require.Error(t, err, tc.name)
		}
	}

	// A path read may add newer segments but never lose one.
	opts := observeOptions{live: true}
	a := []observedSegment{{Index: 0, Sealed: true, Events: evs(1)}, {Index: 1, Events: evs(2)}}
	b := []observedSegment{{Index: 0, Sealed: true, Events: evs(1)}, {Index: 1, Sealed: true, Events: evs(2, 3)}, {Index: 2, Events: evs(4)}}
	require.NoError(t, crossCheckObservers(catalog.Main, a, b, opts))
	require.Error(t, crossCheckObservers(catalog.Main, b, a, opts))
	require.Error(t, crossCheckObservers(catalog.Main, a, b, observeOptions{}), "quiescent reads must match exactly")
}
