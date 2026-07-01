package timestamp_test

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/timestamp"
	"github.com/stretchr/testify/require"
)

// fakeSelector is a scriptable Selector. routes maps a DID to the segment
// indices it currently resolves to; gen is the manifest generation. Tests
// mutate routes and bump gen to simulate a concurrent seal/compaction.
type fakeSelector struct {
	gen    uint64
	routes map[string][]uint64
	calls  int
	// onSelect, if set, runs at the start of each SelectBlocksForDID call so a
	// test can mutate state mid-selection (race simulation).
	onSelect func(did string)
}

func (f *fakeSelector) Generation() uint64 { return f.gen }

func (f *fakeSelector) SelectBlocksForDID(did string) ([]manifest.SegmentBlockSelection, error) {
	// Snapshot the routes at call entry, THEN run onSelect. This models a real
	// manifest reading its resident set under lock at a single instant: a seal
	// that "commits during" the call (onSelect bumps gen + rewrites routes)
	// does not tear this in-flight result -- it returns the pre-seal set while
	// the generation has already advanced.
	idxs := f.routes[did]
	if f.onSelect != nil {
		f.onSelect(did)
	}
	f.calls++
	out := make([]manifest.SegmentBlockSelection, 0, len(idxs))
	for _, idx := range idxs {
		out = append(out, manifest.SegmentBlockSelection{
			Idx:    idx,
			Path:   fmt.Sprintf("/segments/seg_%010d.jss", idx),
			Blocks: []int{0},
		})
	}
	return out, nil
}

// readOffsets reads a per-segment offset file back into a slice of int64.
func readOffsets(t *testing.T, jobDir string, idx uint64) []int64 {
	t.Helper()
	path := filepath.Join(jobDir, timestamp.OffsetFileName(idx))
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		require.NoError(t, err)
	}
	require.Zero(t, len(data)%8, "offset file must be a whole number of uint64s")
	out := make([]int64, 0, len(data)/8)
	for i := 0; i < len(data); i += 8 {
		out = append(out, int64(binary.LittleEndian.Uint64(data[i:i+8])))
	}
	return out
}

func newBucketer(t *testing.T, sel timestamp.Selector, cfg timestamp.BucketerConfig) (*timestamp.Bucketer, string) {
	t.Helper()
	jobDir := t.TempDir()
	cfg.Selector = sel
	cfg.JobDir = jobDir
	b, err := timestamp.NewBucketer(cfg)
	require.NoError(t, err)
	return b, jobDir
}

func TestBucket_RoutesToCandidateSegments(t *testing.T) {
	t.Parallel()
	sel := &fakeSelector{gen: 1, routes: map[string][]uint64{
		"did:plc:alice": {2, 5},
		"did:plc:bob":   {5},
	}}
	b, jobDir := newBucketer(t, sel, timestamp.BucketerConfig{})

	require.NoError(t, b.Route(timestamp.Row{DID: "did:plc:alice", Offset: 100}))
	require.NoError(t, b.Route(timestamp.Row{DID: "did:plc:bob", Offset: 200}))
	require.NoError(t, b.Route(timestamp.Row{DID: "did:plc:alice", Offset: 300}))
	require.NoError(t, b.Close())

	require.Equal(t, []int64{100, 300}, readOffsets(t, jobDir, 2))
	require.Equal(t, []int64{100, 200, 300}, readOffsets(t, jobDir, 5))

	stats := b.Stats()
	require.EqualValues(t, 3, stats.RowsRouted)
	require.EqualValues(t, 0, stats.RowsNoCandidate)
	require.EqualValues(t, 5, stats.OffsetsWritten) // alice(2)+bob(1)+alice(2)
	require.Equal(t, 2, stats.SegmentsTouched)
}

// TestBucket_CloseSyncsJobDir guards the durability anchor the import
// manager's Bucketed=true checkpoint depends on: Close must fsync the job dir
// (and so must notice it vanished), not just close file descriptors. Real
// power-loss durability isn't testable here; error-on-missing-dir proves the
// directory fsync is present and ordered before Close returns success.
func TestBucket_CloseSyncsJobDir(t *testing.T) {
	t.Parallel()
	sel := &fakeSelector{gen: 1, routes: map[string][]uint64{"did:plc:alice": {2}}}
	b, jobDir := newBucketer(t, sel, timestamp.BucketerConfig{})
	require.NoError(t, b.Route(timestamp.Row{DID: "did:plc:alice", Offset: 100}))
	require.NoError(t, os.RemoveAll(jobDir))
	require.Error(t, b.Close(), "Close must surface a job-dir sync failure")
}

func TestBucket_NoCandidateRowsCounted(t *testing.T) {
	t.Parallel()
	sel := &fakeSelector{gen: 1, routes: map[string][]uint64{
		"did:plc:present": {0},
	}}
	b, jobDir := newBucketer(t, sel, timestamp.BucketerConfig{})

	require.NoError(t, b.Route(timestamp.Row{DID: "did:plc:present", Offset: 10}))
	require.NoError(t, b.Route(timestamp.Row{DID: "did:plc:absent", Offset: 20}))
	require.NoError(t, b.Close())

	require.Equal(t, []int64{10}, readOffsets(t, jobDir, 0))
	stats := b.Stats()
	require.EqualValues(t, 1, stats.RowsRouted)
	require.EqualValues(t, 1, stats.RowsNoCandidate)
}

// TestBucket_CacheHitsAvoidReselect proves the DID cache collapses a same-DID
// burst to a single Selector call (the DID-grouped-input optimization).
func TestBucket_CacheHitsAvoidReselect(t *testing.T) {
	t.Parallel()
	sel := &fakeSelector{gen: 1, routes: map[string][]uint64{"did:plc:a": {1}}}
	b, _ := newBucketer(t, sel, timestamp.BucketerConfig{})
	for i := range 100 {
		require.NoError(t, b.Route(timestamp.Row{DID: "did:plc:a", Offset: int64(i)}))
	}
	require.NoError(t, b.Close())
	require.Equal(t, 1, sel.calls, "same-DID burst must reselect only once")
	require.EqualValues(t, 99, b.Stats().DIDCacheHits)
	require.EqualValues(t, 1, b.Stats().DIDCacheMisses)
}

// TestBucket_StaleCacheEntryRecomputesOnGenerationBump is the coherence anchor
// (Jim's stale-cache worry). When the manifest generation advances, a cached
// DID selection is discarded and recomputed, so a segment that appeared after
// the first selection is picked up rather than silently dropped.
func TestBucket_StaleCacheEntryRecomputesOnGenerationBump(t *testing.T) {
	t.Parallel()
	sel := &fakeSelector{gen: 1, routes: map[string][]uint64{"did:plc:a": {1}}}
	b, jobDir := newBucketer(t, sel, timestamp.BucketerConfig{})

	// First route caches did:plc:a -> {seg 1} at generation 1.
	require.NoError(t, b.Route(timestamp.Row{DID: "did:plc:a", Offset: 10}))

	// A compaction/seal happens: the DID now also lives in a new segment 7,
	// and the manifest generation advances.
	sel.routes["did:plc:a"] = []uint64{1, 7}
	sel.gen = 2

	// Second route must recompute (stale cache) and pick up segment 7.
	require.NoError(t, b.Route(timestamp.Row{DID: "did:plc:a", Offset: 20}))
	require.NoError(t, b.Close())

	require.Equal(t, []int64{10, 20}, readOffsets(t, jobDir, 1))
	require.Equal(t, []int64{20}, readOffsets(t, jobDir, 7),
		"row after the generation bump must reach the newly-added segment")

	stats := b.Stats()
	require.EqualValues(t, 1, stats.StaleEvictions)
	require.EqualValues(t, 2, stats.SelectorCallCount, "one initial select + one recompute")
}

// TestBucket_RaceSealDuringSelectionIsNotFalselyFresh covers the subtle window:
// the manifest advances DURING a SelectBlocksForDID call. Because the bucketer
// samples the generation BEFORE selecting, the resulting entry is tagged with
// the pre-bump generation, so the very next lookup sees it as stale and
// recomputes against the now-current manifest -- it is never falsely treated as
// fresh (which would strand the row that raced the seal).
func TestBucket_RaceSealDuringSelectionIsNotFalselyFresh(t *testing.T) {
	t.Parallel()
	sel := &fakeSelector{gen: 1, routes: map[string][]uint64{"did:plc:a": {1}}}
	b, jobDir := newBucketer(t, sel, timestamp.BucketerConfig{})

	// The seal fires the instant we start selecting: generation advances to 2
	// and segment 7 appears, but this in-flight select still returns the old
	// {1} (it read the pre-seal resident set). The entry is tagged gen=1.
	sel.onSelect = func(string) {
		if sel.gen == 1 {
			sel.gen = 2
			sel.routes["did:plc:a"] = []uint64{1, 7}
		}
	}

	require.NoError(t, b.Route(timestamp.Row{DID: "did:plc:a", Offset: 10}))
	// Next row: cached entry is gen=1, current gen=2 -> stale -> recompute,
	// now returning {1,7}.
	require.NoError(t, b.Route(timestamp.Row{DID: "did:plc:a", Offset: 20}))
	require.NoError(t, b.Close())

	require.Equal(t, []int64{10, 20}, readOffsets(t, jobDir, 1))
	require.Equal(t, []int64{20}, readOffsets(t, jobDir, 7),
		"a seal racing the selection must not leave a permanently-stale cache entry")
	require.EqualValues(t, 1, b.Stats().StaleEvictions)
}

// TestBucket_DIDCacheEvictionRecomputes proves LRU eviction is transparent to
// correctness: an evicted DID is recomputed (extra Selector call) but routes
// identically.
func TestBucket_DIDCacheEvictionRecomputes(t *testing.T) {
	t.Parallel()
	sel := &fakeSelector{gen: 1, routes: map[string][]uint64{
		"did:plc:a": {1},
		"did:plc:b": {2},
	}}
	b, jobDir := newBucketer(t, sel, timestamp.BucketerConfig{DIDCacheSize: 1})

	require.NoError(t, b.Route(timestamp.Row{DID: "did:plc:a", Offset: 10})) // caches a, select #1
	require.NoError(t, b.Route(timestamp.Row{DID: "did:plc:b", Offset: 20})) // evicts a, select #2
	require.NoError(t, b.Route(timestamp.Row{DID: "did:plc:a", Offset: 30})) // a was evicted -> select #3
	require.NoError(t, b.Close())

	require.Equal(t, []int64{10, 30}, readOffsets(t, jobDir, 1))
	require.Equal(t, []int64{20}, readOffsets(t, jobDir, 2))
	require.EqualValues(t, 3, b.Stats().SelectorCallCount)
}

// TestBucket_FDPoolEvictionReopensAppend proves the bounded FD pool is
// correct: with a limit below the fan-out, evicted files are reopened O_APPEND
// and no offsets are lost or truncated.
func TestBucket_FDPoolEvictionReopensAppend(t *testing.T) {
	t.Parallel()
	// Three segments, but only one open FD allowed at a time. Interleave writes
	// so every segment is repeatedly evicted and reopened.
	sel := &fakeSelector{gen: 1, routes: map[string][]uint64{
		"did:plc:a": {0},
		"did:plc:b": {1},
		"did:plc:c": {2},
	}}
	b, jobDir := newBucketer(t, sel, timestamp.BucketerConfig{OpenFileLimit: 1})

	dids := []string{"did:plc:a", "did:plc:b", "did:plc:c"}
	for round := range 5 {
		for si, did := range dids {
			off := int64(round*10 + si)
			require.NoError(t, b.Route(timestamp.Row{DID: did, Offset: off}))
		}
	}
	require.NoError(t, b.Close())

	for si := range dids {
		got := readOffsets(t, jobDir, uint64(si))
		want := []int64{int64(si), int64(10 + si), int64(20 + si), int64(30 + si), int64(40 + si)}
		require.Equal(t, want, got, "segment %d must retain all appended offsets across FD eviction", si)
	}
}

func TestBucket_FanOutCountsOffsets(t *testing.T) {
	t.Parallel()
	sel := &fakeSelector{gen: 1, routes: map[string][]uint64{
		"did:plc:wide": {0, 1, 2, 3},
	}}
	b, _ := newBucketer(t, sel, timestamp.BucketerConfig{})
	require.NoError(t, b.Route(timestamp.Row{DID: "did:plc:wide", Offset: 42}))
	require.NoError(t, b.Close())
	stats := b.Stats()
	require.EqualValues(t, 1, stats.RowsRouted)
	require.EqualValues(t, 4, stats.OffsetsWritten)
	require.Equal(t, 4, stats.SegmentsTouched)
}

func TestNewBucketer_Validation(t *testing.T) {
	t.Parallel()
	_, err := timestamp.NewBucketer(timestamp.BucketerConfig{JobDir: t.TempDir()})
	require.ErrorContains(t, err, "Selector")

	_, err = timestamp.NewBucketer(timestamp.BucketerConfig{Selector: &fakeSelector{}})
	require.ErrorContains(t, err, "JobDir")

	// JobDir points at a file, not a directory.
	f := filepath.Join(t.TempDir(), "afile")
	require.NoError(t, os.WriteFile(f, []byte("x"), 0o644))
	_, err = timestamp.NewBucketer(timestamp.BucketerConfig{Selector: &fakeSelector{}, JobDir: f})
	require.ErrorContains(t, err, "not a directory")
}

// TestBucket_EndToEndWithParse wires Parse -> Bucketer.Route and verifies the
// recorded offsets actually seek back to the right rows in the source CSV.
// This is the full Phase A+B contract that Phase C will consume.
func TestBucket_EndToEndWithParse(t *testing.T) {
	t.Parallel()
	rowA := "at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,,\n"
	rowB := "at://did:plc:bob/app.bsky.feed.post/r2,2023-01-02T03:04:05Z,,\n"
	rowA2 := "at://did:plc:alice/app.bsky.feed.like/r3,2024-01-02T03:04:05Z,,\n"
	src := "uri,timestamp,scope,cid\n" + rowA + rowB + rowA2

	sel := &fakeSelector{gen: 1, routes: map[string][]uint64{
		"did:plc:alice": {3},
		"did:plc:bob":   {8},
	}}
	b, jobDir := newBucketer(t, sel, timestamp.BucketerConfig{})

	stats, err := timestamp.Parse(strings.NewReader(src), timestamp.Options{OnRow: b.Route})
	require.NoError(t, err)
	require.NoError(t, b.Close())
	require.EqualValues(t, 3, stats.RowsValid)

	// Segment 3 got alice's two rows; segment 8 got bob's one row.
	aliceOffsets := readOffsets(t, jobDir, 3)
	bobOffsets := readOffsets(t, jobDir, 8)
	require.Len(t, aliceOffsets, 2)
	require.Len(t, bobOffsets, 1)

	// Every recorded offset must seek back to the exact row bytes in src.
	require.True(t, strings.HasPrefix(src[aliceOffsets[0]:], rowA))
	require.True(t, strings.HasPrefix(src[aliceOffsets[1]:], rowA2))
	require.True(t, strings.HasPrefix(src[bobOffsets[0]:], rowB))
}
