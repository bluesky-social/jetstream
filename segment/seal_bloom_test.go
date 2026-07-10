package segment

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// sealSegmentForBloomTest writes events at the DEFAULT MaxEventsPerBlock
// (4096) and flushes every flushEvery events, modelling the production
// live-write shape where the 30s timer flushes partial blocks. This is
// exactly the shape where capacity-from-config oversizes blooms.
func sealSegmentForBloomTest(t *testing.T, events []Event, flushEvery int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "seg.jss")
	w, err := New(Config{Path: path})
	require.NoError(t, err)
	for i, ev := range events {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if full || (i+1)%flushEvery == 0 {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
	return path
}

// TestSealRightSizesPerBlockBlooms pins issue #302: per-block DID
// blooms must be sized for the segment's actual max per-block
// unique-DID cardinality, not for MaxEventsPerBlock. A segment whose
// blocks each hold a handful of DIDs must carry proportionally small
// blooms (the old fixed sizing was 8,409 bytes for capacity 4096).
func TestSealRightSizesPerBlockBlooms(t *testing.T) {
	t.Parallel()

	// 4 blocks x 64 events, but every block holds only 2 unique DIDs.
	var events []Event
	seq := uint64(0)
	for b := range 4 {
		for i := range 64 {
			seq++
			events = append(events, Event{
				Seq: seq, WitnessedAt: int64(seq), Kind: KindCreate,
				DID:        fmt.Sprintf("did:plc:block%d-user%d", b, i%2),
				Collection: "app.bsky.feed.post",
				Rkey:       fmt.Sprintf("k%d", seq), Rev: "v", Payload: []byte("p"),
			})
		}
	}
	path := sealSegmentForBloomTest(t, events, 64)

	ins, err := Inspect(path)
	require.NoError(t, err)
	require.True(t, ins.Sealed)
	require.Len(t, ins.Blocks, 4)

	// Capacity 2 at FP 0.001 marshals to well under 300 bytes; the old
	// fixed capacity-4096 sizing was 8,409. Assert "proportionally
	// small" rather than an exact byte count so gloom tuning changes
	// don't break this test.
	require.NotZero(t, ins.PerBlockBloomBytes)
	require.Lessf(t, ins.PerBlockBloomBytes, uint32(300),
		"per-block blooms not right-sized: %d bytes for 2 unique DIDs/block",
		ins.PerBlockBloomBytes)
}

// TestSealBloomSizeScalesWithCardinality: a DID-dense segment must
// still size its blooms up so the FP target holds.
func TestSealBloomSizeScalesWithCardinality(t *testing.T) {
	t.Parallel()

	// One block of 512 events, all distinct DIDs.
	var events []Event
	for i := range 512 {
		events = append(events, Event{
			Seq: uint64(i + 1), WitnessedAt: int64(i + 1), Kind: KindCreate,
			DID:        fmt.Sprintf("did:plc:user%08d", i),
			Collection: "app.bsky.feed.post",
			Rkey:       fmt.Sprintf("k%d", i), Rev: "v", Payload: []byte("p"),
		})
	}
	path := sealSegmentForBloomTest(t, events, 512)

	ins, err := Inspect(path)
	require.NoError(t, err)
	require.Len(t, ins.Blocks, 1)

	// 512 distinct DIDs at FP 0.001 needs on the order of a KiB of
	// filter; anything under that means the bloom was undersized and
	// the FP contract is broken.
	require.Greaterf(t, ins.PerBlockBloomBytes, uint32(700),
		"bloom undersized for 512 unique DIDs: %d bytes", ins.PerBlockBloomBytes)
}

// TestSealMixedCardinalityUsesSegmentMax: with one dense block among
// sparse ones, every bloom is sized for the max (region invariant:
// equal sizes within a segment) and no false negatives appear in any
// block.
func TestSealMixedCardinalityUsesSegmentMax(t *testing.T) {
	t.Parallel()

	var events []Event
	seq := uint64(0)
	// Block 0: sparse (1 DID x 32 events).
	for range 32 {
		seq++
		events = append(events, Event{
			Seq: seq, WitnessedAt: int64(seq), Kind: KindCreate,
			DID: "did:plc:sparse", Collection: "app.bsky.feed.post",
			Rkey: fmt.Sprintf("k%d", seq), Rev: "v", Payload: []byte("p"),
		})
	}
	// Block 1: dense (32 distinct DIDs).
	for i := range 32 {
		seq++
		events = append(events, Event{
			Seq: seq, WitnessedAt: int64(seq), Kind: KindCreate,
			DID:        fmt.Sprintf("did:plc:dense%04d", i),
			Collection: "app.bsky.feed.post",
			Rkey:       fmt.Sprintf("k%d", seq), Rev: "v", Payload: []byte("p"),
		})
	}
	path := sealSegmentForBloomTest(t, events, 32)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	require.Len(t, r.Blocks(), 2)

	// Region invariant: equal-sized blooms, loadable in bulk.
	blooms, err := r.LoadAllBlockBlooms()
	require.NoError(t, err)
	require.Len(t, blooms, 2)

	// One-sided contract: every DID selects its true block.
	sel, err := r.BlocksContainingDID("did:plc:sparse")
	require.NoError(t, err)
	require.Contains(t, sel, 0)
	for i := range 32 {
		sel, err := r.BlocksContainingDID(fmt.Sprintf("did:plc:dense%04d", i))
		require.NoError(t, err)
		require.Containsf(t, sel, 1, "dense DID %d lost from block 1", i)
	}

	// Full metadata verification (includes per-block bloom FN checks).
	require.NoError(t, VerifySealedMetadata(r))
}

// TestSealEmptyDIDOnlySegment: events with no DID (identity/account/
// sync markers) contribute nothing to blooms; capacity clamps to a
// minimal valid filter and the segment still opens and verifies.
func TestSealEmptyDIDOnlySegment(t *testing.T) {
	t.Parallel()

	var events []Event
	for i := range 8 {
		events = append(events, Event{
			Seq: uint64(i + 1), WitnessedAt: int64(i + 1),
			Kind: KindAccount, DID: "", Payload: []byte("p"),
		})
	}
	path := sealSegmentForBloomTest(t, events, 4)

	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	require.NoError(t, VerifySealedMetadata(r))

	ins, err := Inspect(path)
	require.NoError(t, err)
	// Minimal filter: one 512-bit gloom block + framing, ~90 bytes.
	require.NotZero(t, ins.PerBlockBloomBytes)
	require.Less(t, ins.PerBlockBloomBytes, uint32(150))
}
