package client

import (
	"bytes"
	"os"
	"strconv"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// decodeOneFrameForTest builds a single sealed block frame from rows and runs it
// through the production decodeFrame (the per-block Commit-slab path), returning
// the decoded events. MaxEventsPerBlock is set high so all rows land in one block.
func decodeOneFrameForTest(t *testing.T, sel RowSelector, rows []segment.Event) []Event {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/seg_slab.jss"
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: len(rows) + 1})
	require.NoError(t, err)
	for i := range rows {
		_, err := w.Append(rows[i])
		require.NoError(t, err)
	}
	_, err = w.Seal()
	require.NoError(t, err)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	hdr, err := segment.ReadSealedHeader(bytes.NewReader(raw))
	require.NoError(t, err)
	require.Equal(t, uint32(1), hdr.BlockCount, "test expects a single block")
	frame, err := segment.ReadBlockFrame(bytes.NewReader(raw), hdr, 0)
	require.NoError(t, err)

	d := NewDownloader(nil, 1, sel)
	out, err := d.decodeFrame(frame, "seg_slab.jss", 0)
	require.NoError(t, err)
	return out
}

// TestDecodeFrameSlabDistinctCommits is the core safety guard for the per-block
// Commit slab: every commit event in a block must reference a DISTINCT *Commit
// with its own correct fields. A slab indexing bug (e.g. reusing one slot, or
// off-by-one slot assignment when non-commit kinds are interleaved) would alias
// two events to the same Commit or shift fields, which this catches by asserting
// pointer-distinctness and per-event field correctness.
func TestDecodeFrameSlabDistinctCommits(t *testing.T) {
	t.Parallel()
	var rows []segment.Event
	for i := uint64(1); i <= 200; i++ {
		rows = append(rows, makeCreate(t, i, "did:plc:a", "app.bsky.feed.post", "r"+strconv.FormatUint(i, 10)))
	}
	out := decodeOneFrameForTest(t, nil, rows)
	require.Len(t, out, 200)

	seen := make(map[*Commit]bool, len(out))
	for i, ev := range out {
		require.Equal(t, KindCommit, ev.Kind)
		require.NotNil(t, ev.Commit)
		require.False(t, seen[ev.Commit], "event %d aliases an already-used *Commit (slab slot reused)", i)
		seen[ev.Commit] = true
		// Field correctness: each commit must carry its OWN rkey, not a neighbor's.
		require.Equal(t, "r"+strconv.FormatUint(uint64(i+1), 10), ev.Commit.Rkey,
			"commit %d has the wrong rkey — slab slots are misaligned", i)
		require.Equal(t, uint64(i+1), ev.Seq)
	}
}

// TestDecodeFrameSlabMixedKindsAndFilter exercises the slot-assignment logic
// when non-commit kinds are interleaved (they consume NO slab slot) and a
// selector drops some commits (dropped rows consume no slot). The surviving
// commits must still each get a distinct, correctly-populated *Commit.
func TestDecodeFrameSlabMixedKindsAndFilter(t *testing.T) {
	t.Parallel()
	// Interleave commits in two collections with identity/account/sync rows.
	rows := []segment.Event{
		makeCreate(t, 1, "did:plc:a", "app.bsky.feed.post", "keep1"),
		identityRow(t, 2, "did:plc:a"),
		makeCreate(t, 3, "did:plc:a", "app.bsky.feed.like", "drop1"), // filtered out
		makeCreate(t, 4, "did:plc:a", "app.bsky.feed.post", "keep2"),
		accountRow(t, 5, "did:plc:a"),
		makeCreate(t, 6, "did:plc:a", "app.bsky.feed.like", "drop2"), // filtered out
		makeCreate(t, 7, "did:plc:a", "app.bsky.feed.post", "keep3"),
	}
	// Selector keeps only app.bsky.feed.post commits; non-commits always pass the
	// matcher (identity/account have no collection) — mirrors real filtering.
	sel := newRowSelector(NewMatcher(PlanRequest{Collections: []string{"app.bsky.feed.post"}}), NewSuppressor())
	out := decodeOneFrameForTest(t, sel, rows)

	// Expect the 3 kept posts (identity/account are dropped by the collection
	// filter per issue #142 semantics; likes are filtered).
	var gotRkeys []string
	commits := map[*Commit]bool{}
	for _, ev := range out {
		if ev.Kind == KindCommit {
			require.NotNil(t, ev.Commit)
			require.False(t, commits[ev.Commit], "slab slot reused across surviving commits")
			commits[ev.Commit] = true
			gotRkeys = append(gotRkeys, ev.Commit.Rkey)
		}
	}
	require.Equal(t, []string{"keep1", "keep2", "keep3"}, gotRkeys,
		"surviving commits must keep their own rkeys despite interleaved non-commits and filtered rows")
}

// TestDecodeFrameSlabAllocations pins the win: decoding a block of N commits
// allocates a BOUNDED number of slices/structs for the Commit+Event backing
// (the two slabs), not O(N) separate *Commit allocations. We assert the total
// allocations for the frame decode scale sub-linearly: doubling N must not
// double the per-commit slab/event-struct allocation count. (Per-record map and
// CID allocations still scale with N; this guards the struct-slab specifically
// by comparing the marginal allocs/commit, which the slab makes ~0 for the
// Commit/Event structs.)
// Not parallel: testing.AllocsPerRun panics under t.Parallel.
//
//nolint:paralleltest // AllocsPerRun cannot run under t.Parallel
func TestDecodeFrameSlabAllocations(t *testing.T) {
	// Build delete rows: deletes skip the record-map/CID/clone work, so their
	// per-commit allocation is dominated by the *Commit + Event structs — exactly
	// what the slab removes. Without the slab each delete commit is 1 *Commit
	// alloc; with the slab a whole block of deletes is ~2 slab allocs total.
	build := func(n int) []byte {
		dir := t.TempDir()
		path := dir + "/seg.jss"
		w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: n + 1})
		require.NoError(t, err)
		for i := 0; i < n; i++ {
			_, err := w.Append(segment.Event{
				Seq: uint64(i + 1), IndexedAt: int64(i + 1), Kind: segment.KindDelete,
				DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r" + strconv.Itoa(i), Rev: "rev",
			})
			require.NoError(t, err)
		}
		_, err = w.Seal()
		require.NoError(t, err)
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		hdr, err := segment.ReadSealedHeader(bytes.NewReader(raw))
		require.NoError(t, err)
		frame, err := segment.ReadBlockFrame(bytes.NewReader(raw), hdr, 0)
		require.NoError(t, err)
		return frame
	}
	d := NewDownloader(nil, 1, nil)

	frame100 := build(100)
	frame200 := build(200)
	a100 := testing.AllocsPerRun(50, func() {
		out, err := d.decodeFrame(frame100, "seg.jss", 0)
		if err != nil || len(out) != 100 {
			t.Fatalf("decode100: %v len=%d", err, len(out))
		}
	})
	a200 := testing.AllocsPerRun(50, func() {
		out, err := d.decodeFrame(frame200, "seg.jss", 0)
		if err != nil || len(out) != 200 {
			t.Fatalf("decode200: %v len=%d", err, len(out))
		}
	})
	// With per-commit *Commit allocation, doubling rows (100->200) would add ~100
	// allocations. With the slab, the Commit/Event structs are 2 slabs regardless
	// of N, so the marginal growth from doubling is small. Deletes do no per-record
	// map/CID work, so any remaining growth is just the (slab) slices resizing —
	// the delta must be a small constant, far below the +100 a per-commit alloc
	// would add.
	t.Logf("decodeFrame deletes: 100 rows=%.0f allocs, 200 rows=%.0f allocs (delta=%.0f)", a100, a200, a200-a100)
	require.Less(t, a200-a100, float64(10),
		"doubling delete-commit rows must not add ~N allocations; the per-block Commit slab should keep the delta near-constant")
}

func identityRow(t *testing.T, seq uint64, did string) segment.Event {
	t.Helper()
	return segment.Event{Seq: seq, IndexedAt: int64(seq), Kind: segment.KindIdentity, DID: did, Payload: identityPayload(t, did)}
}

func accountRow(t *testing.T, seq uint64, did string) segment.Event {
	t.Helper()
	return segment.Event{Seq: seq, IndexedAt: int64(seq), Kind: segment.KindAccount, DID: did, Payload: accountActivePayload(t, did)}
}

func identityPayload(t *testing.T, did string) []byte {
	t.Helper()
	id := comatproto.SyncSubscribeRepos_Identity{DID: did, Handle: gt.Some("h.test"), Seq: 1, Time: "t"}
	payload, err := id.MarshalCBOR()
	require.NoError(t, err)
	return payload
}

func accountActivePayload(t *testing.T, did string) []byte {
	t.Helper()
	acct := comatproto.SyncSubscribeRepos_Account{DID: did, Active: true, Seq: 1, Time: "t"}
	payload, err := acct.MarshalCBOR()
	require.NoError(t, err)
	return payload
}
