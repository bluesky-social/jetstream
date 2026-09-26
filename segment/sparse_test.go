package segment

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// sparseTestRule is the drop rule SparseRewrite must apply, written out
// independently of Tombstones as the decide function computeRewrite
// takes.
func sparseTestRule(dids map[string]uint64, records map[RecordKey]uint64, maxSeq uint64) func(*Event) RowDecision {
	return func(ev *Event) RowDecision {
		if !ev.Kind.IsMaterialization() || (maxSeq != 0 && ev.Seq > maxSeq) {
			return RowKeep
		}
		if seq, ok := dids[ev.DID]; ok && seq > ev.Seq {
			return RowDrop
		}
		if seq, ok := records[RecordKey{ev.DID, ev.Collection, ev.Rkey}]; ok && seq > ev.Seq {
			return RowDrop
		}
		return RowKeep
	}
}

// sparseGeneration is one sealed generation held as parts.
type sparseGeneration struct {
	header, footer []byte
	frames         [][]byte
}

func (g sparseGeneration) reader(t testing.TB) *Reader {
	t.Helper()
	r, err := OpenReaderParts(g.header, g.footer, frameFetcher(g.frames), ReaderOptions{})
	require.NoError(t, err)
	return r
}

// requireSparseMatchesRewrite runs SparseRewrite and computeRewrite over
// src with the same drop rule and requires the §12.3 equivalence: the
// sparse output passes VerifySealedMetadata, decodes to exactly
// Rewrite's rows, re-encodes changed blocks to the same bytes, and
// agrees on every field VerifySealedMetadata checks. It returns both
// outputs so a caller can chain another generation.
func requireSparseMatchesRewrite(t testing.TB, src sparseGeneration, dids map[string]uint64, records map[RecordKey]uint64, maxSeq uint64, opts SparseOptions) (sparse, dense sparseGeneration, rewritten bool) {
	t.Helper()
	plan, err := computeRewrite(src.reader(t), sparseTestRule(dids, records, maxSeq))
	require.NoError(t, err)

	var fetched []int
	fetch := func(i int) ([]byte, error) {
		fetched = append(fetched, i)
		return frameFetcher(src.frames)(i)
	}
	var onDrop int
	opts.OnDrop = func(ev *Event, didLevel bool) {
		onDrop++
		if didLevel {
			require.Greater(t, dids[ev.DID], ev.Seq)
		}
	}
	res, err := SparseRewrite(src.header, src.footer, fetch, NewTombstones(dids, records, maxSeq), opts)
	require.NoError(t, err)
	require.Equal(t, plan.rowsDropped > 0, res.Rewritten)
	require.Equal(t, len(fetched), res.BlocksFetched)
	if !res.Rewritten {
		return src, src, false
	}
	require.Equal(t, plan.rowsDropped, res.RowsDropped)
	require.Equal(t, int(res.RowsDropped), onDrop)
	require.Equal(t, int(plan.blocksTouched), len(res.Frames))

	frames := slices.Clone(src.frames)
	for _, f := range res.Frames {
		frames[f.Block] = f.Frame
	}
	for i := range frames {
		require.Equalf(t, plan.frames[i], frames[i], "block %d frame", i)
	}
	reused := make([]int, 0, len(src.frames))
	for i := range src.frames {
		if !slices.ContainsFunc(res.Frames, func(f SparseFrame) bool { return f.Block == i }) {
			reused = append(reused, i)
		}
	}
	require.Equal(t, reused, res.Reused)

	sparse = sparseGeneration{header: res.HeaderBytes, footer: res.Footer, frames: frames}
	dense = sparseGeneration{header: plan.headerBytes, footer: plan.footerBytes, frames: plan.frames}
	sr, dr := sparse.reader(t), dense.reader(t)
	require.NoError(t, VerifySealedMetadata(sr))
	require.NoError(t, VerifySealedMetadata(dr))
	require.Equal(t, res.Header, sr.Header())

	sh, dh := sr.Header(), dr.Header()
	require.Equal(t, dh.BlockCount, sh.BlockCount)
	require.Equal(t, dh.EventCount, sh.EventCount)
	require.Equal(t, dh.UniqueDIDCount, sh.UniqueDIDCount)
	require.Equal(t, [2]uint64{dh.MinSeq, dh.MaxSeq}, [2]uint64{sh.MinSeq, sh.MaxSeq})
	require.Equal(t, [2]int64{dh.MinWitnessedAt, dh.MaxWitnessedAt}, [2]int64{sh.MinWitnessedAt, sh.MaxWitnessedAt})
	require.Equal(t, dh.FooterOffset, sh.FooterOffset)
	require.Equal(t, dr.Blocks(), sr.Blocks())
	require.Equal(t, collectionCounts(t, dr), collectionCounts(t, sr))
	for i := range dr.Blocks() {
		require.Equalf(t, blockCollectionNames(t, dr, i), blockCollectionNames(t, sr, i), "block %d collections", i)
		de, err := dr.DecodeBlock(i)
		require.NoError(t, err)
		se, err := sr.DecodeBlock(i)
		require.NoError(t, err)
		require.Equalf(t, de, se, "block %d rows", i)
	}
	return sparse, dense, true
}

func collectionCounts(t testing.TB, r *Reader) map[string]uint32 {
	t.Helper()
	out := map[string]uint32{}
	for i, name := range r.Collections() {
		out[name] = r.CollectionEventCounts()[i]
	}
	return out
}

func blockCollectionNames(t testing.TB, r *Reader, i int) []string {
	t.Helper()
	ids, err := r.BlockCollections(i)
	require.NoError(t, err)
	names := make([]string, len(ids))
	for j, id := range ids {
		names[j] = r.Collections()[id]
	}
	slices.Sort(names)
	return names
}

func sealParts(t testing.TB, frames [][]byte) sparseGeneration {
	t.Helper()
	header, footer, _, err := BuildSealed(SliceFrameSource(frames))
	require.NoError(t, err)
	return sparseGeneration{header: header, footer: footer, frames: frames}
}

// genSparseSegment draws a small segment whose rows collide on few DIDs,
// collections, and rkeys, so tombstones hit often and whole blocks and
// DIDs vanish.
func genSparseSegment(r *rand.Rand) (events []Event, perBlock int) {
	dids := []string{"did:plc:a", "did:plc:b", "did:plc:c", "did:web:d", "did:plc:e", ""}
	colls := []string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.graph.follow", ""}
	n := 1 + r.IntN(80)
	for seq := uint64(1); seq <= uint64(n); seq++ {
		events = append(events, Event{
			Seq:         seq*2 + uint64(r.IntN(2)),
			WitnessedAt: int64(r.IntN(1000)),
			Kind:        Kind(1 + r.IntN(7)),
			DID:         dids[r.IntN(len(dids))],
			Collection:  colls[r.IntN(len(colls))],
			Rkey:        fmt.Sprintf("k%d", r.IntN(3)),
			Payload:     []byte(fmt.Sprintf("p%d", r.IntN(100))),
		})
	}
	return events, 1 + r.IntN(8)
}

// genSparseTombstones draws tombstones over events' coordinates, seqs
// spread over the segment's range.
func genSparseTombstones(r *rand.Rand, events []Event) (dids map[string]uint64, records map[RecordKey]uint64, maxSeq uint64) {
	top := events[len(events)-1].Seq + 2
	dids = map[string]uint64{}
	records = map[RecordKey]uint64{}
	for range r.IntN(3) {
		ev := events[r.IntN(len(events))]
		dids[ev.DID] = uint64(r.IntN(int(top)))
	}
	for range r.IntN(8) {
		ev := events[r.IntN(len(events))]
		records[RecordKey{ev.DID, ev.Collection, ev.Rkey}] = uint64(r.IntN(int(top)))
	}
	// A tombstone for a DID the segment never saw.
	if r.IntN(4) == 0 {
		dids["did:plc:absent"] = top
	}
	if r.IntN(3) == 0 {
		maxSeq = uint64(r.IntN(int(top)))
	}
	return dids, records, maxSeq
}

// TestSparseRewriteMatchesRewrite is the §12.3 equivalence property: over
// random segments and tombstones, the sparse rewrite and the full
// rewrite leave the same rows and the same verified metadata, through
// two generations, with and without bloom narrowing.
func TestSparseRewriteMatchesRewrite(t *testing.T) {
	t.Parallel()
	iters := 300
	if testing.Short() {
		iters = 100
	}
	r := rand.New(rand.NewPCG(12, 2))
	var rewrites, partial int
	for it := range iters {
		events, perBlock := genSparseSegment(r)
		src := sealParts(t, encodeFrames(t, events, perBlock))
		opts := SparseOptions{Name: fmt.Sprintf("iter-%d", it)}
		switch r.IntN(4) {
		case 0:
			opts.ProbeLimit = -1
		case 1:
			opts.ProbeLimit = 1 + r.IntN(8)
		}
		dids, records, maxSeq := genSparseTombstones(r, events)
		sparse, dense, ok := requireSparseMatchesRewrite(t, src, dids, records, maxSeq, opts)
		if !ok {
			continue
		}
		rewrites++
		if len(dense.frames) > 0 && sparse.reader(t).Header().EventCount > 0 {
			partial++
		}

		// The second generation starts from the sparse output, whose
		// blooms are the source's supersets; the full rewrite starts from
		// its own output. Both must still agree.
		dids2, records2, maxSeq2 := genSparseTombstones(r, events)
		plan, err := computeRewrite(dense.reader(t), sparseTestRule(dids2, records2, maxSeq2))
		require.NoError(t, err)
		sparse2, _, ok := requireSparseMatchesRewrite(t, sparse, dids2, records2, maxSeq2, opts)
		require.Equal(t, plan.rowsDropped > 0, ok)
		if ok {
			sr := sparse2.reader(t)
			for i := range plan.frames {
				se, err := sr.DecodeBlock(i)
				require.NoError(t, err)
				de, _, err := decodeBlockCompressedSized(plan.frames[i])
				require.NoError(t, err)
				require.Equal(t, de, se)
			}
			require.Equal(t, plan.header.UniqueDIDCount, sr.Header().UniqueDIDCount)
		}
	}
	require.Greater(t, rewrites, iters/4, "too few iterations dropped rows")
	require.Greater(t, partial, iters/8, "too few iterations kept rows")
}

// TestSparseRewriteFetchesOnlyCandidates checks the point of the sparse
// path: a tombstone on one DID fetches the blocks that DID is in and
// leaves the others alone, and a DID that keeps rows elsewhere costs no
// vanished-check fetch beyond its blocks.
func TestSparseRewriteFetchesOnlyCandidates(t *testing.T) {
	t.Parallel()
	var events []Event
	seq := uint64(0)
	for _, did := range []string{"did:plc:a", "did:plc:b", "did:plc:c", "did:plc:d"} {
		for range 4 {
			seq++
			events = append(events, Event{Seq: seq, Kind: KindCreate, DID: did, Collection: "app.bsky.feed.post", Rkey: fmt.Sprint(seq)})
		}
	}
	src := sealParts(t, encodeFrames(t, events, 4))

	var fetched []int
	fetch := func(i int) ([]byte, error) {
		fetched = append(fetched, i)
		return src.frames[i], nil
	}
	ts := NewTombstones(map[string]uint64{"did:plc:c": 100}, nil, 0)
	res, err := SparseRewrite(src.header, src.footer, fetch, ts, SparseOptions{})
	require.NoError(t, err)
	require.True(t, res.Rewritten)
	require.Equal(t, []int{2}, fetched, "only did:plc:c's block")
	require.Equal(t, uint64(4), res.RowsDropped)
	require.Equal(t, uint32(1), res.VanishedDIDs)
	require.Equal(t, []int{0, 1, 3}, res.Reused)

	// A record tombstone in a collection a block does not hold selects
	// nothing, even though the DID bloom hits.
	fetched = nil
	ts = NewTombstones(nil, map[RecordKey]uint64{{"did:plc:a", "app.bsky.feed.like", "1"}: 100}, 0)
	res, err = SparseRewrite(src.header, src.footer, fetch, ts, SparseOptions{})
	require.NoError(t, err)
	require.False(t, res.Rewritten)
	require.Empty(t, fetched)

	// A tombstone below a block's rows selects nothing either.
	ts = NewTombstones(map[string]uint64{"did:plc:d": 13}, nil, 0)
	res, err = SparseRewrite(src.header, src.footer, fetch, ts, SparseOptions{})
	require.NoError(t, err)
	require.False(t, res.Rewritten)
	require.Empty(t, fetched)

	// Dropping one row of did:plc:a keeps the DID: no vanished-check
	// fetch is needed because its surviving rows share the block.
	ts = NewTombstones(nil, map[RecordKey]uint64{{"did:plc:a", "app.bsky.feed.post", "1"}: 100}, 0)
	res, err = SparseRewrite(src.header, src.footer, fetch, ts, SparseOptions{})
	require.NoError(t, err)
	require.True(t, res.Rewritten)
	require.Equal(t, []int{0}, fetched)
	require.Equal(t, uint32(0), res.VanishedDIDs)
}

// TestSparseRewriteVanishedCheckFetches covers the exact vanished-DID
// check: a DID that loses every row in the candidate blocks but keeps
// one in a block no tombstone selects is not vanished, and finding that
// out fetches that block.
func TestSparseRewriteVanishedCheckFetches(t *testing.T) {
	t.Parallel()
	events := []Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "1"},
		{Seq: 2, Kind: KindCreate, DID: "did:plc:b", Collection: "app.bsky.feed.post", Rkey: "2"},
		{Seq: 3, Kind: KindIdentity, DID: "did:plc:a"},
		{Seq: 4, Kind: KindCreate, DID: "did:plc:b", Collection: "app.bsky.feed.post", Rkey: "4"},
	}
	src := sealParts(t, encodeFrames(t, events, 2))
	var fetched []int
	fetch := func(i int) ([]byte, error) {
		fetched = append(fetched, i)
		return src.frames[i], nil
	}
	ts := NewTombstones(map[string]uint64{"did:plc:a": 100}, nil, 0)
	res, err := SparseRewrite(src.header, src.footer, fetch, ts, SparseOptions{})
	require.NoError(t, err)
	require.True(t, res.Rewritten)
	require.Equal(t, uint32(0), res.VanishedDIDs, "the identity row keeps did:plc:a")
	require.Equal(t, []int{0, 1}, fetched)
	require.NoError(t, VerifySealedMetadata(sparseGeneration{res.HeaderBytes, res.Footer, [][]byte{res.Frames[0].Frame, src.frames[1]}}.reader(t)))
}

// TestSparseRewriteReserve checks the memory hook: every decode reserves
// before it fetches and releases after, and a refused reservation fails
// the rewrite without fetching.
func TestSparseRewriteReserve(t *testing.T) {
	t.Parallel()
	events, perBlock := genSparseSegment(rand.New(rand.NewPCG(3, 3)))
	src := sealParts(t, encodeFrames(t, events, perBlock))
	all := map[string]uint64{}
	for _, ev := range events {
		all[ev.DID] = 1 << 40
	}
	ts := NewTombstones(all, nil, 0)

	var held, reservations int
	fetch := func(i int) ([]byte, error) {
		require.Equal(t, 1, held, "fetch outside a reservation")
		return src.frames[i], nil
	}
	res, err := SparseRewrite(src.header, src.footer, fetch, ts, SparseOptions{
		Reserve: func(n int64) (func(), error) {
			require.Positive(t, n)
			require.Zero(t, held, "more than one block held")
			held++
			reservations++
			return func() { held-- }, nil
		},
	})
	require.NoError(t, err)
	require.Zero(t, held)
	require.Equal(t, res.BlocksFetched, reservations)

	refused := errors.New("over budget")
	_, err = SparseRewrite(src.header, src.footer, func(int) ([]byte, error) {
		t.Fatal("fetched without a reservation")
		return nil, nil
	}, ts, SparseOptions{Reserve: func(int64) (func(), error) { return nil, refused }})
	require.ErrorIs(t, err, refused)
}

// TestSparseRewriteRejectsCorruptSource checks that a block holding a
// different row count than its index entry is corruption, not a silent
// miscount.
func TestSparseRewriteRejectsCorruptSource(t *testing.T) {
	t.Parallel()
	events := []Event{
		{Seq: 1, Kind: KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "1"},
		{Seq: 2, Kind: KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "2"},
	}
	src := sealParts(t, encodeFrames(t, events, 2))
	short := encodeFrames(t, events[:1], 2)[0]
	fetch := func(int) ([]byte, error) { return short, nil }
	_, err := SparseRewrite(src.header, src.footer, fetch, NewTombstones(map[string]uint64{"did:plc:a": 9}, nil, 0), SparseOptions{})
	require.Error(t, err)

	_, err = SparseRewrite(src.header, src.footer, frameFetcher(src.frames), nil, SparseOptions{})
	require.ErrorIs(t, err, ErrInvalidConfig)
}

// FuzzSparseRewrite checks the §12.3 equivalence over fuzz-derived
// segments and tombstones, chained through a second generation.
func FuzzSparseRewrite(f *testing.F) {
	f.Add([]byte{3, 1, 0, 0, 2, 'h', 'i', 2, 1, 1, 0, 3, 2, 2, 5, 'p', 'a', 'y', 'l', 'd'}, []byte{0, 5, 1, 9, 2, 2, 0x81, 0, 40})
	f.Add([]byte{1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 0, 2, 0, 0, 0}, []byte{0x80, 0xff, 3, 0x82, 1, 9})
	f.Add([]byte{7, 1, 2, 3, 0, 2, 3, 1, 0, 1, 4, 0, 0, 0, 1, 2, 0}, []byte{1, 1, 4, 2, 3, 3, 0, 7, 0x80})
	f.Fuzz(func(t *testing.T, data, tomb []byte) {
		frames, _, raw := fuzzSegmentFrames(t, data)
		if raw || len(frames) == 0 {
			return
		}
		src := sealParts(t, frames)
		didNames := []string{"did:plc:a", "did:plc:b", "did:plc:c", "did:web:d", "", "did:plc:z"}
		colls := []string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.graph.follow", ""}
		// Each 3-byte group is one tombstone: the high bit of the first
		// byte picks DID-level, the rest pick coordinates, the third its
		// seq. A leftover byte sets the chunk bound and the probe limit.
		gen := func(tomb []byte) (map[string]uint64, map[RecordKey]uint64, uint64, SparseOptions) {
			dids, records := map[string]uint64{}, map[RecordKey]uint64{}
			for len(tomb) >= 3 {
				g := tomb[:3]
				tomb = tomb[3:]
				did := didNames[int(g[0]&0x7f)%len(didNames)]
				if g[0]&0x80 != 0 {
					dids[did] = uint64(g[2])
				} else {
					records[RecordKey{did, colls[int(g[1])%len(colls)], "k"}] = uint64(g[2])
				}
			}
			var maxSeq uint64
			var opts SparseOptions
			if len(tomb) > 0 {
				maxSeq = uint64(tomb[0] >> 1)
				if tomb[0]&1 != 0 {
					opts.ProbeLimit = -1
				}
			}
			return dids, records, maxSeq, opts
		}
		// The first half of tomb is the first generation's tombstones,
		// the second half the second's.
		dids, records, maxSeq, opts := gen(tomb[:len(tomb)/2])
		sparse, _, ok := requireSparseMatchesRewrite(t, src, dids, records, maxSeq, opts)
		if !ok {
			return
		}
		dids, records, maxSeq, opts = gen(tomb[len(tomb)/2:])
		requireSparseMatchesRewrite(t, sparse, dids, records, maxSeq, opts)
	})
}
