package segment

import (
	"fmt"
	"slices"

	"github.com/jcalabro/gloom"
)

// VerifySealedMetadata re-derives a sealed segment's footer metadata from its
// decoded rows and checks that the persisted footer has no false negatives.
//
// The check is intentionally row-derived: it validates the collection-count
// table, per-block collection summaries, segment DID bloom, and per-block DID
// blooms against the block data a sequential reader observes. This catches
// footer/index corruption that a pure row scan cannot see.
//
// Compaction rewrites preserve block topology and historical seq/witnessed_at
// envelopes, so this verifier does not require block/header min/max bounds to
// equal the remaining rows after compaction. It only requires every remaining
// row to fit inside those persisted envelopes.
func VerifySealedMetadata(r *Reader) error {
	if r == nil {
		return fmt.Errorf("%w: nil Reader", ErrInvalidConfig)
	}
	if r.closed.Load() {
		return ErrClosed
	}

	h := r.Header()
	blocks := r.Blocks()
	if len(blocks) != int(h.BlockCount) {
		return fmt.Errorf("segment: verify %s: block count mismatch: header=%d index=%d",
			r.path, h.BlockCount, len(blocks))
	}

	collections := r.Collections()
	counts := r.CollectionEventCounts()
	if len(counts) != len(collections) {
		return fmt.Errorf("segment: verify %s: collection count table length mismatch: collections=%d counts=%d",
			r.path, len(collections), len(counts))
	}

	blockBlooms, err := r.LoadAllBlockBlooms()
	if err != nil {
		return fmt.Errorf("segment: verify %s: load block blooms: %w", r.path, err)
	}
	if len(blockBlooms) != len(blocks) {
		return fmt.Errorf("segment: verify %s: block bloom count mismatch: blocks=%d blooms=%d",
			r.path, len(blocks), len(blockBlooms))
	}

	expected := verifyExpected{
		collectionCounts: make(map[string]uint32),
		seenCollections:  make(map[string]struct{}),
		uniqueDIDs:       make(map[string]struct{}),
	}

	for blockIdx := range blocks {
		events, err := r.DecodeBlock(blockIdx)
		if err != nil {
			return fmt.Errorf("segment: verify %s: decode block %d: %w", r.path, blockIdx, err)
		}
		if err := expected.addBlock(r.path, blockIdx, blocks[blockIdx], events); err != nil {
			return err
		}
	}

	if expected.totalEvents != h.EventCount {
		return fmt.Errorf("segment: verify %s: header event_count mismatch: footer=%d rows=%d",
			r.path, h.EventCount, expected.totalEvents)
	}
	if uint32(len(expected.uniqueDIDs)) != h.UniqueDIDCount {
		return fmt.Errorf("segment: verify %s: header unique_did_count mismatch: footer=%d rows=%d",
			r.path, h.UniqueDIDCount, len(expected.uniqueDIDs))
	}
	if err := expected.checkHeaderEnvelope(r.path, h); err != nil {
		return err
	}
	if err := verifyCollectionCounts(r.path, collections, counts, expected); err != nil {
		return err
	}
	if err := verifyBlockCollections(r.path, r, collections, expected.blockCollections); err != nil {
		return err
	}
	if err := verifyDIDBlooms(r.path, r.SegmentBloom(), blockBlooms, expected); err != nil {
		return err
	}
	return nil
}

type verifyExpected struct {
	totalEvents uint32
	uniqueDIDs  map[string]struct{}

	collectionCounts map[string]uint32
	seenCollections  map[string]struct{}
	blockCollections []map[string]struct{}
	blockDIDs        []map[string]struct{}

	sawAny           bool
	minSeq, maxSeq   uint64
	minTime, maxTime int64
}

func (e *verifyExpected) addBlock(path string, blockIdx int, info BlockInfo, events []Event) error {
	if uint32(len(events)) != info.EventCount {
		return fmt.Errorf("segment: verify %s: block %d event_count mismatch: footer=%d rows=%d",
			path, blockIdx, info.EventCount, len(events))
	}
	blockCollections := make(map[string]struct{})
	blockDIDs := make(map[string]struct{})

	for rowIdx := range events {
		ev := &events[rowIdx]
		if info.EventCount > 0 {
			if ev.Seq < info.MinSeq || ev.Seq > info.MaxSeq {
				return fmt.Errorf("segment: verify %s: block %d row %d seq %d outside footer envelope [%d,%d]",
					path, blockIdx, rowIdx, ev.Seq, info.MinSeq, info.MaxSeq)
			}
			if ev.WitnessedAt < info.MinWitnessedAt || ev.WitnessedAt > info.MaxWitnessedAt {
				return fmt.Errorf("segment: verify %s: block %d row %d witnessed_at %d outside footer envelope [%d,%d]",
					path, blockIdx, rowIdx, ev.WitnessedAt, info.MinWitnessedAt, info.MaxWitnessedAt)
			}
		}

		e.totalEvents++
		if ev.DID != "" {
			e.uniqueDIDs[ev.DID] = struct{}{}
			blockDIDs[ev.DID] = struct{}{}
		}
		if ev.Collection != "" {
			e.collectionCounts[ev.Collection]++
			e.seenCollections[ev.Collection] = struct{}{}
			blockCollections[ev.Collection] = struct{}{}
		}
		if sentinel := didMarkerSentinel(ev.Kind); sentinel != "" {
			e.seenCollections[sentinel] = struct{}{}
			blockCollections[sentinel] = struct{}{}
		}

		if !e.sawAny {
			e.minSeq, e.maxSeq = ev.Seq, ev.Seq
			e.minTime, e.maxTime = ev.WitnessedAt, ev.WitnessedAt
			e.sawAny = true
			continue
		}
		if ev.Seq < e.minSeq {
			e.minSeq = ev.Seq
		}
		if ev.Seq > e.maxSeq {
			e.maxSeq = ev.Seq
		}
		if ev.WitnessedAt < e.minTime {
			e.minTime = ev.WitnessedAt
		}
		if ev.WitnessedAt > e.maxTime {
			e.maxTime = ev.WitnessedAt
		}
	}

	e.blockCollections = append(e.blockCollections, blockCollections)
	e.blockDIDs = append(e.blockDIDs, blockDIDs)
	return nil
}

func (e verifyExpected) checkHeaderEnvelope(path string, h Header) error {
	if !e.sawAny {
		return nil
	}
	if e.minSeq < h.MinSeq || e.maxSeq > h.MaxSeq {
		return fmt.Errorf("segment: verify %s: decoded seq range [%d,%d] outside header envelope [%d,%d]",
			path, e.minSeq, e.maxSeq, h.MinSeq, h.MaxSeq)
	}
	if e.minTime < h.MinWitnessedAt || e.maxTime > h.MaxWitnessedAt {
		return fmt.Errorf("segment: verify %s: decoded witnessed_at range [%d,%d] outside header envelope [%d,%d]",
			path, e.minTime, e.maxTime, h.MinWitnessedAt, h.MaxWitnessedAt)
	}
	return nil
}

func verifyCollectionCounts(path string, collections []string, counts []uint32, expected verifyExpected) error {
	seenFooter := make(map[string]struct{}, len(collections))
	for i, collection := range collections {
		if _, dup := seenFooter[collection]; dup {
			return fmt.Errorf("segment: verify %s: duplicate footer collection %q (id=%d)",
				path, collection, i)
		}
		seenFooter[collection] = struct{}{}
		if _, ok := expected.seenCollections[collection]; !ok {
			return fmt.Errorf("segment: verify %s: footer collection %q (id=%d) is absent from decoded rows",
				path, collection, i)
		}
		want := expected.collectionCounts[collection]
		if got := counts[i]; got != want {
			return fmt.Errorf("segment: verify %s: collection %q count mismatch: footer=%d rows=%d",
				path, collection, got, want)
		}
	}
	for collection := range expected.seenCollections {
		if !slices.Contains(collections, collection) {
			return fmt.Errorf("segment: verify %s: decoded collection %q missing from footer index",
				path, collection)
		}
	}
	return nil
}

func verifyBlockCollections(path string, r *Reader, collections []string, expected []map[string]struct{}) error {
	for blockIdx, want := range expected {
		gotIDs, err := r.BlockCollections(blockIdx)
		if err != nil {
			return fmt.Errorf("segment: verify %s: read block %d collections: %w", path, blockIdx, err)
		}
		got := make(map[string]struct{}, len(gotIDs))
		for _, id := range gotIDs {
			if int(id) >= len(collections) {
				return fmt.Errorf("segment: verify %s: block %d references collection id %d beyond table len %d",
					path, blockIdx, id, len(collections))
			}
			got[collections[id]] = struct{}{}
		}
		if len(got) != len(want) {
			return fmt.Errorf("segment: verify %s: block %d collection set size mismatch: footer=%d rows=%d",
				path, blockIdx, len(got), len(want))
		}
		for collection := range want {
			if _, ok := got[collection]; !ok {
				return fmt.Errorf("segment: verify %s: block %d missing collection %q in footer index",
					path, blockIdx, collection)
			}
		}
		for collection := range got {
			if _, ok := want[collection]; !ok {
				return fmt.Errorf("segment: verify %s: block %d footer index has extra collection %q",
					path, blockIdx, collection)
			}
		}
	}
	return nil
}

func verifyDIDBlooms(path string, segmentBloom *gloom.Filter, blockBlooms []*gloom.Filter, expected verifyExpected) error {
	if segmentBloom == nil && len(expected.uniqueDIDs) > 0 {
		return fmt.Errorf("segment: verify %s: missing segment DID bloom with %d decoded DIDs",
			path, len(expected.uniqueDIDs))
	}
	for did := range expected.uniqueDIDs {
		if segmentBloom != nil && !segmentBloom.TestString(did) {
			return fmt.Errorf("segment: verify %s: segment DID bloom false negative for %q",
				path, did)
		}
	}

	for blockIdx, dids := range expected.blockDIDs {
		if blockIdx >= len(blockBlooms) {
			return fmt.Errorf("segment: verify %s: missing block DID bloom %d", path, blockIdx)
		}
		if blockBlooms[blockIdx] == nil && len(dids) > 0 {
			return fmt.Errorf("segment: verify %s: missing block %d DID bloom with %d decoded DIDs",
				path, blockIdx, len(dids))
		}
		for did := range dids {
			if blockBlooms[blockIdx] != nil && !blockBlooms[blockIdx].TestString(did) {
				return fmt.Errorf("segment: verify %s: block %d DID bloom false negative for %q",
					path, blockIdx, did)
			}
		}
	}
	return nil
}
