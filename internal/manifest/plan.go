package manifest

import (
	"errors"
	"math"
	"slices"
	"strings"

	"github.com/bluesky-social/jetstream/segment"
)

var ErrInvalidPlanRequest = errors.New("manifest: invalid plan request")

type PlanMode string

const (
	PlanModeSegment PlanMode = "segment"
	PlanModeBlocks  PlanMode = "blocks"
)

type PlanBackfillRequest struct {
	DIDs        []string
	Collections []string
	// CollectionPrefixes are namespace prefixes from wildcard filters
	// (e.g. "app.bsky.feed." from "app.bsky.feed.*"). Each entry ends in
	// ".". A segment collection matches if its NSID is in Collections OR
	// has any of these as a prefix. Like Collections, an empty set here
	// imposes no collection constraint; the two are combined as a union.
	CollectionPrefixes []string

	AfterSeq     uint64
	HasAfterSeq  bool
	BeforeSeq    uint64
	HasBeforeSeq bool

	// MaxEntries caps the number of work entries a single plan page may
	// contain. When the matched work exceeds it, the plan is truncated at a
	// work-unit boundary (one whole-segment entry or one coalesced block range)
	// and PlannedThroughSeq is set to the continuation cursor so the caller can
	// fetch the next page from afterSeq=PlannedThroughSeq. 0 means unlimited (no
	// pagination); a negative value is a malformed limit and is rejected with
	// ErrInvalidPlanRequest. At least one unit is always admitted per page even
	// if that single unit exceeds the cap, so pagination cannot livelock.
	MaxEntries            int
	WholeSegmentThreshold float64
}

type PlanBackfillResult struct {
	// PlannedThroughSeq is the continuation cursor: the highest sealed seq this
	// page authoritatively accounts for. When the page is truncated by
	// MaxEntries it is the MaxSeq of the last included work unit (so the next
	// page resumes at afterSeq=PlannedThroughSeq, exclusive); otherwise it
	// equals SealedTipSeq. A caller has consumed the whole sealed archive once
	// PlannedThroughSeq >= SealedTipSeq.
	PlannedThroughSeq uint64
	// SealedTipSeq is the pagination goal: the sealed-archive tip (capped by
	// beforeSeq when provided), independent of how many units this page matched
	// or whether it truncated. It is request-stable across pages of the same
	// archive snapshot, so a paginating client pins it once and loops until
	// PlannedThroughSeq reaches it.
	SealedTipSeq uint64
	Segments     []PlannedSegment
	Stats        PlanBackfillStats
}

type PlannedSegment struct {
	Idx      uint64
	Checksum uint64
	MinSeq   uint64
	MaxSeq   uint64
	Mode     PlanMode
	Blocks   []BlockRange
}

type BlockRange struct {
	First int
	Last  int
}

type PlanBackfillStats struct {
	SegmentsExamined int
	SegmentsMatched  int
	BlocksMatched    int
	Entries          int
}

// PlanBackfill selects sealed archive segment work using manifest-resident
// metadata only. It has a one-sided contract: no false negatives, possible
// false positives. Callers must exact-filter decoded rows.
func (m *Manifest) PlanBackfill(req PlanBackfillRequest) (PlanBackfillResult, error) {
	if err := m.waitReady(); err != nil {
		return PlanBackfillResult{}, err
	}
	if req.HasAfterSeq && req.HasBeforeSeq && req.BeforeSeq <= req.AfterSeq {
		return PlanBackfillResult{}, ErrInvalidPlanRequest
	}
	// MaxEntries == 0 disables per-page truncation; a negative value is a
	// malformed limit (a misconfigured caller), rejected here rather than
	// silently treated as unlimited.
	if req.MaxEntries < 0 {
		return PlanBackfillResult{}, ErrInvalidPlanRequest
	}
	if req.WholeSegmentThreshold <= 0 || req.WholeSegmentThreshold > 1 {
		return PlanBackfillResult{}, ErrInvalidPlanRequest
	}

	// The requested collection set is request-invariant; resolve it to a lookup
	// set once rather than rebuilding it for every matched segment.
	var wantCollections map[string]struct{}
	if len(req.Collections) > 0 {
		wantCollections = make(map[string]struct{}, len(req.Collections))
		for _, collection := range req.Collections {
			wantCollections[collection] = struct{}{}
		}
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	// SealedTipSeq is the pagination goal: the sealed-archive tip (capped by
	// beforeSeq), independent of how many segments the filters matched or
	// whether this page truncates. A filter that matches nothing in a non-empty
	// archive still reports the tip, because the planner has confirmed there is
	// no matching sealed data at or below it. Clients pin it and page until the
	// continuation cursor reaches it.
	var result PlanBackfillResult
	if len(m.segments) > 0 {
		tip := m.segments[len(m.segments)-1].MaxSeq
		result.SealedTipSeq = tip
		if req.HasBeforeSeq && req.BeforeSeq < result.SealedTipSeq {
			result.SealedTipSeq = req.BeforeSeq
		}
	}
	// PlannedThroughSeq defaults to the tip (untruncated case); a truncation
	// below overwrites it with the last included unit's MaxSeq.
	result.PlannedThroughSeq = result.SealedTipSeq

	// lastUnitMaxSeq tracks the MaxSeq of the most recently admitted work unit
	// (a whole segment, or a single coalesced block range). On truncation it
	// becomes the continuation cursor. Within a segment, blocks are seq-disjoint
	// and index-monotonic (the writer assigns seqs under a single lock and seal
	// walks frames in ascending file offset), so a block range's MaxSeq cleanly
	// separates included blocks (<= it) from not-yet-included ones (MinSeq > it)
	// — the next page's exclusive afterSeq re-admits exactly the next block.
	var lastUnitMaxSeq uint64
	truncated := false

	// atCap reports whether the page has reached its per-unit entry cap and the
	// next unit must be deferred to the following page. The first unit of a page
	// is always admitted (Entries == 0) even when MaxEntries is 1, so a page can
	// never return zero units with the cursor unadvanced (which would livelock a
	// paginating client). Once at least one unit is in, reaching the cap
	// truncates before the next unit.
	atCap := func() bool {
		return req.MaxEntries > 0 && result.Stats.Entries >= req.MaxEntries
	}

	for i := range m.segments {
		seg := &m.segments[i]
		result.Stats.SegmentsExamined++
		if !segmentOverlapsSeq(seg, req) {
			continue
		}

		selected := selectPlanBlocks(seg, req, wantCollections)
		if len(selected) == 0 {
			continue
		}

		planned := PlannedSegment{
			Idx:      seg.Idx,
			Checksum: seg.Header.Checksum,
			MinSeq:   seg.Header.MinSeq,
			MaxSeq:   seg.Header.MaxSeq,
		}

		// Density is selected blocks over the segment's *total* block count,
		// not over the in-window/candidate subset. This intentionally biases a
		// narrow seq window (or a heavily-compacted segment with many empty
		// blocks) toward mode=blocks, so clients fetch only the few blocks they
		// need instead of a whole segment. Both modes are correct under the
		// one-sided contract; this only trades transport precision.
		density := float64(len(selected)) / float64(max(len(seg.Blocks), 1))
		if density >= req.WholeSegmentThreshold {
			// Whole-segment unit (one entry).
			if atCap() {
				truncated = true
				break
			}
			planned.Mode = PlanModeSegment
			result.Stats.Entries++
			result.Stats.SegmentsMatched++
			result.Stats.BlocksMatched += len(selected)
			result.Segments = append(result.Segments, planned)
			lastUnitMaxSeq = seg.Header.MaxSeq
			continue
		}

		// Block mode: each coalesced range is its own work unit, so truncation
		// can land partway through a segment. Admit ranges one at a time and
		// stop at the cap; the continuation cursor then points strictly inside
		// this segment (the last included range's MaxSeq), so the un-included
		// tail blocks are re-planned on the next page rather than skipped.
		planned.Mode = PlanModeBlocks
		for _, br := range coalesceBlocks(selected) {
			if atCap() {
				truncated = true
				break
			}
			planned.Blocks = append(planned.Blocks, br)
			result.Stats.Entries++
			result.Stats.BlocksMatched += br.Last - br.First + 1
			lastUnitMaxSeq = seg.Blocks[br.Last].MaxSeq
		}
		if len(planned.Blocks) > 0 {
			result.Stats.SegmentsMatched++
			result.Segments = append(result.Segments, planned)
		}
		if truncated {
			break
		}
	}

	if truncated {
		result.PlannedThroughSeq = lastUnitMaxSeq
	}

	return result, nil
}

func segmentOverlapsSeq(seg *SegmentMetadata, req PlanBackfillRequest) bool {
	if len(seg.Blocks) == 0 || seg.Header.EventCount == 0 {
		return false
	}
	if req.HasAfterSeq && seg.Header.MaxSeq <= req.AfterSeq {
		return false
	}
	if req.HasBeforeSeq && seg.Header.MinSeq > req.BeforeSeq {
		return false
	}
	return true
}

func blockOverlapsSeq(block segment.BlockInfo, req PlanBackfillRequest) bool {
	if block.EventCount == 0 {
		return false
	}
	if req.HasAfterSeq && block.MaxSeq <= req.AfterSeq {
		return false
	}
	if req.HasBeforeSeq && block.MinSeq > req.BeforeSeq {
		return false
	}
	return true
}

func selectPlanBlocks(seg *SegmentMetadata, req PlanBackfillRequest, wantCollections map[string]struct{}) []int {
	collectionMatchAll := len(req.Collections) == 0 && len(req.CollectionPrefixes) == 0
	didMatchAll := len(req.DIDs) == 0

	if !didMatchAll && !segmentBloomMayContainAny(seg, req.DIDs) {
		return nil
	}

	var collectionIDs map[uint32]struct{}
	if !collectionMatchAll {
		collectionIDs = collectionIDsForSegment(seg, wantCollections, req.CollectionPrefixes)
		if len(collectionIDs) == 0 {
			return nil
		}
	}

	out := make([]int, 0, len(seg.Blocks))
	for i, block := range seg.Blocks {
		if !blockOverlapsSeq(block, req) {
			continue
		}
		if !collectionMatchAll && !blockHasAnyCollection(seg, i, collectionIDs) {
			continue
		}
		if !didMatchAll && !blockBloomMayContainAny(seg, i, req.DIDs) {
			continue
		}
		out = append(out, i)
	}
	return out
}

func segmentBloomMayContainAny(seg *SegmentMetadata, dids []string) bool {
	if seg.SegmentBloom == nil {
		return true
	}
	return slices.ContainsFunc(dids, seg.SegmentBloom.TestString)
}

func blockBloomMayContainAny(seg *SegmentMetadata, blockIdx int, dids []string) bool {
	if blockIdx < 0 || blockIdx >= len(seg.BlockBlooms) {
		return true
	}
	bloom := seg.BlockBlooms[blockIdx]
	if bloom == nil {
		return true
	}
	return slices.ContainsFunc(dids, bloom.TestString)
}

// collectionIDsForSegment returns the segment-local collection indices whose
// NSID is exact-matched by want OR is covered by one of prefixes, PLUS the
// segment's reserved DID-level marker sentinel ids (segment.IsDIDMarkerSentinelCollection).
// Both the request match and the sentinel union only widen the matched set,
// preserving the planner's one-sided contract (no false negatives).
//
// The sentinel union is what closes the collection-filtered DID-tombstone gap:
// #account/#identity/#sync markers carry no real collection, so the seal/rewrite
// index tags their blocks with a reserved sentinel collection instead. Always
// admitting those sentinels under a collection filter makes the marker-bearing
// blocks selectable; the per-block DID bloom still narrows by DID. A client can
// never request a sentinel itself — the names are invalid NSIDs and the request
// validator only accepts NSIDs/NSID-authority prefixes — so the sentinels enter
// the matched set only here, never via want/prefixes.
//
// Matching prefixes against this segment's own resident collection table is
// equivalent to expanding each prefix against the global collection union and
// exact-matching, because a segment can only contain collections that exist in
// that union — but it needs no global cache and stays current under the manifest
// read lock.
func collectionIDsForSegment(seg *SegmentMetadata, want map[string]struct{}, prefixes []string) map[uint32]struct{} {
	out := make(map[uint32]struct{}, min(len(want)+len(prefixes), len(seg.Collections)))
	for id, collection := range seg.Collections {
		// BlockCollections references collections by uint32 index, so an index
		// past MaxUint32 can never appear in a block and matching it would be
		// dead weight. Skipping it cannot cause a false negative (no block
		// could reference it), preserving the one-sided contract.
		if id > math.MaxUint32 {
			continue
		}
		// DID-level marker sentinels are always admitted under a collection
		// filter so the markers (which carry no real collection) stay
		// selectable; see the doc comment.
		if segment.IsDIDMarkerSentinelCollection(collection) {
			out[uint32(id)] = struct{}{}
			continue
		}
		if _, ok := want[collection]; ok {
			out[uint32(id)] = struct{}{}
			continue
		}
		for _, prefix := range prefixes {
			if strings.HasPrefix(collection, prefix) {
				out[uint32(id)] = struct{}{}
				break
			}
		}
	}
	return out
}

func blockHasAnyCollection(seg *SegmentMetadata, blockIdx int, ids map[uint32]struct{}) bool {
	if blockIdx < 0 || blockIdx >= len(seg.BlockCollections) {
		return true
	}
	for _, id := range seg.BlockCollections[blockIdx] {
		if _, ok := ids[id]; ok {
			return true
		}
	}
	return false
}

func coalesceBlocks(blocks []int) []BlockRange {
	if len(blocks) == 0 {
		return nil
	}
	out := make([]BlockRange, 0, len(blocks))
	cur := BlockRange{First: blocks[0], Last: blocks[0]}
	for _, block := range blocks[1:] {
		if block == cur.Last+1 {
			cur.Last = block
			continue
		}
		out = append(out, cur)
		cur = BlockRange{First: block, Last: block}
	}
	out = append(out, cur)
	return out
}
