package manifest

import (
	"errors"
	"math"
	"slices"
	"strings"

	"github.com/bluesky-social/jetstream/segment"
)

var (
	ErrInvalidPlanRequest = errors.New("manifest: invalid plan request")
	ErrPlanTooLarge       = errors.New("manifest: plan too large")
)

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

	// MaxEntries caps the number of work entries a plan may accumulate before
	// failing with ErrPlanTooLarge. 0 means unlimited (no cap); a negative
	// value is a malformed limit and is rejected with ErrInvalidPlanRequest.
	MaxEntries            int
	WholeSegmentThreshold float64
}

type PlanBackfillResult struct {
	PlannedThroughSeq uint64
	Segments          []PlannedSegment
	Stats             PlanBackfillStats
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
	// MaxEntries == 0 means unlimited; a negative value is a malformed limit,
	// not an oversized plan. Returning ErrPlanTooLarge for it would mislabel a
	// misconfigured caller; reserve that sentinel for an actually-exceeded
	// positive limit below.
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

	// PlannedThroughSeq is the sealed-archive coverage horizon: the highest
	// sealed seq the planner authoritatively accounted for (the sealed tip,
	// capped by beforeSeq), independent of how many segments the filters
	// matched. A filter that matches nothing in a non-empty archive still
	// reports the tip, because the planner has confirmed there is no matching
	// sealed data at or below it. Clients use this as the /subscribe cursor.
	var result PlanBackfillResult
	if len(m.segments) > 0 {
		tip := m.segments[len(m.segments)-1].MaxSeq
		result.PlannedThroughSeq = tip
		if req.HasBeforeSeq && req.BeforeSeq < result.PlannedThroughSeq {
			result.PlannedThroughSeq = req.BeforeSeq
		}
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

		result.Stats.SegmentsMatched++
		result.Stats.BlocksMatched += len(selected)

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
			planned.Mode = PlanModeSegment
			result.Stats.Entries++
		} else {
			planned.Mode = PlanModeBlocks
			planned.Blocks = coalesceBlocks(selected)
			result.Stats.Entries += len(planned.Blocks)
		}
		if req.MaxEntries > 0 && result.Stats.Entries > req.MaxEntries {
			return PlanBackfillResult{}, ErrPlanTooLarge
		}
		result.Segments = append(result.Segments, planned)
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
// NSID is exact-matched by want OR is covered by one of prefixes. Prefixes only
// widen the matched set, preserving the planner's one-sided contract (no false
// negatives). Matching prefixes against this segment's own resident collection
// table is equivalent to expanding each prefix against the global collection
// union and exact-matching, because a segment can only contain collections that
// exist in that union — but it needs no global cache and stays current under
// the manifest read lock.
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
