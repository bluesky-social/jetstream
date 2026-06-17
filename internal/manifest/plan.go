package manifest

import (
	"errors"
	"math"

	"github.com/bluesky-social/jetstream-v2/segment"
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

	AfterSeq     uint64
	HasAfterSeq  bool
	BeforeSeq    uint64
	HasBeforeSeq bool

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
	if req.MaxEntries <= 0 {
		return PlanBackfillResult{}, ErrPlanTooLarge
	}
	if req.WholeSegmentThreshold <= 0 || req.WholeSegmentThreshold > 1 {
		return PlanBackfillResult{}, ErrInvalidPlanRequest
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

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

		selected := selectPlanBlocks(seg, req)
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

		density := float64(len(selected)) / float64(max(len(seg.Blocks), 1))
		if density >= req.WholeSegmentThreshold {
			planned.Mode = PlanModeSegment
			result.Stats.Entries++
		} else {
			planned.Mode = PlanModeBlocks
			planned.Blocks = coalesceBlocks(selected)
			result.Stats.Entries += len(planned.Blocks)
		}
		if result.Stats.Entries > req.MaxEntries {
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

func selectPlanBlocks(seg *SegmentMetadata, req PlanBackfillRequest) []int {
	collectionMatchAll := len(req.Collections) == 0
	didMatchAll := len(req.DIDs) == 0

	if !didMatchAll && !segmentBloomMayContainAny(seg, req.DIDs) {
		return nil
	}

	var collectionIDs map[uint32]struct{}
	if !collectionMatchAll {
		collectionIDs = collectionIDsForSegment(seg, req.Collections)
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
	for _, did := range dids {
		if seg.SegmentBloom.TestString(did) {
			return true
		}
	}
	return false
}

func blockBloomMayContainAny(seg *SegmentMetadata, blockIdx int, dids []string) bool {
	if blockIdx < 0 || blockIdx >= len(seg.BlockBlooms) {
		return true
	}
	bloom := seg.BlockBlooms[blockIdx]
	if bloom == nil {
		return true
	}
	for _, did := range dids {
		if bloom.TestString(did) {
			return true
		}
	}
	return false
}

func collectionIDsForSegment(seg *SegmentMetadata, collections []string) map[uint32]struct{} {
	want := make(map[string]struct{}, len(collections))
	for _, collection := range collections {
		want[collection] = struct{}{}
	}
	out := make(map[uint32]struct{}, min(len(collections), len(seg.Collections)))
	for id, collection := range seg.Collections {
		if _, ok := want[collection]; ok && id <= math.MaxUint32 {
			out[uint32(id)] = struct{}{}
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
