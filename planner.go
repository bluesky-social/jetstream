package jetstream

import (
	"context"
	"fmt"
	"math"

	"github.com/bluesky-social/jetstream/api/jetstream"
	"github.com/jcalabro/atmos/xrpc"
)

// downloadMode selects how a planned segment's rows are fetched.
type downloadMode uint8

const (
	// modeWholeSegment downloads the entire segment file with getSegment.
	modeWholeSegment downloadMode = iota
	// modeBlocks downloads only the listed block ranges with getBlock.
	modeBlocks
)

func (m downloadMode) String() string {
	switch m {
	case modeWholeSegment:
		return "segment"
	case modeBlocks:
		return "blocks"
	default:
		return fmt.Sprintf("downloadMode(%d)", uint8(m))
	}
}

// blockRange is an inclusive range of block indices within a segment.
type blockRange struct {
	First uint32
	Last  uint32
}

// planEntry is one unit of sealed-archive transport work: either a whole
// segment or a set of inclusive block ranges within a segment. Entries are
// emitted in ascending segment order; rows within an entry preserve on-disk
// (per-DID) order.
type planEntry struct {
	// SegmentName is the filename accepted by getSegment and getBlock.
	SegmentName string
	// Index is the zero-based segment index (ascending = creation order).
	Index uint32
	// Checksum is the segment-format xxh3 metadata checksum (16-char hex).
	// It equals the getSegment ETag and uniquely identifies a segment
	// generation: a compaction rewrite produces a new checksum. Used to key
	// decoded-block caches and (later) resumable-download progress.
	Checksum string
	// MinSeq and MaxSeq bound the sequence numbers the planner believes this
	// entry may contain. Transport hints, not exact: the planner has a
	// one-sided contract (no false negatives, possible false positives).
	MinSeq uint64
	MaxSeq uint64
	// Mode selects whole-segment vs block-range download.
	Mode downloadMode
	// Blocks holds the inclusive block ranges to fetch when Mode is
	// modeBlocks; nil/empty when Mode is modeWholeSegment.
	Blocks []blockRange
}

// archivePlan is the ordered transport plan returned by the server for a historical
// backfill query, plus the sealed-archive coverage horizon.
type archivePlan struct {
	// Entries are the segments/block-ranges to download, in ascending order.
	Entries []planEntry
	// PlannedThroughSeq is the continuation cursor: the highest sealed seq this
	// page accounts for. When the server truncates the page to fit its per-page
	// entry limit it is the MaxSeq of the last included work unit; otherwise it
	// equals SealedTipSeq. Fetch the next page with AfterSeq=PlannedThroughSeq
	// (exclusive). The whole sealed archive has been consumed once
	// PlannedThroughSeq >= SealedTipSeq.
	PlannedThroughSeq uint64
	// SealedTipSeq is the pagination goal: the sealed-archive tip, capped by
	// BeforeSeq when provided. Stable across pages of the same archive
	// snapshot; the cutover loop pins it once and pages until PlannedThroughSeq
	// reaches it, then resumes the live tail from here.
	SealedTipSeq uint64
}

// planRequest is the resolved filter set for a snapshot plan. Empty kind, DID,
// and collection slices mean "match all". AfterSeq is an exclusive lower bound;
// BeforeSeq (when set) is an inclusive upper bound.
type planRequest struct {
	Kinds        []Kind
	DIDs         []string
	Collections  []string
	AfterSeq     uint64
	HasBeforeSeq bool
	BeforeSeq    uint64
}

// planner negotiates snapshot plans with a Jetstream server over XRPC.
type planner struct {
	xc *xrpc.Client
}

// newPlanner returns a planner that issues planSnapshot calls on xc.
func newPlanner(xc *xrpc.Client) *planner {
	return &planner{xc: xc}
}

// archivePlan calls network.bsky.jetstream.planSnapshot and converts the response
// into an ordered archivePlan. The plan may be a truncated page when the server's
// per-page entry limit is exceeded; the caller pages by re-issuing archivePlan with
// AfterSeq=archivePlan.PlannedThroughSeq until PlannedThroughSeq reaches SealedTipSeq.
func (p *planner) archivePlan(ctx context.Context, req planRequest) (*archivePlan, error) {
	// The planSnapshot lexicon fields are int64; reject a uint64 cursor that
	// would wrap negative rather than silently plan from the wrong range
	// (symmetric with the negative-seq guards on the response side below).
	if req.AfterSeq > math.MaxInt64 {
		return nil, fmt.Errorf("jetstream: afterSeq %d exceeds int64 max", req.AfterSeq)
	}
	if req.HasBeforeSeq && req.BeforeSeq > math.MaxInt64 {
		return nil, fmt.Errorf("jetstream: beforeSeq %d exceeds int64 max", req.BeforeSeq)
	}
	in := planInput(req)
	out, err := jetstream.JetstreamPlanSnapshot(ctx, p.xc, in)
	if err != nil {
		return nil, fmt.Errorf("jetstream: planSnapshot: %w", err)
	}
	return planFromOutput(out)
}

func planInput(req planRequest) *jetstream.JetstreamPlanSnapshot_Input {
	in := &jetstream.JetstreamPlanSnapshot_Input{}
	if len(req.Kinds) > 0 {
		in.Kinds = make([]string, len(req.Kinds))
		for i, kind := range req.Kinds {
			in.Kinds[i] = string(kind)
		}
	}
	if len(req.DIDs) > 0 {
		in.Dids = req.DIDs
	}
	if len(req.Collections) > 0 {
		in.Collections = req.Collections
	}
	// afterSeq is always meaningful (0 = from the start). The lexicon treats
	// a missing afterSeq as 0, so only set it when non-zero to keep the wire
	// minimal; either way the server applies seq > afterSeq.
	if req.AfterSeq > 0 {
		in.AfterSeq = optInt64(req.AfterSeq)
	}
	if req.HasBeforeSeq {
		in.BeforeSeq = optInt64(req.BeforeSeq)
	}
	return in
}

func planFromOutput(out *jetstream.JetstreamPlanSnapshot_Output) (*archivePlan, error) {
	if out.PlannedThroughSeq < 0 {
		return nil, fmt.Errorf("jetstream: planSnapshot returned negative plannedThroughSeq %d", out.PlannedThroughSeq)
	}
	if out.SealedTipSeq < 0 {
		return nil, fmt.Errorf("jetstream: planSnapshot returned negative sealedTipSeq %d", out.SealedTipSeq)
	}
	if out.PlannedThroughSeq > out.SealedTipSeq {
		return nil, fmt.Errorf("jetstream: planSnapshot plannedThroughSeq %d exceeds sealedTipSeq %d", out.PlannedThroughSeq, out.SealedTipSeq)
	}
	plan := &archivePlan{
		PlannedThroughSeq: uint64(out.PlannedThroughSeq),
		SealedTipSeq:      uint64(out.SealedTipSeq),
		Entries:           make([]planEntry, 0, len(out.Segments)),
	}
	for i := range out.Segments {
		entry, err := planEntryFromSegment(&out.Segments[i])
		if err != nil {
			return nil, err
		}
		plan.Entries = append(plan.Entries, entry)
	}
	return plan, nil
}

func planEntryFromSegment(seg *jetstream.JetstreamPlanSnapshot_Segment) (planEntry, error) {
	if seg.Name == "" {
		return planEntry{}, fmt.Errorf("jetstream: planSnapshot segment missing name (index %d)", seg.Index)
	}
	if seg.Index < 0 || seg.MinSeq < 0 || seg.MaxSeq < 0 {
		return planEntry{}, fmt.Errorf("jetstream: planSnapshot segment %q has negative index/seq", seg.Name)
	}
	if seg.MaxSeq < seg.MinSeq {
		return planEntry{}, fmt.Errorf("jetstream: planSnapshot segment %q has inverted seq range [%d,%d]", seg.Name, seg.MinSeq, seg.MaxSeq)
	}
	// Index is narrowed to uint32 below; reject values that would wrap silently
	// rather than key a download under the wrong index. MinSeq/MaxSeq widen to
	// uint64 and cannot overflow after the negative check above.
	if seg.Index > math.MaxUint32 {
		return planEntry{}, fmt.Errorf("jetstream: planSnapshot segment %q index %d exceeds uint32 max", seg.Name, seg.Index)
	}
	entry := planEntry{
		SegmentName: seg.Name,
		Index:       uint32(seg.Index),
		Checksum:    seg.Checksum,
		MinSeq:      uint64(seg.MinSeq),
		MaxSeq:      uint64(seg.MaxSeq),
	}
	switch seg.Mode {
	case "segment":
		entry.Mode = modeWholeSegment
	case "blocks":
		entry.Mode = modeBlocks
		entry.Blocks = make([]blockRange, 0, len(seg.Blocks))
		for _, br := range seg.Blocks {
			if br.First < 0 || br.Last < 0 || br.Last < br.First {
				return planEntry{}, fmt.Errorf("jetstream: planSnapshot segment %q has invalid block range [%d,%d]", seg.Name, br.First, br.Last)
			}
			if br.Last > math.MaxUint32 {
				return planEntry{}, fmt.Errorf("jetstream: planSnapshot segment %q block range [%d,%d] exceeds uint32 max", seg.Name, br.First, br.Last)
			}
			entry.Blocks = append(entry.Blocks, blockRange{First: uint32(br.First), Last: uint32(br.Last)})
		}
		if len(entry.Blocks) == 0 {
			return planEntry{}, fmt.Errorf("jetstream: planSnapshot segment %q has mode=blocks but no block ranges", seg.Name)
		}
	default:
		return planEntry{}, fmt.Errorf("jetstream: planSnapshot segment %q has unknown mode %q", seg.Name, seg.Mode)
	}
	return entry, nil
}
