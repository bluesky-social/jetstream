// Package client implements the Jetstream Go client's orchestration:
// backfill-plan negotiation, sealed-segment/block download, tombstone
// suppression, the backfill-to-live cutover, and the live tail. It is
// internal: third parties consume only the root jetstream package, which
// wires this engine behind its public Client.
package client

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/bluesky-social/jetstream/api/jetstream"
	"github.com/jcalabro/atmos/xrpc"
)

// DownloadMode selects how a planned segment's rows are fetched.
type DownloadMode uint8

const (
	// ModeWholeSegment downloads the entire segment file with getSegment.
	ModeWholeSegment DownloadMode = iota
	// ModeBlocks downloads only the listed block ranges with getBlock.
	ModeBlocks
)

func (m DownloadMode) String() string {
	switch m {
	case ModeWholeSegment:
		return "segment"
	case ModeBlocks:
		return "blocks"
	default:
		return fmt.Sprintf("DownloadMode(%d)", uint8(m))
	}
}

// BlockRange is an inclusive range of block indices within a segment.
type BlockRange struct {
	First uint32
	Last  uint32
}

// PlanEntry is one unit of sealed-archive transport work: either a whole
// segment or a set of inclusive block ranges within a segment. Entries are
// emitted in ascending segment order; rows within an entry preserve on-disk
// (per-DID) order.
type PlanEntry struct {
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
	Mode DownloadMode
	// Blocks holds the inclusive block ranges to fetch when Mode is
	// ModeBlocks; nil/empty when Mode is ModeWholeSegment.
	Blocks []BlockRange
}

// Plan is the ordered transport plan returned by the server for a historical
// backfill query, plus the sealed-archive coverage horizon.
type Plan struct {
	// Entries are the segments/block-ranges to download, in ascending order.
	Entries []PlanEntry
	// PlannedThroughSeq is the highest sealed seq this plan accounts for
	// (the sealed tip, capped by beforeSeq when provided). It is the record-
	// stream cutover cursor: the live tail resumes from here so the active-
	// segment gap above the sealed tip is re-covered live. Independent of how
	// many entries the filters matched — a filter that matches nothing in a
	// non-empty archive still reports the tip.
	PlannedThroughSeq uint64
	// Stats are server-side planner diagnostics (segments examined/matched,
	// blocks matched, work entries). Useful for anti-vacuity assertions.
	Stats PlanStats
}

// PlanStats mirrors the server planner's reported work accounting.
type PlanStats struct {
	SegmentsExamined uint64
	SegmentsMatched  uint64
	BlocksMatched    uint64
	Entries          uint64
}

// PlanRequest is the resolved filter set for a backfill plan. Empty DID and
// collection slices mean "match all". AfterSeq is an exclusive lower bound;
// BeforeSeq (when set) is an inclusive upper bound.
type PlanRequest struct {
	DIDs         []string
	Collections  []string
	AfterSeq     uint64
	HasBeforeSeq bool
	BeforeSeq    uint64
}

// ErrPlanTooLarge is returned by Plan when the server refuses the query
// because it would exceed the configured response/work-entry limit. The
// caller must narrow the query (tighter seq window, fewer DIDs/collections)
// rather than expect a silently truncated plan.
var ErrPlanTooLarge = errors.New("jetstream: backfill plan too large; narrow the query")

// Planner negotiates backfill plans with a Jetstream server over XRPC.
type Planner struct {
	xc *xrpc.Client
}

// NewPlanner returns a Planner that issues planBackfill calls on xc.
func NewPlanner(xc *xrpc.Client) *Planner {
	return &Planner{xc: xc}
}

// Plan calls network.bsky.jetstream.planBackfill and converts the response
// into an ordered Plan. It returns ErrPlanTooLarge (wrapped) when the server
// rejects the query as too large.
func (p *Planner) Plan(ctx context.Context, req PlanRequest) (*Plan, error) {
	// The planBackfill lexicon fields are int64; reject a uint64 cursor that
	// would wrap negative rather than silently plan from the wrong range
	// (symmetric with the negative-seq guards on the response side below).
	if req.AfterSeq > math.MaxInt64 {
		return nil, fmt.Errorf("jetstream: afterSeq %d exceeds int64 max", req.AfterSeq)
	}
	if req.HasBeforeSeq && req.BeforeSeq > math.MaxInt64 {
		return nil, fmt.Errorf("jetstream: beforeSeq %d exceeds int64 max", req.BeforeSeq)
	}
	in := planInput(req)
	out, err := jetstream.JetstreamPlanBackfill(ctx, p.xc, in)
	if err != nil {
		if isPlanTooLarge(err) {
			return nil, fmt.Errorf("%w: %w", ErrPlanTooLarge, err)
		}
		return nil, fmt.Errorf("jetstream: planBackfill: %w", err)
	}
	return planFromOutput(out)
}

func planInput(req PlanRequest) *jetstream.JetstreamPlanBackfill_Input {
	in := &jetstream.JetstreamPlanBackfill_Input{}
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

func planFromOutput(out *jetstream.JetstreamPlanBackfill_Output) (*Plan, error) {
	if out.PlannedThroughSeq < 0 {
		return nil, fmt.Errorf("jetstream: planBackfill returned negative plannedThroughSeq %d", out.PlannedThroughSeq)
	}
	plan := &Plan{
		PlannedThroughSeq: uint64(out.PlannedThroughSeq),
		Entries:           make([]PlanEntry, 0, len(out.Segments)),
		Stats: PlanStats{
			SegmentsExamined: nonNegU64(out.Stats.SegmentsExamined),
			SegmentsMatched:  nonNegU64(out.Stats.SegmentsMatched),
			BlocksMatched:    nonNegU64(out.Stats.BlocksMatched),
			Entries:          nonNegU64(out.Stats.Entries),
		},
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

func planEntryFromSegment(seg *jetstream.JetstreamPlanBackfill_Segment) (PlanEntry, error) {
	if seg.Name == "" {
		return PlanEntry{}, fmt.Errorf("jetstream: planBackfill segment missing name (index %d)", seg.Index)
	}
	if seg.Index < 0 || seg.MinSeq < 0 || seg.MaxSeq < 0 {
		return PlanEntry{}, fmt.Errorf("jetstream: planBackfill segment %q has negative index/seq", seg.Name)
	}
	if seg.MaxSeq < seg.MinSeq {
		return PlanEntry{}, fmt.Errorf("jetstream: planBackfill segment %q has inverted seq range [%d,%d]", seg.Name, seg.MinSeq, seg.MaxSeq)
	}
	// Index is narrowed to uint32 below; reject values that would wrap silently
	// rather than key a download under the wrong index. MinSeq/MaxSeq widen to
	// uint64 and cannot overflow after the negative check above.
	if seg.Index > math.MaxUint32 {
		return PlanEntry{}, fmt.Errorf("jetstream: planBackfill segment %q index %d exceeds uint32 max", seg.Name, seg.Index)
	}
	entry := PlanEntry{
		SegmentName: seg.Name,
		Index:       uint32(seg.Index),
		Checksum:    seg.Checksum,
		MinSeq:      uint64(seg.MinSeq),
		MaxSeq:      uint64(seg.MaxSeq),
	}
	switch seg.Mode {
	case "segment":
		entry.Mode = ModeWholeSegment
	case "blocks":
		entry.Mode = ModeBlocks
		entry.Blocks = make([]BlockRange, 0, len(seg.Blocks))
		for _, br := range seg.Blocks {
			if br.First < 0 || br.Last < 0 || br.Last < br.First {
				return PlanEntry{}, fmt.Errorf("jetstream: planBackfill segment %q has invalid block range [%d,%d]", seg.Name, br.First, br.Last)
			}
			if br.Last > math.MaxUint32 {
				return PlanEntry{}, fmt.Errorf("jetstream: planBackfill segment %q block range [%d,%d] exceeds uint32 max", seg.Name, br.First, br.Last)
			}
			entry.Blocks = append(entry.Blocks, BlockRange{First: uint32(br.First), Last: uint32(br.Last)})
		}
		if len(entry.Blocks) == 0 {
			return PlanEntry{}, fmt.Errorf("jetstream: planBackfill segment %q has mode=blocks but no block ranges", seg.Name)
		}
	default:
		return PlanEntry{}, fmt.Errorf("jetstream: planBackfill segment %q has unknown mode %q", seg.Name, seg.Mode)
	}
	return entry, nil
}

func isPlanTooLarge(err error) bool {
	if xErr, ok := errors.AsType[*xrpc.Error](err); ok {
		return xErr.Name == jetstream.ErrJetstreamPlanBackfill_PlanTooLarge
	}
	return false
}
