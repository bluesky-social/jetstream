package xrpcapi

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"

	"github.com/bluesky-social/jetstream-v2/api/jetstream"
	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/manifest"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/atmos/xrpcserver"
)

const (
	DefaultPlanMaxDIDs               = 1000
	DefaultPlanMaxCollections        = 25
	DefaultPlanMaxEntries            = 100000
	DefaultPlanWholeSegmentThreshold = 0.75
)

type PlanConfig struct {
	MaxDIDs               int
	MaxCollections        int
	MaxEntries            int
	WholeSegmentThreshold float64
}

func (c PlanConfig) withDefaults() PlanConfig {
	if c.MaxEntries == 0 {
		c.MaxEntries = DefaultPlanMaxEntries
	}
	if c.WholeSegmentThreshold == 0 {
		c.WholeSegmentThreshold = DefaultPlanWholeSegmentThreshold
	}
	return c
}

// validate reports whether the operator-supplied plan limits are sane. These
// invariants are the server's responsibility, so failures here map to an
// InternalError, never a client-facing InvalidRequest.
func (c PlanConfig) validate() error {
	if c.MaxDIDs < 0 {
		return fmt.Errorf("plan max DIDs must be >= 0, got %d", c.MaxDIDs)
	}
	if c.MaxCollections < 0 {
		return fmt.Errorf("plan max collections must be >= 0, got %d", c.MaxCollections)
	}
	if c.MaxEntries <= 0 {
		return fmt.Errorf("plan max entries must be positive, got %d", c.MaxEntries)
	}
	if c.WholeSegmentThreshold <= 0 || c.WholeSegmentThreshold > 1 {
		return fmt.Errorf("plan whole segment threshold must be > 0 and <= 1, got %g", c.WholeSegmentThreshold)
	}
	return nil
}

func newPlanBackfillHandler(src SegmentSource, cfg PlanConfig) xrpcserver.Handler {
	cfg = cfg.withDefaults()
	// Validate once at construction rather than per request. runtime.Build
	// already validates these limits at startup, so a non-nil cfgErr only
	// arises from direct construction with a bad config; it is a server fault,
	// surfaced as InternalError below.
	cfgErr := cfg.validate()
	return xrpcserver.Procedure(func(ctx context.Context, _ xrpcserver.Params, input *jetstream.JetstreamPlanBackfill_Input) (*jetstream.JetstreamPlanBackfill_Output, error) {
		if cfgErr != nil {
			return nil, xrpcserver.InternalError("planBackfill is misconfigured")
		}
		req, err := planRequestFromInput(input, cfg)
		if err != nil {
			return nil, err
		}
		plan, err := src.PlanBackfill(req)
		if err != nil {
			if errors.Is(err, manifest.ErrPlanTooLarge) {
				return nil, &xrpc.Error{
					StatusCode: http.StatusBadRequest,
					Name:       jetstream.ErrJetstreamPlanBackfill_PlanTooLarge,
					Message:    "plan would exceed configured limit",
				}
			}
			if errors.Is(err, manifest.ErrInvalidPlanRequest) {
				// Defense in depth: planRequestFromInput already rejects the
				// window/threshold conditions the planner guards, so this is
				// unreachable on the normal path. The generic message is fine
				// because the specific cause was already reported upstream when
				// reachable.
				return nil, xrpcserver.InvalidRequest("invalid plan request")
			}
			return nil, xrpcserver.InternalError("failed to plan backfill")
		}
		out, err := planOutput(plan)
		if err != nil {
			return nil, err
		}
		return out, nil
	})
}

func planRequestFromInput(input *jetstream.JetstreamPlanBackfill_Input, cfg PlanConfig) (manifest.PlanBackfillRequest, error) {
	if input == nil {
		input = &jetstream.JetstreamPlanBackfill_Input{}
	}
	dids, err := validatePlanDIDs(input.Dids, cfg.MaxDIDs)
	if err != nil {
		return manifest.PlanBackfillRequest{}, err
	}
	collections, err := validatePlanCollections(input.Collections, cfg.MaxCollections)
	if err != nil {
		return manifest.PlanBackfillRequest{}, err
	}

	req := manifest.PlanBackfillRequest{
		DIDs:                  dids,
		Collections:           collections,
		MaxEntries:            cfg.MaxEntries,
		WholeSegmentThreshold: cfg.WholeSegmentThreshold,
	}
	if input.AfterSeq.HasVal() {
		seq := input.AfterSeq.Val()
		if seq < 0 {
			return manifest.PlanBackfillRequest{}, xrpcserver.InvalidRequest("afterSeq must be >= 0")
		}
		req.AfterSeq = uint64(seq)
		req.HasAfterSeq = true
	}
	if input.BeforeSeq.HasVal() {
		seq := input.BeforeSeq.Val()
		if seq < 0 {
			return manifest.PlanBackfillRequest{}, xrpcserver.InvalidRequest("beforeSeq must be >= 0")
		}
		req.BeforeSeq = uint64(seq)
		req.HasBeforeSeq = true
	}
	if req.HasAfterSeq && req.HasBeforeSeq && req.BeforeSeq <= req.AfterSeq {
		return manifest.PlanBackfillRequest{}, xrpcserver.InvalidRequest("beforeSeq must be greater than afterSeq")
	}
	return req, nil
}

// validatePlanDIDs returns the distinct, syntactically-valid DIDs from raw.
// The limit is on the DISTINCT count. Deduplication happens before parsing
// (ParseDID returns its input verbatim, so the distinct set is identical
// either way), and the loop stops once the limit is reached. This bounds parse
// work and map growth to maxDIDs even when an adversary submits a body full of
// duplicate DIDs.
func validatePlanDIDs(raw []string, maxDIDs int) ([]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if maxDIDs == 0 {
		return nil, xrpcserver.InvalidRequest("DID filters are disabled")
	}
	seen := make(map[string]struct{}, min(len(raw), maxDIDs))
	out := make([]string, 0, min(len(raw), maxDIDs))
	for _, value := range raw {
		if _, ok := seen[value]; ok {
			continue
		}
		if len(out) == maxDIDs {
			return nil, xrpcserver.InvalidRequest("too many DIDs")
		}
		seen[value] = struct{}{}
		did, err := atmos.ParseDID(value)
		if err != nil {
			return nil, xrpcserver.InvalidRequest("invalid DID: " + value)
		}
		out = append(out, string(did))
	}
	return out, nil
}

// validatePlanCollections mirrors validatePlanDIDs for collection NSIDs.
func validatePlanCollections(raw []string, maxCollections int) ([]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if maxCollections == 0 {
		return nil, xrpcserver.InvalidRequest("collection filters are disabled")
	}
	seen := make(map[string]struct{}, min(len(raw), maxCollections))
	out := make([]string, 0, min(len(raw), maxCollections))
	for _, value := range raw {
		if _, ok := seen[value]; ok {
			continue
		}
		if len(out) == maxCollections {
			return nil, xrpcserver.InvalidRequest("too many collections")
		}
		seen[value] = struct{}{}
		nsid, err := atmos.ParseNSID(value)
		if err != nil {
			return nil, xrpcserver.InvalidRequest("invalid collection: " + value)
		}
		out = append(out, string(nsid))
	}
	return out, nil
}

func planOutput(plan manifest.PlanBackfillResult) (*jetstream.JetstreamPlanBackfill_Output, error) {
	plannedThrough, err := int64FromUint64(plan.PlannedThroughSeq)
	if err != nil {
		return nil, err
	}
	out := &jetstream.JetstreamPlanBackfill_Output{
		PlannedThroughSeq: plannedThrough,
		Segments:          make([]jetstream.JetstreamPlanBackfill_Segment, 0, len(plan.Segments)),
		Stats: jetstream.JetstreamPlanBackfill_Stats{
			SegmentsExamined: int64(plan.Stats.SegmentsExamined),
			SegmentsMatched:  int64(plan.Stats.SegmentsMatched),
			BlocksMatched:    int64(plan.Stats.BlocksMatched),
			Entries:          int64(plan.Stats.Entries),
		},
	}
	for _, seg := range plan.Segments {
		index, err := int64FromUint64(seg.Idx)
		if err != nil {
			return nil, err
		}
		minSeq, err := int64FromUint64(seg.MinSeq)
		if err != nil {
			return nil, err
		}
		maxSeq, err := int64FromUint64(seg.MaxSeq)
		if err != nil {
			return nil, err
		}
		row := jetstream.JetstreamPlanBackfill_Segment{
			Name:     ingest.SegmentFilename(seg.Idx),
			Index:    index,
			Checksum: checksumHex(seg.Checksum),
			MinSeq:   minSeq,
			MaxSeq:   maxSeq,
			Mode:     string(seg.Mode),
		}
		if seg.Mode == manifest.PlanModeBlocks {
			row.Blocks = make([]jetstream.JetstreamPlanBackfill_BlockRange, 0, len(seg.Blocks))
			for _, block := range seg.Blocks {
				// First/Last are small non-negative block indices (bounded by a
				// segment's block_count), so the int->int64 widening is always
				// lossless and needs no overflow guard, unlike the uint64 seq
				// fields routed through int64FromUint64.
				row.Blocks = append(row.Blocks, jetstream.JetstreamPlanBackfill_BlockRange{
					First: int64(block.First),
					Last:  int64(block.Last),
				})
			}
		}
		out.Segments = append(out.Segments, row)
	}
	return out, nil
}

func int64FromUint64(v uint64) (int64, error) {
	if v > math.MaxInt64 {
		return 0, xrpcserver.InternalError("plan value exceeds int64")
	}
	return int64(v), nil
}
