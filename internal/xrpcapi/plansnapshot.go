package xrpcapi

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/bluesky-social/jetstream/api/jetstream"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/xrpcserver"
)

const (
	DefaultPlanMaxDIDs               = 10000
	DefaultPlanMaxCollections        = 100
	DefaultPlanMaxEntries            = 100000
	DefaultPlanWholeSegmentThreshold = 0.75
	maxPlanKinds                     = 4
)

type PlanConfig struct {
	MaxDIDs               int
	MaxCollections        int
	MaxEntries            int
	WholeSegmentThreshold float64
}

func (c PlanConfig) withDefaults() PlanConfig {
	// MaxEntries is intentionally not defaulted here: 0 is a meaningful value
	// (unlimited), so remapping it would clobber an operator's explicit choice.
	// The CLI flag supplies DefaultPlanMaxEntries when the operator omits it.
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
	if c.MaxDIDs > DefaultPlanMaxDIDs {
		return fmt.Errorf("plan max DIDs must be <= %d, got %d", DefaultPlanMaxDIDs, c.MaxDIDs)
	}
	if c.MaxCollections < 0 {
		return fmt.Errorf("plan max collections must be >= 0, got %d", c.MaxCollections)
	}
	if c.MaxCollections > DefaultPlanMaxCollections {
		return fmt.Errorf("plan max collections must be <= %d, got %d", DefaultPlanMaxCollections, c.MaxCollections)
	}
	if c.MaxEntries < 0 {
		return fmt.Errorf("plan max entries must be >= 0, got %d", c.MaxEntries)
	}
	if c.WholeSegmentThreshold <= 0 || c.WholeSegmentThreshold > 1 {
		return fmt.Errorf("plan whole segment threshold must be > 0 and <= 1, got %g", c.WholeSegmentThreshold)
	}
	return nil
}

func newPlanSnapshotHandler(src SegmentSource, cfg PlanConfig) xrpcserver.Handler {
	cfg = cfg.withDefaults()
	// Validate once at construction rather than per request. runtime.Build
	// already validates these limits at startup, so a non-nil cfgErr only
	// arises from direct construction with a bad config; it is a server fault,
	// surfaced as InternalError below.
	cfgErr := cfg.validate()
	return xrpcserver.Procedure(func(ctx context.Context, _ xrpcserver.Params, input *jetstream.JetstreamPlanSnapshot_Input) (*jetstream.JetstreamPlanSnapshot_Output, error) {
		if cfgErr != nil {
			return nil, xrpcserver.InternalError("planSnapshot is misconfigured")
		}
		req, err := planRequestFromInput(input, cfg)
		if err != nil {
			return nil, err
		}
		plan, err := src.PlanSnapshot(req)
		if err != nil {
			if errors.Is(err, manifest.ErrInvalidPlanRequest) {
				// Defense in depth: planRequestFromInput already rejects the
				// window/threshold conditions the planner guards, so this is
				// unreachable on the normal path. The generic message is fine
				// because the specific cause was already reported upstream when
				// reachable.
				return nil, xrpcserver.InvalidRequest("invalid plan request")
			}
			return nil, xrpcserver.InternalError("failed to plan snapshot")
		}
		out, err := planOutput(plan)
		if err != nil {
			return nil, err
		}
		return out, nil
	})
}

func planRequestFromInput(input *jetstream.JetstreamPlanSnapshot_Input, cfg PlanConfig) (manifest.PlanSnapshotRequest, error) {
	if input == nil {
		input = &jetstream.JetstreamPlanSnapshot_Input{}
	}
	kinds, err := validatePlanKinds(input.Kinds)
	if err != nil {
		return manifest.PlanSnapshotRequest{}, err
	}
	dids, err := validatePlanDIDs(input.Dids, cfg.MaxDIDs)
	if err != nil {
		return manifest.PlanSnapshotRequest{}, err
	}
	collections, collectionPrefixes, err := validatePlanCollections(input.Collections, cfg.MaxCollections)
	if err != nil {
		return manifest.PlanSnapshotRequest{}, err
	}

	req := manifest.PlanSnapshotRequest{
		Kinds:                 kinds,
		DIDs:                  dids,
		Collections:           collections,
		CollectionPrefixes:    collectionPrefixes,
		MaxEntries:            cfg.MaxEntries,
		WholeSegmentThreshold: cfg.WholeSegmentThreshold,
	}
	if (len(collections) > 0 || len(collectionPrefixes) > 0) && kinds != 0 && kinds&manifest.KindCommit == 0 {
		return manifest.PlanSnapshotRequest{}, xrpcserver.InvalidRequest("collections filter can never apply: kinds excludes commit")
	}
	if input.AfterSeq.HasVal() {
		seq := input.AfterSeq.Val()
		if seq < 0 {
			return manifest.PlanSnapshotRequest{}, xrpcserver.InvalidRequest("afterSeq must be >= 0")
		}
		req.AfterSeq = uint64(seq)
		req.HasAfterSeq = true
	}
	if input.BeforeSeq.HasVal() {
		seq := input.BeforeSeq.Val()
		if seq < 0 {
			return manifest.PlanSnapshotRequest{}, xrpcserver.InvalidRequest("beforeSeq must be >= 0")
		}
		req.BeforeSeq = uint64(seq)
		req.HasBeforeSeq = true
	}
	if req.HasAfterSeq && req.HasBeforeSeq && req.BeforeSeq <= req.AfterSeq {
		return manifest.PlanSnapshotRequest{}, xrpcserver.InvalidRequest("beforeSeq must be greater than afterSeq")
	}
	return req, nil
}

func validatePlanKinds(raw []string) (manifest.KindMask, error) {
	if len(raw) > maxPlanKinds {
		return 0, xrpcserver.InvalidRequest("too many kinds")
	}
	var out manifest.KindMask
	for _, value := range raw {
		switch value {
		case "commit":
			out |= manifest.KindCommit
		case "identity":
			out |= manifest.KindIdentity
		case "account":
			out |= manifest.KindAccount
		case "sync":
			out |= manifest.KindSync
		default:
			return 0, xrpcserver.InvalidRequest("invalid kind: " + value)
		}
	}
	return out, nil
}

// validatePlanDIDs returns the distinct, syntactically-valid DIDs from raw.
// The raw array is capped before dedupe, matching subscribeEvents and the
// lexicon's maxLength contract.
func validatePlanDIDs(raw []string, maxDIDs int) ([]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if maxDIDs == 0 {
		return nil, xrpcserver.InvalidRequest("DID filters are disabled")
	}
	if len(raw) > maxDIDs {
		return nil, xrpcserver.InvalidRequest("too many DIDs")
	}
	seen := make(map[string]struct{}, min(len(raw), maxDIDs))
	out := make([]string, 0, min(len(raw), maxDIDs))
	for _, value := range raw {
		if _, ok := seen[value]; ok {
			continue
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

// wildcardSuffix is the only glob shape planSnapshot accepts: a trailing ".*"
// on a namespace prefix (e.g. "app.bsky.feed.*"). This mirrors the one shape
// /subscribe allows.
const wildcardSuffix = ".*"

// classifyCollectionPattern decides whether raw is an exact NSID or a namespace
// wildcard, returning exactly one of (exact, prefix). A wildcard "<head>.*" is
// accepted iff head is a valid NSID authority, which we check by appending a
// synthetic, known-valid name label and reusing atmos.ParseNSID as the single
// source of truth for NSID grammar. "x" is a valid probe label and keeps
// near-maximum-length wildcard heads valid. It is never stored. The returned prefix is "<head>." (e.g.
// "app.bsky.feed."), matched elsewhere with strings.HasPrefix.
func classifyCollectionPattern(raw string) (exact string, prefix string, err error) {
	if head, ok := strings.CutSuffix(raw, wildcardSuffix); ok {
		if _, perr := atmos.ParseNSID(head + ".x"); perr != nil {
			return "", "", xrpcserver.InvalidRequest("invalid collection wildcard: " + raw)
		}
		return "", head + ".", nil
	}
	nsid, perr := atmos.ParseNSID(raw)
	if perr != nil {
		return "", "", xrpcserver.InvalidRequest("invalid collection: " + raw)
	}
	return string(nsid), "", nil
}

// validatePlanCollections splits raw collection filters into distinct exact
// NSIDs and distinct namespace prefixes (from wildcards). The raw array is
// capped before dedupe, matching subscribeEvents and the lexicon maxLength;
// one wildcard still expands only against resident segment metadata. Both
// returned slices are nil when raw is empty,
// which the planner treats as match-all; a non-empty prefix set that matches no
// archived collection correctly yields an empty plan (see design doc,
// "match-nothing boundary").
func validatePlanCollections(raw []string, maxCollections int) (exact []string, prefixes []string, err error) {
	if len(raw) == 0 {
		return nil, nil, nil
	}
	if maxCollections == 0 {
		return nil, nil, xrpcserver.InvalidRequest("collection filters are disabled")
	}
	if len(raw) > maxCollections {
		return nil, nil, xrpcserver.InvalidRequest("too many collections")
	}
	// Dedup on the raw string BEFORE classifying. classifyCollectionPattern is
	// deterministic, so identical raw values classify identically; deduping
	// first bounds repeated parse work after the raw maxLength check above.
	seen := make(map[string]struct{}, min(len(raw), maxCollections))
	for _, value := range raw {
		if _, dup := seen[value]; dup {
			continue
		}
		ex, pre, cerr := classifyCollectionPattern(value)
		if cerr != nil {
			return nil, nil, cerr
		}
		seen[value] = struct{}{}
		if ex != "" {
			exact = append(exact, ex)
		} else {
			prefixes = append(prefixes, pre)
		}
	}
	return exact, prefixes, nil
}

func planOutput(plan manifest.PlanSnapshotResult) (*jetstream.JetstreamPlanSnapshot_Output, error) {
	plannedThrough, err := int64FromUint64(plan.PlannedThroughSeq)
	if err != nil {
		return nil, err
	}
	sealedTip, err := int64FromUint64(plan.SealedTipSeq)
	if err != nil {
		return nil, err
	}
	out := &jetstream.JetstreamPlanSnapshot_Output{
		PlannedThroughSeq: plannedThrough,
		SealedTipSeq:      sealedTip,
		Segments:          make([]jetstream.JetstreamPlanSnapshot_Segment, 0, len(plan.Segments)),
		Stats: jetstream.JetstreamPlanSnapshot_Stats{
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
		row := jetstream.JetstreamPlanSnapshot_Segment{
			Name:     ingest.SegmentFilename(seg.Idx),
			Index:    index,
			Checksum: checksumHex(seg.Checksum),
			MinSeq:   minSeq,
			MaxSeq:   maxSeq,
			Mode:     string(seg.Mode),
		}
		if seg.Mode == manifest.PlanModeBlocks {
			row.Blocks = make([]jetstream.JetstreamPlanSnapshot_BlockRange, 0, len(seg.Blocks))
			for _, block := range seg.Blocks {
				// First/Last are small non-negative block indices (bounded by a
				// segment's block_count), so the int->int64 widening is always
				// lossless and needs no overflow guard, unlike the uint64 seq
				// fields routed through int64FromUint64.
				row.Blocks = append(row.Blocks, jetstream.JetstreamPlanSnapshot_BlockRange{
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
