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
	if c.MaxCollections < 0 {
		return fmt.Errorf("plan max collections must be >= 0, got %d", c.MaxCollections)
	}
	if c.MaxEntries < 0 {
		return fmt.Errorf("plan max entries must be >= 0, got %d", c.MaxEntries)
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
	collections, collectionPrefixes, err := validatePlanCollections(input.Collections, cfg.MaxCollections)
	if err != nil {
		return manifest.PlanBackfillRequest{}, err
	}

	req := manifest.PlanBackfillRequest{
		DIDs:                  dids,
		Collections:           collections,
		CollectionPrefixes:    collectionPrefixes,
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

// wildcardSuffix is the only glob shape planBackfill accepts: a trailing ".*"
// on a namespace prefix (e.g. "app.bsky.feed.*"). This mirrors the one shape
// /subscribe allows.
const wildcardSuffix = ".*"

// classifyCollectionPattern decides whether raw is an exact NSID or a namespace
// wildcard, returning exactly one of (exact, prefix). A wildcard "<head>.*" is
// accepted iff head is a valid NSID authority, which we check by appending a
// synthetic, known-valid name label and reusing atmos.ParseNSID as the single
// source of truth for NSID grammar. atmos requires the name segment to start
// with a letter and be alphanumeric, so "wildcard" is always a valid probe
// label and is never stored. The returned prefix is "<head>." (e.g.
// "app.bsky.feed."), matched elsewhere with strings.HasPrefix.
//
// Validation here is intentionally stricter than /subscribe (which is
// deliberately v1-lax and skips head validation): planBackfill is a new
// endpoint with no v1 wire contract.
func classifyCollectionPattern(raw string) (exact string, prefix string, err error) {
	if head, ok := strings.CutSuffix(raw, wildcardSuffix); ok {
		if _, perr := atmos.ParseNSID(head + ".wildcard"); perr != nil {
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
// NSIDs and distinct namespace prefixes (from wildcards). The cap counts
// distinct PATTERNS (exact + prefix), not expanded collections, so one wildcard
// counts as one regardless of how many collections it covers — matching
// /subscribe's cap semantics. Both returned slices are nil when raw is empty,
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
	// Dedup on the raw string BEFORE classifying. classifyCollectionPattern is
	// deterministic, so identical raw values classify identically; deduping
	// first bounds parse work (each value runs up to two atmos.ParseNSID scans)
	// and map growth to maxCollections distinct patterns even when a hostile
	// caller submits a body full of duplicates. This mirrors validatePlanDIDs
	// above; the collections array has no maxLength on the wire, so this bound
	// is the only thing standing between a duplicate-stuffed body and O(N) parse
	// work. The cap counts distinct PATTERNS (exact + prefix), not expanded
	// collections, so one wildcard counts as one regardless of coverage —
	// matching /subscribe's cap semantics.
	seen := make(map[string]struct{}, min(len(raw), maxCollections))
	for _, value := range raw {
		if _, dup := seen[value]; dup {
			continue
		}
		if len(exact)+len(prefixes) == maxCollections {
			return nil, nil, xrpcserver.InvalidRequest("too many collections")
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

func planOutput(plan manifest.PlanBackfillResult) (*jetstream.JetstreamPlanBackfill_Output, error) {
	plannedThrough, err := int64FromUint64(plan.PlannedThroughSeq)
	if err != nil {
		return nil, err
	}
	sealedTip, err := int64FromUint64(plan.SealedTipSeq)
	if err != nil {
		return nil, err
	}
	out := &jetstream.JetstreamPlanBackfill_Output{
		PlannedThroughSeq: plannedThrough,
		SealedTipSeq:      sealedTip,
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
