package subscribe

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/seqspace"
)

// CursorSeqMaxThreshold splits the v2 seq cursor namespace from the
// v1 unix-microsecond cursor namespace. Cursors strictly less than
// this value are interpreted as v2 seq numbers; values >= are
// interpreted as v1 unix-microsecond timestamps.
//
// The split is provably non-overlapping under our 36h lookback ceiling:
//
//   - Any legitimate v1 cursor within the 36h window has
//     time_us >= now() - 36h (~1.74e15 as of 2026-05-28), comfortably
//     above the threshold.
//   - v2 seq is a monotonic counter starting at 1. At sustained 100K
//     events/sec (10x current network throughput), reaching 1e15 takes
//     >300 years. Unclean restarts additionally consume at most one configured
//     block lease (4096 seqs by default); even with no events, exhausting 1e15
//     would require more than 244 billion such abandoned leases.
//
// See specs/notes/2026-05-27-cursor-replay-design.md (the non-overlap argument)
// and docs/README.md §5.1, which carry the authoritative 1e15 value.
const CursorSeqMaxThreshold = seqspace.CursorSeqMaxThreshold

// CursorMode discriminates the resolved cursor's intended replay
// behavior. ModeLive means "subscribe to the live tip; no replay."
// The two replay modes share their downstream code path; the mode is
// preserved for metrics and logging.
type CursorMode int

const (
	ModeLive CursorMode = iota
	ModeReplaySeq
	ModeReplayTimeUS
)

// CursorPlan is the resolver's output: enough information for the
// handler and replay engine to act, plus diagnostic context for logs
// and metrics.
type CursorPlan struct {
	// Mode controls the dispatch. ModeLive bypasses the replay engine.
	Mode CursorMode

	// StartSeq is the first seq the replay engine should emit. Set
	// only for replay modes.
	StartSeq uint64

	// Requested is the raw integer parsed from the query string. Timestamp
	// replay also uses it to discard rows before the exact in-block boundary.
	// It is never sent on the wire.
	Requested int64

	// Clamped is true when the resolved StartSeq differs from the
	// requested cursor (because of lookback floor or future cursor).
	Clamped bool

	// ClampReason distinguishes lossless registered-gap clamps from legacy
	// floor/future/sentinel clamps for observability.
	ClampReason string
}

// CursorEnv bundles the runtime dependencies the resolver consults.
// Each field is independently optional so tests can drive narrow
// scenarios without constructing a full manifest.
type CursorEnv struct {
	// Manifest is the in-memory segment manifest. Required for
	// timestamp-mode resolution and lookback-floor clamping. nil is
	// equivalent to "no sealed segments yet": floor is 0; timestamp
	// mode falls through to the active segment.
	Manifest *manifest.Manifest

	// Catalog and Fetcher read the candidate block for timestamp-mode
	// resolution. The manifest picks the segment by witnessed range; the
	// catalog supplies its block index and bytes. nil, or a catalog that
	// does not hold the manifest's candidate yet, resolves to the
	// candidate's MinSeq: coarser but lossless, because the subscriber loop
	// drops rows witnessed before the requested timestamp.
	Catalog catalog.Catalog
	Fetcher catalog.Fetcher

	// NextSeq is the writer's next-to-be-allocated seq value. A
	// requested cursor >= NextSeq drops into ModeLive (future cursor).
	// Zero is treated as "writer not yet started"; in that case any
	// finite requested seq looks "in the future" and we drop to live.
	NextSeq uint64

	// Lookback is the configured cursor-lookback duration. Zero or
	// negative disables clamping (cursor still replays as far back as
	// the manifest can serve).
	Lookback time.Duration

	// RejectBelowFloor selects the v2 too-old policy for the SEQ cursor
	// path: when set, a seq cursor that resolves below the lookback floor
	// returns ErrCursorTooOld instead of being silently clamped up to the
	// floor, so the client learns it fell behind and can re-backfill rather
	// than silently skipping (requestedSeq, floor]. Unset (the v1 default)
	// keeps the legacy silent clamp. It governs ONLY the seq path; the
	// timestamp path always clamps (legacy v1 timestamp translation), under
	// both endpoints.
	RejectBelowFloor bool

	// Gaps is the immutable set of durable, explicitly-authorized vacancies.
	Gaps *seqspace.Gaps

	// ActiveTimeFloor returns the first active-segment seq worth scanning for
	// timeUS, or the snapshotted live edge when no active event reaches it.
	// The resolver rechecks the manifest afterward to cover an active-to-sealed
	// rotation racing timestamp resolution.
	ActiveTimeFloor func(timeUS int64) uint64
}

// ErrInvalidCursor wraps any user-visible parse failure of the
// ?cursor= query parameter. The handler converts this into HTTP 400.
var ErrInvalidCursor = errors.New("subscribe: invalid cursor")

// ErrCursorTooOld is returned (only when CursorEnv.RejectBelowFloor is set —
// the v2 policy) when a seq cursor resolves below the lookback floor. The
// handler converts it into a pre-upgrade HTTP 400 XRPC envelope with error
// name "CursorTooOld" (the wire contract clients match on — declared in the
// network.bsky.jetstream.subscribeEvents lexicon); the message carries both the
// requested seq and the floor seq so the client can re-backfill from its
// last seq. v1 never returns this (it clamps instead).
var ErrCursorTooOld = errors.New("subscribe: cursor too old")

// ErrCursorResolveFailed marks a SERVER-side failure while resolving a
// well-formed cursor — a segment read/decode/index-load fault during
// timestamp-to-seq translation, not bad client input. It is distinct from
// ErrInvalidCursor (a client parse error → HTTP 400): a disk/decode fault is
// 5xx-class, the client should retry, operators must see it on the 5xx signal,
// and the internal segment path/index it wraps must NOT be echoed to the
// client. The handler maps it to HTTP 503 with a generic body and logs the
// wrapped detail server-side.
var ErrCursorResolveFailed = errors.New("subscribe: cursor resolution failed")

// ResolveCursor parses the raw query value and decides how the
// connection should proceed. See cursor_test.go for the matrix of
// behaviors.
//
// This is the basic-cases skeleton: empty input, parse errors, future
// cursors, and the magnitude split into seq vs timestamp mode. The
// seq-mode lookback-floor clamp and the timestamp-to-seq translation
// land in follow-up tasks.
func ResolveCursor(raw string, env CursorEnv) (CursorPlan, error) {
	if raw == "" {
		return CursorPlan{Mode: ModeLive}, nil
	}

	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return CursorPlan{}, fmt.Errorf("%w: %q", ErrInvalidCursor, raw)
	}
	if n < 0 {
		return CursorPlan{}, fmt.Errorf("%w: negative value %d", ErrInvalidCursor, n)
	}

	plan := CursorPlan{Requested: n}

	if uint64(n) < CursorSeqMaxThreshold {
		plan.Mode = ModeReplaySeq
		// Future-seq check: a requested cursor at or beyond the writer's
		// next-to-allocate seq has no events to replay, so we drop to
		// live mode. NextSeq==0 means the writer has not started (see the
		// CursorEnv.NextSeq contract): any finite requested seq is "in the
		// future", so it also drops to live rather than falling through to
		// seq replay or the RejectBelowFloor too-old rejection.
		if env.NextSeq == 0 || uint64(n) >= env.NextSeq {
			return CursorPlan{Mode: ModeLive, Requested: n, Clamped: true}, nil
		}
		startSeq := uint64(n)
		// Seq 0 is the pure "nothing yet" sentinel (design §R8): it is never
		// allocated to an event, so the lowest real event is seq 1. The
		// bufferless cutover sends cursor=0 to mean "replay from the first
		// event" on an empty archive (see the root client live consumer). With
		// the writer now flooring NextSeq to 1, cursor 0 falls through here as a
		// replay; floor it to 1 so the cold reader walks from the first real
		// event instead of returning a non-advancing next==0 that disconnects
		// the subscriber.
		if startSeq == 0 {
			startSeq = 1
			plan.Clamped = true
		}
		if end, ok := env.Gaps.EndContaining(startSeq); ok {
			startSeq = end
			plan.Clamped = true
			plan.ClampReason = "gap"
		}
		// Clamp to the lookback floor when the manifest knows the floor
		// and lookback clamping is enabled. A zero or negative Lookback
		// disables clamping (replays as far back as the manifest can).
		if env.Manifest != nil && env.Lookback > 0 {
			floorSeq, _ := env.Manifest.LookbackFloor(env.Lookback)
			if startSeq < floorSeq {
				// v2 (RejectBelowFloor) refuses a too-old seq cursor with a
				// typed error rather than silently clamping, so the client
				// learns it fell behind and re-backfills from its last seq
				// instead of silently skipping (requestedSeq, floorSeq]. v1
				// keeps the legacy silent clamp for wire compatibility.
				if env.RejectBelowFloor {
					return CursorPlan{}, fmt.Errorf("%w: cursor %d below lookback floor %d; re-backfill from your last seq", ErrCursorTooOld, n, floorSeq)
				}
				startSeq = floorSeq
				plan.Clamped = true
			}
			// A segment envelope may span a registered gap, so its MinSeq can
			// place the lookback floor at the gap boundary. Normalize once more
			// after applying the floor; only an explicitly registered vacancy
			// may be skipped here.
			if end, ok := env.Gaps.EndContaining(startSeq); ok {
				startSeq = end
				plan.Clamped = true
				plan.ClampReason = "gap"
			}
		}
		plan.StartSeq = startSeq
		return plan, nil
	}

	plan.Mode = ModeReplayTimeUS
	if n > time.Now().UnixMicro() {
		return CursorPlan{Mode: ModeLive, Requested: n, Clamped: true}, nil
	}
	startSeq, clamped, err := translateTimeUSToSeq(env, n)
	if err != nil {
		// A segment read/decode/index-load fault here is a SERVER fault, not bad
		// client input: tag it so the handler returns 5xx (retryable, visible on
		// the 5xx signal) rather than a 400 that the client treats as permanent —
		// and so the wrapped internal segment path is not echoed to the client.
		return CursorPlan{}, fmt.Errorf("%w: %w", ErrCursorResolveFailed, err)
	}
	plan.StartSeq = startSeq
	if clamped {
		plan.Clamped = true
	}
	// Floor the replay start to seq 1 before gap normalization: seq 0 is the
	// pure "nothing yet" sentinel and is never allocated. Translation returns
	// 0 when there are no sealed segments, including after an unclean empty-
	// archive restart whose first registered vacancy begins at seq 1.
	if plan.StartSeq == 0 {
		plan.StartSeq = 1
		plan.Clamped = true
	}
	if end, ok := env.Gaps.EndContaining(plan.StartSeq); ok {
		plan.StartSeq = end
		plan.Clamped = true
		plan.ClampReason = "gap"
	}
	// Apply lookback floor on top of translation. The floor is in
	// seq units; if the translated seq is below it, we clamp.
	//
	// This path always clamps, even under RejectBelowFloor (v2): a timestamp
	// cursor is the legacy jetstream-v1 translation, whose documented contract
	// is that a too-old timestamp simply starts at the oldest retained event
	// (2026-05-27-cursor-replay-design.md). RejectBelowFloor governs only the
	// v2 SEQ path; rejecting a legacy timestamp would break that contract. The
	// asymmetry is intentional (finding #14) — v1's silent clamp is bounded,
	// deliberately-retained legacy debt, kept observable via the metric label.
	if env.Manifest != nil && env.Lookback > 0 {
		floorSeq, _ := env.Manifest.LookbackFloor(env.Lookback)
		if plan.StartSeq < floorSeq {
			plan.StartSeq = floorSeq
			plan.Clamped = true
		}
	}
	if end, ok := env.Gaps.EndContaining(plan.StartSeq); ok {
		plan.StartSeq = end
		plan.Clamped = true
		plan.ClampReason = "gap"
	}
	return plan, nil
}

// translateTimeUSToSeq finds the smallest seq whose WitnessedAt >=
// timeUS, walking the manifest's sealed segments for the candidate
// segment, then its block index for the candidate block, then the
// block's witnessed_at column.
//
// Returns (seq, clamped, error). clamped is true iff timeUS is older
// than every sealed segment (caller resolves to the first segment's
// MinSeq). When timeUS is newer than every sealed segment, consults
// the active writer's block bounds and returns the first candidate
// block's MinSeq.
func translateTimeUSToSeq(env CursorEnv, timeUS int64) (uint64, bool, error) {
	var candidate manifest.SegmentBounds
	var found bool
	if env.Manifest != nil && env.Manifest.SegmentCount() > 0 {
		candidate, found = env.Manifest.SegmentForTimeUS(timeUS)
	}
	if !found {
		// No sealed candidate: use active block bounds to avoid replaying
		// from the active segment's beginning. A miss returns the live edge.
		// Then recheck the manifest: the generation inspected by
		// ActiveTimeFloor may have rotated after our first manifest lookup.
		// This is necessary even when the current active generation has a
		// candidate, because the just-sealed generation is earlier.
		if env.ActiveTimeFloor != nil {
			activeSeq := env.ActiveTimeFloor(timeUS)
			if env.Manifest != nil && env.Manifest.SegmentCount() > 0 {
				candidate, found = env.Manifest.SegmentForTimeUS(timeUS)
			}
			if !found {
				return activeSeq, false, nil
			}
			// Continue below and resolve the earlier sealed candidate.
		} else {
			// Preserve the resolver's standalone fallback for tests and tools
			// that do not have a live writer.
			if env.Manifest == nil || env.Manifest.SegmentCount() == 0 {
				return 0, false, nil
			}
			all := env.Manifest.AllBounds()
			last := all[len(all)-1]
			return last.MaxSeq + 1, false, nil
		}
	}

	// timeUS may be older than every sealed segment, in which case
	// SegmentForTimeUS returns the first segment (per its contract)
	// and we clamp to that segment's MinSeq without scanning.
	all := env.Manifest.AllBounds()
	first := all[0]
	if timeUS <= first.MinWitnessedAt {
		return first.MinSeq, true, nil
	}

	return seqInCandidate(env, candidate, timeUS)
}

// seqInCandidate resolves timeUS inside the manifest's candidate segment
// through the catalog: a binary search over the segment's block index, then
// the first row of the chosen block witnessed at or after timeUS.
func seqInCandidate(env CursorEnv, candidate manifest.SegmentBounds, timeUS int64) (uint64, bool, error) {
	if env.Catalog == nil || env.Fetcher == nil {
		return candidate.MinSeq, false, nil
	}
	for stale := 0; ; stale++ {
		ref, ok := candidateBlock(env.Catalog.Snapshot(), candidate.Idx, timeUS)
		if !ok {
			return candidate.MinSeq, false, nil
		}
		events, err := catalog.DecodeRef(context.Background(), env.Fetcher, ref)
		if errors.Is(err, catalog.ErrStaleRef) && stale < maxStaleRetries {
			// A compaction rewrote the segment between the snapshot and
			// the fetch. Block topology survives a rewrite, so a fresh
			// view resolves to the same position.
			continue
		}
		if err != nil {
			return 0, false, fmt.Errorf("decode seg %d block %d: %w", ref.Segment, ref.Block, err)
		}
		for _, ev := range events {
			if ev.WitnessedAt >= timeUS {
				return ev.Seq, false, nil
			}
		}
		// Every surviving row is older than timeUS, though the block's
		// envelope says MaxWitnessedAt >= timeUS: compaction dropped the
		// rows that reached it. Starting at the block's MaxSeq stays
		// lossless; the subscriber loop drops the older rows.
		return ref.MaxSeq, false, nil
	}
}

// candidateBlock returns the ref of the first block of segment idx whose
// MaxWitnessedAt reaches timeUS. It reports false when the view does not
// hold idx or no block reaches timeUS; the manifest guarantees the latter
// cannot happen for its candidate, so either means the view and the
// manifest disagree and the caller falls back to the segment's MinSeq.
func candidateBlock(view catalog.CatalogView, idx uint64, timeUS int64) (catalog.BlockRef, bool) {
	segs := view.Segments(catalog.Main)
	i := sort.Search(len(segs), func(i int) bool { return segs[i].Index >= idx })
	if i == len(segs) || segs[i].Index != idx {
		return catalog.BlockRef{}, false
	}
	blocks := segs[i].Blocks
	b := sort.Search(len(blocks), func(i int) bool { return blocks[i].MaxWitnessedAt >= timeUS })
	if b == len(blocks) {
		return catalog.BlockRef{}, false
	}
	for ref := range view.RefsFrom(catalog.Main, blocks[b].MinSeq) {
		if ref.Segment == idx && ref.Block == b {
			return ref, true
		}
		break
	}
	return catalog.BlockRef{}, false
}
