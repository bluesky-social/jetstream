package oracle

import "fmt"

// CompactedFailureVerdict classifies WHY the client-backfill serving path
// tripped CheckCompacted, by re-running the identical contract check against
// the on-disk segments at the same watermark.
//
// The served (client-backfill) stream and the on-disk segments are two
// independent observation surfaces of the same compaction contract
// (docs/README.md §3.3). The served surface is the paginated client backfill —
// planBackfill -> getSegment/getBlock -> /subscribe-v2 cutover — that real
// clients use (this replaced the bespoke whole-archive /subscribe?cursor=0
// replay; see specs/oracle.md "Client-Driven Historical Tier"). When that
// surface reports a superseded survivor, the on-disk surface tells us which
// side is actually wrong:
//
//   - on-disk ALSO violates  -> Jetstream physically persisted a superseded
//     row: a durable storage/compaction defect (highest severity).
//   - on-disk is CLEAN       -> the bytes are correct; the violation is a
//     serving-path artifact, e.g. a cold-batch handoff across the paginated
//     getSegment/getBlock download mixing a pre-compaction prefix with a
//     post-compaction suffix. This points at the serving/transport path, not a
//     storage bug.
//
// A clean on-disk result is only trustworthy when no compaction pass mutated
// the segment directory during the scan: a rename mid-scan can hide or
// fabricate rows, so a clean-but-raced scan is reported INCONCLUSIVE rather
// than SERVING. A disk VIOLATION, by contrast, is durable regardless of a
// racing pass, because a pass can only remove rows (segment.Rewrite is
// strictly subtractive), so a surviving superseded row is real.
type CompactedFailureVerdict struct {
	// Verdict is the classification.
	Verdict Verdict
	// ServedErr is the original client-backfill CheckCompacted failure that
	// triggered the bisection. Always non-nil.
	ServedErr error
	// DiskErr is the result of CheckCompacted over the on-disk segments at
	// the same watermark. nil means the durable segments satisfy the
	// contract.
	DiskErr error
	// CompactionRacedScan is true when one or more compaction passes
	// completed while the on-disk scan was running, so a clean DiskErr is
	// not a coherent point-in-time snapshot.
	CompactionRacedScan bool
	// Watermark is the compaction watermark both checks were run at.
	Watermark uint64
}

// Verdict is the bisection outcome.
type Verdict string

const (
	// VerdictDurableDefect: the on-disk segments violate the compaction
	// contract. Jetstream persisted wrong bytes. Highest severity.
	VerdictDurableDefect Verdict = "DURABLE_DEFECT"
	// VerdictServingDefect: on-disk is clean and the scan was not raced, so
	// the violation lives only in the client-backfill serving surface
	// (serving/transport), not in the durable bytes.
	VerdictServingDefect Verdict = "SERVING_DEFECT"
	// VerdictInconclusive: on-disk is clean but a compaction pass raced the
	// scan, so the clean result cannot be trusted to rule out a durable defect.
	VerdictInconclusive Verdict = "INCONCLUSIVE"
)

// ClassifyCompactedFailure bisects a client-backfill CheckCompacted failure.
// servedErr MUST be the non-nil error returned by CheckCompacted over the
// client-backfill serving stream; disk is the on-disk segment stream observed
// at the same watermark; passesDuringScan is the number of compaction passes
// that completed while the on-disk scan ran (0 means the scan was not raced).
//
// PRECONDITION on watermark capture: watermark MUST be captured no later than
// the scan's first segment read. The DURABLE verdict's soundness under a race
// rests on it. The compactor commits a chunk's rewrites to disk before it
// advances the committed watermark (orchestrator applyCompactionChunk then
// saveCompactionWatermark), so "watermark >= S" implies the rewrite that drops
// S already renamed into place. With watermark captured first, a survivor at
// seq S <= watermark seen on disk therefore provably persisted past its
// compaction deadline -> a real durable defect, and a later racing pass can
// only drop more rows, never resurrect S. If a caller instead captured the
// watermark AFTER the scan, a row legitimately present at the read-time
// watermark but dropped by a pass that then advanced the watermark past S
// could be mislabeled DURABLE_DEFECT -- so don't.
//
// It panics if servedErr is nil: the bisection is only meaningful after the
// served check has already failed, and a nil error would yield a misleadingly
// clean verdict.
func ClassifyCompactedFailure(servedErr error, disk []ObservedEvent, watermark uint64, passesDuringScan int) CompactedFailureVerdict {
	if servedErr == nil {
		panic("oracle: ClassifyCompactedFailure called with nil servedErr; bisection only runs after the served check fails")
	}

	v := CompactedFailureVerdict{
		ServedErr:           servedErr,
		DiskErr:             CheckCompacted(disk, watermark),
		CompactionRacedScan: passesDuringScan > 0,
		Watermark:           watermark,
	}

	switch {
	case v.DiskErr != nil:
		// A surviving superseded row on disk is real even if the scan was
		// raced: compaction only ever removes rows.
		v.Verdict = VerdictDurableDefect
	case v.CompactionRacedScan:
		// Clean on disk, but the scan was not isolated from a concurrent
		// rewrite, so we cannot trust "clean" to mean "no durable defect".
		v.Verdict = VerdictInconclusive
	default:
		v.Verdict = VerdictServingDefect
	}
	return v
}

// Err renders the verdict as a single diagnostic error suitable for a test
// failure message. It always returns a non-nil error: ClassifyCompactedFailure
// is only ever constructed from a real served failure.
func (v CompactedFailureVerdict) Err() error {
	switch v.Verdict {
	case VerdictDurableDefect:
		return fmt.Errorf("compaction bisection: %s at watermark=%d: on-disk segments ALSO violate the compaction contract -> Jetstream persisted a superseded row (storage/compaction defect, NOT a serving artifact). served=[%w] disk=[%w]",
			v.Verdict, v.Watermark, v.ServedErr, v.DiskErr)
	case VerdictServingDefect:
		return fmt.Errorf("compaction bisection: %s at watermark=%d: on-disk segments are clean but the client backfill (planBackfill -> getSegment/getBlock -> /subscribe-v2 cutover) surfaced a superseded row -> serving/transport inconsistency (e.g. a cold-batch handoff across the paginated download mixing a pre- and post-compaction generation), NOT a storage defect. served=[%w]",
			v.Verdict, v.Watermark, v.ServedErr)
	case VerdictInconclusive:
		return fmt.Errorf("compaction bisection: %s at watermark=%d: the client backfill surfaced a superseded row and on-disk segments are clean, BUT a compaction pass raced the on-disk scan so the clean result cannot rule out a durable defect. Re-run with the compaction trigger quiesced around the scan, or capture the on-disk segments under a paused compactor. served=[%w]",
			v.Verdict, v.Watermark, v.ServedErr)
	default:
		return fmt.Errorf("compaction bisection: unknown verdict %q at watermark=%d: served=[%w] disk=[%s]",
			v.Verdict, v.Watermark, v.ServedErr, errorString(v.DiskErr))
	}
}

func errorString(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}
