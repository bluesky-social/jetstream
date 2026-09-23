package oracle

import "fmt"

// CompactedFailureVerdict classifies a client-backfill CheckCompacted failure
// by checking disk segments at the same watermark.
//
// A disk violation identifies a storage or compaction defect. Clean disk data
// identifies a serving defect, such as mixing segment generations during
// download. If compaction raced the scan, a clean result is inconclusive. A
// violation remains evidence even during a race because Rewrite only removes
// rows.
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

// ClassifyCompactedFailure compares a failed client CheckCompacted result
// with disk observations at the same watermark. servedErr must be non-nil or
// it panics. passesDuringScan counts compaction passes completed during the
// scan.
//
// Capture watermark before the first segment read. Compaction renames
// replacements before advancing the watermark, so a superseded survivor at or
// below that captured value is a durable defect. Capturing it after the scan
// could wrongly condemn a row read before a legitimate concurrent rewrite.
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
		return fmt.Errorf("compaction bisection: %s at watermark=%d: on-disk segments are clean but the client backfill (planSnapshot -> getSegment/getBlock -> live-subscribe cutover) surfaced a superseded row -> serving/transport inconsistency (e.g. a cold-batch handoff across the paginated download mixing a pre- and post-compaction generation), NOT a storage defect. served=[%w]",
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
