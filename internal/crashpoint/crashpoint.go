// Package crashpoint defines named test-only crash checkpoints at durable
// lifecycle boundaries. Production code wires a nil Injector, so checkpoints
// are no-ops outside deterministic oracle/recovery tests.
package crashpoint

import (
	"context"
	"fmt"

	"github.com/bluesky-social/jetstream/segment"
)

// Point identifies a deterministic crash simulation checkpoint.
type Point string

// Injector simulates a crash when a configured Point is reached. Implementations
// are test harness concerns; production code should pass nil.
type Injector interface {
	SimulateCrash(context.Context, Point) error
}

const (
	// AfterRepoComplete fires after a repo completion row is durable. Recovery
	// must skip the completed repo without losing its already-flushed segment rows.
	AfterRepoComplete Point = "after-repo-complete"

	// AfterMergeDstFlushBeforeSourceCommit fires after a merge source segment's
	// survivors are fsynced to the destination but before the source cursor is
	// advanced. Recovery may replay duplicates, but must not lose survivors.
	AfterMergeDstFlushBeforeSourceCommit Point = "after-merge-dst-flush-before-source-commit"

	// AfterMergeDstSealBeforeDiscovery fires after merge destination sealing but
	// before discovery and cleanup. Recovery must rerun discovery and remove
	// bootstrap artifacts idempotently.
	AfterMergeDstSealBeforeDiscovery Point = "after-merge-dst-seal-before-discovery"

	// AfterMergeDiscoveryBeforeCleanup fires after post-merge discovery has
	// durably written any newly observed DID rows but before the temporary
	// backfill tree and merge cursor keys are removed. Recovery must replay
	// discovery idempotently and still complete cleanup.
	AfterMergeDiscoveryBeforeCleanup Point = "after-merge-discovery-before-cleanup"

	// AfterMergeCleanupComplete fires at the tail of merge cleanup, after the
	// backfill subtree has been removed and both merge cursor keys have been
	// durably deleted, but before runMerge returns (and thus before
	// phase=steady_state is written). Recovery re-enters PhaseMerging and must
	// still converge: the backfill removal must be durable, so the
	// restart-after-cleanup guard sees live_segments gone rather than re-running
	// the drain from cursor 0 and duplicating already-merged events.
	AfterMergeCleanupComplete Point = "after-merge-cleanup-complete"

	// AfterBootstrapLiveCloseBeforeSeal fires after bootstrap-live Close flushes
	// data and cursor state but before its active segment is sealed for merge.
	// Recovery must not assume the source tree is already fully sealed.
	AfterBootstrapLiveCloseBeforeSeal Point = "after-bootstrap-live-close-before-seal"

	// AfterSteadyPhaseBeforeSteadyRun fires after phase=steady_state is durable
	// but before the steady-state live consumer starts. Recovery must dispatch
	// directly to steady-state without rerunning bootstrap or merge.
	AfterSteadyPhaseBeforeSteadyRun Point = "after-steady-phase-before-steady-run"

	// AfterCompactionRewriteBeforeWatermark fires after a compaction chunk has
	// rewritten all candidate segments but before compaction/seq advances.
	// Recovery must rerun the chunk idempotently and then advance the watermark.
	AfterCompactionRewriteBeforeWatermark Point = "after-compaction-rewrite-before-watermark"

	// AfterCompactionChunkWatermark fires after a compaction chunk's
	// compaction/seq watermark has been durably advanced. Recovery must resume
	// at the next chunk without reintroducing already-applied tombstones.
	AfterCompactionChunkWatermark Point = "after-compaction-chunk-watermark"

	// The four segment-rewrite seams are owned by package segment (their
	// firing site), so the constants below are derived from segment's
	// strings rather than re-declared. This is a compile-time link: if a
	// segment crash-point string changes, these follow automatically and the
	// oracle's enumeration cannot reference a stale value.

	// AfterSegmentRewriteTempWritten fires after a segment rewrite has written
	// all bytes to the temporary replacement file but before fsyncing it.
	AfterSegmentRewriteTempWritten Point = Point(segment.CrashPointRewriteTempWritten)

	// AfterSegmentRewriteTempSynced fires after a segment rewrite has fsynced
	// the temporary replacement file but before renaming it over the original.
	AfterSegmentRewriteTempSynced Point = Point(segment.CrashPointRewriteTempSynced)

	// AfterSegmentRewriteRenamed fires after a segment rewrite has renamed the
	// replacement file over the original but before fsyncing the parent dir.
	AfterSegmentRewriteRenamed Point = Point(segment.CrashPointRewriteRenamed)

	// AfterSegmentRewriteDirSynced fires after a segment rewrite has fsynced
	// the parent dir. The replacement is durable; callers must still tolerate
	// receiving an error at this checkpoint.
	AfterSegmentRewriteDirSynced Point = Point(segment.CrashPointRewriteDirSynced)

	// The disaggregated leader's seams (design §10). A crash at any of them
	// may leave an uploaded object no catalog row references, or a committed
	// row whose acks never ran; the next leader must neither lose nor repeat
	// an event.

	// AfterHotBatchCutBeforeUpload fires after a pointer batch is encoded
	// but before its upload starts.
	AfterHotBatchCutBeforeUpload Point = "after-hot-batch-cut-before-upload"

	// AfterHotBatchUploadBeforeCommit fires after a pointer batch's object
	// is uploaded but before the hot batch transaction.
	AfterHotBatchUploadBeforeCommit Point = "after-hot-batch-upload-before-commit"

	// AfterHotBatchCommitBeforeAck fires after a hot batch commits but
	// before the writer releases its acks and advances the relay cursor it
	// reports upstream.
	AfterHotBatchCommitBeforeAck Point = "after-hot-batch-commit-before-ack"

	// AfterFoldUploadBeforeCommit fires after a folded block is uploaded but
	// before the fold transaction replaces its hot batches.
	AfterFoldUploadBeforeCommit Point = "after-fold-upload-before-commit"

	// AfterSealFooterUploadBeforeCommit fires after a seal's footer is
	// uploaded but before the seal transaction.
	AfterSealFooterUploadBeforeCommit Point = "after-seal-footer-upload-before-commit"

	// AfterDirectBlockCutBeforeUpload fires after a direct-mode block is
	// frozen and encoded but before its upload starts.
	AfterDirectBlockCutBeforeUpload Point = "after-direct-block-cut-before-upload"

	// AfterDirectBlockUploadBeforeCommit fires after a direct-mode block's
	// object is uploaded but before its block transaction.
	AfterDirectBlockUploadBeforeCommit Point = "after-direct-block-upload-before-commit"

	// AfterDirectBlockCommitBeforeAck fires after a direct-mode block
	// commits, with its metadata batch, but before the writer releases the
	// block's acks and durable-batch callbacks.
	AfterDirectBlockCommitBeforeAck Point = "after-direct-block-commit-before-ack"
)

// AllPoints is the single source of truth for the set of declared
// crashpoints. knownPoints (used by Known/Parse) is derived from it, so
// adding a constant here automatically makes it parseable — there is no
// second map to keep in sync. A test asserts every constant is listed.
var AllPoints = []Point{
	AfterRepoComplete,
	AfterMergeDstFlushBeforeSourceCommit,
	AfterMergeDstSealBeforeDiscovery,
	AfterMergeDiscoveryBeforeCleanup,
	AfterMergeCleanupComplete,
	AfterBootstrapLiveCloseBeforeSeal,
	AfterSteadyPhaseBeforeSteadyRun,
	AfterCompactionRewriteBeforeWatermark,
	AfterCompactionChunkWatermark,
	AfterSegmentRewriteTempWritten,
	AfterSegmentRewriteTempSynced,
	AfterSegmentRewriteRenamed,
	AfterSegmentRewriteDirSynced,
	AfterHotBatchCutBeforeUpload,
	AfterHotBatchUploadBeforeCommit,
	AfterHotBatchCommitBeforeAck,
	AfterFoldUploadBeforeCommit,
	AfterSealFooterUploadBeforeCommit,
	AfterDirectBlockCutBeforeUpload,
	AfterDirectBlockUploadBeforeCommit,
	AfterDirectBlockCommitBeforeAck,
}

var knownPoints = func() map[Point]struct{} {
	m := make(map[Point]struct{}, len(AllPoints))
	for _, p := range AllPoints {
		m[p] = struct{}{}
	}
	return m
}()

// ForSegment adapts an Injector to segment.CrashInjector, whose seam is
// string-typed (package segment owns the rewrite crash points and must not
// import crashpoint). Returns nil when inj is nil so production — which wires
// no injector — passes a nil segment.CrashInjector and every rewrite seam
// stays a no-op.
func ForSegment(inj Injector) segment.CrashInjector {
	if inj == nil {
		return nil
	}
	return segmentInjector{inj: inj}
}

type segmentInjector struct{ inj Injector }

func (s segmentInjector) SimulateCrash(ctx context.Context, point string) error {
	return s.inj.SimulateCrash(ctx, Point(point))
}

// String returns the stable environment/test name for p.
func (p Point) String() string {
	return string(p)
}

// Known reports whether p is one of the declared crashpoints.
func Known(p Point) bool {
	_, ok := knownPoints[p]
	return ok
}

// Parse converts a stable crashpoint name into a typed Point.
func Parse(s string) (Point, error) {
	if s == "" {
		return "", fmt.Errorf("crashpoint: empty crashpoint")
	}
	p := Point(s)
	if !Known(p) {
		return "", fmt.Errorf("crashpoint: unknown crashpoint %q", s)
	}
	return p, nil
}
