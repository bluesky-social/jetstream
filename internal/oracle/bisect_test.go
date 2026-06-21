package oracle

import (
	"errors"
	"testing"

	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// A served CheckCompacted failure whose on-disk segments ALSO violate the
// compaction contract at the same watermark is a durable storage/compaction
// defect: the bytes Jetstream persisted are wrong, independent of the serving
// transport.
func TestClassifyCompactedFailure_DiskAlsoViolates_DurableDefect(t *testing.T) {
	t.Parallel()

	// did:plc:a/c/r created at seq 1, deleted at seq 2; watermark 2 means
	// compaction should have physically removed the create. It survived on
	// disk -> durable defect.
	disk := []ObservedEvent{
		{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Payload: []byte("old")},
		{Seq: 2, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r"},
	}
	servedErr := errors.New("oracle: superseded record row survived: did=did:plc:a collection=c rkey=r seq=1 tombstone_seq=2")

	v := ClassifyCompactedFailure(servedErr, disk, 2, 0)

	require.Equal(t, VerdictDurableDefect, v.Verdict)
	require.Error(t, v.DiskErr)
	require.ErrorContains(t, v.DiskErr, "superseded record row survived")
	require.False(t, v.CompactionRacedScan)
	require.ErrorContains(t, v.Err(), "DURABLE")
}

// A served failure whose on-disk segments are CLEAN at the same watermark is a
// serving-path inconsistency (a torn cross-batch /subscribe read), not a
// storage bug. This is the outcome that points at the live-tail-transport
// misuse tracked in #77.
func TestClassifyCompactedFailure_DiskClean_ServingDefect(t *testing.T) {
	t.Parallel()

	// On disk the create is already gone (compaction removed it); only the
	// retained delete tombstone remains. Disk is contract-clean at W=2.
	disk := []ObservedEvent{
		{Seq: 2, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r"},
	}
	servedErr := errors.New("oracle: superseded record row survived: did=did:plc:a collection=c rkey=r seq=1 tombstone_seq=2")

	v := ClassifyCompactedFailure(servedErr, disk, 2, 0)

	require.Equal(t, VerdictServingDefect, v.Verdict)
	require.NoError(t, v.DiskErr)
	require.False(t, v.CompactionRacedScan)
	require.ErrorContains(t, v.Err(), "SERVING")
}

// When a compaction pass ran concurrently with the on-disk scan, the disk read
// is not a coherent point-in-time snapshot: a clean disk result cannot be
// trusted to mean "no durable defect" (a rename mid-scan can hide or fabricate
// rows). The verdict must say so rather than silently asserting SERVING.
func TestClassifyCompactedFailure_DiskClean_ButScanRaced_Inconclusive(t *testing.T) {
	t.Parallel()

	disk := []ObservedEvent{
		{Seq: 2, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r"},
	}
	servedErr := errors.New("oracle: superseded record row survived: seq=1 tombstone_seq=2")

	v := ClassifyCompactedFailure(servedErr, disk, 2, 1 /* passesDuringScan */)

	require.Equal(t, VerdictInconclusive, v.Verdict)
	require.True(t, v.CompactionRacedScan)
	require.ErrorContains(t, v.Err(), "INCONCLUSIVE")
}

// A disk violation is a durable defect regardless of a racing pass: a pass can
// only remove rows, so a surviving superseded row on disk is real even if the
// scan was not perfectly isolated.
func TestClassifyCompactedFailure_DiskViolates_ScanRaced_StillDurable(t *testing.T) {
	t.Parallel()

	disk := []ObservedEvent{
		{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Payload: []byte("old")},
		{Seq: 2, Kind: segment.KindDelete, DID: "did:plc:a", Collection: "c", Rkey: "r"},
	}
	servedErr := errors.New("oracle: superseded record row survived: seq=1 tombstone_seq=2")

	v := ClassifyCompactedFailure(servedErr, disk, 2, 3 /* passesDuringScan */)

	require.Equal(t, VerdictDurableDefect, v.Verdict)
	require.Error(t, v.DiskErr)
	require.True(t, v.CompactionRacedScan)
	require.ErrorContains(t, v.Err(), "DURABLE")
}

// The classifier must never be called with a nil served error; doing so is a
// caller bug (the bisection only runs after the served check already failed).
// Guard it loudly rather than returning a misleading clean verdict.
func TestClassifyCompactedFailure_NilServedErr_Panics(t *testing.T) {
	t.Parallel()

	require.Panics(t, func() {
		ClassifyCompactedFailure(nil, nil, 0, 0)
	})
}

// bisectPassesDuringScan mirrors the harness's bisect bracket: a pass that
// straddles the scan (starts before, ends after) must be detected via the
// started counter even though the completed counter does not move during
// the bracket (#106).
func bisectPassesDuringScan(r *compactionPassRecorder, scan func()) int {
	completedBefore := r.Count()
	startedBefore := r.StartedCount()
	scan()
	return max(r.Count()-completedBefore, r.StartedCount()-startedBefore)
}

// A compaction pass that STRADDLES the on-disk scan (begins before the
// scan, ends after it) increments neither completed-count endpoint during
// the bracket. A completed-only bracket would read passesDuringScan==0 and
// mislabel a possibly-torn clean read as SERVING_DEFECT; the started
// counter must surface it so the verdict is INCONCLUSIVE (#106).
func TestCompactionPassRecorder_StraddlingPass_DetectedAsInFlight(t *testing.T) {
	t.Parallel()

	r := newCompactionPassRecorder()

	// Pass begins BEFORE the scan...
	r.ObserveStart()
	// ...the scan runs while the pass is in flight (no completion yet)...
	passes := bisectPassesDuringScan(r, func() {
		// during the scan the pass completes
		r.Observe(jetstreamd.CompactionPassResult{Watermark: 10})
	})
	require.Positive(t, passes, "a pass in flight across the scan must be detected as raced")

	// Sanity: the classifier turns a raced clean disk into INCONCLUSIVE.
	v := ClassifyCompactedFailure(errors.New("served check failed"), nil, 10, passes)
	require.Equal(t, VerdictInconclusive, v.Verdict)
}

// A pass that fully PRECEDED the scan (started and completed before it)
// must NOT count as racing the scan, or every post-pass bisect would be
// spuriously INCONCLUSIVE.
func TestCompactionPassRecorder_PriorPass_NotInFlight(t *testing.T) {
	t.Parallel()

	r := newCompactionPassRecorder()
	r.ObserveStart()
	r.Observe(jetstreamd.CompactionPassResult{Watermark: 5})

	passes := bisectPassesDuringScan(r, func() { /* quiescent scan */ })
	require.Zero(t, passes, "a pass that completed before the scan must not count as racing it")
}
