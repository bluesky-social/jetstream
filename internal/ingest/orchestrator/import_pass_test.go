package orchestrator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/cbor"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// importTestRig is a minimal Orchestrator plus a real manifest over a segments
// dir, enough to exercise RunImport end-to-end without the full Run lifecycle.
type importTestRig struct {
	o           *Orchestrator
	segmentsDir string
	dataDir     string
	mft         *manifest.Manifest
	refreshed   []uint64
}

func newImportTestRig(t *testing.T, events []segment.Event) *importTestRig {
	return newImportTestRigSegs(t, events)
}

// newImportTestRigSegs is newImportTestRig for multiple segments: segs[i]
// becomes segment i.
func newImportTestRigSegs(t *testing.T, segs ...[]segment.Event) *importTestRig {
	t.Helper()
	dataDir := t.TempDir()
	segmentsDir := filepath.Join(dataDir, "segments")
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
	for i, events := range segs {
		writeCompactionSegment(t, segmentsDir, uint64(i), events)
	}

	mft, err := manifest.Open(manifest.Options{
		SegmentsDir: segmentsDir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	rig := &importTestRig{segmentsDir: segmentsDir, dataDir: dataDir, mft: mft}
	rig.o = &Orchestrator{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		cfg: Config{
			DataDir:        dataDir,
			ImportSelector: mft,
			OnSegmentCompacted: func(idx uint64, path string) error {
				rig.refreshed = append(rig.refreshed, idx)
				return mft.OnSegmentCompacted(idx, path)
			},
			SegmentManifestChecksums: mft.SegmentChecksums,
		},
	}
	return rig
}

func (r *importTestRig) segmentEvents(t *testing.T) []segment.Event {
	t.Helper()
	return readCompactionSegment(t, filepath.Join(r.segmentsDir, "seg_0000000000.jss"))
}

func writeImportCSVFile(t *testing.T, header string, rows ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "import.csv")
	var b strings.Builder
	b.WriteString(header)
	b.WriteByte('\n')
	for _, row := range rows {
		b.WriteString(row)
		b.WriteByte('\n')
	}
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o644))
	return path
}

// TestRunImport_EndToEndAllVersions is the M5 anchor: an all_versions import
// patches the display column of the matching rows, leaves witnessed/seq/every
// other column and the segment envelope untouched, and DisplayTimeUS reflects
// the import. A second identical run is a no-op (idempotent).
func TestRunImport_EndToEndAllVersions(t *testing.T) {
	t.Parallel()
	events := []segment.Event{
		{Seq: 1, WitnessedAt: 1_000, Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "1", Payload: []byte("v1")},
		{Seq: 2, WitnessedAt: 2_000, Kind: segment.KindUpdate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "2", Payload: []byte("v2")},
		{Seq: 3, WitnessedAt: 3_000, Kind: segment.KindCreate, DID: "did:plc:bob", Collection: "app.bsky.feed.post", Rkey: "r2", Rev: "1", Payload: []byte("v3")},
	}
	rig := newImportTestRig(t, events)
	before := rig.segmentEvents(t)

	const importedTS = int64(1_640_000_000_000_000)
	csv := writeImportCSVFile(t, "uri,timestamp,scope,cid",
		"at://did:plc:alice/app.bsky.feed.post/r1,2021-12-20T11:33:20Z,all_versions,")
	// (2021-12-20T11:33:20Z == 1_640_000_000 s == importedTS micros.)

	jobDir := filepath.Join(t.TempDir(), "job1")
	res, err := rig.o.RunImport(context.Background(), ImportJob{CSVPath: csv, JobDir: jobDir})
	require.NoError(t, err)

	require.EqualValues(t, 1, res.Parse.RowsValid)
	require.EqualValues(t, 1, res.SegmentsExamined)
	require.EqualValues(t, 1, res.SegmentsPatched)
	require.EqualValues(t, 2, res.RowsMutated, "both alice/r1 versions patched")
	require.EqualValues(t, 2, res.RowsMatchedAllVersions)
	require.Equal(t, []uint64{0}, rig.refreshed, "manifest refreshed for the patched segment")

	after := rig.segmentEvents(t)
	require.Len(t, after, len(before))
	for i := range before {
		b, a := before[i], after[i]
		// Everything but IndexedAt is preserved.
		require.Equal(t, b.Seq, a.Seq)
		require.Equal(t, b.WitnessedAt, a.WitnessedAt)
		require.Equal(t, b.Kind, a.Kind)
		require.Equal(t, b.DID, a.DID)
		require.Equal(t, b.Collection, a.Collection)
		require.Equal(t, b.Rkey, a.Rkey)
		require.Equal(t, b.Rev, a.Rev)
		require.Equal(t, b.Payload, a.Payload)

		if a.DID == "did:plc:alice" {
			require.Equal(t, importedTS, a.IndexedAt)
			require.Equal(t, importedTS, a.DisplayTimeUS())
		} else {
			require.EqualValues(t, 0, a.IndexedAt, "untargeted row keeps the sentinel")
			require.Equal(t, a.WitnessedAt, a.DisplayTimeUS())
		}
	}

	// Idempotent re-run: same CSV, fresh job dir -> zero mutations, no patch.
	res2, err := rig.o.RunImport(context.Background(), ImportJob{CSVPath: csv, JobDir: filepath.Join(t.TempDir(), "job2")})
	require.NoError(t, err)
	require.EqualValues(t, 1, res2.SegmentsExamined)
	require.EqualValues(t, 0, res2.SegmentsPatched, "re-import is a no-op")
	require.EqualValues(t, 0, res2.RowsMutated)
}

// TestRunImport_SpecificVersion patches only the row whose payload CID matches,
// and reports an unmatched specific CID.
func TestRunImport_SpecificVersion(t *testing.T) {
	t.Parallel()
	payloadV1 := []byte("cbor-v1")
	payloadV2 := []byte("cbor-v2")
	events := []segment.Event{
		{Seq: 1, WitnessedAt: 1_000, Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "1", Payload: payloadV1},
		{Seq: 2, WitnessedAt: 2_000, Kind: segment.KindUpdate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "2", Payload: payloadV2},
	}
	rig := newImportTestRig(t, events)

	const importedTS = int64(1_640_000_000_000_000)
	cidV1 := cbor.ComputeCID(cbor.CodecDagCBOR, payloadV1).String()
	csv := writeImportCSVFile(t, "uri,timestamp,scope,cid",
		"at://did:plc:alice/app.bsky.feed.post/r1,2021-12-20T11:33:20Z,specific_version,"+cidV1)

	res, err := rig.o.RunImport(context.Background(), ImportJob{CSVPath: csv, JobDir: filepath.Join(t.TempDir(), "job")})
	require.NoError(t, err)
	require.EqualValues(t, 1, res.RowsMutated, "only the v1-CID row patched")
	require.EqualValues(t, 1, res.RowsMatchedSpecific)
	require.EqualValues(t, 0, res.SpecificCIDsUnmatched)

	after := rig.segmentEvents(t)
	for _, a := range after {
		if string(a.Payload) == "cbor-v1" {
			require.Equal(t, importedTS, a.IndexedAt)
		} else {
			require.EqualValues(t, 0, a.IndexedAt, "v2 row untouched")
		}
	}
}

// TestRunImport_DisabledWithoutSelector: RunImport errors clearly when import
// is not configured.
func TestRunImport_DisabledWithoutSelector(t *testing.T) {
	t.Parallel()
	o := &Orchestrator{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	_, err := o.RunImport(context.Background(), ImportJob{CSVPath: "x", JobDir: "y"})
	require.ErrorIs(t, err, ErrImportUnavailable)
}

// TestRunImport_NoCandidateSegmentIsNoop: a row whose DID is in no sealed
// segment routes nowhere and patches nothing (valid, not an error).
func TestRunImport_NoCandidateSegmentIsNoop(t *testing.T) {
	t.Parallel()
	events := []segment.Event{
		{Seq: 1, WitnessedAt: 1_000, Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "1", Payload: []byte("v1")},
	}
	rig := newImportTestRig(t, events)
	csv := writeImportCSVFile(t, "uri,timestamp,scope,cid",
		"at://did:plc:nobody/app.bsky.feed.post/r1,2021-12-20T11:33:20Z,all_versions,")
	res, err := rig.o.RunImport(context.Background(), ImportJob{CSVPath: csv, JobDir: filepath.Join(t.TempDir(), "job")})
	require.NoError(t, err)
	require.EqualValues(t, 1, res.Parse.RowsValid)
	require.EqualValues(t, 1, res.Bucket.RowsNoCandidate)
	require.EqualValues(t, 0, res.SegmentsExamined)
	require.EqualValues(t, 0, res.SegmentsPatched)
}

// TestRunImport_ResumeSkipsCheckpointedSegments proves the resume seam: a job
// that reuses an existing offset file (SkipBucket) and reports a segment as
// already-done (SkipSegment) never opens that segment, and the phase/applied
// callbacks fire with the expected shape.
func TestRunImport_ResumeSkipsCheckpointedSegments(t *testing.T) {
	t.Parallel()
	events := []segment.Event{
		{Seq: 1, WitnessedAt: 1_000, Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "1", Payload: []byte("v1")},
	}
	rig := newImportTestRig(t, events)
	csv := writeImportCSVFile(t, "uri,timestamp,scope,cid",
		"at://did:plc:alice/app.bsky.feed.post/r1,2021-12-20T11:33:20Z,all_versions,")

	// First run: bucket + apply normally, capturing which segments completed.
	jobDir := filepath.Join(t.TempDir(), "job")
	var applied []uint64
	var phases []ImportPhase
	_, err := rig.o.RunImport(context.Background(), ImportJob{
		CSVPath:          csv,
		JobDir:           jobDir,
		OnSegmentApplied: func(idx uint64) error { applied = append(applied, idx); return nil },
		OnPhase:          func(p ImportPhase, _ int) error { phases = append(phases, p); return nil },
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, applied, "segment 0 applied+checkpointed")
	require.Equal(t, []ImportPhase{ImportPhaseParseBucket, ImportPhaseApply}, phases)

	// Second run resumes: reuse the offset files (SkipBucket) and treat segment
	// 0 as already done. It must open nothing and apply nothing.
	rig.refreshed = nil
	var applyPhaseSegments = -1
	res, err := rig.o.RunImport(context.Background(), ImportJob{
		CSVPath:     csv,
		JobDir:      jobDir,
		SkipBucket:  true,
		SkipSegment: func(idx uint64) bool { return idx == 0 },
		OnPhase: func(p ImportPhase, n int) error {
			if p == ImportPhaseApply {
				applyPhaseSegments = n
			}
			return nil
		},
	})
	require.NoError(t, err)
	require.EqualValues(t, 0, res.SegmentsExamined, "checkpointed segment skipped, none examined")
	require.EqualValues(t, 0, res.SegmentsPatched)
	require.Equal(t, 0, applyPhaseSegments, "apply phase reports zero segments to process after resume-skip")
	require.Nil(t, rig.refreshed, "no manifest refresh on a fully-resumed job")
}

// TestRunImport_SkipBucketDoesNotReparse proves SkipBucket reuses the offset
// files verbatim: with SkipBucket set and no offset files present, Phase C has
// nothing to do (it does not re-run the parser).
func TestRunImport_SkipBucketDoesNotReparse(t *testing.T) {
	t.Parallel()
	events := []segment.Event{
		{Seq: 1, WitnessedAt: 1_000, Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "1", Payload: []byte("v1")},
	}
	rig := newImportTestRig(t, events)
	csv := writeImportCSVFile(t, "uri,timestamp,scope,cid",
		"at://did:plc:alice/app.bsky.feed.post/r1,2021-12-20T11:33:20Z,all_versions,")

	jobDir := filepath.Join(t.TempDir(), "empty-job")
	require.NoError(t, os.MkdirAll(jobDir, 0o755))
	res, err := rig.o.RunImport(context.Background(), ImportJob{
		CSVPath:    csv,
		JobDir:     jobDir,
		SkipBucket: true,
	})
	require.NoError(t, err)
	require.EqualValues(t, 0, res.Parse.RowsValid, "parser did not run under SkipBucket")
	require.EqualValues(t, 0, res.SegmentsExamined, "no offset files -> nothing to apply")
}

// TestRunImport_SegmentAppliedCheckpointErrorAborts proves a failed checkpoint
// aborts the job (resume safety lost -> stop loudly, not continue silently).
func TestRunImport_SegmentAppliedCheckpointErrorAborts(t *testing.T) {
	t.Parallel()
	events := []segment.Event{
		{Seq: 1, WitnessedAt: 1_000, Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "1", Payload: []byte("v1")},
	}
	rig := newImportTestRig(t, events)
	csv := writeImportCSVFile(t, "uri,timestamp,scope,cid",
		"at://did:plc:alice/app.bsky.feed.post/r1,2021-12-20T11:33:20Z,all_versions,")

	sentinel := errors.New("checkpoint write failed")
	_, err := rig.o.RunImport(context.Background(), ImportJob{
		CSVPath:          csv,
		JobDir:           filepath.Join(t.TempDir(), "job"),
		OnSegmentApplied: func(uint64) error { return sentinel },
	})
	require.ErrorIs(t, err, sentinel)

	// The abort must NOT skip the manifest refresh for the segment whose
	// rewrite already hit the disk (fsync+rename): the in-memory manifest
	// would otherwise serve the old checksum/size for the replaced file until
	// restart.
	require.Equal(t, []uint64{0}, rig.refreshed,
		"durably-rewritten segment still refreshes the manifest on abort")
}

// TestRunImport_MetricsObserved proves the import metrics fold the job's
// counters and reset the phase gauge to idle on completion.
func TestRunImport_MetricsObserved(t *testing.T) {
	t.Parallel()
	events := []segment.Event{
		{Seq: 1, WitnessedAt: 1_000, Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "1", Payload: []byte("v1")},
		{Seq: 2, WitnessedAt: 2_000, Kind: segment.KindUpdate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "2", Payload: []byte("v2")},
	}
	rig := newImportTestRig(t, events)
	im := NewImportMetrics(prometheus.NewRegistry())
	rig.o.cfg.ImportMetrics = im

	csv := writeImportCSVFile(t, "uri,timestamp,scope,cid",
		"at://did:plc:alice/app.bsky.feed.post/r1,2021-12-20T11:33:20Z,all_versions,",
		"bad-row-no-uri,,,")
	_, err := rig.o.RunImport(context.Background(), ImportJob{CSVPath: csv, JobDir: filepath.Join(t.TempDir(), "job")})
	require.NoError(t, err)

	require.EqualValues(t, 2, testutil.ToFloat64(im.RowsParsed), "2 rows read (1 valid + 1 rejected)")
	require.EqualValues(t, 2, testutil.ToFloat64(im.RowsMutated), "both alice versions patched")
	require.EqualValues(t, 2, testutil.ToFloat64(im.RowsMatched.WithLabelValues("all_versions")))
	require.EqualValues(t, 1, testutil.ToFloat64(im.SegmentsPatched))
	require.EqualValues(t, 1, testutil.ToFloat64(im.Jobs.WithLabelValues("ok")))
	require.Greater(t, testutil.ToFloat64(im.BytesRewritten), 0.0, "patched file bytes accounted")
	require.EqualValues(t, ImportPhaseGaugeIdle, testutil.ToFloat64(im.Phase), "phase reset to idle after job")
}

// TestRunImport_CancelledJobNotCountedAsTerminal proves a context-cancelled
// run — a graceful pause the manager will auto-resume — does not increment
// jobs_total in either label (it is not a terminal result), while the phase
// gauge still resets to idle.
func TestRunImport_CancelledJobNotCountedAsTerminal(t *testing.T) {
	t.Parallel()
	events := []segment.Event{
		{Seq: 1, WitnessedAt: 1_000, Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "1", Payload: []byte("v1")},
	}
	rig := newImportTestRig(t, events)
	im := NewImportMetrics(prometheus.NewRegistry())
	rig.o.cfg.ImportMetrics = im

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before the run starts: parse aborts on the first row
	csv := writeImportCSVFile(t, "uri,timestamp,scope,cid",
		"at://did:plc:alice/app.bsky.feed.post/r1,2021-12-20T11:33:20Z,all_versions,")
	_, err := rig.o.RunImport(ctx, ImportJob{CSVPath: csv, JobDir: filepath.Join(t.TempDir(), "job")})
	require.ErrorIs(t, err, context.Canceled)

	require.EqualValues(t, 0, testutil.ToFloat64(im.Jobs.WithLabelValues("error")), "pause is not a terminal error")
	require.EqualValues(t, 0, testutil.ToFloat64(im.Jobs.WithLabelValues("ok")))
	require.EqualValues(t, ImportPhaseGaugeIdle, testutil.ToFloat64(im.Phase), "phase reset to idle on pause")
	// Partial work still folds in: the row parsed before the abort counts.
	require.EqualValues(t, 1, testutil.ToFloat64(im.RowsParsed), "partial parse counters survive a pause")
}

// TestRunImport_CancelMidApplyIsPauseNotSuccess pins the Phase C cancellation
// contract: a job cancelled between segments must surface context.Canceled so
// the manager classifies it as a pause and keeps the checkpoint — NOT return
// nil, which would mark the job StateComplete and delete the checkpoint with
// segments still unapplied. The undelivered segments are silently dropped by
// the send loop on cancel; only the returned error tells the caller the pass
// was cut short.
func TestRunImport_CancelMidApplyIsPauseNotSuccess(t *testing.T) {
	t.Parallel()
	seg0 := []segment.Event{
		{Seq: 1, WitnessedAt: 1_000, Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "1", Payload: []byte("v1")},
	}
	seg1 := []segment.Event{
		{Seq: 2, WitnessedAt: 2_000, Kind: segment.KindCreate, DID: "did:plc:bob", Collection: "app.bsky.feed.post", Rkey: "r2", Rev: "1", Payload: []byte("v2")},
	}
	rig := newImportTestRigSegs(t, seg0, seg1)
	// One worker so the send loop is still holding segment 1 when the cancel
	// from segment 0's checkpoint hook fires, making the drop deterministic.
	rig.o.cfg.CompactionRewriteWorkers = 1

	csv := writeImportCSVFile(t, "uri,timestamp,scope,cid",
		"at://did:plc:alice/app.bsky.feed.post/r1,2021-12-20T11:33:20Z,all_versions,",
		"at://did:plc:bob/app.bsky.feed.post/r2,2021-12-20T11:33:20Z,all_versions,")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jobDir := filepath.Join(t.TempDir(), "job")
	var applied []uint64
	_, err := rig.o.RunImport(ctx, ImportJob{
		CSVPath: csv,
		JobDir:  jobDir,
		OnSegmentApplied: func(idx uint64) error {
			applied = append(applied, idx)
			cancel() // operator pause lands between segments
			return nil
		},
	})
	require.ErrorIs(t, err, context.Canceled,
		"a run cut short by cancellation must not report success: the manager would mark the job complete and delete the checkpoint with segments unapplied")
	require.Equal(t, []uint64{0}, applied, "only segment 0 completed before the pause")

	// Segment 1 really was left unapplied on disk.
	bobRows := readCompactionSegment(t, filepath.Join(rig.segmentsDir, "seg_0000000001.jss"))
	require.Len(t, bobRows, 1)
	require.EqualValues(t, 0, bobRows[0].IndexedAt, "segment 1 must be untouched after the pause")

	// Resume (SkipBucket + skip the checkpointed segment 0) finishes the job.
	res, err := rig.o.RunImport(context.Background(), ImportJob{
		CSVPath:     csv,
		JobDir:      jobDir,
		SkipBucket:  true,
		SkipSegment: func(idx uint64) bool { return idx == 0 },
	})
	require.NoError(t, err)
	require.Equal(t, 1, res.SegmentsPatched, "resume applies the dropped segment")
	bobRows = readCompactionSegment(t, filepath.Join(rig.segmentsDir, "seg_0000000001.jss"))
	require.EqualValues(t, 1_640_000_000_000_000, bobRows[0].IndexedAt)
}

// TestRunImport_MutualExclusionWithRewriteLock proves an in-flight import holds
// the rewrite lock: a delete-compaction-style rewrite (simulated by acquiring
// the same lock) cannot run concurrently. We block the import inside Phase C
// via a slow OnSegmentCompacted and assert a competing withRewriteLock caller
// only proceeds after the import releases.
func TestRunImport_MutualExclusionWithRewriteLock(t *testing.T) {
	t.Parallel()
	events := []segment.Event{
		{Seq: 1, WitnessedAt: 1_000, Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "1", Payload: []byte("v1")},
	}
	rig := newImportTestRig(t, events)

	var importInside atomic.Bool
	var competitorRanDuringImport atomic.Bool
	release := make(chan struct{})
	// Slow refresh: while the import holds the rewrite lock (Phase C), signal
	// and block so a competitor can try to acquire.
	rig.o.cfg.OnSegmentCompacted = func(idx uint64, path string) error {
		importInside.Store(true)
		<-release
		importInside.Store(false)
		return rig.mft.OnSegmentCompacted(idx, path)
	}

	csv := writeImportCSVFile(t, "uri,timestamp,scope,cid",
		"at://did:plc:alice/app.bsky.feed.post/r1,2021-12-20T11:33:20Z,all_versions,")

	var wg sync.WaitGroup
	wg.Go(func() {
		_, err := rig.o.RunImport(context.Background(), ImportJob{CSVPath: csv, JobDir: filepath.Join(t.TempDir(), "job")})
		require.NoError(t, err)
	})

	// Wait until the import is inside the lock (its slow refresh fired).
	require.Eventually(t, importInside.Load, time.Second, time.Millisecond)

	competitorDone := make(chan struct{})
	wg.Go(func() {
		_ = rig.o.withRewriteLock(func() error {
			// If mutual exclusion holds, the import must no longer be inside.
			if importInside.Load() {
				competitorRanDuringImport.Store(true)
			}
			return nil
		})
		close(competitorDone)
	})

	// Give the competitor a chance to (wrongly) proceed if the lock failed.
	time.Sleep(20 * time.Millisecond)
	select {
	case <-competitorDone:
		t.Fatal("competitor acquired the rewrite lock while import held it")
	default:
	}

	close(release)
	wg.Wait()
	<-competitorDone
	require.False(t, competitorRanDuringImport.Load(),
		"competitor must not run concurrently with the import's locked phase")
}
