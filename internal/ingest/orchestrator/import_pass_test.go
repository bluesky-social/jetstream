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
	t.Helper()
	dataDir := t.TempDir()
	segmentsDir := filepath.Join(dataDir, "segments")
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
	writeCompactionSegment(t, segmentsDir, 0, events)

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
