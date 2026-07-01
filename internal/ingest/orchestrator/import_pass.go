package orchestrator

// import_pass.go wires the timestamp-import pipeline (design §8, milestones
// M4/M5) into the orchestrator so it shares the segment-rewrite lock and the
// manifest-refresh/notification path with delete-compaction (design §3.3,
// §6 H).
//
// Phases (design §3.2):
//   - A+B (parse + bucket): stream the plain import CSV, validate each row, and
//     append its byte offset to the per-segment offset file its DID's blooms
//     select. Reads the manifest and writes offset files only -- it touches no
//     segment, so it runs OUTSIDE the rewrite lock.
//   - C (apply): for each segment with an offset file, build the per-path patch
//     plan and run one segment.Patch, in a worker pool, UNDER the rewrite lock
//     (mutually exclusive with delete-compaction; the loser waits).
//
// The whole operation is idempotent and re-runnable (design §3.4): a re-run
// produces zero mutations on already-applied segments, so segment.Patch skips
// the rename. That is the crash-resume backstop -- a job interrupted in Phase C
// resumes by re-running rather than restarting.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/timestamp"
	"github.com/bluesky-social/jetstream/segment"
	"golang.org/x/sync/errgroup"
)

// ErrImportUnavailable is returned by RunImport when the orchestrator was not
// configured with an ImportSelector (import disabled).
var ErrImportUnavailable = errors.New("orchestrator: timestamp import not configured")

// ImportJob describes one timestamp-import run.
type ImportJob struct {
	// CSVPath is the plain (uncompressed) import CSV, staged server-local
	// (design Q-TRANSPORT). Must be seekable.
	CSVPath string

	// JobDir is a per-job scratch directory for the Phase B offset files. It
	// is created if absent. Callers should use a unique dir per job so a
	// resumed/re-run job's offset files do not mingle with another's.
	JobDir string
}

// ImportResult aggregates the pipeline's counters for job status (design §6 J).
type ImportResult struct {
	Parse  timestamp.Stats
	Bucket timestamp.BucketStats

	// SegmentsExamined is the number of segments with an offset file that
	// Phase C opened. SegmentsPatched is those that actually changed (a
	// segment whose rows were all already at their target is examined but not
	// patched -- the idempotent-rerun case).
	SegmentsExamined int
	SegmentsPatched  int

	// RowsMutated is the total rows whose IndexedAt changed across all patched
	// segments. RowsMatchedSpecific / SpecificCIDsUnmatched aggregate the §4a
	// specific_version accounting.
	RowsMutated            uint64
	RowsMatchedAllVersions uint64
	RowsMatchedSpecific    uint64
	SpecificCIDsUnmatched  uint64
	RowsCorruptOffset      uint64
}

// RunImport executes a full timestamp-import job: Phase A+B (parse + bucket,
// unlocked) then Phase C (apply, under the rewrite lock). It is safe to call
// from a request-handler goroutine concurrently with steady-state compaction;
// the rewrite lock serializes the two passes.
func (o *Orchestrator) RunImport(ctx context.Context, job ImportJob) (ImportResult, error) {
	if o.cfg.ImportSelector == nil {
		return ImportResult{}, ErrImportUnavailable
	}
	if job.CSVPath == "" {
		return ImportResult{}, fmt.Errorf("orchestrator: import: CSVPath is required")
	}
	if job.JobDir == "" {
		return ImportResult{}, fmt.Errorf("orchestrator: import: JobDir is required")
	}

	var result ImportResult
	err := obs.Span(ctx, func(ctx context.Context) error {
		if err := os.MkdirAll(job.JobDir, 0o755); err != nil {
			return fmt.Errorf("orchestrator: import: create job dir: %w", err)
		}

		// Phase A+B: parse + bucket. No segment is touched, so this runs
		// outside the rewrite lock. The offset files it writes are the
		// hand-off to Phase C.
		parseStats, bucketStats, err := o.runImportBucket(ctx, job)
		if err != nil {
			return err
		}
		result.Parse = parseStats
		result.Bucket = bucketStats

		// Phase C: apply, under the rewrite lock (mutually exclusive with
		// delete-compaction).
		return o.withRewriteLock(func() error {
			return o.runImportApply(ctx, job, &result)
		})
	})
	if err != nil {
		return ImportResult{}, err
	}
	return result, nil
}

// runImportBucket runs Phase A+B: parse the CSV and route each valid row's
// offset to its candidate segments' offset files.
func (o *Orchestrator) runImportBucket(ctx context.Context, job ImportJob) (timestamp.Stats, timestamp.BucketStats, error) {
	f, err := os.Open(job.CSVPath)
	if err != nil {
		return timestamp.Stats{}, timestamp.BucketStats{}, fmt.Errorf("orchestrator: import: open csv: %w", err)
	}
	defer func() { _ = f.Close() }()

	bucketer, err := timestamp.NewBucketer(timestamp.BucketerConfig{
		Selector: o.cfg.ImportSelector,
		JobDir:   job.JobDir,
	})
	if err != nil {
		return timestamp.Stats{}, timestamp.BucketStats{}, fmt.Errorf("orchestrator: import: new bucketer: %w", err)
	}

	parseStats, err := timestamp.Parse(f, timestamp.Options{
		OnRow: func(row timestamp.Row) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return bucketer.Route(row)
		},
	})
	if closeErr := bucketer.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		return timestamp.Stats{}, timestamp.BucketStats{}, fmt.Errorf("orchestrator: import: parse/bucket: %w", err)
	}
	o.logger.InfoContext(ctx, "timestamp import parse+bucket complete",
		"rows_valid", parseStats.RowsValid,
		"rows_rejected", parseStats.RowsRejected,
		"segments_touched", bucketer.Stats().SegmentsTouched,
	)
	return parseStats, bucketer.Stats(), nil
}

// importSegment pairs a touched segment's file with its offset file.
type importSegment struct {
	file       ingest.SegmentFile
	offsetPath string
}

// runImportApply runs Phase C: patch every segment that has an offset file.
// Called under the rewrite lock.
func (o *Orchestrator) runImportApply(ctx context.Context, job ImportJob, result *ImportResult) error {
	segments, err := o.importTouchedSegments(job.JobDir)
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		return nil
	}
	result.SegmentsExamined = len(segments)

	rr, err := timestamp.OpenRowReader(job.CSVPath)
	if err != nil {
		return fmt.Errorf("orchestrator: import: open row reader: %w", err)
	}
	defer func() { _ = rr.Close() }()

	workers := o.cfg.CompactionRewriteWorkers
	if workers <= 0 {
		workers = defaultCompactionRewriteWorkers()
	}
	workers = min(workers, len(segments))

	jobs := make(chan importSegment)
	var (
		mu      sync.Mutex
		results []importApplyResult
	)
	g, gctx := errgroup.WithContext(ctx)
	for range workers {
		g.Go(func() error {
			for seg := range jobs {
				if err := gctx.Err(); err != nil {
					return err
				}
				res, err := o.applyImportSegment(seg, rr)
				if err != nil {
					return err
				}
				mu.Lock()
				results = append(results, res)
				mu.Unlock()
			}
			return nil
		})
	}
	for _, seg := range segments {
		select {
		case jobs <- seg:
		case <-gctx.Done():
			close(jobs)
			return g.Wait()
		}
	}
	close(jobs)
	if err := g.Wait(); err != nil {
		return err
	}

	// Deterministic order for the manifest-refresh loop and logs.
	sort.Slice(results, func(i, j int) bool { return results[i].idx < results[j].idx })
	for _, r := range results {
		result.RowsCorruptOffset += r.plan.RowsCorrupt
		result.RowsMatchedAllVersions += r.apply.RowsMatchedAllVersions
		result.RowsMatchedSpecific += r.apply.RowsMatchedSpecific
		result.SpecificCIDsUnmatched += r.apply.SpecificCIDsUnmatched
		if !r.patch.Patched {
			continue
		}
		result.SegmentsPatched++
		result.RowsMutated += r.patch.RowsMutated
		o.logger.InfoContext(ctx, "timestamp import patched segment",
			"segment", r.path,
			"rows_mutated", r.patch.RowsMutated,
			"blocks_touched", r.patch.BlocksTouched,
		)
		if o.cfg.OnSegmentCompacted != nil {
			if err := o.cfg.OnSegmentCompacted(r.idx, r.path); err != nil {
				return fmt.Errorf("orchestrator: import: refresh manifest %s: %w", r.path, err)
			}
		}
	}
	o.logger.InfoContext(ctx, "timestamp import apply complete",
		"segments_examined", result.SegmentsExamined,
		"segments_patched", result.SegmentsPatched,
		"rows_mutated", result.RowsMutated,
	)
	return nil
}

type importApplyResult struct {
	idx   uint64
	path  string
	plan  timestamp.PlanStats
	apply timestamp.ApplyStats
	patch segment.PatchResult
}

// applyImportSegment builds the patch plan for one segment from its offset file
// and runs a single segment.Patch. Called from a worker goroutine; rr is
// concurrency-safe (positioned reads).
func (o *Orchestrator) applyImportSegment(seg importSegment, rr *timestamp.RowReader) (importApplyResult, error) {
	plan, err := timestamp.BuildPatchPlan(seg.offsetPath, rr)
	if err != nil {
		return importApplyResult{}, fmt.Errorf("orchestrator: import: build plan %s: %w", seg.file.Path, err)
	}
	res := importApplyResult{idx: seg.file.Idx, path: seg.file.Path, plan: plan.Stats()}
	if plan.Empty() {
		// Every offset in this segment's file was corrupt (desync surfaced via
		// RowsCorrupt); nothing to patch.
		return res, nil
	}

	mutate := plan.BuildMutate()
	patchRes, err := segment.Patch(seg.file.Path, mutate.Fn(), segment.PatchOptions{
		CrashInjector: crashpoint.ForSegment(o.cfg.CrashInjector),
		CandidateDIDs: plan.CandidateDIDs(),
	})
	if err != nil {
		return importApplyResult{}, fmt.Errorf("orchestrator: import: patch %s: %w", seg.file.Path, err)
	}
	res.apply = mutate.Stats()
	res.patch = patchRes
	return res, nil
}

// importTouchedSegments pairs each offset file in jobDir with its live segment
// file. An offset file whose segment no longer exists on disk (compacted away
// between Phase B and Phase C) is skipped with a warning -- its rows were
// already re-homed or dropped; a re-run against the current manifest re-buckets
// them (design §3.4).
func (o *Orchestrator) importTouchedSegments(jobDir string) ([]importSegment, error) {
	entries, err := os.ReadDir(jobDir)
	if err != nil {
		return nil, fmt.Errorf("orchestrator: import: read job dir: %w", err)
	}
	segmentsDir := filepath.Join(o.cfg.DataDir, "segments")
	var out []importSegment
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		idx, ok := timestamp.ParseOffsetFileName(e.Name())
		if !ok {
			continue
		}
		segPath := filepath.Join(segmentsDir, ingest.SegmentFilename(idx))
		if _, err := os.Stat(segPath); err != nil {
			if os.IsNotExist(err) {
				o.logger.Warn("timestamp import: segment for offset file vanished; skipping",
					"segment_idx", idx, "offset_file", e.Name())
				continue
			}
			return nil, fmt.Errorf("orchestrator: import: stat segment %d: %w", idx, err)
		}
		out = append(out, importSegment{
			file:       ingest.SegmentFile{Idx: idx, Path: segPath},
			offsetPath: filepath.Join(jobDir, e.Name()),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].file.Idx < out[j].file.Idx })
	return out, nil
}
