package orchestrator

// import_pass.go wires the timestamp-import pipeline (design §8, milestones
// M4/M5) into the orchestrator so it shares the segment-rewrite lock and the
// manifest-refresh path with delete-compaction (design §3.3, §6 H).
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
	"time"

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

// ErrImportNotSteadyState is returned when an operator submits an import before
// the steady-state writer exists. Timestamp import is intentionally steady-state
// only; before that there is no stable archive surface to repair.
var ErrImportNotSteadyState = errors.New("orchestrator: timestamp import requires steady state")

// ImportPhase identifies which phase of a running import a progress callback
// refers to (design §3.2). The string values are stable: they surface on the
// operator status page and in persisted job records, so alerting/dashboards
// key on them.
type ImportPhase string

const (
	// ImportPhaseParseBucket is Phase A+B: streaming the CSV, validating rows,
	// and routing offsets to per-segment offset files.
	ImportPhaseParseBucket ImportPhase = "parse_bucket"
	// ImportPhaseApply is Phase C: patching each touched segment via
	// segment.Patch under the rewrite lock.
	ImportPhaseApply ImportPhase = "apply"
)

// ImportJob describes one timestamp-import run.
//
// The Skip*/On* hooks are the seam a durable job manager (internal/importer,
// M6) uses to checkpoint progress and resume after a crash. They are all
// optional: a zero-value ImportJob (only CSVPath+JobDir set) runs a complete,
// un-checkpointed import — the pre-M6 behavior — and stays correct because the
// whole pipeline is idempotent (design §3.4). The hooks only make a resumed
// job cheaper (skip re-parsing, skip already-applied segments), never change
// the result.
type ImportJob struct {
	// CSVPath is the plain (uncompressed) import CSV, staged server-local
	// (design Q-TRANSPORT). Must be seekable.
	CSVPath string

	// JobDir is a per-job scratch directory for the Phase B offset files. It
	// is created if absent. Callers should use a unique dir per job so a
	// resumed/re-run job's offset files do not mingle with another's.
	JobDir string

	// SkipBucket, when true, skips Phase A+B and applies the offset files
	// already present in JobDir. A resuming manager sets this only after it
	// durably recorded that bucketing completed on a prior run: re-running
	// Phase A+B would O_APPEND a second copy of every offset onto the existing
	// files (double-counting, though still idempotent at apply). When false
	// (the default) the caller is responsible for JobDir being empty of stale
	// offset files, exactly as before.
	SkipBucket bool

	// SkipSegment, when non-nil, is consulted in Phase C before each touched
	// segment is opened; returning true skips it (a prior run already applied
	// and durably checkpointed it). This is an optimization over the
	// full-idempotency backstop — always returning false is correct, just
	// re-decompresses already-patched segments. Called concurrently from Phase
	// C worker goroutines, so it must be safe for concurrent use.
	SkipSegment func(idx uint64) bool

	// OnSegmentApplied, when non-nil, is called after a touched segment has
	// been processed in Phase C (patched, or confirmed already at target) and
	// its bytes are durable on disk, so a manager can add it to its resume
	// done-set. It fires from the worker goroutine right after the segment's
	// atomic rewrite, so a crash later in the pass still leaves the completed
	// segments checkpointed. Returning an error aborts the whole job: a
	// checkpoint that cannot be persisted means resume safety is lost, and per
	// the durability directive we stop loudly rather than continue
	// un-checkpointed. Called concurrently from Phase C workers.
	OnSegmentApplied func(idx uint64) error

	// OnPhase, when non-nil, is called once as the import enters each phase,
	// from the RunImport goroutine (never a worker). segmentsToApply is the
	// number of touched segments Phase C will process (after resume-skips);
	// it is 0 for ImportPhaseParseBucket, where the count is not yet known. A
	// returned error aborts the job.
	OnPhase func(phase ImportPhase, segmentsToApply int) error
}

// ImportResult aggregates the pipeline's counters for job status (design §6 J).
type ImportResult struct {
	Parse  timestamp.Stats
	Bucket timestamp.BucketStats
	Rules  timestamp.RuleIngestResult

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

	// BytesRewritten is the total on-disk size of the segment files import
	// rewrote (summed patched-file sizes), for the bytes_rewritten metric.
	BytesRewritten uint64
}

// ParseStats returns the operator-visible CSV parse counters for a run. Normal
// runs report Phase A+B's bucketing parse. If the run failed or paused during
// the #269 rule-ingest preamble before bucketing began, the rule-ingest parser
// is the only parser that observed rows, so surface its partial counters.
func (r ImportResult) ParseStats() timestamp.Stats {
	if r.Parse.RowsTotal > 0 || len(r.Parse.RejectsByReason) > 0 || len(r.Parse.RejectSample) > 0 {
		return r.Parse
	}
	return r.Rules.Parse
}

// RunImport executes a full timestamp-import job: Phase A+B (parse + bucket,
// unlocked) then Phase C (apply, under the rewrite lock). It is safe to call
// from a request-handler goroutine concurrently with steady-state compaction;
// the rewrite lock serializes the two passes.
func (o *Orchestrator) RunImport(ctx context.Context, job ImportJob) (ImportResult, error) {
	if o.cfg.ImportSelector == nil || o.cfg.ImportRules == nil {
		return ImportResult{}, ErrImportUnavailable
	}
	liveWriter := o.steadyWriter.Load()
	if liveWriter == nil {
		return ImportResult{}, ErrImportNotSteadyState
	}
	if job.CSVPath == "" {
		return ImportResult{}, fmt.Errorf("orchestrator: import: CSVPath is required")
	}
	if job.JobDir == "" {
		return ImportResult{}, fmt.Errorf("orchestrator: import: JobDir is required")
	}

	start := time.Now()
	var result ImportResult
	err := obs.Span(ctx, func(ctx context.Context) error {
		if err := os.MkdirAll(job.JobDir, 0o755); err != nil {
			return fmt.Errorf("orchestrator: import: create job dir: %w", err)
		}

		// Rule ingestion and Phase A+B run only before the durable bucketed
		// checkpoint. Rule ingestion comes first and activates the append-time
		// stamper before we rotate the steady writer. The subsequent bucket pass
		// then sees the just-sealed active segment and Phase C patches rows that
		// were active before activation; later appends are stamped at birth.
		//
		// A resuming job that already durably bucketed skips this whole preamble:
		// the rules were ingested and the active segment was rotated before the
		// bucketed checkpoint was written, so the existing offset files are a
		// complete hand-off to Phase C.
		if !job.SkipBucket {
			o.cfg.ImportMetrics.setPhase(ImportPhaseGaugeParseBucket)
			if job.OnPhase != nil {
				if err := job.OnPhase(ImportPhaseParseBucket, 0); err != nil {
					return err
				}
			}
			ruleResult, err := o.cfg.ImportRules.ImportRulesFromCSV(ctx, timestamp.RuleIngestConfig{
				CSVPath:    job.CSVPath,
				ScratchDir: filepath.Join(job.JobDir, "rules"),
			})
			result.Rules = ruleResult
			if err != nil {
				return err
			}
			if err := liveWriter.ForceRotate(ctx); err != nil {
				return fmt.Errorf("orchestrator: import: force rotate active segment after rule activation: %w", err)
			}
			// Fold the stats in BEFORE the error check: a cancelled or failed
			// parse still counted rows, and observeJob's partial-counter fold
			// on the error path depends on them being present.
			parseStats, bucketStats, err := o.runImportBucket(ctx, job)
			result.Parse = parseStats
			result.Bucket = bucketStats
			if err != nil {
				return err
			}
		}

		// Phase C: apply, under the rewrite lock (mutually exclusive with
		// delete-compaction).
		o.cfg.ImportMetrics.setPhase(ImportPhaseGaugeApply)
		return o.withRewriteLock(func() error {
			return o.runImportApply(ctx, job, &result)
		})
	})
	// Fold the job's counters into metrics and reset the phase gauge to idle,
	// on both the success and error paths (a partial result still carries the
	// work done before the failure). The partial result is also RETURNED with
	// the error so the job manager can fold the same counters into the durable
	// job record — a paused or failed run's progress must not vanish from
	// status while the metrics remember it.
	o.cfg.ImportMetrics.observeJob(start, result, err)
	return result, err
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
		// Return the partial stats alongside the error: Parse accumulates
		// them up to the aborting row, and the metrics fold on the error path
		// records that work (a paused run's rows_parsed must not vanish).
		return parseStats, bucketer.Stats(), fmt.Errorf("orchestrator: import: parse/bucket: %w", err)
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
	all, err := o.importTouchedSegments(job.JobDir)
	if err != nil {
		return err
	}
	// Drop segments a prior run already applied and checkpointed. The
	// full-idempotency backstop makes skipping optional, but it avoids
	// re-decompressing every already-patched segment on a resume.
	segments := all
	if job.SkipSegment != nil {
		segments = segments[:0:0]
		for _, seg := range all {
			if job.SkipSegment(seg.file.Idx) {
				continue
			}
			segments = append(segments, seg)
		}
	}

	if job.OnPhase != nil {
		if err := job.OnPhase(ImportPhaseApply, len(segments)); err != nil {
			return err
		}
	}
	if len(segments) == 0 {
		return nil
	}

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
				// Record the result BEFORE the checkpoint hook: the rewrite is
				// already durable on disk (segment.Patch fsync+rename+dir-
				// sync'd), so even if checkpointing fails the fold below must
				// still refresh the in-memory manifest for this segment, or
				// the process would serve the old checksum/size for a file
				// that was replaced.
				mu.Lock()
				results = append(results, res)
				mu.Unlock()
				// Checkpoint AFTER the segment's atomic rewrite is durable, or
				// confirmed the segment was already at target. A crash later
				// in the pass then resumes past this segment. Fired before the
				// same-process manifest refresh, which is safe: a restart
				// rebuilds the manifest from the patched file's own header, so
				// a skipped-on-resume segment still serves correct metadata.
				if job.OnSegmentApplied != nil {
					if err := job.OnSegmentApplied(seg.file.Idx); err != nil {
						return fmt.Errorf("orchestrator: import: checkpoint segment %d: %w", seg.file.Idx, err)
					}
				}
			}
			return nil
		})
	}
sendLoop:
	for _, seg := range segments {
		select {
		case jobs <- seg:
		case <-gctx.Done():
			break sendLoop
		}
	}
	close(jobs)
	waitErr := g.Wait()
	// A cancel can land while every worker is idling in `for seg := range jobs`:
	// the send loop drops the undelivered segments, the workers drain nothing
	// and return nil, and g.Wait() is nil even though the pass was cut short.
	// Fold the context in so a paused run never reports success — the manager
	// would otherwise mark the job complete and delete its checkpoint with
	// segments still unapplied.
	if waitErr == nil {
		waitErr = ctx.Err()
	}

	// Fold results and refresh the manifest even when a worker errored:
	// other workers may have durably rewritten segments (fsync+rename) before
	// the failure, and skipping their OnSegmentCompacted would leave the
	// in-memory manifest serving the OLD checksum/size for a file already
	// replaced on disk until restart. Deterministic order for the refresh
	// loop and logs.
	sort.Slice(results, func(i, j int) bool { return results[i].idx < results[j].idx })
	// Count from results, not len(segments): a paused/failed pass examined only
	// the segments its workers actually processed.
	result.SegmentsExamined = len(results)
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
		result.BytesRewritten += r.bytes
		o.logger.InfoContext(ctx, "timestamp import patched segment",
			"segment", r.path,
			"rows_mutated", r.patch.RowsMutated,
			"blocks_touched", r.patch.BlocksTouched,
		)
		if o.cfg.OnSegmentCompacted != nil {
			if err := o.cfg.OnSegmentCompacted(r.idx, r.path); err != nil {
				return errors.Join(waitErr, fmt.Errorf("orchestrator: import: refresh manifest %s: %w", r.path, err))
			}
		}
	}
	if waitErr != nil {
		return waitErr
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
	bytes uint64 // on-disk size of the patched file (0 when not patched)
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
		FS:              o.cfg.FS,
		CrashInjector:   crashpoint.ForSegment(o.cfg.CrashInjector),
		IOFaultInjector: o.cfg.SegmentIOFaultInjector,
		CandidateDIDs:   plan.CandidateDIDs(),
	})
	if err != nil {
		return importApplyResult{}, ingest.WrapDiskFull(o.cfg.DataDir, "patching segment during timestamp import",
			fmt.Errorf("orchestrator: import: patch %s: %w", seg.file.Path, err))
	}
	res.apply = mutate.Stats()
	res.patch = patchRes
	if patchRes.Patched {
		// Record the rewritten file's on-disk size for the bytes_rewritten
		// metric. A stat failure here is non-fatal: the rewrite already
		// succeeded and is durable, so we log-and-continue rather than fail the
		// whole job over a metric.
		if info, statErr := statStorageFS(o.cfg.FS, seg.file.Path); statErr == nil {
			res.bytes = uint64(info.Size())
		} else {
			o.logger.Warn("timestamp import: stat patched segment for byte accounting",
				"segment", seg.file.Path, "err", statErr)
		}
	}
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
		if _, err := statStorageFS(o.cfg.FS, segPath); err != nil {
			if isStorageNotExist(err) {
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
