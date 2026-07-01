// Package importer is the operator-facing job manager for timestamp import
// (design §8, milestone M6). It wraps the orchestrator's idempotent RunImport
// core (Phases A/B/C) with the durable, single-at-a-time job lifecycle the
// bearer-gated XRPC surface needs:
//
//   - Submit validates + confines the operator-supplied CSV path, refuses a
//     second concurrent job (design Q-JOBMODEL: one import at a time, 409), and
//     launches the run asynchronously, returning a job id immediately (202).
//   - Progress is checkpointed in pebble under import/job/<id>/ so a process
//     restart auto-resumes the in-flight job (design Q-RESUME): per-segment
//     done markers let a resumed run skip already-patched segments, and a
//     "bucketed" flag lets it skip re-parsing the CSV. The full-idempotency of
//     RunImport is the backstop — a lost checkpoint degrades to a cheap re-scan,
//     never corruption.
//   - Status is served from the live in-memory record for the running job and
//     from pebble for finished jobs, feeding both getImportStatus and the
//     operator status page.
//
// Path confinement (design Q-TRANSPORT guard rail): the CSV path is resolved
// through EvalSymlinks and must live within the configured import directory, so
// the endpoint cannot be used to read arbitrary host files via .. or a symlink.
package importer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest/orchestrator"
	"github.com/bluesky-social/jetstream/internal/store"
)

// Runner is the import core the manager drives. *orchestrator.Orchestrator
// satisfies it; tests pass a fake to exercise the lifecycle without real
// segments.
type Runner interface {
	RunImport(ctx context.Context, job orchestrator.ImportJob) (orchestrator.ImportResult, error)
}

// State is a job's lifecycle state. Stable string values: they are persisted
// and surfaced on the status page / getImportStatus.
type State string

const (
	// StateRunning: the job is executing (parse/bucket or apply).
	StateRunning State = "running"
	// StateComplete: the job finished successfully.
	StateComplete State = "complete"
	// StateFailed: the job aborted with an error (see Record.Error).
	StateFailed State = "failed"
)

// Terminal reports whether s is a finished state.
func (s State) Terminal() bool { return s == StateComplete || s == StateFailed }

// Sentinel errors. Submit callers map these to HTTP status codes:
// ErrJobInProgress -> 409, ErrPathRequired/ErrPathEscape/ErrPathNotFound ->
// 400, ErrNotADir/anything else -> 500.
var (
	// ErrJobInProgress is returned by Submit when another import is running.
	ErrJobInProgress = errors.New("importer: an import job is already in progress")
	// ErrPathRequired is returned when the submitted path is empty.
	ErrPathRequired = errors.New("importer: csv path is required")
	// ErrPathEscape is returned when the resolved path escapes the import dir.
	ErrPathEscape = errors.New("importer: csv path escapes the import directory")
	// ErrPathNotFound is returned when the resolved path does not exist.
	ErrPathNotFound = errors.New("importer: csv path not found")
	// ErrNotAFile is returned when the resolved path is not a regular file
	// (a directory, FIFO, device, or socket).
	ErrNotAFile = errors.New("importer: csv path is not a regular file")
	// ErrJobNotFound is returned by Status for an unknown job id.
	ErrJobNotFound = errors.New("importer: job not found")
)

// Record is the durable + rendering view of one import job. Persisted as JSON
// under import/job/<id>/meta; also the shape getImportStatus and the status
// page consume. Counts are best-effort progress: SegmentsApplied advances live
// during Phase C, while the parse/mutation totals are filled from the final
// ImportResult (they are not known until the run returns).
type Record struct {
	ID          string                   `json:"id"`
	CSVPath     string                   `json:"csvPath"`
	State       State                    `json:"state"`
	Phase       orchestrator.ImportPhase `json:"phase,omitempty"`
	Error       string                   `json:"error,omitempty"`
	SubmittedAt time.Time                `json:"submittedAt"`
	FinishedAt  time.Time                `json:"finishedAt,omitzero"`

	// Bucketed is set once Phase A+B completes and the offset files are durable.
	// A resumed run keys on it to decide whether to skip re-parsing.
	Bucketed bool `json:"bucketed"`

	// Live Phase C progress.
	SegmentsToApply int `json:"segmentsToApply"`
	SegmentsApplied int `json:"segmentsApplied"`

	// Final totals, filled from ImportResult on success.
	RowsTotal              uint64            `json:"rowsTotal"`
	RowsValid              uint64            `json:"rowsValid"`
	RowsRejected           uint64            `json:"rowsRejected"`
	RejectsByReason        map[string]uint64 `json:"rejectsByReason,omitempty"`
	SegmentsExamined       int               `json:"segmentsExamined"`
	SegmentsPatched        int               `json:"segmentsPatched"`
	RowsMutated            uint64            `json:"rowsMutated"`
	RowsMatchedAllVersions uint64            `json:"rowsMatchedAllVersions"`
	RowsMatchedSpecific    uint64            `json:"rowsMatchedSpecific"`
	SpecificCIDsUnmatched  uint64            `json:"specificCidsUnmatched"`
	RowsCorruptOffset      uint64            `json:"rowsCorruptOffset"`
}

func (r Record) clone() Record {
	if r.RejectsByReason != nil {
		m := make(map[string]uint64, len(r.RejectsByReason))
		maps.Copy(m, r.RejectsByReason)
		r.RejectsByReason = m
	}
	return r
}

// Config configures a Manager. Store, Runner, ImportDir, and ScratchDir are
// required.
type Config struct {
	// Store is the metadata pebble db for job records + checkpoints.
	Store *store.Store
	// Runner is the import core (the orchestrator in production).
	Runner Runner
	// ImportDir is the confinement root: a submitted CSV path is resolved
	// within this directory. Must exist.
	ImportDir string
	// ScratchDir is the parent for per-job offset-file directories
	// (ScratchDir/<jobID>). Created on demand; distinct from ImportDir so job
	// scratch never mingles with the operator's staged CSVs.
	ScratchDir string
	// Logger is optional; nil uses slog.Default().
	Logger *slog.Logger
	// Now is the clock; nil uses time.Now. Injected for tests.
	Now func() time.Time
	// NewJobID mints a job id; nil uses a UTC-timestamp + random-suffix scheme.
	// Injected for deterministic tests.
	NewJobID func() string
}

// Manager owns the single-import-at-a-time lifecycle. Safe for concurrent use.
type Manager struct {
	store      *store.Store
	runner     Runner
	importDir  string
	scratchDir string
	logger     *slog.Logger
	now        func() time.Time
	newJobID   func() string

	mu      sync.Mutex
	running *Record // the live record while a job runs; nil when idle

	// wg tracks in-flight background run goroutines so Wait can drain them
	// before the store is closed at shutdown.
	wg sync.WaitGroup
}

// Wait blocks until any in-flight background import goroutine returns, or ctx
// is done. Call it during shutdown (after cancelling the run context) so the
// job's final store writes complete before the metadata store is closed. A
// paused (ctx-cancelled) job returns promptly; Wait then observes it drained.
func (m *Manager) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// New validates cfg and returns a Manager.
func New(cfg Config) (*Manager, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("importer: Store is required")
	}
	if cfg.Runner == nil {
		return nil, fmt.Errorf("importer: Runner is required")
	}
	if cfg.ImportDir == "" {
		return nil, fmt.Errorf("importer: ImportDir is required")
	}
	if cfg.ScratchDir == "" {
		return nil, fmt.Errorf("importer: ScratchDir is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	newJobID := cfg.NewJobID
	if newJobID == nil {
		newJobID = func() string { return defaultJobID(now()) }
	}
	return &Manager{
		store:      cfg.Store,
		runner:     cfg.Runner,
		importDir:  cfg.ImportDir,
		scratchDir: cfg.ScratchDir,
		logger:     logger.With(slog.String("component", "importer")),
		now:        now,
		newJobID:   newJobID,
	}, nil
}

// Submit validates + confines requestedPath, refuses a second concurrent job,
// persists an initial record, and launches the run asynchronously. It returns
// the new job id (the caller maps this to a 202). runCtx roots the background
// run; cancel it (e.g. on shutdown) to stop the job — a stopped mid-run job is
// left non-terminal and auto-resumes on the next ResumeIncomplete.
func (m *Manager) Submit(runCtx context.Context, requestedPath string) (string, error) {
	csvPath, err := m.resolveImportPath(requestedPath)
	if err != nil {
		return "", err
	}

	m.mu.Lock()
	if m.running != nil && !m.running.State.Terminal() {
		m.mu.Unlock()
		return "", ErrJobInProgress
	}
	// The in-memory pointer is not enough: at startup the HTTP server begins
	// serving before ResumeIncomplete adopts a persisted non-terminal job, so a
	// submit landing in that window would overwrite import/current and orphan
	// the resumable job. The persisted pointer is the cross-restart source of
	// truth; consult it under the same lock.
	if currentID, ok, err := m.getCurrent(); err != nil {
		m.mu.Unlock()
		return "", err
	} else if ok {
		current, found, err := m.getRecord(currentID)
		if err != nil {
			m.mu.Unlock()
			return "", err
		}
		if found && !current.State.Terminal() {
			m.mu.Unlock()
			return "", ErrJobInProgress
		}
	}
	id := m.newJobID()
	rec := &Record{
		ID:          id,
		CSVPath:     csvPath,
		State:       StateRunning,
		Phase:       orchestrator.ImportPhaseParseBucket,
		SubmittedAt: m.now(),
	}
	// Persist the record and the current-job pointer before releasing the lock
	// so a concurrent Status/restart sees a consistent view. A persistence
	// failure here means we cannot guarantee resume — fail the submit.
	if err := m.putRecordLocked(rec); err != nil {
		m.mu.Unlock()
		return "", err
	}
	if err := m.setCurrent(id); err != nil {
		m.mu.Unlock()
		return "", err
	}
	m.running = rec
	m.wg.Add(1)
	m.mu.Unlock()

	go func() {
		defer m.wg.Done()
		m.run(runCtx, rec.clone(), false)
	}()
	return id, nil
}

// ResumeIncomplete relaunches the persisted current job if it is non-terminal
// (a crash/restart mid-import). It is a no-op when there is no current job or
// the current job already finished. Call once at startup before serving.
func (m *Manager) ResumeIncomplete(runCtx context.Context) error {
	id, ok, err := m.getCurrent()
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	rec, ok, err := m.getRecord(id)
	if err != nil {
		return err
	}
	if !ok || rec.State.Terminal() {
		return nil
	}

	m.mu.Lock()
	if m.running != nil && !m.running.State.Terminal() {
		m.mu.Unlock()
		return nil // already running (defensive; ResumeIncomplete is start-only)
	}
	rec.State = StateRunning
	rec.Error = ""
	m.running = &rec
	m.wg.Add(1)
	m.mu.Unlock()

	m.logger.Info("resuming incomplete import", "job", id, "bucketed", rec.Bucketed)
	go func() {
		defer m.wg.Done()
		m.run(runCtx, rec.clone(), true)
	}()
	return nil
}

// Status returns the job record for id. Prefers the live in-memory record when
// id is the running job, else reads pebble. ErrJobNotFound for an unknown id.
func (m *Manager) Status(id string) (Record, error) {
	m.mu.Lock()
	if m.running != nil && m.running.ID == id {
		out := m.running.clone()
		m.mu.Unlock()
		return out, nil
	}
	m.mu.Unlock()

	rec, ok, err := m.getRecord(id)
	if err != nil {
		return Record{}, err
	}
	if !ok {
		return Record{}, ErrJobNotFound
	}
	return rec, nil
}

// Current returns the live record of the running (or most recently submitted)
// job, if any. Used by the status page to show the active import without a job
// id. ok is false when no job has been submitted this process lifetime and none
// is persisted as current.
func (m *Manager) Current() (Record, bool) {
	m.mu.Lock()
	if m.running != nil {
		out := m.running.clone()
		m.mu.Unlock()
		return out, true
	}
	m.mu.Unlock()

	id, ok, err := m.getCurrent()
	if err != nil || !ok {
		return Record{}, false
	}
	rec, ok, err := m.getRecord(id)
	if err != nil || !ok {
		return Record{}, false
	}
	return rec, true
}

// run executes the job on a background goroutine, threading resume + checkpoint
// hooks into RunImport and folding the final result / any error into the record.
func (m *Manager) run(ctx context.Context, rec Record, resume bool) {
	jobDir := m.jobDir(rec.ID)

	// On a fresh (non-resume) run, or a resume that had not yet finished
	// bucketing, start from a clean scratch dir so Phase A+B does not O_APPEND
	// onto partially-written offset files from a prior attempt.
	skipBucket := resume && rec.Bucketed
	if !skipBucket {
		if err := os.RemoveAll(jobDir); err != nil {
			m.finish(rec.ID, fmt.Errorf("importer: clear scratch dir: %w", err))
			return
		}
	}

	done, err := m.loadDoneSegments(rec.ID)
	if err != nil {
		m.finish(rec.ID, err)
		return
	}

	job := orchestrator.ImportJob{
		CSVPath:    rec.CSVPath,
		JobDir:     jobDir,
		SkipBucket: skipBucket,
		SkipSegment: func(idx uint64) bool {
			_, ok := done[idx]
			return ok
		},
		OnSegmentApplied: func(idx uint64) error {
			return m.onSegmentApplied(rec.ID, idx)
		},
		OnPhase: func(phase orchestrator.ImportPhase, segmentsToApply int) error {
			return m.onPhase(rec.ID, phase, segmentsToApply)
		},
	}

	result, runErr := m.runner.RunImport(ctx, job)
	if runErr != nil {
		// A cancellation error is a graceful stop (shutdown), not a job
		// failure: leave the record non-terminal (StateRunning) and the
		// current-job pointer set so the next boot auto-resumes from the
		// checkpoint. Only a genuine error marks the job failed (terminal),
		// which does NOT auto-resume — re-running a deterministically-failing
		// job would loop; the operator re-submits. Classify by the RETURNED
		// error, not ctx.Err(): a real failure racing shutdown cancellation
		// must not be laundered into a resumable pause.
		if isCancellationOnly(runErr) {
			m.pause(rec.ID)
			return
		}
		m.finish(rec.ID, runErr)
		return
	}
	m.finishSuccess(rec.ID, result)
}

// isCancellationOnly reports whether every leaf of err's tree is a context
// cancellation. errors.Is alone is not enough: the orchestrator can return
// errors.Join(context.Canceled, realFailure) — a worker cancelled at shutdown
// joined with, say, a failed manifest refresh — and errors.Is matches ANY
// leaf, which would launder the real failure into a resumable pause. Pause
// only when cancellation is the whole story.
func isCancellationOnly(err error) bool {
	if err == nil {
		return false
	}
	if err == context.Canceled || err == context.DeadlineExceeded { //nolint:errorlint // leaves compared after unwrapping below
		return true
	}
	switch u := err.(type) { //nolint:errorlint // deliberate tree walk
	case interface{ Unwrap() []error }:
		children := u.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !isCancellationOnly(child) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return isCancellationOnly(u.Unwrap())
	}
	return false
}

// onPhase records phase entry. Entering the apply phase means Phase A+B is
// complete and the offset files are durable, so we flip Bucketed (the resume
// key) and persist.
func (m *Manager) onPhase(id string, phase orchestrator.ImportPhase, segmentsToApply int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running == nil || m.running.ID != id {
		return nil
	}
	m.running.Phase = phase
	if phase == orchestrator.ImportPhaseApply {
		m.running.Bucketed = true
		m.running.SegmentsToApply = segmentsToApply
	}
	return m.putRecordLocked(m.running)
}

// onSegmentApplied durably marks a segment done and advances the live count. It
// fires concurrently from Phase C workers; the mutex serializes the record
// update and the seg-key write. A pebble write failure is returned so RunImport
// aborts the job (resume safety lost -> stop loudly).
func (m *Manager) onSegmentApplied(id string, idx uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkpointSegmentLocked(id, idx); err != nil {
		return err
	}
	if m.running != nil && m.running.ID == id {
		m.running.SegmentsApplied++
	}
	return nil
}

// pause handles a graceful stop (ctx cancelled mid-run): it clears the
// in-memory running pointer but leaves the persisted record non-terminal and
// import/current set, so the next process boot auto-resumes from the
// checkpoint. The on-disk record already reflects the last durable phase +
// per-segment progress.
func (m *Manager) pause(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running != nil && m.running.ID == id {
		m.running = nil
	}
	m.logger.Info("import job paused (context cancelled); will resume on next start", "job", id)
}

// finish records a terminal failure.
func (m *Manager) finish(id string, cause error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec := m.running
	if rec == nil || rec.ID != id {
		// The running pointer moved on (shouldn't happen: one job at a time).
		// Persist a standalone failed record so status reflects the failure.
		stored, ok, err := m.getRecord(id)
		if err != nil || !ok {
			m.logger.Error("import job failed but record is missing", "job", id, "err", cause)
			return
		}
		rec = &stored
	}
	rec.State = StateFailed
	rec.Error = cause.Error()
	rec.FinishedAt = m.now()
	m.logger.Error("import job failed", "job", id, "err", cause)
	if err := m.putRecordLocked(rec); err != nil {
		m.logger.Error("persist failed import record", "job", id, "err", err)
	}
	// Leave import/current pointing at this job: a failed run is resumable
	// (idempotent re-apply), and clearing it would drop the auto-resume. An
	// operator re-submit starts a fresh job and overwrites current.
}

// finishSuccess folds the final result into the record, marks it complete,
// clears the current-job pointer, and removes the now-unneeded scratch dir.
func (m *Manager) finishSuccess(id string, result orchestrator.ImportResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec := m.running
	if rec == nil || rec.ID != id {
		stored, ok, err := m.getRecord(id)
		if err != nil || !ok {
			m.logger.Error("import job completed but record is missing", "job", id)
			return
		}
		rec = &stored
	}
	rec.State = StateComplete
	rec.Phase = orchestrator.ImportPhaseApply
	rec.FinishedAt = m.now()
	rec.RowsTotal = result.Parse.RowsTotal
	rec.RowsValid = result.Parse.RowsValid
	rec.RowsRejected = result.Parse.RowsRejected
	if len(result.Parse.RejectsByReason) > 0 {
		rec.RejectsByReason = make(map[string]uint64, len(result.Parse.RejectsByReason))
		for reason, n := range result.Parse.RejectsByReason {
			rec.RejectsByReason[string(reason)] = n
		}
	}
	rec.SegmentsExamined = result.SegmentsExamined
	rec.SegmentsPatched = result.SegmentsPatched
	rec.RowsMutated = result.RowsMutated
	rec.RowsMatchedAllVersions = result.RowsMatchedAllVersions
	rec.RowsMatchedSpecific = result.RowsMatchedSpecific
	rec.SpecificCIDsUnmatched = result.SpecificCIDsUnmatched
	rec.RowsCorruptOffset = result.RowsCorruptOffset
	if err := m.putRecordLocked(rec); err != nil {
		m.logger.Error("persist completed import record", "job", id, "err", err)
	}
	if err := m.clearCurrent(); err != nil {
		m.logger.Error("clear current import pointer", "job", id, "err", err)
	}
	if err := os.RemoveAll(m.jobDir(id)); err != nil {
		m.logger.Warn("remove import scratch dir", "job", id, "err", err)
	}
	// Clear the live pointer: the job is done and current/ is cleared, so
	// Current() reports no active import. Status(id) still serves this record
	// from pebble.
	if m.running != nil && m.running.ID == id {
		m.running = nil
	}
	m.logger.Info("import job complete",
		"job", id,
		"rows_valid", rec.RowsValid,
		"rows_rejected", rec.RowsRejected,
		"segments_patched", rec.SegmentsPatched,
		"rows_mutated", rec.RowsMutated,
	)
}

func (m *Manager) jobDir(id string) string { return filepath.Join(m.scratchDir, id) }

// resolveImportPath confines requestedPath to the import directory. A relative
// path is joined to importDir; an absolute path is confined as-is. The path is
// resolved through EvalSymlinks (so a symlink pointing outside is rejected) and
// checked to be a regular file within the resolved import root.
func (m *Manager) resolveImportPath(requestedPath string) (string, error) {
	requestedPath = strings.TrimSpace(requestedPath)
	if requestedPath == "" {
		return "", ErrPathRequired
	}

	// Resolve the confinement root first; a non-existent import dir is an
	// operator/config error, surfaced as a generic error (not a 400). The root
	// is absolutized before symlink resolution: a relative import dir (the
	// default data dir is relative) would otherwise make filepath.Rel compare
	// a relative root against absolute submissions and reject them all.
	aliasRoot, err := filepath.Abs(m.importDir)
	if err != nil {
		return "", fmt.Errorf("importer: resolve import dir %q: %w", m.importDir, err)
	}
	root, err := filepath.EvalSymlinks(aliasRoot)
	if err != nil {
		return "", fmt.Errorf("importer: resolve import dir %q: %w", m.importDir, err)
	}

	// Relative candidates join to the RESOLVED root, not the alias: when the
	// configured import dir is itself a symlink the two spellings diverge, and
	// joining to the unresolved spelling would make every valid relative path
	// look like an escape against root below.
	cand := requestedPath
	if !filepath.IsAbs(cand) {
		cand = filepath.Join(root, cand)
	}
	cand = filepath.Clean(cand)

	// Lexical confinement FIRST, before touching the filesystem: a ".."
	// traversal that resolves to a non-existent path outside the root must be
	// reported as an escape, not "not found" (which would leak whether an
	// out-of-root file exists and mislabel a clear escape attempt). Checking
	// the cleaned path against the resolved root catches ".." regardless of
	// existence; the post-EvalSymlinks check below then additionally catches a
	// symlink whose target escapes. An absolute path spelled through the
	// symlinked import-dir alias must also pass: it is checked against the
	// alias spelling here, and the authoritative post-resolution check against
	// root still gates it.
	if escapes(root, cand) && escapes(aliasRoot, cand) {
		return "", fmt.Errorf("%w: %s", ErrPathEscape, requestedPath)
	}

	resolved, err := filepath.EvalSymlinks(cand)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("%w: %s", ErrPathNotFound, requestedPath)
		}
		return "", fmt.Errorf("importer: resolve csv path %q: %w", requestedPath, err)
	}
	// Symlink escape: EvalSymlinks has collapsed any link into its real target,
	// so a link inside the root pointing out is caught here even though the
	// lexical check above passed.
	if escapes(root, resolved) {
		return "", fmt.Errorf("%w: %s", ErrPathEscape, requestedPath)
	}

	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("importer: stat csv path %q: %w", requestedPath, err)
	}
	// Mode().IsRegular, not !IsDir: a FIFO would pass Submit and then block
	// RunImport's os.Open in an open(2) that context cancellation cannot
	// interrupt, wedging the single job slot.
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%w: %s", ErrNotAFile, requestedPath)
	}
	return resolved, nil
}

// escapes reports whether path lies outside root (path is neither root itself
// nor strictly under it). Both must be cleaned absolute paths.
func escapes(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return true
	}
	return rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// defaultJobID mints a lexicographically-sortable id: a UTC timestamp plus a
// short random suffix to disambiguate submits within the same second.
func defaultJobID(now time.Time) string {
	var b [4]byte
	// crypto/rand.Read never returns a short read; on the astronomically
	// unlikely error we fall back to the timestamp alone, still unique enough
	// under the one-job-at-a-time invariant.
	if _, err := rand.Read(b[:]); err != nil {
		return now.UTC().Format("20060102T150405.000Z")
	}
	return now.UTC().Format("20060102T150405.000Z") + "-" + hex.EncodeToString(b[:])
}
