package importer_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/importer"
	"github.com/bluesky-social/jetstream/internal/ingest/orchestrator"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

// fakeRunner drives the manager lifecycle deterministically. Its RunImport
// invokes the job's phase/segment/apply hooks per the scripted plan, then
// returns result/err. gate, when non-nil, blocks RunImport until closed so a
// test can observe the running state.
type fakeRunner struct {
	mu       sync.Mutex
	calls    []orchestrator.ImportJob
	segments []uint64 // segment idxs to drive through Phase C
	result   orchestrator.ImportResult
	err      error
	gate     chan struct{}
	started  chan struct{}
}

func (f *fakeRunner) RunImport(ctx context.Context, job orchestrator.ImportJob) (orchestrator.ImportResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, job)
	started, gate := f.started, f.gate
	f.started = nil // fire the started signal at most once across runs
	segs := f.segments
	res, err := f.result, f.err
	f.mu.Unlock()

	if started != nil {
		close(started)
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return orchestrator.ImportResult{}, ctx.Err()
		}
	}
	if err != nil {
		return orchestrator.ImportResult{}, err
	}

	if !job.SkipBucket && job.OnPhase != nil {
		if e := job.OnPhase(orchestrator.ImportPhaseParseBucket, 0); e != nil {
			return orchestrator.ImportResult{}, e
		}
	}
	var toApply []uint64
	for _, s := range segs {
		if job.SkipSegment != nil && job.SkipSegment(s) {
			continue
		}
		toApply = append(toApply, s)
	}
	if job.OnPhase != nil {
		if e := job.OnPhase(orchestrator.ImportPhaseApply, len(toApply)); e != nil {
			return orchestrator.ImportResult{}, e
		}
	}
	for _, s := range toApply {
		if job.OnSegmentApplied != nil {
			if e := job.OnSegmentApplied(s); e != nil {
				return orchestrator.ImportResult{}, e
			}
		}
	}
	return res, nil
}

// testEnv is a data dir shared across manager instances so a restart can be
// simulated by constructing a second Manager over the same store + dirs.
type testEnv struct {
	dataDir    string
	importDir  string
	scratchDir string
	store      *store.Store
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	importDir := filepath.Join(dataDir, "imports")
	require.NoError(t, os.MkdirAll(importDir, 0o755))
	return &testEnv{
		dataDir:    dataDir,
		importDir:  importDir,
		scratchDir: filepath.Join(dataDir, "import-scratch"),
		store:      st,
	}
}

func (e *testEnv) manager(t *testing.T, runner importer.Runner) *importer.Manager {
	t.Helper()
	m, err := importer.New(importer.Config{
		Store:      e.store,
		Runner:     runner,
		ImportDir:  e.importDir,
		ScratchDir: e.scratchDir,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	// Drain in-flight background runs before the store closes (cleanups run
	// LIFO, so this fires before newTestEnv's store-close cleanup).
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = m.Wait(ctx)
	})
	return m
}

func newTestManager(t *testing.T, runner importer.Runner) (*importer.Manager, string, *store.Store) {
	t.Helper()
	env := newTestEnv(t)
	return env.manager(t, runner), env.importDir, env.store
}

func writeCSV(t *testing.T, dir, name string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, []byte("uri,timestamp,scope,cid\n"), 0o644))
	return p
}

func waitTerminal(t *testing.T, m *importer.Manager, id string) importer.Record {
	t.Helper()
	var rec importer.Record
	require.Eventually(t, func() bool {
		r, err := m.Status(id)
		if err != nil {
			return false
		}
		rec = r
		return r.State.Terminal()
	}, 2*time.Second, time.Millisecond)
	return rec
}

func TestSubmit_HappyPathCompletes(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{
		segments: []uint64{0, 1},
		result: orchestrator.ImportResult{
			SegmentsExamined: 2, SegmentsPatched: 2, RowsMutated: 5,
		},
	}
	m, importDir, _ := newTestManager(t, runner)
	csv := writeCSV(t, importDir, "atlantis.csv")

	id, err := m.Submit(context.Background(), "atlantis.csv")
	require.NoError(t, err)
	require.NotEmpty(t, id)

	rec := waitTerminal(t, m, id)
	require.Equal(t, importer.StateComplete, rec.State)
	require.Equal(t, csv, rec.CSVPath)
	require.EqualValues(t, 2, rec.SegmentsPatched)
	require.EqualValues(t, 5, rec.RowsMutated)
	require.Equal(t, 2, rec.SegmentsApplied)
	require.True(t, rec.Bucketed)

	// A completed job clears the current pointer.
	_, ok := m.Current()
	require.False(t, ok, "current pointer cleared after completion")
}

func TestSubmit_ConcurrentJobRejected(t *testing.T) {
	t.Parallel()
	gate := make(chan struct{})
	started := make(chan struct{})
	runner := &fakeRunner{gate: gate, started: started}
	m, importDir, _ := newTestManager(t, runner)
	writeCSV(t, importDir, "a.csv")

	id1, err := m.Submit(context.Background(), "a.csv")
	require.NoError(t, err)
	<-started // job 1 is inside RunImport, blocked on the gate

	_, err = m.Submit(context.Background(), "a.csv")
	require.ErrorIs(t, err, importer.ErrJobInProgress)

	close(gate)
	rec := waitTerminal(t, m, id1)
	require.Equal(t, importer.StateComplete, rec.State)

	// After completion a new job is accepted.
	_, err = m.Submit(context.Background(), "a.csv")
	require.NoError(t, err)
}

func TestSubmit_PathConfinement(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{}
	m, importDir, _ := newTestManager(t, runner)
	writeCSV(t, importDir, "ok.csv")

	t.Run("empty", func(t *testing.T) {
		_, err := m.Submit(context.Background(), "")
		require.ErrorIs(t, err, importer.ErrPathRequired)
	})
	t.Run("dotdot escape", func(t *testing.T) {
		_, err := m.Submit(context.Background(), "../../etc/passwd")
		require.ErrorIs(t, err, importer.ErrPathEscape)
	})
	t.Run("not found", func(t *testing.T) {
		_, err := m.Submit(context.Background(), "missing.csv")
		require.ErrorIs(t, err, importer.ErrPathNotFound)
	})
	t.Run("directory", func(t *testing.T) {
		require.NoError(t, os.MkdirAll(filepath.Join(importDir, "sub"), 0o755))
		_, err := m.Submit(context.Background(), "sub")
		require.ErrorIs(t, err, importer.ErrNotAFile)
	})
	t.Run("symlink escape", func(t *testing.T) {
		outside := filepath.Join(t.TempDir(), "secret.csv")
		require.NoError(t, os.WriteFile(outside, []byte("x"), 0o644))
		link := filepath.Join(importDir, "link.csv")
		require.NoError(t, os.Symlink(outside, link))
		_, err := m.Submit(context.Background(), "link.csv")
		require.ErrorIs(t, err, importer.ErrPathEscape)
	})
}

func TestSubmit_AbsolutePathWithinImportDirAccepted(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{}
	m, importDir, _ := newTestManager(t, runner)
	csv := writeCSV(t, importDir, "abs.csv")

	id, err := m.Submit(context.Background(), csv) // absolute, inside importDir
	require.NoError(t, err)
	rec := waitTerminal(t, m, id)
	require.Equal(t, importer.StateComplete, rec.State)
}

func TestStatus_UnknownJob(t *testing.T) {
	t.Parallel()
	m, _, _ := newTestManager(t, &fakeRunner{})
	_, err := m.Status("nope")
	require.ErrorIs(t, err, importer.ErrJobNotFound)
}

func TestRun_FailurePersistsError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("boom")
	runner := &fakeRunner{err: sentinel}
	m, importDir, _ := newTestManager(t, runner)
	writeCSV(t, importDir, "a.csv")

	id, err := m.Submit(context.Background(), "a.csv")
	require.NoError(t, err)
	rec := waitTerminal(t, m, id)
	require.Equal(t, importer.StateFailed, rec.State)
	require.Contains(t, rec.Error, "boom")

	// A failed job leaves current set (resumable); a re-submit is allowed
	// because the running pointer is terminal.
	_, err = m.Submit(context.Background(), "a.csv")
	require.NoError(t, err)
}

// crashAfterSegmentRunner reports bucketing complete, checkpoints the segments
// in applyOK, then blocks until its context is cancelled and returns ctx.Err()
// — modelling a graceful stop / crash after some segments were durably
// checkpointed but before the job finished. A ctx-cancelled run is left
// resumable (non-terminal), which is the auto-resume trigger.
type crashAfterSegmentRunner struct {
	segments []uint64
	applyOK  []uint64
	stopped  chan struct{} // closed once RunImport is about to block on ctx
}

func (r *crashAfterSegmentRunner) RunImport(ctx context.Context, job orchestrator.ImportJob) (orchestrator.ImportResult, error) {
	if !job.SkipBucket && job.OnPhase != nil {
		if err := job.OnPhase(orchestrator.ImportPhaseParseBucket, 0); err != nil {
			return orchestrator.ImportResult{}, err
		}
	}
	if job.OnPhase != nil {
		if err := job.OnPhase(orchestrator.ImportPhaseApply, len(r.segments)); err != nil {
			return orchestrator.ImportResult{}, err
		}
	}
	for _, s := range r.applyOK {
		if job.OnSegmentApplied != nil {
			if err := job.OnSegmentApplied(s); err != nil {
				return orchestrator.ImportResult{}, err
			}
		}
	}
	if r.stopped != nil {
		close(r.stopped)
	}
	<-ctx.Done()
	return orchestrator.ImportResult{}, ctx.Err()
}

// recordingRunner captures the job it was handed so a resume test can assert
// the SkipBucket flag and the SkipSegment done-set. It completes cleanly.
type recordingRunner struct {
	segments []uint64
	mu       sync.Mutex
	last     orchestrator.ImportJob
}

func (r *recordingRunner) RunImport(ctx context.Context, job orchestrator.ImportJob) (orchestrator.ImportResult, error) {
	r.mu.Lock()
	r.last = job
	r.mu.Unlock()
	if job.OnPhase != nil {
		if err := job.OnPhase(orchestrator.ImportPhaseApply, len(r.segments)); err != nil {
			return orchestrator.ImportResult{}, err
		}
	}
	for _, s := range r.segments {
		if job.SkipSegment != nil && job.SkipSegment(s) {
			continue
		}
		if job.OnSegmentApplied != nil {
			if err := job.OnSegmentApplied(s); err != nil {
				return orchestrator.ImportResult{}, err
			}
		}
	}
	return orchestrator.ImportResult{}, nil
}

func (r *recordingRunner) lastJob() orchestrator.ImportJob {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last
}

// TestResumeIncomplete_SkipsCheckpointedSegments proves a crash-and-resume:
// the first run checkpoints segment 0 then crashes; a second Manager over the
// same store (a process restart) auto-resumes with SkipBucket + the done-set so
// segment 0 is skipped and only segment 1 is applied.
func TestResumeIncomplete_SkipsCheckpointedSegments(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	writeCSV(t, env.importDir, "a.csv")

	stopped := make(chan struct{})
	crashRunner := &crashAfterSegmentRunner{segments: []uint64{0, 1}, applyOK: []uint64{0}, stopped: stopped}
	m1 := env.manager(t, crashRunner)
	runCtx, cancel := context.WithCancel(context.Background())
	id, err := m1.Submit(runCtx, "a.csv")
	require.NoError(t, err)
	<-stopped // segment 0 checkpointed; runner now blocked on ctx
	cancel()  // simulate shutdown/crash mid-import

	// The paused job stays current + resumable (non-terminal); it is not
	// cleared like a completed job.
	require.Eventually(t, func() bool {
		rec, ok := m1.Current()
		return ok && !rec.State.Terminal() && rec.Bucketed
	}, 2*time.Second, time.Millisecond, "paused job stays current + resumable")

	// Restart: a fresh Manager over the same store auto-resumes.
	resumeRunner := &recordingRunner{segments: []uint64{0, 1}}
	m2 := env.manager(t, resumeRunner)
	require.NoError(t, m2.ResumeIncomplete(context.Background()))
	rec := waitTerminal(t, m2, id)
	require.Equal(t, importer.StateComplete, rec.State)

	job := resumeRunner.lastJob()
	require.True(t, job.SkipBucket, "resume reused offset files")
	require.NotNil(t, job.SkipSegment)
	require.True(t, job.SkipSegment(0), "segment 0 checkpointed -> skipped")
	require.False(t, job.SkipSegment(1), "segment 1 not checkpointed -> applied")
}
