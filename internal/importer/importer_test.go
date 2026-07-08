package importer_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/importer"
	"github.com/bluesky-social/jetstream/internal/ingest/orchestrator"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/timestamp"
	"github.com/cockroachdb/pebble"
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

func resolvedPath(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	require.NoError(t, err)
	return resolved
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
	require.Equal(t, resolvedPath(t, csv), rec.CSVPath)
	require.EqualValues(t, 2, rec.SegmentsPatched)
	require.EqualValues(t, 5, rec.RowsMutated)
	require.Equal(t, 2, rec.SegmentsApplied)
	require.True(t, rec.Bucketed)

	// The completed job stays "most recent": Current() keeps serving its
	// terminal record so the no-param getImportStatus and the status page can
	// show the final summary (the lexicon promises "the current or most recent
	// job"). Failed jobs already behave this way; complete must match.
	cur, ok := m.Current()
	require.True(t, ok, "completed job remains the most-recent job")
	require.Equal(t, id, cur.ID)
	require.Equal(t, importer.StateComplete, cur.State)
	require.EqualValues(t, 5, cur.RowsMutated)
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

func TestSubmit_NotReadyRejectedWithoutJob(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	writeCSV(t, env.importDir, "a.csv")
	runner := &fakeRunner{}
	m, err := importer.New(importer.Config{
		Store:      env.store,
		Runner:     runner,
		ImportDir:  env.importDir,
		ScratchDir: env.scratchDir,
		Ready:      func() error { return importer.ErrNotReady },
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	_, err = m.Submit(context.Background(), "a.csv")
	require.ErrorIs(t, err, importer.ErrNotReady)
	_, ok := m.Current()
	require.False(t, ok, "not-ready submit must not create a job record")
	require.Empty(t, runner.calls, "not-ready submit must not launch RunImport")
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
	t.Run("fifo", func(t *testing.T) {
		require.NoError(t, syscall.Mkfifo(filepath.Join(importDir, "pipe.csv"), 0o644))
		_, err := m.Submit(context.Background(), "pipe.csv")
		require.ErrorIs(t, err, importer.ErrNotAFile)
	})
}

// TestSubmit_SymlinkedImportDirAccepted: the configured import dir may itself
// be a symlink (common operator layout: /data/import -> /mnt/storage/csv).
// Valid paths within it — relative, absolute via the real dir, and absolute
// via the symlink alias — must all be accepted, and escapes still rejected.
func TestSubmit_SymlinkedImportDirAccepted(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	realDir := filepath.Join(base, "real-imports")
	require.NoError(t, os.MkdirAll(realDir, 0o755))
	linkDir := filepath.Join(base, "import-alias")
	require.NoError(t, os.Symlink(realDir, linkDir))

	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	m, err := importer.New(importer.Config{
		Store:      st,
		Runner:     &fakeRunner{},
		ImportDir:  linkDir, // configured through the symlink
		ScratchDir: filepath.Join(dataDir, "scratch"),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = m.Wait(ctx)
	})
	csv := writeCSV(t, realDir, "a.csv")

	id, err := m.Submit(context.Background(), "a.csv")
	require.NoError(t, err, "relative path within a symlinked import dir")
	waitTerminal(t, m, id)

	id, err = m.Submit(context.Background(), csv)
	require.NoError(t, err, "absolute path via the real dir")
	waitTerminal(t, m, id)

	id, err = m.Submit(context.Background(), filepath.Join(linkDir, "a.csv"))
	require.NoError(t, err, "absolute path via the symlink alias")
	waitTerminal(t, m, id)

	_, err = m.Submit(context.Background(), "../../etc/passwd")
	require.ErrorIs(t, err, importer.ErrPathEscape, "escape still rejected")
}

// TestSubmit_AbsolutePathViaSiblingAncestorAliasAccepted covers macOS-style
// path aliases such as /var -> /private/var. The import dir and requested CSV
// can be spelled through different aliases of the same ancestor; the resolved
// target is still under the resolved import root and must be accepted.
func TestSubmit_AbsolutePathViaSiblingAncestorAliasAccepted(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	realBase := filepath.Join(base, "real-base")
	require.NoError(t, os.MkdirAll(filepath.Join(realBase, "real-imports"), 0o755))

	aliasBase := filepath.Join(base, "alias-base")
	require.NoError(t, os.Symlink(realBase, aliasBase))
	linkDir := filepath.Join(aliasBase, "import-alias")
	require.NoError(t, os.Symlink(filepath.Join(aliasBase, "real-imports"), linkDir))

	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	m, err := importer.New(importer.Config{
		Store:      st,
		Runner:     &fakeRunner{},
		ImportDir:  linkDir,
		ScratchDir: filepath.Join(dataDir, "scratch"),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = m.Wait(ctx)
	})
	csv := writeCSV(t, filepath.Join(aliasBase, "real-imports"), "a.csv")

	id, err := m.Submit(context.Background(), csv)
	require.NoError(t, err)
	rec := waitTerminal(t, m, id)
	require.Equal(t, resolvedPath(t, csv), rec.CSVPath)
}

// TestSubmit_RelativeImportDirAcceptsAbsolutePath: the default data dir is
// relative, so the import dir often is too. An absolute submitted path inside
// it must still be accepted (Rel against a relative root would reject it).
//
//nolint:paralleltest // intentionally serial: t.Chdir mutates the process-global cwd
func TestSubmit_RelativeImportDirAcceptsAbsolutePath(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	absImportDir := filepath.Join(base, "imports")
	require.NoError(t, os.MkdirAll(absImportDir, 0o755))

	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	m, err := importer.New(importer.Config{
		Store:      st,
		Runner:     &fakeRunner{},
		ImportDir:  "imports", // relative spelling, resolves against cwd
		ScratchDir: filepath.Join(dataDir, "scratch"),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = m.Wait(ctx)
	})
	csv := writeCSV(t, absImportDir, "a.csv")

	id, err := m.Submit(context.Background(), csv) // absolute path, relative root
	require.NoError(t, err)
	waitTerminal(t, m, id)

	id, err = m.Submit(context.Background(), "a.csv") // relative still works
	require.NoError(t, err)
	waitTerminal(t, m, id)
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

	// A failed job is terminal (ResumeIncomplete skips it; only a fresh
	// submit retries), so a re-submit is allowed.
	_, err = m.Submit(context.Background(), "a.csv")
	require.NoError(t, err)
}

// countSegCheckpoints counts the persisted import/job/<id>/seg/* keys — the
// per-segment resume done-set. Terminal jobs must not leave these behind.
func countSegCheckpoints(t *testing.T, st *store.Store, id string) int {
	t.Helper()
	prefix := []byte("import/job/" + id + "/seg/")
	it, err := st.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: store.PrefixUpperBound(prefix),
	})
	require.NoError(t, err)
	defer func() { _ = it.Close() }()
	n := 0
	for it.First(); it.Valid(); it.Next() {
		n++
	}
	require.NoError(t, it.Error())
	return n
}

// failAfterApplyRunner checkpoints its segments (driving OnPhase +
// OnSegmentApplied like a real Phase C), then fails. It models a job that
// died partway with durable per-segment progress on disk.
type failAfterApplyRunner struct {
	segments []uint64
	cause    error
}

func (r *failAfterApplyRunner) RunImport(_ context.Context, job orchestrator.ImportJob) (orchestrator.ImportResult, error) {
	if job.OnPhase != nil {
		if err := job.OnPhase(orchestrator.ImportPhaseApply, len(r.segments)); err != nil {
			return orchestrator.ImportResult{}, err
		}
	}
	for _, s := range r.segments {
		if job.OnSegmentApplied != nil {
			if err := job.OnSegmentApplied(s); err != nil {
				return orchestrator.ImportResult{}, err
			}
		}
	}
	// Leave a scratch artifact like Phase B would, so the test can assert the
	// terminal cleanup removes the whole job dir.
	if err := os.MkdirAll(job.JobDir, 0o755); err != nil {
		return orchestrator.ImportResult{}, err
	}
	if err := os.WriteFile(filepath.Join(job.JobDir, "seg_0000000000.off"), []byte{1}, 0o644); err != nil {
		return orchestrator.ImportResult{}, err
	}
	return orchestrator.ImportResult{}, r.cause
}

// TestTerminalJobs_ReleaseScratchAndCheckpoints: a terminal job — failed or
// complete — can never resume (a re-submit mints a fresh id and job dir), so
// keeping its offset files and seg checkpoint keys would leak them forever:
// per-segment keys accrue per job, and a whole-archive import writes tens of
// thousands. Both terminal paths must drop the scratch dir and seg keys; the
// meta record stays so Status(id) keeps serving the outcome.
func TestTerminalJobs_ReleaseScratchAndCheckpoints(t *testing.T) {
	t.Parallel()

	t.Run("failed", func(t *testing.T) {
		t.Parallel()
		env := newTestEnv(t)
		writeCSV(t, env.importDir, "a.csv")
		m := env.manager(t, &failAfterApplyRunner{segments: []uint64{0, 1, 2}, cause: errors.New("boom")})

		id, err := m.Submit(context.Background(), "a.csv")
		require.NoError(t, err)
		rec := waitTerminal(t, m, id)
		require.Equal(t, importer.StateFailed, rec.State)
		require.Equal(t, 3, rec.SegmentsApplied, "checkpoints landed before the failure")

		require.Zero(t, countSegCheckpoints(t, env.store, id), "failed job's seg checkpoint keys released")
		require.NoDirExists(t, filepath.Join(env.scratchDir, id), "failed job's scratch dir removed")

		// The meta record survives for status.
		got, err := m.Status(id)
		require.NoError(t, err)
		require.Equal(t, importer.StateFailed, got.State)
	})

	t.Run("complete", func(t *testing.T) {
		t.Parallel()
		env := newTestEnv(t)
		writeCSV(t, env.importDir, "a.csv")
		m := env.manager(t, &fakeRunner{segments: []uint64{0, 1}})

		id, err := m.Submit(context.Background(), "a.csv")
		require.NoError(t, err)
		rec := waitTerminal(t, m, id)
		require.Equal(t, importer.StateComplete, rec.State)

		require.Zero(t, countSegCheckpoints(t, env.store, id), "completed job's seg checkpoint keys released")
		require.NoDirExists(t, filepath.Join(env.scratchDir, id), "completed job's scratch dir removed")
	})
}

// funcRunner adapts a closure to importer.Runner so a test can script the
// exact hook sequence and the (result, err) pair a run returns.
type funcRunner struct {
	fn func(context.Context, orchestrator.ImportJob) (orchestrator.ImportResult, error)
}

func (f funcRunner) RunImport(ctx context.Context, job orchestrator.ImportJob) (orchestrator.ImportResult, error) {
	return f.fn(ctx, job)
}

// TestPause_PersistsPartialProgress: RunImport returns its partial counters
// alongside a cancellation error (a graceful pause). The paused record must
// carry that partial progress — the same counters the Prometheus fold already
// records — so the status page and getImportStatus do not show a bucketing
// job stuck at zero rows across a restart.
func TestPause_PersistsPartialProgress(t *testing.T) {
	t.Parallel()
	partial := orchestrator.ImportResult{
		Parse: timestamp.Stats{
			RowsTotal:       10,
			RowsValid:       9,
			RowsRejected:    1,
			RejectsByReason: map[timestamp.RejectReason]uint64{timestamp.ReasonBadURI: 1},
		},
	}
	runner := funcRunner{fn: func(_ context.Context, job orchestrator.ImportJob) (orchestrator.ImportResult, error) {
		if e := job.OnPhase(orchestrator.ImportPhaseParseBucket, 0); e != nil {
			return orchestrator.ImportResult{}, e
		}
		// Pause lands mid-parse: partial stats + a pure cancellation error.
		return partial, fmt.Errorf("orchestrator: import: parse/bucket: %w", context.Canceled)
	}}
	m, importDir, _ := newTestManager(t, runner)
	writeCSV(t, importDir, "a.csv")

	id, err := m.Submit(context.Background(), "a.csv")
	require.NoError(t, err)
	require.NoError(t, m.Wait(context.Background()))

	rec, err := m.Status(id)
	require.NoError(t, err)
	require.Equal(t, importer.StateRunning, rec.State, "paused job stays non-terminal")
	require.EqualValues(t, 10, rec.RowsTotal, "partial parse totals persisted on pause")
	require.EqualValues(t, 9, rec.RowsValid)
	require.EqualValues(t, 1, rec.RowsRejected)
	require.Equal(t, map[string]uint64{"bad_uri": 1}, rec.RejectsByReason)
}

// TestFail_PersistsPartialProgress: a terminal failure also folds the work
// done before the error into the record, matching the metrics fold.
func TestFail_PersistsPartialProgress(t *testing.T) {
	t.Parallel()
	partial := orchestrator.ImportResult{
		Parse:            timestamp.Stats{RowsTotal: 7, RowsValid: 7},
		SegmentsExamined: 2,
		SegmentsPatched:  1,
		RowsMutated:      3,
	}
	runner := funcRunner{fn: func(context.Context, orchestrator.ImportJob) (orchestrator.ImportResult, error) {
		return partial, errors.New("boom")
	}}
	m, importDir, _ := newTestManager(t, runner)
	writeCSV(t, importDir, "a.csv")

	id, err := m.Submit(context.Background(), "a.csv")
	require.NoError(t, err)
	rec := waitTerminal(t, m, id)
	require.Equal(t, importer.StateFailed, rec.State)
	require.EqualValues(t, 7, rec.RowsTotal, "partial parse totals persisted on failure")
	require.EqualValues(t, 1, rec.SegmentsPatched)
	require.EqualValues(t, 3, rec.RowsMutated)
}

// TestResume_AccumulatesCountersAcrossRuns is the pause-mid-apply lifecycle:
// run 1 parses the CSV (totals 10/9/1), applies + checkpoints segment 0 of 2,
// and pauses; run 2 resumes with SkipBucket (so it reports ZERO parse stats),
// applies segment 1, and completes. The final record must keep run 1's parse
// totals — the resumed run never re-parsed, so overwriting from its result
// would zero them — and sum the apply counters across both runs (the
// checkpoint done-set guarantees the runs processed disjoint segments).
func TestResume_AccumulatesCountersAcrossRuns(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	writeCSV(t, env.importDir, "a.csv")

	run1 := funcRunner{fn: func(_ context.Context, job orchestrator.ImportJob) (orchestrator.ImportResult, error) {
		if e := job.OnPhase(orchestrator.ImportPhaseParseBucket, 0); e != nil {
			return orchestrator.ImportResult{}, e
		}
		if e := job.OnPhase(orchestrator.ImportPhaseApply, 2); e != nil {
			return orchestrator.ImportResult{}, e
		}
		if e := job.OnSegmentApplied(0); e != nil {
			return orchestrator.ImportResult{}, e
		}
		return orchestrator.ImportResult{
			Parse: timestamp.Stats{
				RowsTotal:       10,
				RowsValid:       9,
				RowsRejected:    1,
				RejectsByReason: map[timestamp.RejectReason]uint64{timestamp.ReasonBadURI: 1},
			},
			SegmentsExamined:       1,
			SegmentsPatched:        1,
			RowsMutated:            3,
			RowsMatchedAllVersions: 3,
		}, fmt.Errorf("orchestrator: import: %w", context.Canceled)
	}}
	m1 := env.manager(t, run1)
	id, err := m1.Submit(context.Background(), "a.csv")
	require.NoError(t, err)
	require.NoError(t, m1.Wait(context.Background()))

	paused, err := m1.Status(id)
	require.NoError(t, err)
	require.Equal(t, importer.StateRunning, paused.State)
	require.EqualValues(t, 10, paused.RowsTotal)
	require.Equal(t, 1, paused.SegmentsApplied)

	// "Restart": run 2 resumes with SkipBucket, skips checkpointed segment 0,
	// applies segment 1, completes cleanly with only ITS OWN counters.
	run2 := funcRunner{fn: func(_ context.Context, job orchestrator.ImportJob) (orchestrator.ImportResult, error) {
		if !job.SkipBucket {
			return orchestrator.ImportResult{}, errors.New("resume must skip bucketing")
		}
		if !job.SkipSegment(0) {
			return orchestrator.ImportResult{}, errors.New("segment 0 must be checkpointed")
		}
		if e := job.OnPhase(orchestrator.ImportPhaseApply, 1); e != nil {
			return orchestrator.ImportResult{}, e
		}
		if e := job.OnSegmentApplied(1); e != nil {
			return orchestrator.ImportResult{}, e
		}
		return orchestrator.ImportResult{
			SegmentsExamined:       1,
			SegmentsPatched:        1,
			RowsMutated:            2,
			RowsMatchedAllVersions: 2,
		}, nil
	}}
	m2 := env.manager(t, run2)
	require.NoError(t, m2.ResumeIncomplete(context.Background()))
	rec := waitTerminal(t, m2, id)

	require.Equal(t, importer.StateComplete, rec.State)
	require.EqualValues(t, 10, rec.RowsTotal, "parse totals survive a SkipBucket resume")
	require.EqualValues(t, 9, rec.RowsValid)
	require.EqualValues(t, 1, rec.RowsRejected)
	require.Equal(t, map[string]uint64{"bad_uri": 1}, rec.RejectsByReason)
	require.EqualValues(t, 2, rec.SegmentsExamined, "apply counters sum across runs")
	require.EqualValues(t, 2, rec.SegmentsPatched)
	require.EqualValues(t, 5, rec.RowsMutated, "3 from run 1 + 2 from run 2")
	require.EqualValues(t, 5, rec.RowsMatchedAllVersions)
	require.Equal(t, 2, rec.SegmentsApplied, "applied count is cumulative")
	require.Equal(t, 2, rec.SegmentsToApply, "toApply shows the job total, not the resumed run's remainder")
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
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	require.NoError(t, m1.Wait(waitCtx), "first manager must drain its pause write before restart")

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

// failDuringShutdownRunner returns a genuine (non-cancellation) error only
// after the run context is cancelled, modelling a real failure racing a
// graceful shutdown. The job must be marked failed (terminal), not laundered
// into a resumable pause by the coincidentally-cancelled context.
type failDuringShutdownRunner struct {
	cause error
}

func (r *failDuringShutdownRunner) RunImport(ctx context.Context, _ orchestrator.ImportJob) (orchestrator.ImportResult, error) {
	<-ctx.Done()
	return orchestrator.ImportResult{}, r.cause
}

func TestRun_RealErrorDuringShutdownIsTerminal(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("checkpoint write failed")
	m, importDir, _ := newTestManager(t, &failDuringShutdownRunner{cause: sentinel})
	writeCSV(t, importDir, "a.csv")

	runCtx, cancel := context.WithCancel(context.Background())
	id, err := m.Submit(runCtx, "a.csv")
	require.NoError(t, err)
	cancel()

	rec := waitTerminal(t, m, id)
	require.Equal(t, importer.StateFailed, rec.State,
		"a genuine error racing shutdown must be terminal, not a pause")
	require.Contains(t, rec.Error, "checkpoint write failed")
}

// TestRun_JoinedCancellationAndRealErrorIsTerminal: the orchestrator can
// return errors.Join(context.Canceled, realFailure) — a worker cancelled at
// shutdown joined with a failed manifest refresh. errors.Is matches any leaf,
// so a naive check would pause; the real failure must stay terminal.
func TestRun_JoinedCancellationAndRealErrorIsTerminal(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("manifest refresh failed")
	joined := errors.Join(context.Canceled, sentinel)
	m, importDir, _ := newTestManager(t, &failDuringShutdownRunner{cause: joined})
	writeCSV(t, importDir, "a.csv")

	runCtx, cancel := context.WithCancel(context.Background())
	id, err := m.Submit(runCtx, "a.csv")
	require.NoError(t, err)
	cancel()

	rec := waitTerminal(t, m, id)
	require.Equal(t, importer.StateFailed, rec.State,
		"cancellation joined with a real error must be terminal")
	require.Contains(t, rec.Error, "manifest refresh failed")
}

// TestRun_WrappedCancellationPauses: a %w-wrapped pure cancellation (the
// normal shutdown shape from RunImport) must still classify as a pause.
func TestRun_WrappedCancellationPauses(t *testing.T) {
	t.Parallel()
	wrapped := fmt.Errorf("orchestrator: import: parse/bucket: %w", context.Canceled)
	m, importDir, _ := newTestManager(t, &failDuringShutdownRunner{cause: wrapped})
	writeCSV(t, importDir, "a.csv")

	runCtx, cancel := context.WithCancel(context.Background())
	id, err := m.Submit(runCtx, "a.csv")
	require.NoError(t, err)
	cancel()
	require.NoError(t, m.Wait(context.Background()))

	rec, err := m.Status(id)
	require.NoError(t, err)
	require.Equal(t, importer.StateRunning, rec.State,
		"pure wrapped cancellation stays non-terminal (resumable pause)")
}

// TestSubmit_RejectedWhilePersistedJobAwaitsResume models the startup window
// where the HTTP server is already accepting requests but ResumeIncomplete has
// not yet adopted a persisted non-terminal job: a fresh Manager (m.running ==
// nil) over a store whose import/current points at a resumable job must refuse
// a new submit, or it would overwrite the pointer and orphan the paused job.
func TestSubmit_RejectedWhilePersistedJobAwaitsResume(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	writeCSV(t, env.importDir, "a.csv")

	stopped := make(chan struct{})
	crashRunner := &crashAfterSegmentRunner{segments: []uint64{0}, applyOK: nil, stopped: stopped}
	m1 := env.manager(t, crashRunner)
	runCtx, cancel := context.WithCancel(context.Background())
	id, err := m1.Submit(runCtx, "a.csv")
	require.NoError(t, err)
	<-stopped
	cancel() // paused: persisted record stays non-terminal, import/current set
	require.NoError(t, m1.Wait(context.Background()))

	// "Restarted" manager, resume not yet run: submit must 409.
	m2 := env.manager(t, &recordingRunner{})
	_, err = m2.Submit(context.Background(), "a.csv")
	require.ErrorIs(t, err, importer.ErrJobInProgress)

	// After the paused job resumes and completes, a new submit is accepted.
	require.NoError(t, m2.ResumeIncomplete(context.Background()))
	rec := waitTerminal(t, m2, id)
	require.Equal(t, importer.StateComplete, rec.State)
	_, err = m2.Submit(context.Background(), "a.csv")
	require.NoError(t, err)
}
