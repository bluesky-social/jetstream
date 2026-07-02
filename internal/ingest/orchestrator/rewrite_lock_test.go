package orchestrator

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

// newLockTestOrchestrator builds a minimal Orchestrator usable for exercising
// withRewriteLock in isolation (no Run, no subsystems).
func newLockTestOrchestrator(t *testing.T) *Orchestrator {
	t.Helper()
	return &Orchestrator{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// TestWithRewriteLock_MutualExclusion is the design §3.3/§6 H anchor: two
// concurrent rewrite passes (a delete-compaction and a timestamp import, here
// stand-in closures) must never execute their segment-mutating bodies at the
// same time. A shared not-atomic counter incremented/decremented inside the
// lock with an overlap detector proves it: if the lock failed, the two
// goroutines would observe inside > 1.
func TestWithRewriteLock_MutualExclusion(t *testing.T) {
	t.Parallel()
	o := newLockTestOrchestrator(t)

	var inside atomic.Int32
	var maxObserved atomic.Int32
	var wg sync.WaitGroup

	body := func() error {
		n := inside.Add(1)
		for { // record the high-water mark of concurrent occupancy
			m := maxObserved.Load()
			if n <= m || maxObserved.CompareAndSwap(m, n) {
				break
			}
		}
		// Hold briefly to widen the race window a missing lock would expose.
		time.Sleep(time.Millisecond)
		inside.Add(-1)
		return nil
	}

	const goroutines = 8
	const iterations = 50
	for range goroutines {
		wg.Go(func() {
			for range iterations {
				require.NoError(t, o.withRewriteLock(body))
			}
		})
	}
	wg.Wait()

	require.EqualValues(t, 1, maxObserved.Load(),
		"withRewriteLock must serialize: at most one body inside at a time")
}

// TestCompactionTempCleanup_ExcludedByRewriteLock pins that a compaction
// pass's stale-*.jss.tmp sweep cannot run while another rewrite holds the
// rewrite lock: an import's in-flight segment.Patch has a live seg_*.jss.tmp
// on disk, and an unlocked sweep would unlink it, making the import's rename
// fail spuriously. The tmp survives while the lock is held and is reclaimed
// once released.
func TestCompactionTempCleanup_ExcludedByRewriteLock(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	segmentsDir := filepath.Join(dataDir, "segments")
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	o := &Orchestrator{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		cfg: Config{
			DataDir:            dataDir,
			Store:              st,
			Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
			CompactionInterval: time.Hour,
		},
	}

	// Simulate an import rewrite in flight: hold the lock with a live tmp.
	liveTmp := filepath.Join(segmentsDir, "seg_0000000000.jss.tmp")
	require.NoError(t, os.WriteFile(liveTmp, []byte("in-flight rewrite"), 0o644))

	locked := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		_ = o.withRewriteLock(func() error {
			close(locked)
			<-release
			return nil
		})
	})
	<-locked

	passDone := make(chan error, 1)
	go func() { passDone <- o.runDeleteCompaction(t.Context(), compactionSteady, nil) }()

	// The pass must be blocked before its cleanup: the tmp stays on disk.
	time.Sleep(20 * time.Millisecond)
	select {
	case err := <-passDone:
		t.Fatalf("compaction pass finished while the rewrite lock was held: %v", err)
	default:
	}
	_, statErr := os.Stat(liveTmp)
	require.NoError(t, statErr, "live rewrite tmp must survive while the lock is held")

	close(release)
	require.NoError(t, <-passDone)
	_, statErr = os.Stat(liveTmp)
	require.True(t, os.IsNotExist(statErr), "stale tmp must be reclaimed once the lock is free")
	wg.Wait()
}

// TestWithRewriteLock_PropagatesError confirms the helper returns the body's
// error unchanged and still releases the lock (a second acquire succeeds).
func TestWithRewriteLock_PropagatesError(t *testing.T) {
	t.Parallel()
	o := newLockTestOrchestrator(t)

	sentinel := io.ErrUnexpectedEOF
	err := o.withRewriteLock(func() error { return sentinel })
	require.ErrorIs(t, err, sentinel)

	// Lock must have been released despite the error.
	done := make(chan struct{})
	go func() {
		_ = o.withRewriteLock(func() error { return nil })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("withRewriteLock did not release the lock after an error")
	}
}
