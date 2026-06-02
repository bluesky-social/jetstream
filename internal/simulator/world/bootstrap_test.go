package world

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBootstrap_FirstRunPopulates(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Accounts = 50
	cfg.InitialRecords = 2
	cfg.Seed = 7

	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	wantBootstrap, err := w.EnsureSeed()
	require.NoError(t, err)
	require.True(t, wantBootstrap)

	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))

	// Every account has a state row with 2 records.
	for i := range cfg.Accounts {
		state, err := w.loadState(i)
		require.NoError(t, err)
		require.Equal(t, 2, state.RecordCount, "account %d", i)
	}
}

func TestBootstrap_ReRunDoesNotRebuildCompleteAccountWithLowRecordCount(t *testing.T) {
	t.Parallel()

	// A completed account must never be rebuilt, even when its persisted
	// RecordCount sits below the sampled target. That gap happens for real
	// when two of the sampled record keys collide: newRkey draws random
	// TIDs and repo.Create upserts on an equal MST key, so the stored count
	// can legitimately fall short. The resume guard therefore keys on a
	// defined commit, not RecordCount. Reproducing a real collision is
	// probabilistic, so we instead persist the exact post-collision state
	// (a defined commit with a deliberately low RecordCount) and assert the
	// second Bootstrap leaves that account's commit and rev untouched.
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Accounts = 5
	cfg.InitialRecords = 8
	cfg.Seed = 7

	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))

	const victim = 2
	complete, err := w.loadState(victim)
	require.NoError(t, err)
	require.True(t, complete.CommitCID.Defined())
	require.Equal(t, cfg.InitialRecords, complete.RecordCount)

	// Simulate a record-key collision: same durable commit, fewer records
	// than the target. The old RecordCount-based guard would rebuild this.
	lowCount := complete
	lowCount.RecordCount = cfg.InitialRecords - 1
	require.NoError(t, w.db.Set(keyAccountState(victim), encodeState(lowCount), nil))

	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))

	after, err := w.loadState(victim)
	require.NoError(t, err)
	require.Equal(t, complete.CommitCID, after.CommitCID, "completed account was rebuilt on re-bootstrap")
	require.Equal(t, complete.Rev, after.Rev, "completed account rev churned on re-bootstrap")
	require.Equal(t, lowCount.RecordCount, after.RecordCount, "re-bootstrap rewrote a skipped account's state")
}

func TestBootstrap_DeterministicAcrossRuns(t *testing.T) {
	t.Parallel()
	cfg1 := DefaultConfig()
	cfg1.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg1.Accounts = 10
	cfg1.InitialRecords = 1

	w1, err := New(context.Background(), cfg1)
	require.NoError(t, err)
	_, err = w1.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w1.Bootstrap(context.Background(), slog.Default()))
	a1, _ := w1.loadAccount(0)
	require.NoError(t, w1.Close())

	cfg2 := cfg1
	cfg2.DataDir = filepath.Join(t.TempDir(), "simulator")
	w2, err := New(context.Background(), cfg2)
	require.NoError(t, err)
	defer func() { _ = w2.Close() }()
	_, err = w2.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w2.Bootstrap(context.Background(), slog.Default()))
	a2, _ := w2.loadAccount(0)

	require.Equal(t, a1.DID, a2.DID)
}

func TestBootstrap_ZeroInitialRecordsStillMaterializesRepo(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Accounts = 1
	cfg.InitialRecords = 0
	cfg.InitialRecordsMin = 0
	cfg.InitialRecordsMax = 0

	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))

	_, err = w.loadAccount(0)
	require.NoError(t, err)
	state, err := w.loadState(0)
	require.NoError(t, err)
	require.NotEmpty(t, state.Rev)
	require.True(t, state.CommitCID.Defined())
	require.Zero(t, state.RecordCount)
}

func TestInitialRecordCounts_AreZeroInflatedHeavyTail(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.Accounts = 25
	cfg.InitialRecordsMin = 0
	cfg.InitialRecordsMax = 1000
	counts := initialRecordCounts(cfg)
	require.Len(t, counts, 25)

	var zeros, handful, tail int
	for _, n := range counts {
		require.GreaterOrEqual(t, n, 0)
		require.LessOrEqual(t, n, 1000)
		switch {
		case n == 0:
			zeros++
		case n <= 10:
			handful++
		case n >= 100:
			tail++
		}
	}
	require.Greater(t, zeros, 0, "default oracle needs empty repos")
	require.Greater(t, handful, 0, "default oracle needs small repos")
	require.Greater(t, tail, 0, "default oracle needs large-ish repos")
}
