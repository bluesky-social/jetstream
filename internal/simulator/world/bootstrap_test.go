package world

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/jcalabro/atmos/cbor"
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

func TestBootstrap_ResumeAfterCompletedAccountMatchesUninterruptedBootstrap(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.Accounts = 4
	cfg.InitialRecords = 8
	cfg.Seed = 17
	cfg.DataDir = filepath.Join(t.TempDir(), "full")

	full, err := New(context.Background(), cfg)
	require.NoError(t, err)
	_, err = full.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, full.Bootstrap(context.Background(), slog.Default()))
	defer func() { _ = full.Close() }()

	resumeCfg := cfg
	resumeCfg.DataDir = filepath.Join(t.TempDir(), "resume")
	resumed, err := New(context.Background(), resumeCfg)
	require.NoError(t, err)
	_, err = resumed.EnsureSeed()
	require.NoError(t, err)
	defer func() { _ = resumed.Close() }()

	bootstrapOnlyAccount(t, resumed, cfg, 0)
	require.NoError(t, resumed.Bootstrap(context.Background(), slog.Default()))

	for idx := range cfg.Accounts {
		requireRecordMapsEqual(t, accountRecords(t, full, idx), accountRecords(t, resumed, idx), idx)
	}
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

func bootstrapOnlyAccount(t *testing.T, w *World, cfg Config, idx int) {
	t.Helper()

	accounts := make([]account, cfg.Accounts)
	for i := range cfg.Accounts {
		a, err := deriveAccount(cfg.Seed, i)
		require.NoError(t, err)
		accounts[i] = a
	}

	b := w.db.NewBatch()
	require.NoError(t, w.saveAccount(b, accounts[idx]))
	require.NoError(t, b.Commit(nil))
	require.NoError(t, b.Close())

	counts := initialRecordCounts(cfg)
	r := bootstrapRecordRand(cfg.Seed, idx)
	rp, err := newEmptyRepo(accounts[idx])
	require.NoError(t, err)
	for range counts[idx] {
		coll := chooseCreateCollection(r)
		target := accounts[r.IntN(len(accounts))].DID
		rkey := newRkey(r)
		rec := generateRecord(r, coll, string(target))
		require.NoError(t, rp.Create(coll, rkey, rec))
	}
	_, err = w.commitAndPersist(accounts[idx], rp)
	require.NoError(t, err)
}

func accountRecords(t *testing.T, w *World, idx int) map[string][]byte {
	t.Helper()

	a, err := w.loadAccount(idx)
	require.NoError(t, err)
	rp, err := w.loadRepo(a)
	require.NoError(t, err)

	out := make(map[string][]byte)
	require.NoError(t, rp.Tree.Walk(func(key string, cid cbor.CID) error {
		payload, err := rp.Store.GetBlock(cid)
		if err != nil {
			return err
		}
		out[key] = append([]byte(nil), payload...)
		return nil
	}))
	return out
}

func requireRecordMapsEqual(t *testing.T, want, got map[string][]byte, accountIdx int) {
	t.Helper()

	require.Len(t, got, len(want), "account %d record count diverged", accountIdx)
	for key, wantPayload := range want {
		gotPayload, ok := got[key]
		require.True(t, ok, "account %d missing key %s", accountIdx, key)
		require.True(t, bytes.Equal(wantPayload, gotPayload),
			"account %d payload diverged for key %s: want_len=%d got_len=%d",
			accountIdx, key, len(wantPayload), len(gotPayload))
	}
}
