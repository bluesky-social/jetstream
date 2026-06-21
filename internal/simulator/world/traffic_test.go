package world

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/repo"
	"github.com/stretchr/testify/require"
)

func newTestWorld(t *testing.T) *World {
	t.Helper()
	return newRuntimeWorld(t, 25, 1)
}

func newRuntimeWorld(t *testing.T, accounts, initialRecords int) *World {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Accounts = accounts
	cfg.InitialRecords = initialRecords
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(7, 8)), fanout.New(64)))
	return w
}

func TestGenerateOne_ProducesValidCommit(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t)

	frame, err := w.generateOne(context.Background())
	require.NoError(t, err)

	// Decode #commit body off the wire.
	body, ok := bytes.CutPrefix(frame, frameHeaderCommit)
	require.True(t, ok, "expected #commit header")
	var cm comatproto.SyncSubscribeRepos_Commit
	require.NoError(t, cm.UnmarshalCBOR(body))
	require.Equal(t, int64(1), cm.Seq)
	require.NotEmpty(t, cm.Repo)
	require.NotEmpty(t, cm.Ops)
	require.NotEmpty(t, cm.Blocks)

	// The blocks CAR roundtrips through repo.LoadFromCAR — the new
	// commit + record blocks plus enough MST nodes to root.
	rp, commit, err := repo.LoadFromCAR(bytes.NewReader(cm.Blocks))
	require.NoError(t, err)
	require.Equal(t, cm.Repo, string(rp.DID))
	require.Equal(t, cm.Rev, commit.Rev)
	acct := accountForRepo(t, w, cm.Repo)
	require.NoError(t, commit.VerifySignature(acct.priv.PublicKey()))

	// At least one op references its CID; verify it exists in the CAR.
	require.True(t, cm.Ops[0].CID.HasVal())
	link := cm.Ops[0].CID.Val()
	cid, err := cbor.ParseCIDString(link.Link)
	require.NoError(t, err)
	_, err = rp.Store.GetBlock(cid)
	require.NoError(t, err)
}

func TestGenerateSyncForTest_ProducesValidSyncFrame(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t)

	frame, err := w.GenerateSyncForTest(context.Background(), 0)
	require.NoError(t, err)

	body, ok := bytes.CutPrefix(frame, frameHeaderSync)
	require.True(t, ok, "expected #sync header")
	var syncEvt comatproto.SyncSubscribeRepos_Sync
	require.NoError(t, syncEvt.UnmarshalCBOR(body))
	require.Equal(t, int64(1), syncEvt.Seq)
	require.NotEmpty(t, syncEvt.DID)
	require.NotEmpty(t, syncEvt.Rev)
	require.NotEmpty(t, syncEvt.Blocks)

	_, commit, err := repo.LoadFromCAR(bytes.NewReader(syncEvt.Blocks))
	require.NoError(t, err)
	require.Equal(t, syncEvt.DID, commit.DID)
	require.Equal(t, syncEvt.Rev, commit.Rev)
	acct := accountForRepo(t, w, syncEvt.DID)
	require.NoError(t, commit.VerifySignature(acct.priv.PublicKey()))

	frames, err := w.FirehoseRange(0, 10)
	require.NoError(t, err)
	require.Len(t, frames, 1)
	require.Equal(t, frame, frames[0])
}

func TestGenerateSilentMutationThenSyncForTest_EmitsOnlySyncFrame(t *testing.T) {
	t.Parallel()
	w := newRuntimeWorld(t, 1, 1)

	before, err := w.loadState(0)
	require.NoError(t, err)

	frame, err := w.GenerateSilentMutationThenSyncForTest(context.Background(), 0)
	require.NoError(t, err)

	after, err := w.loadState(0)
	require.NoError(t, err)
	require.Greater(t, after.Rev, before.Rev)
	require.Greater(t, after.RecordCount, before.RecordCount)

	body, ok := bytes.CutPrefix(frame, frameHeaderSync)
	require.True(t, ok, "expected #sync header")
	var syncEvt comatproto.SyncSubscribeRepos_Sync
	require.NoError(t, syncEvt.UnmarshalCBOR(body))
	require.Equal(t, after.Rev, syncEvt.Rev)

	frames, err := w.FirehoseRange(0, 10)
	require.NoError(t, err)
	require.Len(t, frames, 1, "silent mutation must not emit the skipped commit")
	require.Equal(t, frame, frames[0])
}

func TestGenerateSilentMutationThenCommitForTest_EmitsOnlyTriggerCommit(t *testing.T) {
	t.Parallel()
	w := newRuntimeWorld(t, 1, 1)

	before, err := w.loadState(0)
	require.NoError(t, err)

	frame, err := w.GenerateSilentMutationThenCommitForTest(context.Background(), 0)
	require.NoError(t, err)

	after, err := w.loadState(0)
	require.NoError(t, err)
	require.Greater(t, after.Rev, before.Rev)
	require.Greater(t, after.RecordCount, before.RecordCount)

	body, ok := bytes.CutPrefix(frame, frameHeaderCommit)
	require.True(t, ok, "expected #commit header")
	var commitEvt comatproto.SyncSubscribeRepos_Commit
	require.NoError(t, commitEvt.UnmarshalCBOR(body))
	require.Equal(t, after.Rev, commitEvt.Rev)
	require.True(t, commitEvt.PrevData.HasVal(), "trigger commit must point at the skipped state")

	frames, err := w.FirehoseRange(0, 10)
	require.NoError(t, err)
	require.Len(t, frames, 1, "silent mutation must not emit the skipped commit")
	require.Equal(t, frame, frames[0])
}

func accountForRepo(t *testing.T, w *World, did string) account {
	t.Helper()
	for i := range w.cfg.Accounts {
		acct, err := w.loadAccount(i)
		require.NoError(t, err)
		if string(acct.DID) == did {
			return acct
		}
	}
	t.Fatalf("no simulator account for repo %s", did)
	return account{}
}

func TestGenerateOne_AdvancesSeq(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t)
	for i := int64(1); i <= 3; i++ {
		_, err := w.generateOne(context.Background())
		require.NoError(t, err)
		require.Equal(t, i, w.CurrentSeq())
	}
}

func TestGenerateOne_RevsStrictlyIncreaseForHotAccount(t *testing.T) {
	t.Parallel()

	w := newRuntimeWorld(t, 1, 1)
	var prev string
	for range 50 {
		_, err := w.GenerateOneForTest(t.Context())
		require.NoError(t, err)
		state, err := w.loadState(0)
		require.NoError(t, err)
		if prev != "" {
			require.Greater(t, state.Rev, prev)
		}
		prev = state.Rev
	}
}

func TestGenerateOne_RevsAdvancePastBootstrapForEveryAccount(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Accounts = 25
	cfg.InitialRecords = 0
	cfg.InitialRecordsMin = 0
	cfg.InitialRecordsMax = 1000
	cfg.Seed = 42

	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(42^0xfeedf00d, 42^0xc0ffee)), fanout.New(4096)))

	lastRevByRepo := make(map[string]string, cfg.Accounts)
	for i := range cfg.Accounts {
		a, err := w.loadAccount(i)
		require.NoError(t, err)
		state, err := w.loadState(i)
		require.NoError(t, err)
		lastRevByRepo[string(a.DID)] = state.Rev
	}

	for i := range 200 {
		frame, err := w.generateOne(context.Background())
		require.NoError(t, err, "iter=%d", i)

		body, ok := bytes.CutPrefix(frame, frameHeaderCommit)
		require.True(t, ok, "iter=%d: expected #commit header", i)
		var cm comatproto.SyncSubscribeRepos_Commit
		require.NoError(t, cm.UnmarshalCBOR(body))

		require.Greater(t, cm.Rev, lastRevByRepo[cm.Repo], "iter=%d repo=%s", i, cm.Repo)
		lastRevByRepo[cm.Repo] = cm.Rev
	}
}

func TestGenerateOne_DeterministicFramesAcrossRuns(t *testing.T) {
	t.Parallel()

	run := func(t *testing.T) [][]byte {
		t.Helper()
		w := newRuntimeWorld(t, 5, 2)
		var frames [][]byte
		for range 50 {
			frame, err := w.GenerateOneForTest(t.Context())
			require.NoError(t, err)
			frames = append(frames, frame)
		}
		return frames
	}

	require.Equal(t, run(t), run(t))
}

func TestRunTraffic_StopsOnContext(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	require.NoError(t, w.RunTraffic(ctx, slog.Default()))
}

func TestConcurrentGeneratedEventsAreSerialized(t *testing.T) {
	t.Parallel()

	w := newRuntimeWorld(t, 8, 0)
	const workers = 16
	start := make(chan struct{})
	errs := make(chan error, workers)

	for i := range workers {
		i := i
		go func() {
			<-start
			if i%2 == 0 {
				_, _, err := w.GenerateRecordOpForTest(
					context.Background(),
					i%w.cfg.Accounts,
					"create",
					collPost,
					fmt.Sprintf("3kconcurrent%04d", i),
				)
				errs <- err
				return
			}
			_, err := w.GenerateSilentMutationThenSyncForTest(context.Background(), i%w.cfg.Accounts)
			errs <- err
		}()
	}

	close(start)
	for range workers {
		require.NoError(t, <-errs)
	}

	require.Equal(t, int64(workers), w.CurrentSeq())
	frames, err := w.FirehoseRange(0, workers+1)
	require.NoError(t, err)
	require.Len(t, frames, workers)
}

// TestGenerateOne_NoDuplicateOpPaths is a swarm-style regression
// against the duplicate-path bug that produced
//
//	sync: duplicate op path in commit for did:plc:... rev=... path="..."
//
// from atmos's verifier when jetstream attached to the simulator.
// applyOp originally chose update/delete targets via a uniform random
// pick over the repo's MST without excluding paths already mutated in
// the current commit; on a small repo a multi-op commit landed two ops
// on the same path, which atmos rejects (a real PDS collapses
// intra-commit duplicates before publishing).
//
// Configuration is tuned to make duplicates likely without the fix:
// 5 accounts, 1 initial record each, RNG biased so multi-op commits
// happen often, run 200 commits.
func TestGenerateOne_NoDuplicateOpPaths(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Accounts = 5
	cfg.InitialRecords = 1
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(11, 22)), fanout.New(64)))

	for i := range 200 {
		frame, err := w.generateOne(context.Background())
		require.NoError(t, err, "iter=%d", i)

		body, ok := bytes.CutPrefix(frame, frameHeaderCommit)
		require.True(t, ok, "iter=%d: expected #commit header", i)
		var cm comatproto.SyncSubscribeRepos_Commit
		require.NoError(t, cm.UnmarshalCBOR(body))

		seen := make(map[string]struct{}, len(cm.Ops))
		for _, op := range cm.Ops {
			_, dup := seen[op.Path]
			require.Falsef(t, dup,
				"iter=%d: duplicate op path %q in commit for %s rev=%s",
				i, op.Path, cm.Repo, cm.Rev)
			seen[op.Path] = struct{}{}
		}
	}
}
