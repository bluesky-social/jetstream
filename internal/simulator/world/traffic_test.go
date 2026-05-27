package world

import (
	"bytes"
	"context"
	"log/slog"
	"math/rand/v2"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/fanout"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/repo"
	"github.com/stretchr/testify/require"
)

func newTestWorld(t *testing.T) *World {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Accounts = 25
	cfg.InitialRecords = 1
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

	// At least one op references its CID; verify it exists in the CAR.
	require.True(t, cm.Ops[0].CID.HasVal())
	link := cm.Ops[0].CID.Val()
	cid, err := cbor.ParseCIDString(link.Link)
	require.NoError(t, err)
	_, err = rp.Store.GetBlock(cid)
	require.NoError(t, err)
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

func TestRunTraffic_StopsOnContext(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.NoError(t, w.RunTraffic(ctx, slog.Default()))
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
// happen often, run 500 commits.
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

	for i := range 500 {
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
