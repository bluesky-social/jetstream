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
