package oracle

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"testing"

	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
	"github.com/stretchr/testify/require"
)

func TestGroundTruthFromWorldIncludesCurrentRecords(t *testing.T) {
	t.Parallel()

	cfg := world.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Accounts = 3
	cfg.InitialRecords = 2
	w, err := world.New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))

	model, err := GroundTruthFromWorld(w)
	require.NoError(t, err)
	require.Len(t, model.Accounts, 3)
	for did, repo := range model.Accounts {
		require.NotEmpty(t, did)
		require.Len(t, repo.Records, 2)
	}
}

func TestGroundTruthFromWorldIncludesEmptyRepos(t *testing.T) {
	t.Parallel()

	cfg := world.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Accounts = 3
	cfg.InitialRecords = 0
	cfg.InitialRecordsMin = 0
	cfg.InitialRecordsMax = 0
	w, err := world.New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))

	model, err := GroundTruthFromWorld(w)
	require.NoError(t, err)
	require.Len(t, model.Accounts, 3)
	for did, repo := range model.Accounts {
		require.NotEmpty(t, did)
		require.NotNil(t, repo.Records)
		require.Empty(t, repo.Records)
	}
}

func TestGroundTruthFromWorldOmitsDeletedAccounts(t *testing.T) {
	t.Parallel()

	cfg := world.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Accounts = 2
	cfg.InitialRecords = 1
	w, err := world.New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(1, 2)), fanout.New(16)))
	deleted, err := w.LoadAccount(0)
	require.NoError(t, err)
	require.NoError(t, err)
	_, err = w.GenerateAccountDeleteForTest(context.Background(), 0)
	require.NoError(t, err)

	model, err := GroundTruthFromWorld(w)
	require.NoError(t, err)
	require.NotContains(t, model.Accounts, string(deleted.DID))
	require.Len(t, model.Accounts, 1)
}

func TestSnapshotRepoCopiesPayloadBytes(t *testing.T) {
	t.Parallel()

	store := mst.NewMemBlockStore()
	payload := []byte("original")
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, payload)
	require.NoError(t, store.PutBlock(cid, payload))
	tree := mst.NewTree(store)
	require.NoError(t, tree.Insert("app.bsky.feed.post/r1", cid))
	rp := &repo.Repo{
		DID:   atmos.DID("did:plc:a"),
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  tree,
	}

	snap, err := snapshotRepo("did:plc:a", rp)
	require.NoError(t, err)
	payload[0] = 'X'

	key := RecordKey{DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1"}
	require.Equal(t, []byte("original"), snap.Records[key].Payload)
}
