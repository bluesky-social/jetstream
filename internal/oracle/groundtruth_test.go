package oracle

import (
	"context"
	"log/slog"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
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
