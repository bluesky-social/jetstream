package live

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/streaming"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// A resync replaces a whole repo, so its rows go through bulk admission
// (design §10.5); ordinary firehose events stay live.
func TestProcessBatch_ResyncAppendsAreBulk(t *testing.T) {
	t.Parallel()
	db := storagefake.New(storagefake.Config{})
	lease := db.NewLease()
	require.NoError(t, lease.Acquire(t.Context(), time.Hour))
	s := catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: lease.Epoch()})
	_, err := s.InitNamespace(t.Context(), catalog.Main, nil)
	require.NoError(t, err)
	m := ingest.NewMetrics(prometheus.NewRegistry())
	c, err := Open(Config{
		Store:         db.MetaStore(nil),
		SeqKey:        catalog.MainSeqKey,
		CursorKey:     catalog.RelayCursorKey,
		RelayURL:      "https://example.invalid",
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:      newTestVerifier(t),
		WriterMetrics: m,
		Hot:           &ingest.HotConfig{Session: s, BatchMaxAge: time.Hour, BlockMaxAge: time.Hour},
	})
	require.NoError(t, err)

	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{
		{Seq: 5, Identity: &comatproto.SyncSubscribeRepos_Identity{DID: "did:plc:aaa", Time: "2026-05-21T00:00:00Z"}},
		{Resync: streaming.ResyncAsync, Sync: &comatproto.SyncSubscribeRepos_Sync{DID: "did:plc:bbb", Rev: "3l3qo2vutsw2c"}},
		{
			Seq: 7, Resync: streaming.ResyncSyncEvent,
			Sync: &comatproto.SyncSubscribeRepos_Sync{DID: "did:plc:ccc", Rev: "3mmoojp7vgo2g", Time: "2026-05-21T00:00:00Z"},
		},
	}))
	require.NoError(t, c.Close())
	require.Equal(t, 1.0, testutil.ToFloat64(m.HotBatches.WithLabelValues("live", "inline")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.HotBatches.WithLabelValues("bulk", "inline")), "both resyncs' rows share one bulk batch")
}
