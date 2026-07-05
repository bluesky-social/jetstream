package oracle

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/ingest"
)

func TestAdversarialFilter_FilterExpectedRows(t *testing.T) {
	t.Parallel()
	filter := newAdversarialFilter([]world.AdversarialEntry{
		{Source: world.AdversarialSourceLive, Layer: world.AdversarialLayerGate,
			Reason: "invalid_rkey", Seq: 5, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: ".."},
		{Source: world.AdversarialSourceLive, Layer: world.AdversarialLayerGate,
			Reason: "invalid_rev", Seq: 9, DID: "did:plc:b", WholeEvent: true},
	})

	rows := []EventLogRow{
		{Seq: 5, Kind: "create", DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "3lgoodsibling"},
		{Seq: 5, Kind: "create", DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: ".."},
		{Seq: 9, Kind: "sync", DID: "did:plc:b"},
		{Seq: 9, Kind: "create_resync", DID: "did:plc:b", Collection: "app.bsky.feed.post", Rkey: "3lreplacement"},
		{Seq: 10, Kind: "create", DID: "did:plc:c", Collection: "app.bsky.feed.post", Rkey: "3lhonest"},
	}
	got := filter.FilterExpectedRows(rows)

	require.Len(t, got, 2)
	require.Equal(t, "3lgoodsibling", got[0].Rkey, "sibling of a per-op drop survives")
	require.Equal(t, "3lhonest", got[1].Rkey, "unrelated rows survive")
}

func TestAdversarialFilter_EmptyLedgerIsIdentity(t *testing.T) {
	t.Parallel()
	filter := newAdversarialFilter(nil)
	rows := []EventLogRow{{Seq: 1, Kind: "create", DID: "did:plc:a"}}
	require.Equal(t, rows, filter.FilterExpectedRows(rows))

	m := &Model{Accounts: map[string]RepoSnapshot{
		"did:plc:a": {Records: map[RecordKey]RecordValue{
			{DID: "did:plc:a", Collection: "c.d.e", Rkey: "r"}: {},
		}},
	}}
	filter.FilterGroundTruth(m)
	require.Len(t, m.Accounts["did:plc:a"].Records, 1)
}

func TestAdversarialFilter_FilterGroundTruth(t *testing.T) {
	t.Parallel()
	filter := newAdversarialFilter([]world.AdversarialEntry{
		{Source: world.AdversarialSourceBackfill, Layer: world.AdversarialLayerGate,
			Reason: "invalid_rkey", DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "bad\xff\xfekey"},
	})
	m := &Model{Accounts: map[string]RepoSnapshot{
		"did:plc:a": {Records: map[RecordKey]RecordValue{
			{DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "bad\xff\xfekey"}: {},
			{DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "3lhonest"}:       {},
		}},
	}}
	filter.FilterGroundTruth(m)
	records := m.Accounts["did:plc:a"].Records
	require.Len(t, records, 1)
	_, ok := records[RecordKey{DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "3lhonest"}]
	require.True(t, ok)
}

func TestExpectedDropFloors_GateEntriesOnly(t *testing.T) {
	t.Parallel()
	floors := ExpectedDropFloors([]world.AdversarialEntry{
		{Source: world.AdversarialSourceLive, Layer: world.AdversarialLayerGate, Reason: "invalid_rkey", Seq: 3},
		{Source: world.AdversarialSourceLive, Layer: world.AdversarialLayerGate, Reason: "invalid_rkey", Seq: 4},
		{Source: world.AdversarialSourceLive, Layer: world.AdversarialLayerGate, Reason: "invalid_rev", Seq: 7, WholeEvent: true},
		// Per-op bookkeeping entry sharing the whole-event seq: ground-truth
		// exclusion only, must NOT double-count on the floor.
		{Source: world.AdversarialSourceLive, Layer: world.AdversarialLayerGate, Reason: "invalid_rev", Seq: 7,
			Collection: "app.bsky.feed.post", Rkey: "3lsilent"},
		{Source: world.AdversarialSourceBackfill, Layer: world.AdversarialLayerGate, Reason: "field_too_long"},
		{Source: world.AdversarialSourceLive, Layer: world.AdversarialLayerVerifier, Reason: "non_tid_rev", Seq: 9, WholeEvent: true},
	})
	require.Equal(t, map[string]map[string]int{
		"live":     {"invalid_rkey": 2, "invalid_rev": 1},
		"backfill": {"field_too_long": 1},
	}, floors)
}

func TestScrapeDropCounters_RoundTrip(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	dm := ingest.NewDropMetrics(reg)
	dm.IncDropped(ingest.DropSourceLive, ingest.DropReasonInvalidRkey)
	dm.IncDropped(ingest.DropSourceLive, ingest.DropReasonInvalidRkey)
	dm.IncDropped(ingest.DropSourceBackfill, ingest.DropReasonFieldTooLong)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	got, err := ScrapeDropCounters(t.Context(), srv.Client(), srv.URL)
	require.NoError(t, err)
	require.Equal(t, float64(2), got["live"]["invalid_rkey"])
	require.Equal(t, float64(1), got["backfill"]["field_too_long"])
	// Pre-bound zero series are visible — the anti-vacuity floor
	// comparison can distinguish "zero drops" from "series missing".
	require.Contains(t, got["live"], "invalid_collection")
	require.Equal(t, float64(0), got["live"]["invalid_collection"])
}
