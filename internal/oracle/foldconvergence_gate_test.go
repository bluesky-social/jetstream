package oracle

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// gateVictimDID is the account whose record the collection-filtered client must
// (after step 3) end up NOT holding.
const gateVictimDID = "did:plc:victim"

// gateCollection is the filtered collection. The victim's record lives here; the
// account-delete that kills it carries an EMPTY collection. The seal index tags
// the marker's block with the reserved $account sentinel collection
// (segment/sentinel.go), which the planner always admits under a collection
// filter, so the marker is selected and downloaded inline.
const gateCollection = "app.bsky.feed.post"

// writeSealedSegment writes events to one sealed segment file at idx, one event
// per block, mirroring the xrpcapi plan-test fixture so the manifest indexes
// per-block collection summaries the way production does.
func writeSealedSegment(t *testing.T, segDir string, idx uint64, events ...segment.Event) {
	t.Helper()
	path := filepath.Join(segDir, ingest.SegmentFilename(idx))
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	for i, ev := range events {
		full, aerr := w.Append(ev)
		require.NoError(t, aerr)
		if full && i < len(events)-1 {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
}

// serveArchive stands up the real read-side archive XRPC server (planBackfill +
// getSegment + getBlock) over a pre-built segments dir, on a real httptest
// socket. It deliberately does NOT use a synctest bubble: the oracle package
// allows exactly one bubble per process (owned by TestOracle_DefaultLifecycle),
// so every other server-driving test runs on real sockets.
//
// No tombstone.Set is wired in, and none is needed: the §R3 gap is closed in
// the archive itself. DID-level markers are indexed under a reserved sentinel
// collection at seal time (segment/sentinel.go), so the planner selects their
// blocks under any collection filter and the markers ride inline through
// getBlock — the same download path record-level deletes already take.
func serveArchive(t *testing.T, segDir string) string {
	t.Helper()
	m, err := manifest.Open(manifest.Options{
		SegmentsDir: segDir,
		Logger:      slog.New(slog.NewTextHandler(testWriter{t: t}, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	require.NoError(t, err)
	srv := xrpcapi.New(xrpcapi.Config{
		Src:    m,
		Logger: slog.New(slog.NewTextHandler(testWriter{t: t}, &slog.HandlerOptions{Level: slog.LevelWarn})),
		Plan: xrpcapi.PlanConfig{
			MaxDIDs:               xrpcapi.DefaultPlanMaxDIDs,
			MaxCollections:        xrpcapi.DefaultPlanMaxCollections,
			MaxEntries:            xrpcapi.DefaultPlanMaxEntries,
			WholeSegmentThreshold: 1,
		},
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}

// TestFoldConvergence_CollectionFilteredDIDTombstoneGap guards the one real gap
// the drop-client-tombstones revision must close (design §R3, issue #174): a
// collection-filtered backfill downloads an in-scope create C and must also
// receive C's DID-level killer D (an account-delete carrying an empty
// collection, sealed below the tip), or a folding consumer keeps C forever — a
// silent violation of the no-data-loss contract (§R1).
//
// Layout (both segments sealed, both below the tip):
//
//	seg 0: create C  (seq 1, did:plc:victim, app.bsky.feed.post) — IN the filter
//	seg 1: account-delete D (seq 2, did:plc:victim, EMPTY collection) — the killer
//
// The gap is closed in the archive itself: seal indexes D's block under the
// reserved $account sentinel collection (segment/sentinel.go), and the planner
// always admits that sentinel under a collection filter. So a client filtered to
// app.bsky.feed.post plans BOTH seg 0 (the real collection) and seg 1 (the
// sentinel), downloads C and D inline, and folds to C-dead — converging with
// ground truth (which matches the killer by DID). No snapshot, no suppression,
// no tombstone.Set on the read path: D rides the same inline download as a
// record-level delete.
//
// A planner that fails to admit the sentinel (or a seal that fails to index it)
// regresses this to the original gap: seg 1 is never selected, the client folds
// to C-live, and CheckFoldConvergence diverges.
func TestFoldConvergence_CollectionFilteredDIDTombstoneGap(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	segDir := filepath.Join(dataDir, "segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	createC := segment.Event{
		Seq:        1,
		IndexedAt:  int64(1_730_000_000_000_000 + 1),
		Kind:       segment.KindCreate,
		DID:        gateVictimDID,
		Collection: gateCollection,
		Rkey:       "rkey",
		Rev:        "rev1",
		Payload:    []byte{0xa0}, // empty DAG-CBOR map; decodes cleanly in map mode
	}
	// D is the DID-level account-delete: empty collection on the wire, but the
	// seal index tags its block with the $account sentinel so a
	// collection-filtered plan still selects it.
	deleteD := segment.Event{
		Seq:       2,
		IndexedAt: int64(1_730_000_000_000_000 + 2),
		Kind:      segment.KindAccount,
		DID:       gateVictimDID,
		Payload:   oracleAccountPayload(t, false, "deleted"),
	}
	writeSealedSegment(t, segDir, 0, createC)
	writeSealedSegment(t, segDir, 1, deleteD)

	baseURL := serveArchive(t, segDir)

	// Independent ground truth: fold the FULL on-disk stream (both segments).
	// Never derived from the filtered client output (§R7).
	full, err := ObserveSegments(dataDir)
	require.NoError(t, err)
	full = EventsSortedBySeq(full)

	// Drive the real collection-filtered client as a one-shot archive dump over
	// (0, tip]. Backfill-only is the deterministic surface: it touches only the
	// archive XRPC endpoints (no live tail), and exercises step 3's inline
	// sentinel path. D is below the tip, so even the live tail would never
	// re-deliver it — backfill-only loses nothing the gap test needs.
	client, err := jetstream.Subscribe(baseURL,
		jetstream.WithCollections([]string{gateCollection}),
		jetstream.WithAfterSeq(0),
		jetstream.WithBeforeSeq(2),
		jetstream.WithBackfillOnly(),
	)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var emitted []ObservedEvent
	for batch, berr := range client.Events(ctx) {
		require.NoError(t, berr, "no recoverable error expected on a clean archive dump")
		for _, ev := range batch.Events() {
			emitted = append(emitted, observedEventFromClient(t, ev))
		}
	}

	// THE GATE: without step 3 the filtered client folds to C-live while ground
	// truth (killer matched by DID) folds to C-dead. With the sentinel index, the
	// account-delete D is selected under the collection filter and delivered
	// inline; the client folds C out and both converge.
	require.NoError(t,
		CheckFoldConvergence(emitted, full, []string{gateCollection}),
		"collection-filtered backfill must converge to ground truth: the DID-level "+
			"account-delete (selected via the $account sentinel) must fold out the "+
			"victim's in-scope record")
}
