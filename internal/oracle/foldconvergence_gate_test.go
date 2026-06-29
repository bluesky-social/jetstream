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
// account-delete that kills it carries an EMPTY collection and so rides in no
// collection block (segment/seal.go only indexes non-empty collections).
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
// so every other server-driving test runs on real sockets. Note there is NO
// tombstone.Set wired in — step 3 adds the DID-tombstone snapshot to
// planBackfill; until then the archive cannot tell a collection-filtered client
// about the empty-collection account-delete.
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

// TestFoldConvergence_CollectionFilteredDIDTombstoneGap is the STEP-3 GATE
// (design §R3/§R4/§R7, issue #174). It reproduces the one real gap the
// drop-client-tombstones revision must close: a collection-filtered backfill
// downloads an in-scope create C but never receives C's DID-level killer D (an
// account-delete carrying an empty collection, sealed below the tip, indexed in
// no collection block), so a folding consumer keeps C forever — a silent
// violation of the no-data-loss contract (§R1).
//
// Layout (both segments sealed, both below the tip):
//
//	seg 0: create C  (seq 1, did:plc:victim, app.bsky.feed.post) — IN the filter
//	seg 1: account-delete D (seq 2, did:plc:victim, EMPTY collection) — the killer
//
// A client filtered to app.bsky.feed.post plans only seg 0 (seg 1's
// empty-collection summary is never selected), downloads C, and ends backfill
// holding C with no killer. CheckFoldConvergence then DIVERGES: ground truth
// (folding the full on-disk stream, killer matched by DID) has C dead, the
// client's filtered fold has C live.
//
// PER REVIEW DECISION (skip-with-step-3-ref): this test FAILS today because the
// gap is real and unclosed. It was run once to capture that failure as the gate
// evidence for step 3, then skipped here so the tree stays green between steps.
// Step 3 (the DID-tombstone start-snapshot piggybacked on planBackfill page 1)
// makes the client suppress C, at which point this test passes and the skip is
// removed.
func TestFoldConvergence_CollectionFilteredDIDTombstoneGap(t *testing.T) {
	t.Skip("gated on step 3 (DID-tombstone start-snapshot); see issue #174. " +
		"Reproduces the §R3 collection-filtered gap and FAILS until #3 lands " +
		"(captured failure: client folds to a record ground truth DELETED — the " +
		"empty-collection account-delete is never delivered to a collection-filtered " +
		"backfill). Step 3's snapshot suppresses C; then remove this skip.")
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
	// D is the DID-level account-delete: empty collection, so it is indexed in
	// no collection block and a collection-filtered plan never selects it.
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
	// archive XRPC endpoints (no live tail), and exercises step 3's
	// runBackfillOnly snapshot path. D is below the tip, so even the live tail
	// would never re-deliver it — backfill-only loses nothing the gap test needs.
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

	// THE GATE: until step 3, the filtered client folds to C-live while ground
	// truth (killer matched by DID) folds to C-dead. After step 3 the
	// start-snapshot suppresses C and both converge to empty.
	require.NoError(t,
		CheckFoldConvergence(emitted, full, []string{gateCollection}),
		"collection-filtered backfill must converge to ground truth: the DID-level "+
			"account-delete must suppress the victim's in-scope record (step 3 snapshot)")
}
