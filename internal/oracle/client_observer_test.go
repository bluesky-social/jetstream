package oracle

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/bsky"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// waitForRuntimePublicURL blocks until the runtime's public listener is bound
// and returns its base URL, or fails on timeout / early runtime exit.
func waitForRuntimePublicURL(t *testing.T, cfg Config, rt *jetstreamd.Runtime, run *runtimeRun) string {
	t.Helper()

	timer := time.NewTimer(oracleWaitTimeout(cfg))
	defer timer.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()

	for {
		if addr := rt.PublicAddr(); addr != "" {
			return "http://" + addr
		}

		select {
		case <-run.exited:
			t.Fatalf("runtime exited before public listener was available: mode=%s seed=%d err=%v",
				cfg.Mode, cfg.Seed, run.err)
		case <-timer.C:
			t.Fatalf("timeout waiting for public listener: mode=%s seed=%d", cfg.Mode, cfg.Seed)
		case <-tick.C:
		}
	}
}

// clientBackfillResult is the outcome of draining the real client through the
// full archive + live path: the complete ordered event stream it emitted, the
// number of recoverable download/live errors it surfaced, and the highest
// jetstream seq seen.
type clientBackfillResult struct {
	events       []ObservedEvent
	downloadErrs int
	maxSeq       uint64
}

// collectClientBackfill drives the REAL public jetstream client through the
// full archive-negotiation path (planBackfill -> getSegment/getBlock -> cutover
// to /subscribe-v2), the transport real clients actually use (issue #77). The
// client is an OBSERVATION SURFACE ONLY — expected state is still derived
// independently from simulator world + firehose history, never from the client
// itself.
//
// It drains the client's full emitted stream — archive AND live tail — until
// the reconstructed final state converges to the independently-derived ground
// truth (converged returns true on a clean Compare), or the deadline fires.
// Draining to convergence is the load-bearing stop condition under the relaxed
// eventually-consistent contract (drop-client-tombstones §R1/§R7): backfill is
// AT-LEAST-ONCE, so the client may emit a create that a later delete supersedes
// (no client-side suppression remains). A per-seq completeness check over any
// fixed window is therefore unsound — transient stale rows are expected. Final
// state, by contrast, is key-based and seq-space-agnostic: Reconstruct folds the
// full emitted stream and it is comparable exactly when the client has caught up
// to the quiescent world, which is what convergence detects. This Reconstruct +
// Compare-to-convergence over the UNFILTERED stream IS the fold-convergence
// invariant (CheckFoldConvergence) for the no-collection-filter query.
//
// The live tail never ends on its own; convergence (or the deadline) is the
// stop condition. Recoverable client errors are COUNTED (not silently
// swallowed) so the caller can assert them against the run's fault budget; a
// client-path error is never expected on a no-fault run.
func collectClientBackfill(t *testing.T, cfg Config, run *runtimeRun, trace *Trace, obsClient *http.Client, baseURL string, targetSeq uint64, converged func(events []ObservedEvent) bool) clientBackfillResult {
	t.Helper()

	recordTraceOrError(t, trace, "client_backfill_start", map[string]any{"target_seq": targetSeq})

	client, err := jetstream.Subscribe(baseURL,
		jetstream.WithHTTPClient(obsClient),
		jetstream.WithAfterSeq(0), // full archive from the start, then cut over to live
		jetstream.WithBatchSize(64),
	)
	require.NoErrorf(t, err, "client backfill subscribe: mode=%s seed=%d", cfg.Mode, cfg.Seed)
	// Close inside the helper (not t.Cleanup): under testing/synctest the bubble
	// fn must return with no live goroutines, but t.Cleanup runs AFTER it
	// returns. The drain below completes before we close, so this is also
	// correct on the real-socket path.
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), oracleWaitTimeout(cfg))
	defer cancel()

	// Stop the runtime-exit watcher from leaking: cancel when the runtime dies.
	go func() {
		select {
		case <-run.exited:
			cancel()
		case <-ctx.Done():
		}
	}()

	res := clientBackfillResult{}
	var batches int
	for batch, err := range client.Events(ctx) {
		if err != nil {
			// Recoverable client errors (e.g. a live reconnect) surface here.
			// Count them rather than swallowing: a silent `continue` let a run
			// where every getSegment/getBlock errored satisfy a coarse
			// high-water guard with an incomplete window (issue #102). The
			// caller asserts this count against the fault budget.
			res.downloadErrs++
			recordTraceOrError(t, trace, "client_backfill_error", map[string]any{
				"err":     err.Error(),
				"max_seq": res.maxSeq,
			})
			continue
		}
		batches++
		for _, ev := range batch.Events() {
			if ev.Seq > res.maxSeq {
				res.maxSeq = ev.Seq
			}
			res.events = append(res.events, observedEventFromClient(t, ev))
		}
		// Converge only once the client has at least reached the sealed
		// watermark; before that the archive is still streaming and a transient
		// empty-diff match is impossible anyway. converged runs a full Compare,
		// so gate it on maxSeq to avoid paying that cost on every early batch.
		if res.maxSeq >= targetSeq && converged(res.events) {
			break
		}
		if ctx.Err() != nil {
			break
		}
	}

	recordTraceOrError(t, trace, "client_backfill_done", map[string]any{
		"target_seq":    targetSeq,
		"event_count":   len(res.events),
		"batches":       batches,
		"download_errs": res.downloadErrs,
		"max_seq":       res.maxSeq,
	})
	require.GreaterOrEqualf(t, res.maxSeq, targetSeq,
		"client backfill did not reach target seq before deadline: mode=%s seed=%d target=%d max=%d",
		cfg.Mode, cfg.Seed, targetSeq, res.maxSeq)
	return res
}

// clientEventsAtOrBelow returns the subset of the client's emitted stream with
// jetstream seq <= watermark, sorted by seq. This is the (-inf, watermark]
// window the compaction contract (CheckCompacted) is asserted over.
func clientEventsAtOrBelow(events []ObservedEvent, watermark uint64) []ObservedEvent {
	out := make([]ObservedEvent, 0, len(events))
	for _, ev := range events {
		if ev.Seq <= watermark {
			out = append(out, ev)
		}
	}
	return EventsSortedBySeq(out)
}

// assertClientBackfillCompacted drives the real client through the full
// archive + live path and asserts the product contract on what it replayed,
// through three independent checks (issue #102):
//
//  1. FINAL STATE: Reconstruct the client's complete emitted stream and
//     Compare it to GroundTruthFromWorld — the authoritative final state
//     derived independently from the simulator MST. This is the load-bearing
//     correctness check: it is key-based (DID/collection/rkey/payload) and so
//     seq-space-agnostic, and it catches a client that drops records, skips
//     DIDs/collections, serves a stale payload, or emits an extra row. The
//     drain runs to CONVERGENCE on this Compare (see collectClientBackfill):
//     the world is quiescent at this point in the lifecycle, so a correct
//     client converges and a defective one never does (fails at the deadline
//     with the precise Compare mismatch).
//
//  2. COMPACTION CONTRACT: CheckCompacted over the client's (-inf, watermark]
//     window — no create/update row superseded by a tombstone at or below the
//     watermark survives. On failure, #94's disk-vs-serving bisection
//     classifies it as a durable defect vs. a serving/client artifact.
//
//  3. ERROR BUDGET: the client must not silently lose the archive behind
//     recoverable download/live errors. On a no-fault run zero are tolerated;
//     the swarm faults target the upstream relay->jetstream path, not the
//     client->jetstream path, so the client should see none even under swarm.
func assertClientBackfillCompacted(t *testing.T, cfg Config, run *runtimeRun, trace *Trace, obsClient *http.Client, dataDir string, w *world.World, compaction *compactionPassRecorder, baseURL string, watermark uint64, phase string) {
	t.Helper()

	ground, err := GroundTruthFromWorld(w)
	require.NoErrorf(t, err, "%s mode=%s seed=%d: build ground truth for client backfill", phase, cfg.Mode, cfg.Seed)

	converged := func(events []ObservedEvent) bool {
		got, rerr := Reconstruct(EventsSortedBySeq(events))
		if rerr != nil {
			return false
		}
		return Compare(ground, got) == nil
	}

	res := collectClientBackfill(t, cfg, run, trace, obsClient, baseURL, watermark, converged)

	// 1. Final-state comparison. If the drain converged this passes; if it
	// timed out (a genuinely dropped/stale/extra record) this surfaces the
	// precise mismatch instead of a bare high-water timeout.
	got, err := Reconstruct(EventsSortedBySeq(res.events))
	require.NoErrorf(t, err, "%s mode=%s seed=%d: reconstruct client stream", phase, cfg.Mode, cfg.Seed)
	require.NoErrorf(t, Compare(ground, got),
		"%s mode=%s seed=%d: client stream final state does not match simulator ground truth (events=%d max_seq=%d)",
		phase, cfg.Mode, cfg.Seed, len(res.events), res.maxSeq)

	// 2. Compaction contract over the (-inf, watermark] window.
	window := clientEventsAtOrBelow(res.events, watermark)
	if clientErr := CheckCompacted(window, watermark); clientErr != nil {
		// Bisect the failure into a durable-defect vs. serving/client-artifact
		// verdict (#94) before failing, so a client-path bug isn't mistaken for
		// a storage defect (or vice versa). bisectServedCompactedFailure is
		// surface-agnostic: it re-runs CheckCompacted against the on-disk
		// segments at the same watermark to classify the failure.
		bisectServedCompactedFailure(t, trace, dataDir, cfg, compaction, watermark, clientErr)
		return
	}

	// 3. Error budget. A recoverable error means the client retried/reconnected;
	// the upstream-only swarm faults never touch the client transport, so on
	// any run mode the client tail should complete clean.
	require.Zerof(t, res.downloadErrs,
		"%s mode=%s seed=%d: client surfaced %d recoverable download/live errors; the archive may be incomplete behind them",
		phase, cfg.Mode, cfg.Seed, res.downloadErrs)

	t.Logf("%s: client backfill matched ground truth over %d emitted events (window=%d watermark=%d) in mode=%s seed=%d",
		phase, len(res.events), len(window), watermark, cfg.Mode, cfg.Seed)
}

// assertTypedLikeBackfill drives the REAL public client through the typed fast
// path (jetstream.TypedEvents[bsky.FeedLike] over WithRawRecords) against the
// running server, decoding every app.bsky.feed.like create on the parallel
// decode workers. It is the end-to-end guard for #146's worker-parallel typed
// decode: it asserts the typed path decodes likes with ZERO decode errors, that
// at least one like decoded, that every decoded like carries the well-formed
// subject strongref the simulator generated, and — the correctness crux — that
// the SET of (DID,rkey) likes the typed path surfaces equals what the map path
// observes from the same server over the same (0, beforeSeq] range. It is run
// as a bounded backfill-only dump (WithBeforeSeq+WithBackfillOnly) so it
// terminates. This helper does NOT assert full watermark coverage: maxSeq
// tracks only like-collection events, which need not reach the global
// beforeSeq watermark; archive-tail completeness against an independent ground
// truth is owned by assertClientBackfillCompacted, run over the same range just
// before this. The map-vs-typed set equality is a differential check (it would
// not catch a truncation that hits both paths identically); its job is to prove
// the two decode paths agree, not to prove coverage.
func assertTypedLikeBackfill(t *testing.T, cfg Config, run *runtimeRun, obsClient *http.Client, baseURL string, beforeSeq uint64) {
	t.Helper()

	client, err := jetstream.Subscribe(baseURL,
		jetstream.WithHTTPClient(obsClient),
		jetstream.WithCollections([]string{"app.bsky.feed.like"}),
		jetstream.WithAfterSeq(0),
		jetstream.WithBeforeSeq(beforeSeq),
		jetstream.WithBackfillOnly(),
		jetstream.WithRawRecords(),
	)
	require.NoErrorf(t, err, "typed subscribe: mode=%s seed=%d", cfg.Mode, cfg.Seed)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), oracleWaitTimeout(cfg))
	defer cancel()
	go func() {
		select {
		case <-run.exited:
			cancel()
		case <-ctx.Done():
		}
	}()

	typedLikes := map[string]string{} // did/rkey -> subject URI (decoded via the typed path)
	var decoded, maxSeq uint64
	for tb, err := range jetstream.TypedEvents[bsky.FeedLike](ctx, client, "app.bsky.feed.like") {
		require.NoErrorf(t, err, "typed backfill stream: mode=%s seed=%d", cfg.Mode, cfg.Seed)
		for _, te := range tb.Events() {
			if te.Event.Seq > maxSeq {
				maxSeq = te.Event.Seq
			}
			// Per-event decode-error guard: this is the load-bearing zero-error
			// assertion (it fails the test on the first decode error). We do NOT
			// keep a redundant aggregate counter — it would be zero by
			// construction here and read as a backstop that can never fire.
			require.NoErrorf(t, te.DecodeErr, "typed like decode error seq=%d", te.Event.Seq)
			if te.Record == nil {
				// Only like creates decode; deletes (no record) pass through.
				continue
			}
			decoded++
			require.NotEmpty(t, te.Record.Subject.URI, "decoded like must carry its subject URI (worker-parallel typed decode)")
			typedLikes[te.Event.DID+"/"+te.Event.Commit.Rkey] = te.Record.Subject.URI
		}
	}
	require.Positivef(t, decoded, "typed backfill decoded no likes: mode=%s seed=%d", cfg.Mode, cfg.Seed)

	// Cross-check against the MAP path over the same bounded range: the set of
	// surviving like creates must match exactly, proving the worker-parallel
	// typed decode neither drops, duplicates, nor mis-decodes vs. the default.
	mapClient, err := jetstream.Subscribe(baseURL,
		jetstream.WithHTTPClient(obsClient),
		jetstream.WithCollections([]string{"app.bsky.feed.like"}),
		jetstream.WithAfterSeq(0),
		jetstream.WithBeforeSeq(beforeSeq),
		jetstream.WithBackfillOnly(),
	)
	require.NoError(t, err)
	defer func() { _ = mapClient.Close() }()
	mapLikes := map[string]string{}
	for batch, err := range mapClient.Events(ctx) {
		require.NoError(t, err)
		for _, ev := range batch.Events() {
			if ev.Kind != jetstream.KindCommit || ev.Commit == nil || ev.Commit.Operation == jetstream.OpDelete {
				continue
			}
			subj, _ := ev.Commit.Record["subject"].(map[string]any)
			uri, _ := subj["uri"].(string)
			mapLikes[ev.DID+"/"+ev.Commit.Rkey] = uri
		}
	}
	require.Equal(t, mapLikes, typedLikes,
		"typed fast path must surface exactly the same like set (DID/rkey -> subject URI) as the map path; mode=%s seed=%d", cfg.Mode, cfg.Seed)
	t.Logf("typed-like-backfill: decoded %d likes via worker-parallel typed path, matched map path exactly (max_seq=%d) mode=%s seed=%d",
		decoded, maxSeq, cfg.Mode, cfg.Seed)
}

// observedEventFromClient adapts a decoded public jetstream.Event into the
// oracle's ObservedEvent. Commit payloads use the byte-exact RecordCBOR the
// client preserves; account/sync rows are re-marshaled so Reconstruct and
// CheckCompacted (which decode the account payload / treat sync as a DID
// tombstone) see the same shape as a direct segment scan.
func observedEventFromClient(t *testing.T, ev jetstream.Event) ObservedEvent {
	t.Helper()
	oe := ObservedEvent{
		Seq:         ev.Seq,
		WitnessedAt: ev.TimeUS,
		DID:         ev.DID,
	}
	switch ev.Kind {
	case jetstream.KindCommit:
		require.NotNilf(t, ev.Commit, "commit event missing commit payload seq=%d", ev.Seq)
		oe.Collection = ev.Commit.Collection
		oe.Rkey = ev.Commit.Rkey
		oe.Rev = ev.Commit.Rev
		switch ev.Commit.Operation {
		case jetstream.OpCreate:
			oe.Kind = segment.KindCreate
			oe.Payload = ev.Commit.RecordCBOR
		case jetstream.OpUpdate:
			oe.Kind = segment.KindUpdate
			oe.Payload = ev.Commit.RecordCBOR
		case jetstream.OpDelete:
			oe.Kind = segment.KindDelete
		default:
			t.Fatalf("unknown client commit operation %q seq=%d", ev.Commit.Operation, ev.Seq)
		}
	case jetstream.KindIdentity:
		oe.Kind = segment.KindIdentity
	case jetstream.KindAccount:
		require.NotNilf(t, ev.Account, "account event missing account payload seq=%d", ev.Seq)
		oe.Kind = segment.KindAccount
		acc := &comatproto.SyncSubscribeRepos_Account{
			DID:    ev.Account.DID,
			Active: ev.Account.Active,
			Seq:    ev.Account.Seq,
			Time:   ev.Account.Time,
		}
		if ev.Account.Status != "" {
			acc.Status = gt.Some(ev.Account.Status)
		}
		payload, err := acc.MarshalCBOR()
		require.NoError(t, err)
		oe.Payload = payload
	case jetstream.KindSync:
		require.NotNilf(t, ev.Sync, "sync event missing sync payload seq=%d", ev.Seq)
		oe.Kind = segment.KindSync
		oe.Rev = ev.Sync.Rev
	default:
		t.Fatalf("unknown client event kind %q seq=%d", ev.Kind, ev.Seq)
	}
	return oe
}
