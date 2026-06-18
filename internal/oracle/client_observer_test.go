package oracle

import (
	"context"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/segment"
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

// collectClientBackfill drives the REAL public jetstream client through the
// full archive-negotiation path (getTombstones -> planBackfill ->
// getSegment/getBlock -> overlay suppression -> cutover to /subscribe-v2),
// the transport real clients actually use (issue #77). The client is an
// OBSERVATION SURFACE ONLY — expected state is still derived independently
// from simulator world + firehose history, never from the client itself.
//
// It drains the client through the full archive path until
// it has observed every event with jetstream seq <= targetSeq, or the deadline
// fires. targetSeq is in jetstream's own seq space (e.g. a sealed compaction
// watermark), the same space the client emits, so the stop is a deterministic
// seq threshold. The live tail never ends on its own; reaching targetSeq is
// the stop condition. Events strictly above targetSeq are dropped so the
// returned slice is a clean (-inf, targetSeq] window for downstream checks
// even though the live tail may race ahead.
func collectClientBackfill(t *testing.T, cfg Config, run *runtimeRun, trace *Trace, baseURL string, targetSeq uint64) []ObservedEvent {
	t.Helper()

	recordTraceOrError(t, trace, "client_backfill_start", map[string]any{"target_seq": targetSeq})

	client, err := jetstream.Subscribe(baseURL,
		jetstream.WithAfterSeq(0), // full archive from the start, then cut over to live
		jetstream.WithBatchSize(64),
	)
	require.NoErrorf(t, err, "client backfill subscribe: mode=%s seed=%d", cfg.Mode, cfg.Seed)
	t.Cleanup(func() { _ = client.Close() })

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

	var (
		out     []ObservedEvent
		maxSeq  uint64
		batches int
	)
	for batch, err := range client.Events(ctx) {
		if err != nil {
			// Recoverable client errors (e.g. a live reconnect) are expected as
			// the tail churns; keep draining until the target is reached.
			continue
		}
		batches++
		for _, ev := range batch.Events() {
			if ev.Seq > maxSeq {
				maxSeq = ev.Seq
			}
			// Keep only the (-inf, targetSeq] window; the live tail may deliver
			// higher seqs but they are outside the asserted envelope.
			if ev.Seq <= targetSeq {
				out = append(out, observedEventFromClient(t, ev))
			}
		}
		if maxSeq >= targetSeq {
			break
		}
		if ctx.Err() != nil {
			break
		}
	}

	recordTraceOrError(t, trace, "client_backfill_done", map[string]any{
		"target_seq":  targetSeq,
		"event_count": len(out),
		"batches":     batches,
		"max_seq":     maxSeq,
	})
	require.GreaterOrEqualf(t, maxSeq, targetSeq,
		"client backfill did not reach target seq before deadline: mode=%s seed=%d target=%d max=%d",
		cfg.Mode, cfg.Seed, targetSeq, maxSeq)
	return out
}

// assertClientBackfillCompacted drives the real client through the full
// archive path up to the compaction watermark and asserts the documented
// compaction contract on what it replayed: no create/update row superseded by
// a tombstone at or below the watermark survives. This is the product-path
// contract #77 requires, observed through the real client (the archive
// negotiation + overlay suppression + cutover) rather than a /subscribe
// whole-archive replay. The watermark is in jetstream seq space, so the drain
// stops deterministically and the asserted window is snapshot-consistent.
func assertClientBackfillCompacted(t *testing.T, cfg Config, run *runtimeRun, trace *Trace, baseURL string, watermark uint64, phase string) {
	t.Helper()

	events := collectClientBackfill(t, cfg, run, trace, baseURL, watermark)
	require.NoErrorf(t, CheckCompacted(events, watermark),
		"%s mode=%s seed=%d: client backfill compacted check failed watermark=%d",
		phase, cfg.Mode, cfg.Seed, watermark)

	t.Logf("%s: client backfill compacted-check passed over %d observed events (watermark=%d) in mode=%s seed=%d",
		phase, len(events), watermark, cfg.Mode, cfg.Seed)
}

// observedEventFromClient adapts a decoded public jetstream.Event into the
// oracle's ObservedEvent. Commit payloads use the byte-exact RecordCBOR the
// client preserves; account/sync rows are re-marshaled so Reconstruct and
// CheckCompacted (which decode the account payload / treat sync as a DID
// tombstone) see the same shape as a direct segment scan.
func observedEventFromClient(t *testing.T, ev jetstream.Event) ObservedEvent {
	t.Helper()
	oe := ObservedEvent{
		Seq:       ev.Seq,
		IndexedAt: ev.TimeUS,
		DID:       ev.DID,
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
