package oracle

import (
	"context"
	"testing"
	"time"

	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/cbor"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// frame_fault_test.go is the #206 oracle tier: frame-level wire adversity
// on the subscribeRepos connection. The relay is treated as untrusted
// input; each scenario injects one class of poison frame through the
// simulator's SubscribeReposInjectFault and asserts the full archive
// contract end-to-end through the REAL consumer:
//
//   - garbage CBOR       → decode_errors, same-conn continue, zero loss;
//   - unknown frame type → unknown_events, seq-carrying unknowns suppress
//     the spurious gap, loss self-heals via chain-break resync;
//   - op=-1 error frame  → stream_error_frames_total{code}, zero loss;
//   - swallowed frame    → a REAL sequence gap end-to-end, loss bounded
//     to exactly the swallowed frame and self-healed by resync;
//   - oversized frame    → client read-limit trips, reconnect, the #205
//     replay guards keep redelivery at zero loss AND zero bloat.
//
// Every scenario ends with the final-state convergence fold, so a poison
// frame can never silently corrupt the archive.

// oracleWireFrame builds the CBOR frame format atmos expects on the
// wire: header {op:1, t:<typ>} concatenated with the body CBOR.
func oracleWireFrame(typ string, body []byte) []byte {
	hdr := cbor.AppendMapHeader(nil, 2)
	hdr = append(hdr, cbor.AppendTextKey(nil, "op")...)
	hdr = cbor.AppendInt(hdr, 1)
	hdr = append(hdr, cbor.AppendTextKey(nil, "t")...)
	hdr = cbor.AppendText(hdr, typ)
	return append(hdr, body...)
}

// oracleUnknownFrame builds a well-formed frame of a type this build
// does not recognize, carrying a top-level seq the way every sequenced
// lexicon message does — the wire shape of a relay speaking a newer
// protocol.
func oracleUnknownFrame(seq int64) []byte {
	body := cbor.AppendMapHeader(nil, 2)
	body = append(body, cbor.AppendTextKey(nil, "seq")...)
	body = cbor.AppendInt(body, seq)
	body = append(body, cbor.AppendTextKey(nil, "did")...)
	body = cbor.AppendText(body, "did:plc:futureproto")
	return oracleWireFrame("#futureThing", body)
}

// oracleErrorFrame builds an op=-1 server error frame.
func oracleErrorFrame(code, message string) []byte {
	hdr := cbor.AppendMapHeader(nil, 1)
	hdr = append(hdr, cbor.AppendTextKey(nil, "op")...)
	hdr = cbor.AppendInt(hdr, -1)
	body := cbor.AppendMapHeader(nil, 2)
	body = append(body, cbor.AppendTextKey(nil, "error")...)
	body = cbor.AppendText(body, code)
	body = append(body, cbor.AppendTextKey(nil, "message")...)
	body = cbor.AppendText(body, message)
	return append(hdr, body...)
}

// recorderHasResyncRow reports whether the durable stream contains a
// create_resync row for (did, rkey) — the self-heal barrier for the
// swallow scenarios. Resync rows ride atmos's synthetic async-resync
// event, which carries no upstream seq (UpstreamRelayCursor=0), so the
// cursor-window waits can't see them.
func recorderHasResyncRow(rec *eventLogRecorder, did, rkey string) bool {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, ev := range rec.events {
		if ev.Kind == segment.KindCreateResync && ev.DID == did && ev.Rkey == rkey {
			return true
		}
	}
	return false
}

// runZeroLossFrameFault drives the shared live-tail harness through the
// zero-loss inject scenarios: 3 pre-generated commits, the scheduled
// inject fault(s), a post-fire barrier commit, and an EXACT multiset
// compare — the injected frames must be pure wire noise, with every real
// row archived exactly once.
func runZeroLossFrameFault(
	t *testing.T,
	schedule []simhttp.SubscribeReposInjectFault,
	waitReady func(h *liveTailHarness),
) ([]ObservedEvent, []EventLogRow, []EventLogRow, *liveTailHarness) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	h := newLiveTailHarness(t, ctx)
	for range 3 {
		_, err := h.World.GenerateOneForTest(ctx)
		require.NoError(t, err)
	}
	h.Faults.SetSubscribeReposInjectSchedule(schedule)

	h.StartConsumer(t, ctx)
	waitReady(h)

	// Post-fault barrier: one more commit and a durable-row wait prove
	// the consumer processed everything that preceded it on the ordered
	// stream (or, for reconnect scenarios, on the healthy connection).
	_, err := h.World.GenerateOneForTest(ctx)
	require.NoError(t, err)
	finalTip := h.World.CurrentSeq()
	expected, err := ExpectedEventLogFromFirehose(h.World, 0, int(finalTip))
	require.NoError(t, err)
	require.True(t, h.Recorder.waitForRowCount(ctx, 0, finalTip, len(expected)),
		"timed out waiting for %d expected durable rows", len(expected))

	h.StopConsumer(t)

	events := h.ObservedEvents(t)
	observed := h.Recorder.RowsByUpstreamCursor(0, finalTip)
	require.NoError(t, CompareEventLogMultiset(expected, observed),
		"durable rows must match the expected once-per-frame event log exactly (injected frames are pure wire noise: no loss, no bloat)")
	assertLiveTailConverged(t, h, events,
		"final state must converge to world ground truth under frame injection")
	return events, observed, expected, h
}

// TestOracle_FrameGarbageInject pins the malformed-CBOR contract: a
// garbage frame between real ones lands on decode_errors_total, the
// connection survives (no reconnect), no spurious gap is reported, and
// the archive is exactly the real stream.
func TestOracle_FrameGarbageInject(t *testing.T) {
	t.Parallel()
	_, _, _, h := runZeroLossFrameFault(t,
		[]simhttp.SubscribeReposInjectFault{{AfterFrames: 2, Frame: []byte{0xff, 0xfe, 0xfd}}},
		func(h *liveTailHarness) {
			require.Eventually(t, func() bool {
				return h.Faults.SubscribeReposInjectsFired() == 1
			}, 30*time.Second, 10*time.Millisecond, "inject fault never fired")
		})

	require.InDelta(t, 1.0, testutil.ToFloat64(h.Metrics.DecodeErrors), 0,
		"the garbage frame must land on decode_errors_total")
	require.Zero(t, testutil.ToFloat64(h.Metrics.SequenceGaps),
		"a decode error must NOT be misattributed as relay data loss")
	require.Zero(t, testutil.ToFloat64(h.Metrics.UnknownEvents))
	require.Equal(t, 1, h.Faults.SubscribeReposConnections(),
		"a garbage frame is advisory: the consumer must stay on the same connection")
}

// TestOracle_FrameErrorFrameInject pins the op=-1 contract: an injected
// FutureCursor error frame lands on stream_error_frames_total{code},
// costs nothing (the relay here keeps the connection open, and the
// consumer must not treat the frame itself as fatal or as data loss).
func TestOracle_FrameErrorFrameInject(t *testing.T) {
	t.Parallel()
	_, _, _, h := runZeroLossFrameFault(t,
		[]simhttp.SubscribeReposInjectFault{{AfterFrames: 2, Frame: oracleErrorFrame("FutureCursor", "cursor beyond relay head")}},
		func(h *liveTailHarness) {
			require.Eventually(t, func() bool {
				return h.Faults.SubscribeReposInjectsFired() == 1
			}, 30*time.Second, 10*time.Millisecond, "inject fault never fired")
		})

	require.InDelta(t, 1.0,
		testutil.ToFloat64(h.Metrics.StreamErrorFrames.WithLabelValues("FutureCursor")), 0,
		"the op=-1 frame must land on stream_error_frames_total{code=FutureCursor}")
	require.Zero(t, testutil.ToFloat64(h.Metrics.DecodeErrors),
		"a well-formed error frame is not a decode error")
	require.Zero(t, testutil.ToFloat64(h.Metrics.SequenceGaps))
	require.Equal(t, 1, h.Faults.SubscribeReposConnections())
}

// TestOracle_FrameOversizedReconnects pins the oversized-frame contract:
// frames above atmos's 2 MiB read limit kill the connection (twice — the
// schedule arms two successive connections, modeling a persistently
// misbehaving relay), atmos reconnects from the watermark cursor, and
// the #205 replay guards keep the redelivered window at zero loss AND
// zero bloat.
func TestOracle_FrameOversizedReconnects(t *testing.T) {
	t.Parallel()
	oversized := make([]byte, 3<<20) // 3 MiB > atmos's 2 MiB read limit
	_, _, _, h := runZeroLossFrameFault(t,
		[]simhttp.SubscribeReposInjectFault{
			{AfterFrames: 2, Frame: oversized},
			{AfterFrames: 1, Frame: oversized},
		},
		func(h *liveTailHarness) {
			// Two poisoned connections then a healthy third: barrier on the
			// third accept so the barrier commit rides a clean connection.
			require.Eventually(t, func() bool {
				return h.Faults.SubscribeReposConnections() >= 3
			}, 30*time.Second, 10*time.Millisecond, "consumer never reached a healthy third connection")
		})

	require.Equal(t, 2, h.Faults.SubscribeReposInjectsFired(),
		"both scheduled oversized injects must fire")
	require.GreaterOrEqual(t, testutil.ToFloat64(h.Metrics.Reconnects), 2.0,
		"each oversized frame must cost one reconnect")
	require.Zero(t, testutil.ToFloat64(h.Metrics.DecodeErrors),
		"the read-limit trip is a connection error, not a frame decode error")
}

// runSwallowHealScenario drives the shape where the injected bytes
// positionally REPLACE a real frame (SwallowNext): the wire loses
// account-0's "swallowed1" commit, the next account-0 commit chain-breaks
// against the verifier's stored state, and the resulting resync re-fetches
// the authoritative repo — recovering the swallowed record. Traffic is
// fully targeted so authorship (and therefore the chain break) is
// deterministic:
//
//	seq 1  acct0 create base1        (delivered)
//	seq 2  acct1 create other1       (delivered; fault fires after this)
//	seq 3  acct0 create swallowed1   (REPLACED by injected bytes)
//	seq 4  acct0 create heal1        (chain break -> resync self-heal)
func runSwallowHealScenario(t *testing.T, inject []byte) *liveTailHarness {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	h := newLiveTailHarness(t, ctx)
	w := h.World
	_, _, err := w.GenerateRecordOpForTest(ctx, 0, "create", "app.bsky.feed.post", "base1")
	require.NoError(t, err)
	_, _, err = w.GenerateRecordOpForTest(ctx, 1, "create", "app.bsky.feed.post", "other1")
	require.NoError(t, err)
	_, _, err = w.GenerateRecordOpForTest(ctx, 0, "create", "app.bsky.feed.post", "swallowed1")
	require.NoError(t, err)
	_, _, err = w.GenerateRecordOpForTest(ctx, 0, "create", "app.bsky.feed.post", "heal1")
	require.NoError(t, err)

	h.Faults.SetSubscribeReposInjectSchedule([]simhttp.SubscribeReposInjectFault{
		{AfterFrames: 2, Frame: inject, SwallowNext: true},
	})

	acct0, err := w.LoadAccount(0)
	require.NoError(t, err)
	did0 := string(acct0.DID)

	h.StartConsumer(t, ctx)

	// The self-heal barrier: the chain-break resync's create_resync row
	// for the swallowed record proves the whole cascade ran — swallow,
	// chain break on seq 4, getRepo against the (wire-fault-unaware)
	// simulator PDS, durable append.
	require.Eventually(t, func() bool {
		return recorderHasResyncRow(h.Recorder, did0, "swallowed1")
	}, 30*time.Second, 10*time.Millisecond,
		"resync never recovered the swallowed record (chain-break self-heal broken)")

	h.StopConsumer(t)

	// Anti-vacuity: the fault consumed a real frame.
	require.Equal(t, 1, h.Faults.SubscribeReposInjectsFired())
	require.Equal(t, 1, h.Faults.SubscribeReposSwallowedFrames())

	// Loss is bounded to EXACTLY the swallowed window: the delivered
	// prefix (seqs 1-2) archives verbatim...
	expectedPrefix, err := ExpectedEventLogFromFirehose(w, 0, 2)
	require.NoError(t, err)
	require.NoError(t, CompareEventLogMultiset(expectedPrefix, h.Recorder.RowsByUpstreamCursor(0, 2)),
		"the pre-swallow prefix must archive exactly")
	// ...and nothing in (2, 4] archives under its own upstream seq: seq 3
	// never reached the wire, and seq 4's ops were consumed as the
	// chain-break trigger (its records return via the resync rows, which
	// carry no upstream seq).
	require.Empty(t, h.Recorder.RowsByUpstreamCursor(2, 4),
		"the swallowed window must not archive under its own upstream seqs")

	// Final state: the resync recovered BOTH the swallowed record and the
	// trigger commit's record from the authoritative repo, so the fold
	// converges to world truth despite real wire loss.
	assertLiveTailConverged(t, h, h.ObservedEvents(t),
		"final state must self-heal to world ground truth after a swallowed frame")
	return h
}

// TestOracle_FrameSwallowGapSelfHeals pins the pure wire-loss contract:
// the swallowed frame is replaced by garbage, so the consumer sees BOTH
// a decode error (the garbage) and — because the garbage carries no seq —
// a REAL sequence gap when seq 4 follows seq 2. This is the first
// end-to-end GapError through the simulator relay: gap accounting must
// report exactly the one-seq hole, and the archive must self-heal.
func TestOracle_FrameSwallowGapSelfHeals(t *testing.T) {
	t.Parallel()
	h := runSwallowHealScenario(t, []byte{0xde, 0xad, 0xbe, 0xef})

	require.InDelta(t, 1.0, testutil.ToFloat64(h.Metrics.DecodeErrors), 0,
		"the garbage replacement must land on decode_errors_total")
	require.InDelta(t, 1.0, testutil.ToFloat64(h.Metrics.SequenceGaps), 0,
		"a genuinely absent seq must surface as exactly one sequence gap")
	require.InDelta(t, 1.0, testutil.ToFloat64(h.Metrics.SequenceGapMissedSeqs), 0,
		"the gap is exactly one seq wide (the swallowed frame)")
	require.Zero(t, testutil.ToFloat64(h.Metrics.UnknownEvents))
}

// TestOracle_FrameUnknownTypeReplacesFrame pins the forward-compat
// contract: a well-formed frame of an unrecognized type occupying a real
// seq slot lands on unknown_events_total, and its body seq suppresses
// the spurious gap (the frame ARRIVED — we just can't represent it; that
// is not relay data loss). The record that seq carried still self-heals
// via the chain-break resync.
func TestOracle_FrameUnknownTypeReplacesFrame(t *testing.T) {
	t.Parallel()
	h := runSwallowHealScenario(t, oracleUnknownFrame(3))

	require.InDelta(t, 1.0, testutil.ToFloat64(h.Metrics.UnknownEvents), 0,
		"the unknown frame type must land on unknown_events_total")
	require.Zero(t, testutil.ToFloat64(h.Metrics.SequenceGaps),
		"the unknown frame's body seq must suppress the spurious gap")
	require.Zero(t, testutil.ToFloat64(h.Metrics.DecodeErrors),
		"a well-formed unknown frame is not a decode error")
}
