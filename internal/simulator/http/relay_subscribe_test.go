package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/stretchr/testify/require"
)

// frameHeaderCommitBytes is the verbatim CBOR header bytes that
// prefix every #commit frame on the wire. Hard-coded here so the
// test doesn't reach into world's unexported state. If the wire
// format ever changes, this constant + the world's encoder both
// fail and we get clear signal from both sides.
var frameHeaderCommitBytes = []byte{
	0xa2,           // map(2)
	0x62, 'o', 'p', // text(2) "op"
	0x01,      // unsigned 1
	0x61, 't', // text(1) "t"
	0x67, '#', 'c', 'o', 'm', 'm', 'i', 't', // text(7) "#commit"
}

func TestSubscribeRepos_DeliversLiveCommit(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)
	srv := httptest.NewServer(nil)
	srv.Config.Handler = simhttp.NewHandler(w, srv.URL)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer func() {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
	}()
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	// Give the handler a beat to register the subscriber.
	time.Sleep(50 * time.Millisecond)
	frame, err := w.GenerateOneForTest(ctx)
	require.NoError(t, err)

	_, got, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, frame, got)
}

func TestSubscribeRepos_ReplaysHistoricalEvents(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Generate two events before any subscriber connects.
	first, err := w.GenerateOneForTest(ctx)
	require.NoError(t, err)
	_, err = w.GenerateOneForTest(ctx)
	require.NoError(t, err)

	srv := httptest.NewServer(nil)
	srv.Config.Handler = simhttp.NewHandler(w, srv.URL)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos?cursor=0"

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer func() {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
	}()
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	// Read the first historical frame and confirm seq=1.
	_, got, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, first, got)

	// Decode body to confirm shape.
	var cm comatproto.SyncSubscribeRepos_Commit
	require.True(t, strings.HasPrefix(string(got), string(frameHeaderCommitBytes)))
	body := got[len(frameHeaderCommitBytes):]
	require.NoError(t, cm.UnmarshalCBOR(body))
	require.Equal(t, int64(1), cm.Seq)
}

func TestSubscribeRepos_DisconnectFaultClosesAfterThresholdAndReconnectResumes(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for range 3 {
		_, err := w.GenerateOneForTest(ctx)
		require.NoError(t, err)
	}

	faults := simhttp.NewFaultPlan()
	faults.SetSubscribeReposDisconnectSchedule([]int{2})

	srv := httptest.NewServer(nil)
	srv.Config.Handler = simhttp.NewHandlerWithOptions(w, srv.URL, simhttp.HandlerOptions{
		Faults: faults,
	})
	defer srv.Close()

	wsBase := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos"
	conn, resp, err := websocket.Dial(ctx, wsBase+"?cursor=0", nil)
	require.NoError(t, err)
	closeResp(resp)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	_, got, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), subscribeCommitSeq(t, got))
	_, got, err = conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), subscribeCommitSeq(t, got))

	_, _, err = conn.Read(ctx)
	require.Error(t, err)
	require.NotEqual(t, websocket.StatusNormalClosure, websocket.CloseStatus(err),
		"fault must use a non-normal close so streaming clients reconnect")
	require.Equal(t, 1, faults.SubscribeReposDisconnects())
	require.Equal(t, 1, faults.SubscribeReposConnections())

	reconn, resp, err := websocket.Dial(ctx, wsBase+"?cursor=2", nil)
	require.NoError(t, err)
	closeResp(resp)
	defer func() { _ = reconn.Close(websocket.StatusNormalClosure, "test done") }()

	_, got, err = reconn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), subscribeCommitSeq(t, got),
		"reconnect from cursor=2 must resume at the next retained frame")
	require.Equal(t, 2, faults.SubscribeReposConnections())
	require.Equal(t, 1, faults.SubscribeReposDisconnects())
}

// TestSubscribeRepos_ReplayFaultDuplicatesLastFrames pins duplicate-N
// mode: after AfterFrames frames, the relay re-sends the last N frames
// verbatim — same bytes, same seqs — modeling a relay double-delivery.
func TestSubscribeRepos_ReplayFaultDuplicatesLastFrames(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for range 3 {
		_, err := w.GenerateOneForTest(ctx)
		require.NoError(t, err)
	}

	faults := simhttp.NewFaultPlan()
	faults.SetSubscribeReposReplaySchedule([]simhttp.SubscribeReposReplayFault{
		{AfterFrames: 3, DuplicateLast: 2},
	})

	srv := httptest.NewServer(nil)
	srv.Config.Handler = simhttp.NewHandlerWithOptions(w, srv.URL, simhttp.HandlerOptions{
		Faults: faults,
	})
	defer srv.Close()

	wsBase := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos"
	conn, resp, err := websocket.Dial(ctx, wsBase+"?cursor=0", nil)
	require.NoError(t, err)
	closeResp(resp)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	wantSeqs := []int64{1, 2, 3, 2, 3}
	for i, want := range wantSeqs {
		_, got, rerr := conn.Read(ctx)
		require.NoError(t, rerr, "frame %d", i)
		require.Equal(t, want, subscribeCommitSeq(t, got), "frame %d", i)
	}
	require.Equal(t, 1, faults.SubscribeReposReplaysFired())
	require.Equal(t, 2, faults.SubscribeReposReplayedFrames())
}

// TestSubscribeRepos_ReplayFaultRegressesToSeq pins regress-to-K mode:
// after AfterFrames frames, the relay re-sends the whole retained window
// above K — the wire shape of a relay restored from a backup at seq K.
func TestSubscribeRepos_ReplayFaultRegressesToSeq(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for range 4 {
		_, err := w.GenerateOneForTest(ctx)
		require.NoError(t, err)
	}

	faults := simhttp.NewFaultPlan()
	faults.SetSubscribeReposReplaySchedule([]simhttp.SubscribeReposReplayFault{
		{AfterFrames: 4, RegressToSeq: 1},
	})

	srv := httptest.NewServer(nil)
	srv.Config.Handler = simhttp.NewHandlerWithOptions(w, srv.URL, simhttp.HandlerOptions{
		Faults: faults,
	})
	defer srv.Close()

	wsBase := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos"
	conn, resp, err := websocket.Dial(ctx, wsBase+"?cursor=0", nil)
	require.NoError(t, err)
	closeResp(resp)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	wantSeqs := []int64{1, 2, 3, 4, 2, 3, 4}
	for i, want := range wantSeqs {
		_, got, rerr := conn.Read(ctx)
		require.NoError(t, rerr, "frame %d", i)
		require.Equal(t, want, subscribeCommitSeq(t, got), "frame %d", i)
	}
	require.Equal(t, 1, faults.SubscribeReposReplaysFired())
	require.Equal(t, 3, faults.SubscribeReposReplayedFrames())

	// Live traffic after the replayed window resumes at the true tip.
	_, err = w.GenerateOneForTest(ctx)
	require.NoError(t, err)
	_, got, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(5), subscribeCommitSeq(t, got),
		"post-replay live traffic must resume at the real tip seq")
}

// TestSubscribeRepos_ReplayAndDisconnectFaultsCompose pins that a replay
// fault and a disconnect fault armed on the same connection fire in a
// deterministic order: replayed frames do not count toward the
// disconnect threshold.
func TestSubscribeRepos_ReplayAndDisconnectFaultsCompose(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for range 3 {
		_, err := w.GenerateOneForTest(ctx)
		require.NoError(t, err)
	}

	faults := simhttp.NewFaultPlan()
	faults.SetSubscribeReposReplaySchedule([]simhttp.SubscribeReposReplayFault{
		{AfterFrames: 2, DuplicateLast: 1},
	})
	faults.SetSubscribeReposDisconnectSchedule([]int{3})

	srv := httptest.NewServer(nil)
	srv.Config.Handler = simhttp.NewHandlerWithOptions(w, srv.URL, simhttp.HandlerOptions{
		Faults: faults,
	})
	defer srv.Close()

	wsBase := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos"
	conn, resp, err := websocket.Dial(ctx, wsBase+"?cursor=0", nil)
	require.NoError(t, err)
	closeResp(resp)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	// Writes 1,2 then the replay fires (dup of 2); write 3 crosses the
	// disconnect threshold (replayed frame did not count).
	wantSeqs := []int64{1, 2, 2, 3}
	for i, want := range wantSeqs {
		_, got, rerr := conn.Read(ctx)
		require.NoError(t, rerr, "frame %d", i)
		require.Equal(t, want, subscribeCommitSeq(t, got), "frame %d", i)
	}
	_, _, err = conn.Read(ctx)
	require.Error(t, err, "disconnect fault must close after the third counted frame")
	require.Equal(t, 1, faults.SubscribeReposReplaysFired())
	require.Equal(t, 1, faults.SubscribeReposDisconnects())
}

// TestSubscribeRepos_InjectFaultInsertsFrameBetweenRealOnes pins
// inject-only mode: after AfterFrames counted frames, the scheduled
// bytes appear verbatim on the wire between real frames, and every
// real frame still arrives. Injected bytes must not advance the
// counted-frame accounting.
func TestSubscribeRepos_InjectFaultInsertsFrameBetweenRealOnes(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for range 3 {
		_, err := w.GenerateOneForTest(ctx)
		require.NoError(t, err)
	}

	garbage := []byte{0xff, 0xfe, 0xfd}
	faults := simhttp.NewFaultPlan()
	faults.SetSubscribeReposInjectSchedule([]simhttp.SubscribeReposInjectFault{
		{AfterFrames: 2, Frame: garbage},
	})

	srv := httptest.NewServer(nil)
	srv.Config.Handler = simhttp.NewHandlerWithOptions(w, srv.URL, simhttp.HandlerOptions{
		Faults: faults,
	})
	defer srv.Close()

	wsBase := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos"
	conn, resp, err := websocket.Dial(ctx, wsBase+"?cursor=0", nil)
	require.NoError(t, err)
	closeResp(resp)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	for i, want := range []int64{1, 2} {
		_, got, rerr := conn.Read(ctx)
		require.NoError(t, rerr, "frame %d", i)
		require.Equal(t, want, subscribeCommitSeq(t, got), "frame %d", i)
	}
	_, got, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, garbage, got, "the injected bytes must appear verbatim after frame 2")
	_, got, err = conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), subscribeCommitSeq(t, got),
		"the real frame after the injection must still be delivered")

	require.Equal(t, 1, faults.SubscribeReposInjectsFired())
	require.Zero(t, faults.SubscribeReposSwallowedFrames())
}

// TestSubscribeRepos_InjectFaultSwallowNextReplacesFrame pins
// replace mode: the injected bytes positionally replace the next real
// frame, which never reaches the wire but stays in the world's
// firehose history (the fault is wire-only).
func TestSubscribeRepos_InjectFaultSwallowNextReplacesFrame(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for range 4 {
		_, err := w.GenerateOneForTest(ctx)
		require.NoError(t, err)
	}

	garbage := []byte{0xde, 0xad, 0xbe, 0xef}
	faults := simhttp.NewFaultPlan()
	faults.SetSubscribeReposInjectSchedule([]simhttp.SubscribeReposInjectFault{
		{AfterFrames: 1, Frame: garbage, SwallowNext: true},
	})

	srv := httptest.NewServer(nil)
	srv.Config.Handler = simhttp.NewHandlerWithOptions(w, srv.URL, simhttp.HandlerOptions{
		Faults: faults,
	})
	defer srv.Close()

	wsBase := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos"
	conn, resp, err := websocket.Dial(ctx, wsBase+"?cursor=0", nil)
	require.NoError(t, err)
	closeResp(resp)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	_, got, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), subscribeCommitSeq(t, got))
	_, got, err = conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, garbage, got, "the injected bytes must replace seq 2 positionally")
	_, got, err = conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), subscribeCommitSeq(t, got),
		"the frame after the swallowed one must arrive: seq 2 is a wire-level hole")
	_, got, err = conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(4), subscribeCommitSeq(t, got))

	require.Equal(t, 1, faults.SubscribeReposInjectsFired())
	require.Equal(t, 1, faults.SubscribeReposSwallowedFrames())

	// Wire-only: the swallowed frame is still in retained history, so a
	// reconnect (or the oracle's expected-eventlog walk) sees it.
	frames, err := w.FirehoseRange(1, 1)
	require.NoError(t, err)
	require.Len(t, frames, 1)
	require.Equal(t, int64(2), subscribeCommitSeq(t, frames[0]),
		"the swallowed frame must remain in the world's firehose history")
}

// TestSubscribeRepos_InjectFaultSwallowOnlyDropsFrame pins pure-drop
// mode: no bytes injected, the next real frame simply vanishes from
// the wire — a genuine relay-side gap.
func TestSubscribeRepos_InjectFaultSwallowOnlyDropsFrame(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for range 3 {
		_, err := w.GenerateOneForTest(ctx)
		require.NoError(t, err)
	}

	faults := simhttp.NewFaultPlan()
	faults.SetSubscribeReposInjectSchedule([]simhttp.SubscribeReposInjectFault{
		{AfterFrames: 1, SwallowNext: true},
	})

	srv := httptest.NewServer(nil)
	srv.Config.Handler = simhttp.NewHandlerWithOptions(w, srv.URL, simhttp.HandlerOptions{
		Faults: faults,
	})
	defer srv.Close()

	wsBase := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos"
	conn, resp, err := websocket.Dial(ctx, wsBase+"?cursor=0", nil)
	require.NoError(t, err)
	closeResp(resp)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	wantSeqs := []int64{1, 3}
	for i, want := range wantSeqs {
		_, got, rerr := conn.Read(ctx)
		require.NoError(t, rerr, "frame %d", i)
		require.Equal(t, want, subscribeCommitSeq(t, got), "frame %d", i)
	}
	require.Equal(t, 1, faults.SubscribeReposInjectsFired())
	require.Equal(t, 1, faults.SubscribeReposSwallowedFrames())
}

// TestSubscribeRepos_InjectAndDisconnectFaultsCompose pins that
// injected bytes do not count toward the disconnect schedule's
// thresholds, so inject and disconnect faults compose deterministically
// on one connection.
func TestSubscribeRepos_InjectAndDisconnectFaultsCompose(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for range 3 {
		_, err := w.GenerateOneForTest(ctx)
		require.NoError(t, err)
	}

	garbage := []byte{0x01, 0x02}
	faults := simhttp.NewFaultPlan()
	faults.SetSubscribeReposInjectSchedule([]simhttp.SubscribeReposInjectFault{
		{AfterFrames: 1, Frame: garbage},
	})
	faults.SetSubscribeReposDisconnectSchedule([]int{3})

	srv := httptest.NewServer(nil)
	srv.Config.Handler = simhttp.NewHandlerWithOptions(w, srv.URL, simhttp.HandlerOptions{
		Faults: faults,
	})
	defer srv.Close()

	wsBase := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos"
	conn, resp, err := websocket.Dial(ctx, wsBase+"?cursor=0", nil)
	require.NoError(t, err)
	closeResp(resp)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	// Frame 1, injected garbage, then frames 2 and 3; the disconnect
	// fires after the third COUNTED frame (garbage did not count).
	_, got, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), subscribeCommitSeq(t, got))
	_, got, err = conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, garbage, got)
	for i, want := range []int64{2, 3} {
		_, got, rerr := conn.Read(ctx)
		require.NoError(t, rerr, "frame %d", i)
		require.Equal(t, want, subscribeCommitSeq(t, got), "frame %d", i)
	}
	_, _, err = conn.Read(ctx)
	require.Error(t, err, "disconnect fault must close after the third counted frame")
	require.Equal(t, 1, faults.SubscribeReposInjectsFired())
	require.Equal(t, 1, faults.SubscribeReposDisconnects())
}

// TestSubscribeRepos_InjectAndReplayFaultsCompose pins the order when
// an inject fault and a replay fault trip on the same counted frame:
// the injection fires first, preserving its documented position
// immediately after the AfterFrames-th counted frame, and the replay
// burst follows it.
func TestSubscribeRepos_InjectAndReplayFaultsCompose(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for range 3 {
		_, err := w.GenerateOneForTest(ctx)
		require.NoError(t, err)
	}

	garbage := []byte{0xba, 0xdc, 0x0f, 0xfe}
	faults := simhttp.NewFaultPlan()
	faults.SetSubscribeReposInjectSchedule([]simhttp.SubscribeReposInjectFault{
		{AfterFrames: 2, Frame: garbage},
	})
	faults.SetSubscribeReposReplaySchedule([]simhttp.SubscribeReposReplayFault{
		{AfterFrames: 2, DuplicateLast: 1},
	})

	srv := httptest.NewServer(nil)
	srv.Config.Handler = simhttp.NewHandlerWithOptions(w, srv.URL, simhttp.HandlerOptions{
		Faults: faults,
	})
	defer srv.Close()

	wsBase := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos"
	conn, resp, err := websocket.Dial(ctx, wsBase+"?cursor=0", nil)
	require.NoError(t, err)
	closeResp(resp)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	// Frames 1,2 then both faults trip on frame 2: garbage first
	// (positional promise), then the replayed dup of 2, then real 3.
	for i, want := range []int64{1, 2} {
		_, got, rerr := conn.Read(ctx)
		require.NoError(t, rerr, "frame %d", i)
		require.Equal(t, want, subscribeCommitSeq(t, got), "frame %d", i)
	}
	_, got, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, garbage, got,
		"injected bytes must land immediately after the 2nd counted frame, before the replay burst")
	for i, want := range []int64{2, 3} {
		_, got, rerr := conn.Read(ctx)
		require.NoError(t, rerr, "post-inject frame %d", i)
		require.Equal(t, want, subscribeCommitSeq(t, got), "post-inject frame %d", i)
	}

	require.Equal(t, 1, faults.SubscribeReposInjectsFired())
	require.Equal(t, 1, faults.SubscribeReposReplaysFired())
	require.Equal(t, 1, faults.SubscribeReposReplayedFrames())
}

func subscribeCommitSeq(t *testing.T, frame []byte) int64 {
	t.Helper()

	require.True(t, strings.HasPrefix(string(frame), string(frameHeaderCommitBytes)))
	var cm comatproto.SyncSubscribeRepos_Commit
	require.NoError(t, cm.UnmarshalCBOR(frame[len(frameHeaderCommitBytes):]))
	return cm.Seq
}

func closeResp(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
}
