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
