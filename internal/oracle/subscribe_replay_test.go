package oracle

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

type subscribeReplayEvent struct {
	DID     string                  `json:"did"`
	TimeUS  int64                   `json:"time_us"`
	Cursor  uint64                  `json:"cursor"`
	Kind    string                  `json:"kind"`
	Commit  *subscribeReplayCommit  `json:"commit,omitempty"`
	Account *subscribeReplayAccount `json:"account,omitempty"`
	Sync    *subscribeReplaySync    `json:"sync,omitempty"`
}

type subscribeReplayCommit struct {
	Rev        string `json:"rev"`
	Operation  string `json:"operation"`
	Collection string `json:"collection"`
	Rkey       string `json:"rkey"`
	RecordCBOR string `json:"record_cbor"`
}

type subscribeReplayAccount struct {
	DID    string  `json:"did"`
	Seq    int64   `json:"seq"`
	Time   string  `json:"time"`
	Active bool    `json:"active"`
	Status *string `json:"status,omitempty"`
}

type subscribeReplaySync struct {
	DID  string `json:"did"`
	Rev  string `json:"rev"`
	Seq  int64  `json:"seq"`
	Time string `json:"time"`
}

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

func collectSubscribeReplay(t *testing.T, cfg Config, run *runtimeRun, trace *Trace, baseURL string, cursor, targetSeq uint64) (out []ObservedEvent) {
	t.Helper()

	wsURL := "ws" + strings.TrimPrefix(baseURL, "http") + "/subscribe?extended=true&cursor=" + strconv.FormatUint(cursor, 10)
	recordTraceOrError(t, trace, "subscribe_replay_start", map[string]any{
		"cursor":     cursor,
		"target_seq": targetSeq,
	})
	defer func() {
		var lastSeq uint64
		if len(out) > 0 {
			lastSeq = out[len(out)-1].Seq
		}
		recordTraceOrError(t, trace, "subscribe_replay_done", map[string]any{
			"cursor":      cursor,
			"target_seq":  targetSeq,
			"event_count": len(out),
			"last_seq":    lastSeq,
		})
	}()
	conn := dialSubscribeReplay(t, cfg, run, wsURL)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "oracle replay complete") }()

	for {
		readTimeout := min(oracleWaitTimeout(cfg), 10*time.Second)
		readCtx, cancel := context.WithTimeout(context.Background(), readTimeout)
		go func() {
			select {
			case <-run.exited:
				cancel()
			case <-readCtx.Done():
			}
		}()

		typ, body, err := conn.Read(readCtx)
		cancel()
		if err != nil {
			select {
			case <-run.exited:
				t.Fatalf("runtime exited while reading subscribe replay target_seq=%d: mode=%s seed=%d err=%v",
					targetSeq, cfg.Mode, cfg.Seed, run.err)
			default:
			}
			// The server closed the replay socket mid-stream while the
			// runtime is still up. This is the SAME fragile observation
			// point as the CheckCompacted failure (a long from-cursor-0
			// cold replay of sealed history over the live-tail transport
			// under heavy concurrent compaction/reconnect load), surfacing
			// as a torn frame instead of a contract violation. Record where
			// the replay died so the trace classifies it rather than
			// leaving an opaque read error. See #77.
			var lastSeq uint64
			if len(out) > 0 {
				lastSeq = out[len(out)-1].Seq
			}
			recordTraceOrError(t, trace, "subscribe_replay_read_error", map[string]any{
				"cursor":      cursor,
				"target_seq":  targetSeq,
				"event_count": len(out),
				"last_seq":    lastSeq,
				"err":         err.Error(),
			})
			t.Fatalf("subscribe replay socket closed mid-stream at seq=%d of target_seq=%d (got %d events) "+
				"while runtime is up: mode=%s seed=%d err=%v -- this is the live-tail transport serving "+
				"sealed history (see #77), not a clean read",
				lastSeq, targetSeq, len(out), cfg.Mode, cfg.Seed, err)
		}
		require.Equalf(t, websocket.MessageText, typ, "subscribe replay must emit text frames mode=%s seed=%d", cfg.Mode, cfg.Seed)

		var msg subscribeReplayEvent
		require.NoErrorf(t, json.Unmarshal(body, &msg),
			"decode subscribe replay frame target_seq=%d mode=%s seed=%d body=%s", targetSeq, cfg.Mode, cfg.Seed, body)
		ev := observedEventFromSubscribeReplay(t, msg)
		out = append(out, ev)
		data := traceObservedEvent(ev)
		data["frame_kind"] = msg.Kind
		recordTraceOrError(t, trace, "subscribe_replay_event", data)
		if targetSeq == 0 || ev.Seq >= targetSeq {
			return out
		}
	}
}

func dialSubscribeReplay(t *testing.T, cfg Config, run *runtimeRun, wsURL string) *websocket.Conn {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), oracleWaitTimeout(cfg))
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()

	for {
		conn, resp, err := websocket.Dial(ctx, wsURL, nil)
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		if err == nil {
			return conn
		}
		if ctx.Err() != nil {
			t.Fatalf("timeout dialing subscribe replay %s: mode=%s seed=%d last_err=%v", wsURL, cfg.Mode, cfg.Seed, err)
		}
		if resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
			require.NoErrorf(t, err, "dial subscribe replay %s: mode=%s seed=%d", wsURL, cfg.Mode, cfg.Seed)
		}

		select {
		case <-run.exited:
			t.Fatalf("runtime exited while dialing subscribe replay %s: mode=%s seed=%d err=%v",
				wsURL, cfg.Mode, cfg.Seed, run.err)
		case <-ctx.Done():
			t.Fatalf("timeout dialing subscribe replay %s: mode=%s seed=%d last_err=%v", wsURL, cfg.Mode, cfg.Seed, err)
		case <-tick.C:
		}
	}
}

func observedEventFromSubscribeReplay(t *testing.T, msg subscribeReplayEvent) ObservedEvent {
	t.Helper()

	ev := ObservedEvent{
		Seq:       msg.Cursor,
		IndexedAt: msg.TimeUS,
		DID:       msg.DID,
	}

	switch msg.Kind {
	case "commit":
		require.NotNil(t, msg.Commit, "subscribe replay commit event missing commit payload seq=%d", msg.Cursor)
		ev.Collection = msg.Commit.Collection
		ev.Rkey = msg.Commit.Rkey
		ev.Rev = msg.Commit.Rev
		switch msg.Commit.Operation {
		case "create":
			ev.Kind = segment.KindCreate
			ev.Payload = decodeSubscribeReplayRecordCBOR(t, msg)
		case "update":
			ev.Kind = segment.KindUpdate
			ev.Payload = decodeSubscribeReplayRecordCBOR(t, msg)
		case "delete":
			ev.Kind = segment.KindDelete
		default:
			t.Fatalf("unknown subscribe replay commit operation %q seq=%d", msg.Commit.Operation, msg.Cursor)
		}
	case "account":
		require.NotNil(t, msg.Account, "subscribe replay account event missing account payload seq=%d", msg.Cursor)
		ev.Kind = segment.KindAccount
		ev.DID = msg.Account.DID
		acc := &comatproto.SyncSubscribeRepos_Account{
			DID:    msg.Account.DID,
			Seq:    msg.Account.Seq,
			Time:   msg.Account.Time,
			Active: msg.Account.Active,
		}
		if msg.Account.Status != nil {
			acc.Status = gt.Some(*msg.Account.Status)
		}
		payload, err := acc.MarshalCBOR()
		require.NoError(t, err)
		ev.Payload = payload
	case "identity":
		ev.Kind = segment.KindIdentity
	case "sync":
		require.NotNil(t, msg.Sync, "subscribe replay sync event missing sync payload seq=%d", msg.Cursor)
		ev.Kind = segment.KindSync
		ev.DID = msg.Sync.DID
		ev.Rev = msg.Sync.Rev
	default:
		t.Fatalf("unknown subscribe replay event kind %q seq=%d", msg.Kind, msg.Cursor)
	}

	return ev
}

func decodeSubscribeReplayRecordCBOR(t *testing.T, msg subscribeReplayEvent) []byte {
	t.Helper()

	require.NotEmpty(t, msg.Commit.RecordCBOR,
		"subscribe replay commit event missing record_cbor operation=%s seq=%d", msg.Commit.Operation, msg.Cursor)
	payload, err := base64.StdEncoding.DecodeString(msg.Commit.RecordCBOR)
	require.NoError(t, err)
	return payload
}
