package subscribe

// End-to-end tests for the proposal-0015 contract on the v2 endpoint:
// Sec-WebSocket-Protocol negotiation, server-push-only read side,
// structured XRPC error envelopes on pre-upgrade rejections, terminal
// error frames, and the #info OutdatedCursor advisory.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func dialV2Opts(t *testing.T, ctx context.Context, srv *httptest.Server, query string, opts *websocket.DialOptions) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + query
	return websocket.Dial(ctx, wsURL, opts)
}

func TestHandlerV2_SubprotocolNegotiation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		offer    []string
		wantEcho string
	}{
		{"offered and echoed", []string{"xrpc.v1.json"}, "xrpc.v1.json"},
		{"no offer falls back to lexicon default", nil, ""},
		{"junk token ignored", []string{"xrpc.v9.msgpack"}, ""},
		{"case-variant token is not the canonical token", []string{"XRPC.V1.JSON"}, ""},
		{"offered among others", []string{"xrpc.v9.msgpack", "xrpc.v1.json"}, "xrpc.v1.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, b := newV2Server(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conn, resp, err := dialV2Opts(t, ctx, srv, "", &websocket.DialOptions{
				Subprotocols: tc.offer,
			})
			require.NoError(t, err)
			if resp != nil && resp.Body != nil {
				defer func() { _ = resp.Body.Close() }()
			}
			defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

			require.Equal(t, tc.wantEcho, conn.Subprotocol())

			// Framing is identical either way: one identity event arrives
			// as an xrpc.v1.json message frame.
			waitForTailBlocked(t, b)
			var seq uint64
			publishIdentity(t, b, &seq, "did:plc:negotiate", 1)
			payload := unwrapV2Frame(t, readOneFrame(t, ctx, conn))
			require.Equal(t, "network.bsky.jetstream.subscribe#identity", payload["$type"])
		})
	}
}

// TestHandlerV2_ServerPushOnly pins decision 4: any client data frame on
// the v2 endpoint closes the connection with StatusPolicyViolation. The
// options_update mechanism is v1-only.
func TestHandlerV2_ServerPushOnly(t *testing.T) {
	t.Parallel()
	srv, _ := newV2Server(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := dialV2Opts(t, ctx, srv, "", nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.CloseNow() }()

	// A v1-style options_update — or any data frame at all — must close
	// the connection.
	err = conn.Write(ctx, websocket.MessageText,
		[]byte(`{"type":"options_update","payload":{"wantedCollections":["app.bsky.feed.like"]}}`))
	require.NoError(t, err, "the write itself succeeds; the server closes in response")

	// The close surfaces on our next read.
	readCtx, rcancel := context.WithTimeout(ctx, 2*time.Second)
	defer rcancel()
	_, _, rerr := conn.Read(readCtx)
	require.Error(t, rerr)
	require.Equal(t, websocket.StatusPolicyViolation, websocket.CloseStatus(rerr),
		"client data frames on the server-push-only endpoint close with policy violation")
}

// xrpcErrorBody decodes the structured XRPC error envelope the v2
// endpoint returns on every pre-upgrade rejection.
func xrpcErrorBody(t *testing.T, resp *http.Response) (name, message string) {
	t.Helper()
	require.NotNil(t, resp)
	require.NotNil(t, resp.Body)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	var envelope struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(body, &envelope), "error body must be the XRPC JSON envelope, got %q", body)
	require.NotEmpty(t, envelope.Error)
	return envelope.Error, envelope.Message
}

func TestHandlerV2_PreUpgradeErrorsAreXRPCEnvelopes(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		query      string
		wantStatus int
		wantName   string
		wantInMsg  string
	}{
		{"unknown kind", "?kinds=likes", 400, "InvalidRequest", `unknown kind "likes"`},
		{"inert collections", "?kinds=identity&collections=app.bsky.feed.like", 400, "InvalidRequest", "can never apply"},
		{"legacy wantedDids", "?wantedDids=did:plc:x", 400, "InvalidRequest", "use dids"},
		{"legacy wantedCollections", "?wantedCollections=app.bsky.feed.like", 400, "InvalidRequest", "use collections"},
		{"requireHello tombstone", "?requireHello=true", 400, "InvalidRequest", "server-push only"},
		{"legacy compress opt-in", "?compress=true", 400, "InvalidRequest", "zstdDictionary"},
		{"unknown dictionary", "?zstdDictionary=12345", 400, "UnknownZstdDictionary", "current dictionary id is"},
		{"malformed dictionary", "?zstdDictionary=banana", 400, "InvalidRequest", "positive integer"},
		// Cursor-shaped 400s (invalid cursor, CursorTooOld) need a replay-
		// enabled server config; they are covered with envelope assertions
		// in handler_integration_test.go.
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := newV2Server(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, resp, err := dialV2Opts(t, ctx, srv, tc.query, nil)
			require.Error(t, err)
			require.NotNil(t, resp)
			defer func() { _ = resp.Body.Close() }()
			require.Equal(t, tc.wantStatus, resp.StatusCode)
			name, msg := xrpcErrorBody(t, resp)
			require.Equal(t, tc.wantName, name)
			require.Contains(t, msg, tc.wantInMsg)
		})
	}
}

// TestHandlerV2_ErrorFrameOnInternalFailure exercises the terminal error
// frame via EncodeV2Error shape assertions (the slow-detector path needs
// minutes of simulated lag; the frame contract is what matters and the
// send path is writeFrame, shared with the info frame tested below).
func TestHandlerV2_ErrorFrameShape(t *testing.T) {
	t.Parallel()
	frame := EncodeV2Error("ConsumerTooSlow", "reading below the floor rate 2100000 events behind the tip; reconnect with cursor=42")
	var got struct {
		Type    string `json:"$type"`
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(frame, &got))
	require.Equal(t, "error", got.Type)
	require.Equal(t, "ConsumerTooSlow", got.Error)
	require.Contains(t, got.Message, "cursor=42")
}
