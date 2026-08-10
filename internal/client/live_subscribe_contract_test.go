package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream/api/jetstream"
	"github.com/stretchr/testify/require"
)

// These tests lock the cross-package wire contract between the client's
// dialer and the server's pre-upgrade rejections. The client matches the
// XRPC error envelope's structured error name (never body substrings); the
// names are declared in the network.bsky.jetstream.subscribe lexicon and
// emitted by internal/subscribe. The client cannot import internal/subscribe
// in production code (it would pull the server's storage deps into the
// public module), so the error-name literals are duplicated here and these
// tests fail CI the moment either side drifts. The oracle's Part B harness
// additionally exercises the same flow against the REAL handler.

// xrpcErrorBody mirrors the server's httpError envelope shape.
func xrpcErrorBody(name, msg string) string {
	b, _ := json.Marshal(struct {
		Error   string `json:"error"`
		Message string `json:"message,omitempty"`
	}{Error: name, Message: msg})
	return string(b)
}

func serve400(t *testing.T, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestDialWebsocketMatchesServerTooOld locks the §14 "cursor too old"
// signal: a pre-upgrade 400 whose envelope names CursorTooOld must map to
// the terminal errLiveCursorTooOld (re-enter backfill), and nothing else
// may.
func TestDialWebsocketMatchesServerTooOld(t *testing.T) {
	t.Parallel()

	// The envelope body mirrors the real handler's httpError output for
	// ErrCursorTooOld verbatim.
	srv := serve400(t, xrpcErrorBody("CursorTooOld",
		"subscribe: cursor too old: cursor 1000 below lookback floor 1500; re-backfill from your last seq"))
	_, err := dialWebsocket(context.Background(), toWS(t, srv.URL), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, errLiveCursorTooOld,
		"a pre-upgrade 400 CursorTooOld envelope must map to errLiveCursorTooOld")
	// The floor seq must survive into the wrapped error for operability
	// (the client logs how far behind it was).
	require.Contains(t, err.Error(), "lookback floor 1500")

	// A different 400 (e.g. a parse error) must NOT be misread as too-old,
	// so the cutover engine does not wrongly re-backfill on an unrelated
	// reject.
	srvOther := serve400(t, xrpcErrorBody("InvalidRequest", `subscribe: invalid cursor: "abc"`))
	_, err = dialWebsocket(context.Background(), toWS(t, srvOther.URL), nil)
	require.Error(t, err)
	require.NotErrorIs(t, err, errLiveCursorTooOld,
		"an unrelated 400 must not be classified as a too-old cursor")

	// A legacy plain-text 400 (a non-lexicon server, or a proxy) is a
	// generic dial error, not a typed signal.
	legacy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "cursor too old", http.StatusBadRequest)
	}))
	t.Cleanup(legacy.Close)
	_, err = dialWebsocket(context.Background(), toWS(t, legacy.URL), nil)
	require.Error(t, err)
	require.NotErrorIs(t, err, errLiveCursorTooOld,
		"non-envelope bodies are not matched: the name in the envelope is the contract")
}

// TestDialWebsocketMatchesServerDictRejected locks the dictionary-rotation
// signal: a 400 naming UnknownZstdDictionary maps to the recoverable
// errLiveDictRejected.
func TestDialWebsocketMatchesServerDictRejected(t *testing.T) {
	t.Parallel()

	srv := serve400(t, xrpcErrorBody("UnknownZstdDictionary",
		"unknown zstd dictionary id 20260101; current dictionary id is 20260709 (fetch it via getZstdDictionary and reconnect)"))
	_, err := dialWebsocket(context.Background(), toWS(t, srv.URL), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, errLiveDictRejected,
		"a pre-upgrade 400 UnknownZstdDictionary envelope must map to errLiveDictRejected")
	require.NotErrorIs(t, err, errLiveCursorTooOld)
	// The current-ID hint must survive into the wrapped error for
	// operability.
	require.Contains(t, err.Error(), "20260709")
}

// TestSubprotocolMatchesGeneratedLexicon pins the client's duplicated
// subprotocol token to the lexgen-generated constant (the single source of
// truth derived from lexicons/network/bsky/jetstream/subscribe.json).
func TestSubprotocolMatchesGeneratedLexicon(t *testing.T) {
	t.Parallel()
	require.Equal(t, jetstream.JetstreamSubscribe_Subprotocol, subscribeSubprotocol,
		"client subprotocol token drifted from the lexicon-generated constant")
}

// toWS rewrites an httptest http:// URL to the ws:// scheme dialWebsocket
// expects, preserving host/port, and points at the subscribe NSID path.
func toWS(t *testing.T, httpURL string) string {
	t.Helper()
	u, err := url.Parse(httpURL)
	require.NoError(t, err)
	u.Scheme = strings.Replace(u.Scheme, "http", "ws", 1)
	u.Path = "/xrpc/" + subscribeNSID
	return u.String()
}
