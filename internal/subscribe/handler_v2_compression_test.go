package subscribe

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// newV2Server builds a /subscribe-v2 handler over a fresh read-log tail.
func newV2Server(t *testing.T) (*httptest.Server, *Tail) {
	t.Helper()
	st := newSteadyStateStore(t)
	b, _ := newReadLogTail(t, 1<<20, noCold)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		V2:     true,
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, b
}

func dialV2(t *testing.T, ctx context.Context, srv *httptest.Server, query string, mode websocket.CompressionMode) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + query
	return websocket.Dial(ctx, wsURL, &websocket.DialOptions{CompressionMode: mode})
}

// TestHandlerV2_ZstdDictionaryParam_DeliversV2DictFrames pins the v2
// compression opt-in: zstdDictionary=<current id> yields BINARY frames
// that decode with the v2 dictionary.
func TestHandlerV2_ZstdDictionaryParam_DeliversV2DictFrames(t *testing.T) {
	t.Parallel()
	srv, b := newV2Server(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := dialV2(t, ctx, srv,
		fmt.Sprintf("?zstdDictionary=%d", DictionaryV2ID), websocket.CompressionDisabled)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	waitForTailBlocked(t, b)
	var seq uint64
	publishIdentity(t, b, &seq, "did:plc:v2dictparam", 1)

	rctx, rcancel := context.WithTimeout(ctx, time.Second)
	defer rcancel()
	mt, frame, err := conn.Read(rctx)
	require.NoError(t, err)
	require.Equal(t, websocket.MessageBinary, mt, "v2 zstd clients must receive binary frames")

	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(zstdDictionaryV2))
	require.NoError(t, err)
	defer dec.Close()
	got, err := dec.DecodeAll(frame, nil)
	require.NoError(t, err, "frame must decode with the v2 dictionary")
	require.Contains(t, string(got), "did:plc:v2dictparam")
	require.Contains(t, string(got), `"kind":"identity"`)

	// The legacy v1 dictionary must NOT decode v2 frames.
	v1Dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(zstdDictionary))
	require.NoError(t, err)
	defer v1Dec.Close()
	_, err = v1Dec.DecodeAll(frame, nil)
	require.Error(t, err)
}

// TestHandlerV2_RejectsUnknownDictionaryID pins the never-serve-undecodable
// rule: an unknown/retired ID is a pre-upgrade 400 whose body carries the
// current ID so the client can re-fetch and reconnect.
func TestHandlerV2_RejectsUnknownDictionaryID(t *testing.T) {
	t.Parallel()
	srv, _ := newV2Server(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, resp, err := dialV2(t, ctx, srv, "?zstdDictionary=12345", websocket.CompressionDisabled)
	require.Error(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Body)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	require.Contains(t, string(body), fmt.Sprintf("current dictionary id is %d", DictionaryV2ID),
		"the 400 body must carry the current dictionary ID")
}

// TestHandlerV2_RejectsMalformedDictionaryID covers non-integer and
// non-positive IDs.
func TestHandlerV2_RejectsMalformedDictionaryID(t *testing.T) {
	t.Parallel()
	srv, _ := newV2Server(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, q := range []string{"?zstdDictionary=abc", "?zstdDictionary=0", "?zstdDictionary=-1"} {
		_, resp, err := dialV2(t, ctx, srv, q, websocket.CompressionDisabled)
		require.Error(t, err, "query %s must be rejected", q)
		require.NotNil(t, resp)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, "query %s", q)
		_ = resp.Body.Close()
	}
}

// TestHandlerV2_RejectsLegacyZstdOptIns pins the clean break from v1: the
// legacy compress=true / Socket-Encoding opt-ins are 400s on v2, not
// silently honored with a dictionary the client may not hold.
func TestHandlerV2_RejectsLegacyZstdOptIns(t *testing.T) {
	t.Parallel()
	srv, _ := newV2Server(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, resp, err := dialV2(t, ctx, srv, "?compress=true", websocket.CompressionDisabled)
	require.Error(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.Contains(t, string(body), "zstdDictionary",
		"the 400 must point the client at the v2 opt-in")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	req.Header.Set("Socket-Encoding", "zstd")
	hresp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = hresp.Body.Close() }()
	require.Equal(t, http.StatusBadRequest, hresp.StatusCode)
}

// TestHandlerV2_NeverNegotiatesDeflate pins deflate removal on v2: a
// client offering permessage-deflate connects fine but the extension is
// absent from the handshake response, and frames arrive uncompressed.
func TestHandlerV2_NeverNegotiatesDeflate(t *testing.T) {
	t.Parallel()
	srv, b := newV2Server(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := dialV2(t, ctx, srv, "", websocket.CompressionContextTakeover)
	require.NoError(t, err, "a deflate offer must not break the v2 handshake")
	require.NotNil(t, resp)
	if resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	require.NotContains(t, resp.Header.Get("Sec-WebSocket-Extensions"), "permessage-deflate",
		"v2 must not accept the deflate extension")

	waitForTailBlocked(t, b)
	var seq uint64
	publishIdentity(t, b, &seq, "did:plc:v2nodeflate", 1)

	rctx, rcancel := context.WithTimeout(ctx, time.Second)
	defer rcancel()
	mt, frame, err := conn.Read(rctx)
	require.NoError(t, err)
	require.Equal(t, websocket.MessageText, mt)
	require.Contains(t, string(frame), "did:plc:v2nodeflate")
}

// TestHandlerV1_StillNegotiatesDeflate pins the v1 endpoint's frozen
// contract: deflate negotiation is untouched by the v2 removal.
func TestHandlerV1_StillNegotiatesDeflate(t *testing.T) {
	t.Parallel()
	st := newSteadyStateStore(t)
	b, _ := newReadLogTail(t, 1<<20, noCold)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		CompressionMode: websocket.CompressionContextTakeover,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	if resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	require.Contains(t, resp.Header.Get("Sec-WebSocket-Extensions"), "permessage-deflate",
		"v1 must keep negotiating deflate when offered")
}

// TestHandlerV1_IgnoresZstdDictionaryParam pins that the v2-only param is
// inert on the frozen v1 endpoint (v1 ParseQuery must not 400 on it, and
// it must not switch the connection to zstd).
func TestHandlerV1_IgnoresZstdDictionaryParam(t *testing.T) {
	t.Parallel()
	st := newSteadyStateStore(t)
	b, _ := newReadLogTail(t, 1<<20, noCold)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?zstdDictionary=12345"
	conn, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	require.NoError(t, err, "v1 must ignore the v2-only zstdDictionary param")
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	waitForTailBlocked(t, b)
	var seq uint64
	publishIdentity(t, b, &seq, "did:plc:v1inert", 1)

	rctx, rcancel := context.WithTimeout(ctx, time.Second)
	defer rcancel()
	mt, frame, err := conn.Read(rctx)
	require.NoError(t, err)
	require.Equal(t, websocket.MessageText, mt, "v1 must stay uncompressed for this dial")
	require.Contains(t, string(frame), "did:plc:v1inert")
}
