package client

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/stretchr/testify/require"
)

// TestDialWebsocketMatchesServerTooOld locks the cross-package wire contract for
// the §14 "cursor too old" signal. The client recognizes the server's
// pre-upgrade HTTP 400 by substring-matching its body (the client cannot import
// internal/subscribe in production code without pulling the server's storage
// deps into the public module), so server-side message drift would otherwise
// silently break re-backfill and wedge a fell-behind consumer in a reconnect
// loop. This test fails CI the moment either side drifts.
func TestDialWebsocketMatchesServerTooOld(t *testing.T) {
	t.Parallel()

	// 1. The two duplicated markers must stay byte-for-byte equal, and the real
	//    server error message must actually contain the client's marker.
	require.Equal(t, subscribe.CursorTooOldMarker, cursorTooOldMarker,
		"client and server too-old markers drifted")
	realServerBody := fmt.Errorf("%w: cursor 1000 below lookback floor 1500; re-backfill from your last seq",
		subscribe.ErrCursorTooOld).Error()
	require.Contains(t, realServerBody, cursorTooOldMarker,
		"server's ErrCursorTooOld message no longer contains the client's match marker")

	// 2. End-to-end: the production dialer must map a real pre-upgrade HTTP 400
	//    carrying that body to the terminal errLiveCursorTooOld (not a generic
	//    transient dial error the consumer would reconnect-loop on).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Mirror handler.go: cursor resolution fails before the websocket
		// upgrade, so the client sees a plain HTTP 400 with the error body.
		http.Error(w, realServerBody, http.StatusBadRequest)
	}))
	defer srv.Close()

	wsURL := toWS(t, srv.URL)
	_, err := dialWebsocket(context.Background(), wsURL, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, errLiveCursorTooOld,
		"a real pre-upgrade 400 carrying the server's too-old body must map to errLiveCursorTooOld")
	// The floor seq from the body must survive into the wrapped error for
	// operability (the client logs how far behind it was).
	require.Contains(t, err.Error(), "lookback floor 1500")

	// 3. A different 400 (e.g. a parse error) must NOT be misread as too-old, so
	//    the cutover engine does not wrongly re-backfill on an unrelated reject.
	srvOther := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "subscribe: invalid cursor: \"abc\"", http.StatusBadRequest)
	}))
	defer srvOther.Close()
	_, err = dialWebsocket(context.Background(), toWS(t, srvOther.URL), nil)
	require.Error(t, err)
	require.NotErrorIs(t, err, errLiveCursorTooOld,
		"an unrelated 400 must not be classified as a too-old cursor")
}

// toWS rewrites an httptest http:// URL to the ws:// scheme dialWebsocket
// expects, preserving host/port, and points at /subscribe-v2.
func toWS(t *testing.T, httpURL string) string {
	t.Helper()
	u, err := url.Parse(httpURL)
	require.NoError(t, err)
	u.Scheme = strings.Replace(u.Scheme, "http", "ws", 1)
	u.Path = "/subscribe-v2"
	return u.String()
}
