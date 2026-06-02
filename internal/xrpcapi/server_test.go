package xrpcapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServer_RoutesMounted(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, 1)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// An unregistered NSID returns 501 (MethodNotImplemented) per xrpcserver.
	resp := doGet(t, ts.URL+"/xrpc/network.bsky.jetstream.doesNotExist")
	_ = resp.Body.Close()
	require.Equal(t, http.StatusNotImplemented, resp.StatusCode, "unregistered NSID should return 501")

	// getSegment without its required name param returns 400.
	resp2 := doGet(t, ts.URL+"/xrpc/network.bsky.jetstream.getSegment")
	_ = resp2.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp2.StatusCode, "getSegment without name param should return 400")

	// listSegments succeeds (200).
	resp3 := doGet(t, ts.URL+"/xrpc/network.bsky.jetstream.listSegments")
	_ = resp3.Body.Close()
	require.Equal(t, http.StatusOK, resp3.StatusCode, "listSegments should return 200")
}

func TestServer_ReadinessGateReturnsXRPC503(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, 1)
	gated := NewWithReady(s.src, s.logger, func(_ context.Context) error {
		return errors.New("bootstrap in progress")
	})
	ts := httptest.NewServer(gated.Handler())
	defer ts.Close()

	resp := doGet(t, ts.URL+"/xrpc/network.bsky.jetstream.listSegments")
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var body struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "ServiceUnavailable", body.Error)
	require.Contains(t, body.Message, "bootstrap in progress")
}
