package xrpcapi

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// testDict builds a minimal structured zstd dictionary header (magic +
// little-endian ID) followed by filler content. Enough for the handler,
// which never parses past what the caller provides.
func testDict(id uint32) []byte {
	d := make([]byte, 64)
	binary.LittleEndian.PutUint32(d[:4], 0xEC30A437)
	binary.LittleEndian.PutUint32(d[4:8], id)
	for i := 8; i < len(d); i++ {
		d[i] = byte(i)
	}
	return d
}

func newDictServer(t *testing.T, dict []byte, id uint32) *httptest.Server {
	t.Helper()
	s := New(Config{
		Src:    nil,
		Logger: slog.Default(),
		Dictionary: DictionaryConfig{
			ID:    id,
			Bytes: dict,
		},
	})
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return srv
}

// get issues a context-carrying GET (noctx-compliant test helper).
func get(t *testing.T, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

const dictNSID = "/xrpc/network.bsky.jetstream.getZstdDictionary"

func TestGetZstdDictionary_ServesCurrentWithoutID(t *testing.T) {
	t.Parallel()
	dict := testDict(20260709)
	srv := newDictServer(t, dict, 20260709)

	resp := get(t, srv.URL+dictNSID)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
	require.Equal(t, `"zstd-dict-20260709"`, resp.Header.Get("ETag"))
	require.Equal(t, "public, max-age=31536000, immutable", resp.Header.Get("Cache-Control"))
	require.Equal(t, "20260709", resp.Header.Get("X-Zstd-Dictionary-Id"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, dict, body, "served bytes must be the exact dictionary")
}

func TestGetZstdDictionary_ServesByExactID(t *testing.T) {
	t.Parallel()
	dict := testDict(42)
	srv := newDictServer(t, dict, 42)

	resp := get(t, srv.URL+dictNSID+"?id=42")
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, dict, body)
}

func TestGetZstdDictionary_UnknownID404s(t *testing.T) {
	t.Parallel()
	srv := newDictServer(t, testDict(42), 42)

	resp := get(t, srv.URL+dictNSID+"?id=41")
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	require.Contains(t, string(body), "DictionaryNotFound")
	require.Contains(t, string(body), "current dictionary id is 42",
		"the 404 must tell the client the current ID so it can re-fetch")
}

func TestGetZstdDictionary_BadID400s(t *testing.T) {
	t.Parallel()
	srv := newDictServer(t, testDict(42), 42)

	for _, q := range []string{"?id=abc", "?id=-1", "?id=0"} {
		resp := get(t, srv.URL+dictNSID+q)
		_ = resp.Body.Close()
		require.Contains(t, []int{http.StatusBadRequest, http.StatusNotFound}, resp.StatusCode,
			"query %s must be rejected", q)
		require.NotEqual(t, http.StatusOK, resp.StatusCode)
	}
}

func TestGetZstdDictionary_ConditionalGet(t *testing.T) {
	t.Parallel()
	srv := newDictServer(t, testDict(7), 7)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+dictNSID, nil)
	require.NoError(t, err)
	req.Header.Set("If-None-Match", `"zstd-dict-7"`)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotModified, resp.StatusCode,
		"a matching If-None-Match must yield 304 (CDN/client revalidation)")
}

func TestGetZstdDictionary_UnregisteredWithoutBytes(t *testing.T) {
	t.Parallel()
	s := New(Config{Src: nil, Logger: slog.Default()})
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)

	resp := get(t, srv.URL+dictNSID)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotImplemented, resp.StatusCode,
		"no dictionary configured -> NSID unregistered -> framework 501 MethodNotImplemented")
}
