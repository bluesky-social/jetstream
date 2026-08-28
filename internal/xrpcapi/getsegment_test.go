package xrpcapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/stretchr/testify/require"
)

func getSegURL(base, name string) string {
	return fmt.Sprintf("%s/xrpc/network.bsky.jetstream.getSegment?name=%s", base, name)
}

func TestGetSegment_WholeFile(t *testing.T) {
	t.Parallel()
	s, dir := newTestServer(t, 1)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	name := ingest.SegmentFilename(0)
	resp := doGet(t, getSegURL(ts.URL, name))
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
	require.Equal(t, "bytes", resp.Header.Get("Accept-Ranges"))
	require.Equal(t, "public, no-cache", resp.Header.Get("Cache-Control"))
	require.NotEmpty(t, resp.Header.Get("ETag"))
	require.Empty(t, resp.Header.Get("Content-Encoding"))

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	want := rawFile(t, dir+"/"+name)
	require.Equal(t, want, got, "body must be byte-identical to the on-disk file")
	require.Equal(t, int64(len(want)), resp.ContentLength)
}

func TestGetSegment_CacheControlFromCompactionDeadline(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, 1)
	deadline := time.Now().Add(2 * time.Hour)
	cached := New(Config{
		Src: s.src, Logger: s.logger,
		CompactionCacheGrace: 10 * time.Minute,
		CompactionDeadline:   fixedCompactionDeadline{next: deadline, ok: true},
	})
	ts := httptest.NewServer(cached.Handler())
	defer ts.Close()

	resp := doGet(t, getSegURL(ts.URL, ingest.SegmentFilename(0)))
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assertCacheAgeRange(t, resp.Header.Get("Cache-Control"), int64((2*time.Hour+9*time.Minute)/time.Second), int64((2*time.Hour+10*time.Minute)/time.Second))
}

func TestCacheControlHeader(t *testing.T) {
	t.Parallel()
	require.Equal(t, "public, no-cache", cacheControlHeader(0))
	require.Equal(t, "public, no-cache", cacheControlHeader(-time.Second))
	require.Equal(t, "public, no-cache", cacheControlHeader(500*time.Millisecond))
	require.Equal(t, "public, max-age=3600", cacheControlHeader(time.Hour))
}

func assertCacheAgeRange(t *testing.T, header string, min, max int64) {
	t.Helper()
	const prefix = "public, max-age="
	require.True(t, len(header) > len(prefix) && header[:len(prefix)] == prefix, "unexpected Cache-Control: %q", header)
	var got int64
	_, err := fmt.Sscanf(header[len(prefix):], "%d", &got)
	require.NoError(t, err)
	require.GreaterOrEqual(t, got, min)
	require.LessOrEqual(t, got, max)
}

func TestGetSegment_RangeRequest(t *testing.T) {
	t.Parallel()
	s, dir := newTestServer(t, 1)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	name := ingest.SegmentFilename(0)
	resp := doGetWith(t, getSegURL(ts.URL, name), func(req *http.Request) {
		req.Header.Set("Range", "bytes=0-99")
	})
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusPartialContent, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Len(t, body, 100)
	want := rawFile(t, dir+"/"+name)
	require.Equal(t, want[:100], body)
}

func TestGetSegment_ConditionalNotModified(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, 1)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	name := ingest.SegmentFilename(0)
	first := doGet(t, getSegURL(ts.URL, name))
	_ = first.Body.Close()
	etag := first.Header.Get("ETag")
	require.NotEmpty(t, etag)

	resp := doGetWith(t, getSegURL(ts.URL, name), func(req *http.Request) {
		req.Header.Set("If-None-Match", etag)
	})
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotModified, resp.StatusCode)
}

func TestGetSegment_Errors(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, 1)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	cases := []struct {
		name string
		want int
	}{
		{"", http.StatusBadRequest},
		{"..%2Fetc%2Fpasswd", http.StatusBadRequest},
		{"seg_x.jss", http.StatusBadRequest},
		{"seg_0000000000.txt", http.StatusBadRequest},
		{"seg_00000000zz.jss", http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := doGet(t, getSegURL(ts.URL, tc.name))
			_ = resp.Body.Close()
			require.Equal(t, tc.want, resp.StatusCode)
		})
	}
}

func TestGetSegment_NotFoundErrorName(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, 1)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// A well-formed but absent segment must return the lexicon's declared
	// error name, not the generic NotFound, so clients can match on it.
	resp := doGet(t, getSegURL(ts.URL, "seg_00000000zz.jss"))
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	var body struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "SegmentNotFound", body.Error)
}

func TestGetSegment_DeletedFileRace(t *testing.T) {
	t.Parallel()
	s, dir := newTestServer(t, 1)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	name := ingest.SegmentFilename(0)
	require.NoError(t, os.Remove(dir+"/"+name))

	resp := doGet(t, getSegURL(ts.URL, name))
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}
