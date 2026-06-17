package xrpcapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/manifest"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// newBlockTestServer seeds one sealed segment (idx 0) with the given block
// shape and returns a running httptest server + segment path. Mirrors the
// inline httptest.NewServer(s.Handler()) pattern used by the other tests.
func newBlockTestServer(t *testing.T, perBlock, blockCount int) (*httptest.Server, string) {
	t.Helper()
	dir := t.TempDir()
	path := writeSealedSegmentBlocks(t, dir, 0, 1, perBlock, blockCount)
	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	srv := New(Config{Src: m, Logger: slog.Default()})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, path
}

func blockURL(base, segName string, idx int) string {
	return base + "/xrpc/network.bsky.jetstream.getBlock?segment=" + segName +
		"&blockIndex=" + fmt.Sprint(idx)
}

func readXRPCError(t *testing.T, resp *http.Response) string {
	t.Helper()
	var body struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	return body.Error
}

func getHeader(t *testing.T, url, name string) string {
	t.Helper()
	resp := doGet(t, url)
	defer func() { _ = resp.Body.Close() }()
	return resp.Header.Get(name)
}

func getStatus(t *testing.T, url string) int {
	t.Helper()
	resp := doGet(t, url)
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

func requireXRPCError(t *testing.T, url string, status int, name string) {
	t.Helper()
	resp := doGet(t, url)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, status, resp.StatusCode)
	require.Equal(t, name, readXRPCError(t, resp))
}

func TestGetBlock_BytesMatchOnDisk(t *testing.T) {
	t.Parallel()

	ts, path := newBlockTestServer(t, 2, 3)
	segName := ingest.SegmentFilename(0)

	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	hdr, err := segment.ReadSealedHeader(f)
	require.NoError(t, err)
	require.Equal(t, uint32(3), hdr.BlockCount)

	for idx := range 3 {
		resp := doGet(t, blockURL(ts.URL, segName, idx))
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		want, err := segment.ReadBlockFrame(f, hdr, idx)
		require.NoError(t, err)
		require.Equal(t, want, body, "block %d", idx)

		require.Equal(t, fmt.Sprintf("%q", checksumHex(hdr.Checksum)+":"+fmt.Sprint(idx)),
			resp.Header.Get("ETag"))
	}
}

func TestGetBlock_ETagDiffersPerBlock(t *testing.T) {
	t.Parallel()

	ts, _ := newBlockTestServer(t, 2, 2)
	segName := ingest.SegmentFilename(0)
	e0 := getHeader(t, blockURL(ts.URL, segName, 0), "ETag")
	e1 := getHeader(t, blockURL(ts.URL, segName, 1), "ETag")
	require.NotEqual(t, e0, e1)
}

func TestGetBlock_NotModified(t *testing.T) {
	t.Parallel()

	ts, _ := newBlockTestServer(t, 2, 2)
	segName := ingest.SegmentFilename(0)
	url := blockURL(ts.URL, segName, 0)
	etag := getHeader(t, url, "ETag")
	resp := doGetWith(t, url, func(r *http.Request) { r.Header.Set("If-None-Match", etag) })
	require.Equal(t, http.StatusNotModified, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Empty(t, body)
	_ = resp.Body.Close()
}

func TestGetBlock_Range(t *testing.T) {
	t.Parallel()

	ts, path := newBlockTestServer(t, 2, 1)
	segName := ingest.SegmentFilename(0)
	url := blockURL(ts.URL, segName, 0)

	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	hdr, err := segment.ReadSealedHeader(f)
	require.NoError(t, err)
	full, err := segment.ReadBlockFrame(f, hdr, 0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(full), 4)

	resp := doGetWith(t, url, func(r *http.Request) { r.Header.Set("Range", "bytes=1-3") })
	require.Equal(t, http.StatusPartialContent, resp.StatusCode)
	require.Equal(t, fmt.Sprintf("bytes 1-3/%d", len(full)), resp.Header.Get("Content-Range"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, full[1:4], body)
}

func TestGetBlock_CacheControl(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	_ = writeSealedSegmentBlocks(t, dir, 0, 1, 2, 1)
	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	srv := New(Config{Src: m, Logger: slog.Default(), CacheMaxAge: 1500 * time.Millisecond})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	resp := doGet(t, blockURL(ts.URL, ingest.SegmentFilename(0), 0))
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, "public, max-age=2", resp.Header.Get("Cache-Control"))
}

func TestGetBlock_MetricsCountOnlyBytesWrittenOnOK200(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := writeSealedSegmentBlocks(t, dir, 0, 1, 2, 1)
	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	metrics := NewMetrics(prometheus.NewRegistry())
	srv := New(Config{Src: m, Logger: slog.Default(), Metrics: metrics})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	hdr, err := segment.ReadSealedHeader(f)
	require.NoError(t, err)
	frame, err := segment.ReadBlockFrame(f, hdr, 0)
	require.NoError(t, err)

	url := blockURL(ts.URL, ingest.SegmentFilename(0), 0)
	resp := doGet(t, url)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	etag := resp.Header.Get("ETag")
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	_ = resp.Body.Close()

	resp = doGetWith(t, url, func(r *http.Request) { r.Header.Set("If-None-Match", etag) })
	require.Equal(t, http.StatusNotModified, resp.StatusCode)
	_ = resp.Body.Close()

	require.InDelta(t, 2.0, testutil.ToFloat64(metrics.requests.WithLabelValues(resultOK)), 0)
	require.InDelta(t, float64(len(frame)), testutil.ToFloat64(metrics.servedByte), 0)
}

func TestGetBlock_TraceAttributesAndErrors(t *testing.T) {
	t.Parallel()

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	dir := t.TempDir()
	path := writeSealedSegmentBlocks(t, dir, 0, 1, 2, 1)
	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	srv := New(Config{Src: m, Logger: slog.Default(), Tracer: tp.Tracer("test")})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	resp := doGet(t, blockURL(ts.URL, ingest.SegmentFilename(0), 0))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	_ = resp.Body.Close()

	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	hdr, err := segment.ReadSealedHeader(f)
	require.NoError(t, err)
	frame, err := segment.ReadBlockFrame(f, hdr, 0)
	require.NoError(t, err)

	resp = doGet(t, blockURL(ts.URL, ingest.SegmentFilename(99), 0))
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	_ = resp.Body.Close()

	spans := rec.Ended()
	require.Len(t, spans, 2)
	require.Equal(t, codes.Ok, spans[0].Status().Code)
	require.Equal(t, "ok", spanAttr(t, spans[0], "result").AsString())
	require.Equal(t, int64(0), spanAttr(t, spans[0], "segment.idx").AsInt64())
	require.Equal(t, int64(0), spanAttr(t, spans[0], "block.index").AsInt64())
	require.Equal(t, int64(len(frame)), spanAttr(t, spans[0], "block.compressed_size").AsInt64())
	require.Equal(t, codes.Error, spans[1].Status().Code)
	require.Equal(t, "not_found", spanAttr(t, spans[1], "result").AsString())
	require.NotEmpty(t, spans[1].Events(), "error spans should record the XRPC error")
}

func spanAttr(t *testing.T, span sdktrace.ReadOnlySpan, key string) attribute.Value {
	t.Helper()
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value
		}
	}
	t.Fatalf("span missing attribute %q", key)
	return attribute.Value{}
}

func TestGetBlock_Errors(t *testing.T) {
	t.Parallel()

	ts, _ := newBlockTestServer(t, 2, 2)
	segName := ingest.SegmentFilename(0)

	// missing segment -> 400
	require.Equal(t, http.StatusBadRequest,
		getStatus(t, ts.URL+"/xrpc/network.bsky.jetstream.getBlock?blockIndex=0"))
	// empty segment -> 400
	require.Equal(t, http.StatusBadRequest,
		getStatus(t, ts.URL+"/xrpc/network.bsky.jetstream.getBlock?segment=&blockIndex=0"))
	// malformed segment name -> 400
	require.Equal(t, http.StatusBadRequest,
		getStatus(t, ts.URL+"/xrpc/network.bsky.jetstream.getBlock?segment=nope&blockIndex=0"))
	// missing blockIndex -> 400
	require.Equal(t, http.StatusBadRequest,
		getStatus(t, ts.URL+"/xrpc/network.bsky.jetstream.getBlock?segment="+segName))
	// non-integer blockIndex -> 400
	require.Equal(t, http.StatusBadRequest,
		getStatus(t, ts.URL+"/xrpc/network.bsky.jetstream.getBlock?segment="+segName+"&blockIndex=nope"))
	// negative blockIndex -> 400
	require.Equal(t, http.StatusBadRequest,
		getStatus(t, blockURL(ts.URL, segName, -1)))
	// unknown segment -> 404 SegmentNotFound
	missing := ingest.SegmentFilename(99)
	requireXRPCError(t, blockURL(ts.URL, missing, 0), http.StatusNotFound, "SegmentNotFound")
	// blockIndex == block_count -> 404 BlockNotFound
	requireXRPCError(t, blockURL(ts.URL, segName, 2), http.StatusNotFound, "BlockNotFound")
	// blockIndex far past end -> 404 BlockNotFound
	requireXRPCError(t, blockURL(ts.URL, segName, 9999), http.StatusNotFound, "BlockNotFound")
}

func TestGetBlock_ReadinessGate(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	_ = writeSealedSegmentBlocks(t, dir, 0, 1, 2, 1)
	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	srv := New(Config{
		Src:    m,
		Logger: slog.Default(),
		Ready: func(context.Context) error {
			return fmt.Errorf("manifest warming")
		},
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	requireXRPCError(t, blockURL(ts.URL, ingest.SegmentFilename(0), 0),
		http.StatusServiceUnavailable, "ServiceUnavailable")
}

func TestGetBlock_FileDeletedAfterManifestLookupReturns500(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := writeSealedSegmentBlocks(t, dir, 0, 1, 2, 1)
	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	require.NoError(t, os.Remove(path))
	srv := New(Config{Src: m, Logger: slog.Default()})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	requireXRPCError(t, blockURL(ts.URL, ingest.SegmentFilename(0), 0),
		http.StatusInternalServerError, "InternalServerError")
}
