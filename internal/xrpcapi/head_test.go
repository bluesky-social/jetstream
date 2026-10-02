package xrpcapi

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func archiveMux(s *Server) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("HEAD /xrpc/network.bsky.jetstream.getSegment", s.HeadHandler(getSegmentNSID))
	mux.Handle("HEAD /xrpc/network.bsky.jetstream.getBlock", s.HeadHandler(getBlockNSID))
	mux.Handle("/xrpc/", s.Handler())
	return mux
}

func responseBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return body
}

func requireHeadParity(t *testing.T, getResp, headResp *http.Response) {
	t.Helper()
	require.Equal(t, http.StatusOK, getResp.StatusCode)
	require.Equal(t, http.StatusOK, headResp.StatusCode)
	require.Empty(t, responseBody(t, headResp))
	for _, name := range []string{"Content-Type", "Accept-Ranges", "Cache-Control", "ETag"} {
		require.Equal(t, getResp.Header.Get(name), headResp.Header.Get(name), name)
	}
	getBody := responseBody(t, getResp)
	require.Equal(t, int64(len(getBody)), getResp.ContentLength)
	require.Equal(t, getResp.ContentLength, headResp.ContentLength)
}

func TestArchiveHead_SegmentParityRangeAndConditional(t *testing.T) {
	t.Parallel()
	s, dir := newTestServer(t, 1)
	ts := httptest.NewServer(archiveMux(s))
	t.Cleanup(ts.Close)
	url := getSegURL(ts.URL, ingest.SegmentFilename(0))

	fullGet := doGet(t, url)
	fullHead := doHead(t, url)
	defer func() {
		_ = fullGet.Body.Close()
		_ = fullHead.Body.Close()
	}()
	requireHeadParity(t, fullGet, fullHead)

	getRange := doGetWith(t, url, func(req *http.Request) { req.Header.Set("Range", "bytes=1-3") })
	headRange := doHeadWith(t, url, func(req *http.Request) { req.Header.Set("Range", "bytes=1-3") })
	require.Equal(t, http.StatusPartialContent, getRange.StatusCode)
	require.Equal(t, http.StatusPartialContent, headRange.StatusCode)
	require.Empty(t, responseBody(t, headRange))
	rangeBody := responseBody(t, getRange)
	require.Equal(t, int64(len(rangeBody)), headRange.ContentLength)
	require.Equal(t, getRange.Header.Get("Content-Range"), headRange.Header.Get("Content-Range"))
	require.Equal(t, rawFile(t, filepath.Join(dir, ingest.SegmentFilename(0)))[1:4], rangeBody)

	replacementDir := t.TempDir()
	replacement := writeSealedSegmentBlocks(t, replacementDir, 0, 100, 1, 2)
	require.NoError(t, os.Rename(replacement, filepath.Join(dir, ingest.SegmentFilename(0))))
	newHead := doHead(t, url)
	newGet := doGet(t, url)
	require.Equal(t, http.StatusOK, newHead.StatusCode)
	require.Equal(t, http.StatusOK, newGet.StatusCode)
	require.Empty(t, responseBody(t, newHead))
	newBody := responseBody(t, newGet)
	require.Equal(t, int64(len(newBody)), newHead.ContentLength)
	require.Equal(t, newHead.Header.Get("ETag"), newGet.Header.Get("ETag"))

	getResp := doGet(t, url)
	etag := getResp.Header.Get("ETag")
	_ = getResp.Body.Close()
	conditional := doHeadWith(t, url, func(req *http.Request) { req.Header.Set("If-None-Match", etag) })
	require.Equal(t, http.StatusNotModified, conditional.StatusCode)
	require.Empty(t, responseBody(t, conditional))
}

func TestArchiveHead_BlockParityRangeAndConditional(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	_ = writeSealedSegmentBlocks(t, dir, 0, 1, 2, 2)
	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	s := New(Config{Src: m, Logger: slog.Default()})
	ts := httptest.NewServer(archiveMux(s))
	t.Cleanup(ts.Close)
	url := blockURL(ts.URL, ingest.SegmentFilename(0), 0)

	fullGet := doGet(t, url)
	fullHead := doHead(t, url)
	defer func() {
		_ = fullGet.Body.Close()
		_ = fullHead.Body.Close()
	}()
	requireHeadParity(t, fullGet, fullHead)

	getRange := doGetWith(t, url, func(req *http.Request) { req.Header.Set("Range", "bytes=1-3") })
	headRange := doHeadWith(t, url, func(req *http.Request) { req.Header.Set("Range", "bytes=1-3") })
	require.Equal(t, http.StatusPartialContent, getRange.StatusCode)
	require.Equal(t, http.StatusPartialContent, headRange.StatusCode)
	require.Empty(t, responseBody(t, headRange))
	rangeBody := responseBody(t, getRange)
	require.Equal(t, int64(len(rangeBody)), headRange.ContentLength)
	require.Equal(t, getRange.Header.Get("Content-Range"), headRange.Header.Get("Content-Range"))

	getResp := doGet(t, url)
	etag := getResp.Header.Get("ETag")
	_ = getResp.Body.Close()
	conditional := doHeadWith(t, url, func(req *http.Request) { req.Header.Set("If-None-Match", etag) })
	require.Equal(t, http.StatusNotModified, conditional.StatusCode)
	require.Empty(t, responseBody(t, conditional))

}

func TestArchiveHead_SegmentValidationAndUnavailableCases(t *testing.T) {
	t.Parallel()
	s, dir := newTestServer(t, 1)
	ts := httptest.NewServer(archiveMux(s))
	t.Cleanup(ts.Close)
	valid := getSegURL(ts.URL, ingest.SegmentFilename(0))

	cases := []struct {
		name string
		url  string
		want int
	}{
		{"missing", ts.URL + "/xrpc/network.bsky.jetstream.getSegment", http.StatusBadRequest},
		{"malformed path", getSegURL(ts.URL, "seg_x.jss"), http.StatusBadRequest},
		{"missing segment", getSegURL(ts.URL, "seg_00000000zz.jss"), http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := doHead(t, tc.url)
			require.Equal(t, tc.want, resp.StatusCode)
			require.Empty(t, responseBody(t, resp))
		})
	}

	require.NoError(t, os.Remove(filepath.Join(dir, ingest.SegmentFilename(0))))
	resp := doHead(t, valid)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	require.Empty(t, responseBody(t, resp))

}

func TestArchiveHead_ReadinessAndCorruption(t *testing.T) {
	t.Parallel()
	s, dir := newTestServer(t, 1)
	readyErr := New(Config{Src: s.src, Logger: s.logger, Ready: lifecycle.ReadinessFunc(func(_ context.Context) error {
		return fmt.Errorf("bootstrap in progress")
	})})
	readyTS := httptest.NewServer(archiveMux(readyErr))
	t.Cleanup(readyTS.Close)
	resp := doHead(t, getSegURL(readyTS.URL, ingest.SegmentFilename(0)))
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Empty(t, responseBody(t, resp))

	path := filepath.Join(dir, ingest.SegmentFilename(0))
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("bad!"), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	corruptTS := httptest.NewServer(archiveMux(s))
	t.Cleanup(corruptTS.Close)
	resp = doHead(t, getSegURL(corruptTS.URL, ingest.SegmentFilename(0)))
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	require.Empty(t, responseBody(t, resp))
}

func TestArchiveHead_BlockValidationAndUnavailableCases(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	_ = writeSealedSegmentBlocks(t, dir, 0, 1, 2, 2)
	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	s := New(Config{Src: m, Logger: slog.Default()})
	ts := httptest.NewServer(archiveMux(s))
	t.Cleanup(ts.Close)
	segName := ingest.SegmentFilename(0)

	cases := []struct {
		name string
		url  string
		want int
	}{
		{"missing segment", ts.URL + "/xrpc/network.bsky.jetstream.getBlock?blockIndex=0", http.StatusBadRequest},
		{"empty segment", blockURL(ts.URL, "", 0), http.StatusBadRequest},
		{"malformed segment", blockURL(ts.URL, "nope", 0), http.StatusBadRequest},
		{"missing block", ts.URL + "/xrpc/network.bsky.jetstream.getBlock?segment=" + segName, http.StatusBadRequest},
		{"non-integer block", ts.URL + "/xrpc/network.bsky.jetstream.getBlock?segment=" + segName + "&blockIndex=nope", http.StatusBadRequest},
		{"negative block", blockURL(ts.URL, segName, -1), http.StatusBadRequest},
		{"missing segment file", blockURL(ts.URL, ingest.SegmentFilename(99), 0), http.StatusNotFound},
		{"block out of range", blockURL(ts.URL, segName, 2), http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := doHead(t, tc.url)
			require.Equal(t, tc.want, resp.StatusCode)
			require.Empty(t, responseBody(t, resp))
		})
	}

	require.NoError(t, os.Remove(dir+"/"+segName))
	resp := doHead(t, blockURL(ts.URL, segName, 0))
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	require.Empty(t, responseBody(t, resp))
}

func TestArchiveHead_BlockCorruptionReadinessAndObservability(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := writeSealedSegmentBlocks(t, dir, 0, 1, 2, 1)
	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)

	metrics := NewMetrics(prometheus.NewRegistry())
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	s := New(Config{Src: m, Logger: slog.Default(), Metrics: metrics, Tracer: tp.Tracer("test")})
	ts := httptest.NewServer(archiveMux(s))
	t.Cleanup(ts.Close)
	url := blockURL(ts.URL, ingest.SegmentFilename(0), 0)

	resp := doHead(t, url)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Empty(t, responseBody(t, resp))
	resp = doHead(t, blockURL(ts.URL, ingest.SegmentFilename(99), 0))
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.Empty(t, responseBody(t, resp))

	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.requests.WithLabelValues(resultOK)), 0)
	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.requests.WithLabelValues(resultNotFound)), 0)
	require.Zero(t, testutil.ToFloat64(metrics.servedByte), "HEAD writes no frame bytes")
	spans := rec.Ended()
	require.Len(t, spans, 2)
	require.Equal(t, codes.Ok, spans[0].Status().Code)
	require.Equal(t, codes.Error, spans[1].Status().Code)
	require.Positive(t, spanAttr(t, spans[0], "block.compressed_size").AsInt64())

	ready := New(Config{Src: m, Logger: slog.Default(), Ready: lifecycle.ReadinessFunc(func(_ context.Context) error {
		return fmt.Errorf("manifest warming")
	})})
	readyTS := httptest.NewServer(archiveMux(ready))
	t.Cleanup(readyTS.Close)
	resp = doHead(t, blockURL(readyTS.URL, ingest.SegmentFilename(0), 0))
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Empty(t, responseBody(t, resp))

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	hdr, err := segment.ReadSealedHeader(f)
	require.NoError(t, err)
	var entry [52]byte
	_, err = f.ReadAt(entry[:], int64(hdr.BlockIndexOffset))
	require.NoError(t, err)
	binary.LittleEndian.PutUint32(entry[8:12], math.MaxUint32)
	_, err = f.WriteAt(entry[:], int64(hdr.BlockIndexOffset))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	resp = doHead(t, url)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	require.Empty(t, responseBody(t, resp))
}

func TestArchiveHead_OpenedGenerationCoherence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := writeSealedSegmentBlocks(t, dir, 0, 1, 2, 1)
	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	s := New(Config{Src: m, Logger: slog.Default()})
	ts := httptest.NewServer(archiveMux(s))
	t.Cleanup(ts.Close)
	url := blockURL(ts.URL, ingest.SegmentFilename(0), 0)

	old := doHead(t, url)
	oldETag := old.Header.Get("ETag")
	require.NoError(t, old.Body.Close())

	replacementDir := t.TempDir()
	replacement := writeSealedSegmentBlocks(t, replacementDir, 0, 100, 1, 2)
	require.NoError(t, os.Rename(replacement, path))

	head := doHead(t, url)
	get := doGet(t, url)
	require.Equal(t, http.StatusOK, head.StatusCode)
	require.Equal(t, http.StatusOK, get.StatusCode)
	require.Empty(t, responseBody(t, head))
	body := responseBody(t, get)
	require.NotEqual(t, oldETag, head.Header.Get("ETag"))
	require.Equal(t, int64(len(body)), head.ContentLength)
	require.Equal(t, head.Header.Get("ETag"), get.Header.Get("ETag"))
}

func TestArchiveHead_RangeFailureAndIfRangeParity(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		makeURL func(string) string
		prepare func(*testing.T, string)
	}{
		{
			name: "segment",
			makeURL: func(base string) string {
				return getSegURL(base, ingest.SegmentFilename(0))
			},
			prepare: func(t *testing.T, _ string) {},
		},
		{
			name: "block",
			makeURL: func(base string) string {
				return blockURL(base, ingest.SegmentFilename(0), 0)
			},
			prepare: func(t *testing.T, dir string) {
				_ = dir
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			if tc.name == "segment" {
				writeSealedSegment(t, dir, 0, 1)
			} else {
				writeSealedSegmentBlocks(t, dir, 0, 1, 2, 2)
			}
			tc.prepare(t, dir)
			m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
			require.NoError(t, err)
			ts := httptest.NewServer(archiveMux(New(Config{Src: m, Logger: slog.Default()})))
			t.Cleanup(ts.Close)
			url := tc.makeURL(ts.URL)

			full := doGet(t, url)
			etag := full.Header.Get("ETag")
			require.NotEmpty(t, etag)
			_ = responseBody(t, full)

			rangeCases := []struct {
				name       string
				rangeValue string
			}{
				{name: "unsatisfiable", rangeValue: "bytes=999999999-"},
				{name: "malformed", rangeValue: "bytes=not-a-range"},
			}
			for _, rangeCase := range rangeCases {
				t.Run(rangeCase.name, func(t *testing.T) {
					getResp := doGetWith(t, url, func(req *http.Request) {
						req.Header.Set("Range", rangeCase.rangeValue)
					})
					headResp := doHeadWith(t, url, func(req *http.Request) {
						req.Header.Set("Range", rangeCase.rangeValue)
					})
					require.Equal(t, getResp.StatusCode, headResp.StatusCode)
					require.Equal(t, getResp.Header.Get("Content-Range"), headResp.Header.Get("Content-Range"))
					require.Equal(t, getResp.Header.Get("Content-Length"), headResp.Header.Get("Content-Length"))
					require.Empty(t, responseBody(t, headResp))
					_ = responseBody(t, getResp)
				})
			}

			for _, ifRange := range []struct {
				name  string
				value string
			}{
				{name: "matching", value: etag},
				{name: "mismatching", value: `"not-current"`},
			} {
				t.Run("if-range-"+ifRange.name, func(t *testing.T) {
					customize := func(req *http.Request) {
						req.Header.Set("Range", "bytes=1-3")
						req.Header.Set("If-Range", ifRange.value)
					}
					getResp := doGetWith(t, url, customize)
					headResp := doHeadWith(t, url, customize)
					require.Equal(t, getResp.StatusCode, headResp.StatusCode)
					require.Equal(t, getResp.Header.Get("Content-Range"), headResp.Header.Get("Content-Range"))
					require.Equal(t, getResp.Header.Get("Content-Length"), headResp.Header.Get("Content-Length"))
					require.Empty(t, responseBody(t, headResp))
					_ = responseBody(t, getResp)
				})
			}
		})
	}
}

func TestArchiveHead_RoutingIsolation(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, 1)
	ts := httptest.NewServer(archiveMux(s))
	t.Cleanup(ts.Close)

	resp := doHead(t, getSegURL(ts.URL, ingest.SegmentFilename(0)))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Empty(t, responseBody(t, resp))
	resp = doHead(t, ts.URL+"/xrpc/network.bsky.jetstream.listSegments")
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	require.Empty(t, responseBody(t, resp))
	resp = doGet(t, getSegURL(ts.URL, ingest.SegmentFilename(0)))
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotEmpty(t, responseBody(t, resp))
}
