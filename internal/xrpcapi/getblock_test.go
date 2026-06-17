package xrpcapi

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/manifest"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
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

func TestGetBlock_BytesMatchOnDisk(t *testing.T) {
	ts, path := newBlockTestServer(t, 2, 3)
	segName := ingest.SegmentFilename(0)

	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	hdr, err := segment.ReadSealedHeader(f)
	require.NoError(t, err)
	require.Equal(t, uint32(3), hdr.BlockCount)

	for idx := 0; idx < 3; idx++ {
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
	ts, _ := newBlockTestServer(t, 2, 2)
	segName := ingest.SegmentFilename(0)
	e0 := doGet(t, blockURL(ts.URL, segName, 0)).Header.Get("ETag")
	e1 := doGet(t, blockURL(ts.URL, segName, 1)).Header.Get("ETag")
	require.NotEqual(t, e0, e1)
}

func TestGetBlock_NotModified(t *testing.T) {
	ts, _ := newBlockTestServer(t, 2, 2)
	segName := ingest.SegmentFilename(0)
	url := blockURL(ts.URL, segName, 0)
	etag := doGet(t, url).Header.Get("ETag")
	resp := doGetWith(t, url, func(r *http.Request) { r.Header.Set("If-None-Match", etag) })
	require.Equal(t, http.StatusNotModified, resp.StatusCode)
}

func TestGetBlock_Errors(t *testing.T) {
	ts, _ := newBlockTestServer(t, 2, 2)
	segName := ingest.SegmentFilename(0)

	// malformed segment name -> 400
	require.Equal(t, http.StatusBadRequest,
		doGet(t, ts.URL+"/xrpc/network.bsky.jetstream.getBlock?segment=nope&blockIndex=0").StatusCode)
	// missing blockIndex -> 400
	require.Equal(t, http.StatusBadRequest,
		doGet(t, ts.URL+"/xrpc/network.bsky.jetstream.getBlock?segment="+segName).StatusCode)
	// negative blockIndex -> 400
	require.Equal(t, http.StatusBadRequest,
		doGet(t, blockURL(ts.URL, segName, -1)).StatusCode)
	// unknown segment -> 404 SegmentNotFound
	missing := ingest.SegmentFilename(99)
	require.Equal(t, http.StatusNotFound, doGet(t, blockURL(ts.URL, missing, 0)).StatusCode)
	// blockIndex == block_count -> 404 BlockNotFound
	require.Equal(t, http.StatusNotFound, doGet(t, blockURL(ts.URL, segName, 2)).StatusCode)
}
