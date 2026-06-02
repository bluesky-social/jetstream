package xrpcapi

import (
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/manifest"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

// doGet issues a context-bound GET (the linter forbids http.Get/NewRequest
// without a context, and it also fails the test fast if the server hangs).
func doGet(t *testing.T, url string) *http.Response {
	t.Helper()
	return doGetWith(t, url, nil)
}

// doGetWith is doGet with an optional hook to set request headers (Range,
// If-None-Match, ...).
func doGetWith(t *testing.T, url string, customize func(*http.Request)) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	if customize != nil {
		customize(req)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// writeSealedSegment writes one sealed seg_<idx>.jss into dir with a few
// events and returns its absolute path.
func writeSealedSegment(t *testing.T, dir string, idx uint64, seqStart uint64) string {
	t.Helper()
	path := filepath.Join(dir, ingest.SegmentFilename(idx))
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 4096})
	require.NoError(t, err)
	for i := uint64(0); i < 4; i++ {
		_, err = w.Append(segment.Event{
			Seq:        seqStart + i,
			IndexedAt:  int64(1_730_000_000_000_000 + (seqStart+i)*1_000),
			Kind:       segment.KindCreate,
			DID:        "did:plc:test",
			Collection: "app.bsky.feed.post",
			Rkey:       "rkey",
			Rev:        "rev",
			Payload:    []byte{0xa0},
		})
		require.NoError(t, err)
	}
	_, err = w.Seal()
	require.NoError(t, err)
	return path
}

// newTestServer seeds n sealed segments (indices 0..n-1) and returns an
// xrpcapi server backed by a real manifest plus the segments dir.
func newTestServer(t *testing.T, n int) (*Server, string) {
	t.Helper()
	dir := t.TempDir()
	for i := 0; i < n; i++ {
		writeSealedSegment(t, dir, uint64(i), uint64(i*4+1))
	}
	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	return New(m, slog.Default()), dir
}

// rawFile reads a segment file's bytes for byte-identical comparison.
func rawFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}
