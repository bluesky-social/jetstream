package xrpcapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bluesky-social/jetstream/internal/overlay"
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/stretchr/testify/require"
)

type fakeOverlay struct{ blob *overlay.Blob }

func (f *fakeOverlay) Current() *overlay.Blob { return f.blob }
func (f *fakeOverlay) ObserveServe(int)       {}

func newOverlayTestServer(t *testing.T, snap tombstone.Snapshot, w, m uint64) *Server {
	t.Helper()
	s, _ := newTestServer(t, 1)
	blob := &overlay.Blob{
		Bytes:     overlay.Encode(snap, w, m),
		ETag:      `"abc123"`,
		Watermark: w, MaxSeq: m,
	}
	return New(Config{Src: s.src, Logger: s.logger, Overlay: &fakeOverlay{blob: blob}})
}

func tombURL(base string) string {
	return base + "/xrpc/network.bsky.jetstream.getTombstones"
}

func TestGetTombstones_ServesBlob(t *testing.T) {
	t.Parallel()
	snap := tombstone.Snapshot{
		Records: map[tombstone.RecordKey]uint64{
			{DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1"}: 110,
		},
		DIDs: map[string]tombstone.DIDTombstone{},
	}
	s := newOverlayTestServer(t, snap, 100, 110)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp := doGet(t, tombURL(ts.URL))
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
	require.Equal(t, `"abc123"`, resp.Header.Get("ETag"))
	require.Equal(t, "100", resp.Header.Get("Jetstream-Overlay-Watermark"))
	require.Equal(t, "110", resp.Header.Get("Jetstream-Overlay-Max-Seq"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, overlay.Encode(snap, 100, 110), body)
}

func TestGetTombstones_EmptyOverlay(t *testing.T) {
	t.Parallel()
	s := newOverlayTestServer(t, tombstone.Snapshot{
		Records: map[tombstone.RecordKey]uint64{}, DIDs: map[string]tombstone.DIDTombstone{},
	}, 200, 200)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp := doGet(t, tombURL(ts.URL))
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NotEmpty(t, body, "empty overlay still has framing + zstd frame")
}

func TestGetTombstones_ReadinessGate(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, 1)
	blob := &overlay.Blob{Bytes: overlay.Encode(tombstone.Snapshot{
		Records: map[tombstone.RecordKey]uint64{}, DIDs: map[string]tombstone.DIDTombstone{}}, 0, 0), ETag: `"x"`}
	gated := New(Config{Src: s.src, Logger: s.logger, Ready: func(_ context.Context) error {
		return errors.New("bootstrap in progress")
	}, Overlay: &fakeOverlay{blob: blob}})
	ts := httptest.NewServer(gated.Handler())
	defer ts.Close()

	resp := doGet(t, tombURL(ts.URL))
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}
