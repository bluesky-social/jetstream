package oracle

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/overlay"
	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
	"github.com/stretchr/testify/require"
)

// fetchOverlay GETs the live getTombstones blob, decodes it, and
// cross-checks the decoded watermark/maxSeq against the response headers.
// It returns the decoded (W, M, snapshot) for an end-to-end overlay
// reconstruction assertion (see CheckOverlayReconstruction).
func fetchOverlay(t *testing.T, cfg Config, run *runtimeRun, baseURL string) (uint64, uint64, tombstone.Snapshot) {
	t.Helper()

	url := baseURL + "/xrpc/network.bsky.jetstream.getTombstones"
	ctx, cancel := context.WithTimeout(context.Background(), oracleWaitTimeout(cfg))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		select {
		case <-run.exited:
			t.Fatalf("runtime exited while fetching overlay: mode=%s seed=%d err=%v", cfg.Mode, cfg.Seed, run.err)
		default:
		}
	}
	require.NoErrorf(t, err, "fetch overlay mode=%s seed=%d", cfg.Mode, cfg.Seed)
	defer func() { _ = resp.Body.Close() }()

	require.Equalf(t, http.StatusOK, resp.StatusCode, "overlay status mode=%s seed=%d", cfg.Mode, cfg.Seed)
	require.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	w, m, snap, err := overlay.Decode(body)
	require.NoErrorf(t, err, "decode overlay blob mode=%s seed=%d", cfg.Mode, cfg.Seed)

	// The W/M headers exist so the future query plan can read the coverage
	// envelope without decompressing; they must agree with the body.
	require.Equal(t, strconv.FormatUint(w, 10), resp.Header.Get("Jetstream-Overlay-Watermark"),
		"overlay watermark header must equal decoded W")
	require.Equal(t, strconv.FormatUint(m, 10), resp.Header.Get("Jetstream-Overlay-Max-Seq"),
		"overlay max-seq header must equal decoded M")

	return w, m, snap
}

// assertOverlayReconstruction reads the durable segment stream and asserts
// the segments(<=W) + overlay((W,M]) + live((M,inf)) reconstruction equals
// the ground-truth live set. Call after graceful shutdown so segments are
// flushed; the fetched blob (W,M,snap) must have been captured while the
// server was still up.
func assertOverlayReconstruction(t *testing.T, dataDir string, cfg Config, w, m uint64, snap tombstone.Snapshot) {
	t.Helper()

	events, err := ObserveSegments(dataDir)
	require.NoErrorf(t, err, "observe segments for overlay reconstruction mode=%s seed=%d", cfg.Mode, cfg.Seed)
	events = EventsSortedBySeq(events)

	require.NoErrorf(t, CheckOverlayReconstruction(events, w, m, snap),
		"overlay reconstruction mismatch: mode=%s seed=%d W=%d M=%d", cfg.Mode, cfg.Seed, w, m)
}
