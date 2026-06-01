package web_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/status"
	"github.com/bluesky-social/jetstream-v2/internal/web"
	"github.com/stretchr/testify/require"
)

type fakeSnapshotter struct {
	snap *status.Snapshot
	err  error
}

func (f *fakeSnapshotter) Snapshot(_ context.Context) (*status.Snapshot, error) {
	return f.snap, f.err
}

func newFixtureSnap() *status.Snapshot {
	return &status.Snapshot{
		GeneratedAt: time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC),
		Process: status.ProcessInfo{
			Version: "v1.2.3", Commit: "abcdef0", BuiltAt: "2026-05-20",
			StartedAt: time.Date(2026, 5, 25, 11, 0, 0, 0, time.UTC),
			Uptime:    time.Hour, GoVersion: "go1.24",
		},
		Phase: status.PhaseInfo{
			Phase:          lifecycle.PhaseSteadyState,
			PhaseEnteredAt: time.Date(2026, 5, 24, 0, 0, 0, 0, time.UTC),
		},
		Backfill: status.BackfillStats{
			TotalDIDs: 100, Discovered: 10, Complete: 80, Failed: 10,
			PercentComplete: 80.0,
			ListReposCursor: "<script>alert('xss')</script>",
		},
		Live: status.LiveStats{UpstreamCursor: 1234567, NextSeq: 999, BootstrapSeq: 0},
		SegmentAggregate: &status.SegmentAggregate{
			Trees: []status.TreeAggregate{
				{
					Dir:               "/tmp/segments",
					SealedCount:       5,
					ActiveCount:       1,
					CompressedBytes:   1024 * 1024,
					UncompressedBytes: 4 * 1024 * 1024,
					DiskBytes:         5 * 1024 * 1024,
					EventCount:        12345,
					BlockCount:        42,
					LatestSegment: &status.SegmentSummary{
						Index:           42,
						Sealed:          true,
						EventCount:      1234,
						UniqueDIDCount:  567,
						BlockCount:      8,
						CollectionCount: 3,
						MinSeq:          100,
						MaxSeq:          1233,
						MinIndexedAt:    time.Date(2026, 5, 24, 0, 0, 0, 0, time.UTC),
						MaxIndexedAt:    time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC),
						SizeBytes:       512 * 1024,
					},
				},
				{Dir: "/tmp/backfill/live_segments"},
			},
			Collections: []status.CollectionAggregate{
				{NSID: "app.bsky.feed.post", EventCount: 9000, SegmentCount: 5, BlockCount: 30},
				{NSID: "app.bsky.feed.like", EventCount: 3000, SegmentCount: 4, BlockCount: 10},
				{NSID: "app.bsky.graph.follow", EventCount: 345, SegmentCount: 2, BlockCount: 2},
				{NSID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaahhh", EventCount: 1, SegmentCount: 1, BlockCount: 1},
			},
			Network: status.NetworkTotals{
				Segments:          6,
				SealedSegments:    5,
				ActiveSegments:    1,
				Events:            12345,
				Blocks:            42,
				Collections:       3,
				CompressedBytes:   1024 * 1024,
				UncompressedBytes: 4 * 1024 * 1024,
				DiskBytes:         5 * 1024 * 1024,
			},
		},
		Pebble: status.PebbleStats{
			DiskBytes: 5 * 1024 * 1024,
			KeyspaceCounts: map[string]uint64{
				"repo/": 100, "sync/chain/": 50, "sync/host/": 50, "relay/": 1,
			},
		},
		CursorLookback: status.CursorLookbackStats{
			ConfiguredLookback:   36 * time.Hour,
			ManifestSegmentCount: 15,
			OldestRetainedSeq:    5000,
			OldestRetainedAt:     time.Date(2026, 5, 24, 0, 0, 0, 0, time.UTC),
		},
	}
}

func newFixtureSnapBackfilling() *status.Snapshot {
	s := newFixtureSnap()
	s.Backfill.ListReposCursor = ""
	return s
}

func TestHandler_RendersOK(t *testing.T) {
	t.Parallel()
	h, err := web.New(web.Options{
		Snapshotter: &fakeSnapshotter{snap: newFixtureSnap()},
		Now:         func() time.Time { return time.Date(2026, 5, 25, 12, 0, 5, 0, time.UTC) },
	})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "text/html; charset=utf-8", rr.Header().Get("Content-Type"))
	require.Equal(t, "no-store", rr.Header().Get("Cache-Control"))
	require.NotEmpty(t, rr.Header().Get("X-Status-Generated-At"))

	body := rr.Body.String()
	require.Contains(t, body, "jetstream")
	require.Contains(t, body, "v1.2.3")
	require.Contains(t, body, "steady_state")
	require.Contains(t, body, "Backfill")
	require.Contains(t, body, "enumerating repos")
	require.Contains(t, body, "Progress so far")
	require.Contains(t, body, "80.00%")
	require.Contains(t, body, "Discovered so far")
	require.Contains(t, body, "100")
	require.Contains(t, body, "Latest segment")
	require.Contains(t, body, "1,234")        // EventCount via humanInt
	require.Contains(t, body, "567")          // UniqueDIDCount via humanInt64Cast
	require.Contains(t, body, "[100, 1,233]") // Seq range
	require.Contains(t, body, "2026-05-24")
	require.Contains(t, body, "Indexed range")
	require.Contains(t, body, "Cursor lookback")
	require.Contains(t, body, "1d 12h") // 36h formatted by humanDuration
	require.Contains(t, body, "15")     // ManifestSegmentCount
	require.Contains(t, body, "5,000")  // OldestRetainedSeq formatted
	require.Contains(t, body, `class="collections-table"`)
	require.Contains(t, body, "overflow-wrap: anywhere")
}

func TestHandler_RendersBackfillingState(t *testing.T) {
	t.Parallel()
	h, err := web.New(web.Options{
		Snapshotter: &fakeSnapshotter{snap: newFixtureSnapBackfilling()},
	})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	require.Contains(t, body, "80.00%")
	require.Contains(t, body, "Progress")
	require.NotContains(t, body, "enumerating repos")
}

func TestHandler_EscapesXSS(t *testing.T) {
	t.Parallel()
	h, err := web.New(web.Options{
		Snapshotter: &fakeSnapshotter{snap: newFixtureSnap()},
	})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status", nil)
	h.ServeHTTP(rr, req)

	body := rr.Body.String()
	require.NotContains(t, body, "<script>alert('xss')</script>")
	require.True(t,
		strings.Contains(body, "&lt;script&gt;") || strings.Contains(body, "&#x3C;script&#x3E;") || strings.Contains(body, "&#34;"),
		"expected the cursor's HTML to be escaped, body=%s", body)
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()
	h, err := web.New(web.Options{
		Snapshotter: &fakeSnapshotter{snap: newFixtureSnap()},
	})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/status", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
	require.Equal(t, "GET, HEAD", rr.Header().Get("Allow"))
}

func TestHandler_503OnError(t *testing.T) {
	t.Parallel()
	h, err := web.New(web.Options{
		Snapshotter: &fakeSnapshotter{err: errors.New("boom")},
	})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusServiceUnavailable, rr.Code)
	require.Contains(t, rr.Body.String(), "temporarily unavailable")
}

func TestHandler_HEAD(t *testing.T) {
	t.Parallel()
	h, err := web.New(web.Options{
		Snapshotter: &fakeSnapshotter{snap: newFixtureSnap()},
	})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodHead, "/status", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Empty(t, rr.Body.Bytes())
	require.NotEmpty(t, rr.Header().Get("Cache-Control"))
}
