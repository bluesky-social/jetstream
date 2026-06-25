package web_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/repoexport"
	"github.com/bluesky-social/jetstream/internal/status"
	"github.com/bluesky-social/jetstream/internal/web"
	"github.com/stretchr/testify/require"
)

type fakeSnapshotter struct {
	snap    *status.Snapshot
	err     error
	lastReq status.Request
}

func (f *fakeSnapshotter) SnapshotForRequest(_ context.Context, req status.Request) (*status.Snapshot, error) {
	f.lastReq = req
	return f.snap, f.err
}

type fakeRepoActions struct {
	verifyReport repoexport.VerifyReport
	verifyErr    error
	verifyCalls  int
	verifyDID    string
}

func (f *fakeRepoActions) VerifyRepo(_ context.Context, did string) (repoexport.VerifyReport, error) {
	f.verifyCalls++
	f.verifyDID = did
	return f.verifyReport, f.verifyErr
}

func newFixtureSnap() *status.Snapshot {
	return &status.Snapshot{
		GeneratedAt: time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC),
		Request:     status.Request{Tab: "summary"},
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
			TotalDIDs: 100, Discovered: 10, Complete: 80, Failed: 10, Unavailable: 5,
			PercentComplete: 80.0,
			ListReposCursor: "<script>alert('xss')</script>",
			StartedAt:       time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC),
			CompletedAt:     time.Date(2026, 5, 23, 7, 5, 0, 0, time.UTC),
			Duration:        3*24*time.Hour + 7*time.Hour + 5*time.Minute,
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
				MinSeq:            100,
				MaxSeq:            1233,
				MinIndexedAt:      time.Date(2026, 5, 24, 0, 0, 0, 0, time.UTC),
				MaxIndexedAt:      time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC),
			},
		},
		Pebble: status.PebbleStats{
			DiskBytes: 5 * 1024 * 1024,
			KeyspaceCounts: map[string]uint64{
				"repo/": 100, "sync/chain/": 50, "sync/host/": 50, "relay/": 1,
			},
		},
		Hosts: status.HostDiagnostics{
			Rows: []status.HostRow{{
				Host:             "pds.example.com",
				Total:            10,
				Active:           9,
				Complete:         7,
				Failed:           2,
				LatestError:      "xrpc: HTTP 503",
				LatestErrorClass: "http_5xx",
			}},
			TopFailing: []status.HostRow{{
				Host:             "pds.example.com",
				Total:            10,
				Active:           9,
				Complete:         7,
				Failed:           2,
				LatestError:      "xrpc: HTTP 503",
				LatestErrorClass: "http_5xx",
			}},
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
	src := &fakeSnapshotter{snap: newFixtureSnap()}
	h, err := web.New(web.Options{
		Snapshotter: src,
		Now:         func() time.Time { return time.Date(2026, 5, 25, 12, 0, 5, 0, time.UTC) },
	})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, status.Request{}, src.lastReq)
	require.Equal(t, "text/html; charset=utf-8", rr.Header().Get("Content-Type"))
	require.Equal(t, "no-store", rr.Header().Get("Cache-Control"))
	require.NotEmpty(t, rr.Header().Get("X-Status-Generated-At"))

	body := rr.Body.String()
	require.Contains(t, body, "jetstream")
	require.Contains(t, body, "v1.2.3")
	require.Contains(t, body, "steady_state")
	require.Contains(t, body, "Backfill")
	require.Contains(t, body, "enumerating repos")
	require.NotContains(t, body, `class="bar"`)
	require.NotContains(t, body, "Progress so far")
	require.NotContains(t, body, "Progress")
	require.NotContains(t, body, "80.00%")
	require.Contains(t, body, "Discovered")
	require.Contains(t, body, "100 repos")
	require.Contains(t, body, "Downloaded")
	require.Contains(t, body, "80 repos")
	require.Contains(t, body, "Errored")
	require.Contains(t, body, "10 repos")
	require.Contains(t, body, "Unavailable")
	require.Contains(t, body, "5 repos")
	require.Contains(t, body, "Duration")
	require.Contains(t, body, "3d 7h")
	require.NotContains(t, body, "Metadata store")
	require.NotContains(t, body, "Latest segment")
	require.Contains(t, body, "Cursor lookback")
	require.Contains(t, body, "1d 12h")       // 36h formatted by humanDuration
	require.Contains(t, body, "5,000")        // OldestRetainedSeq formatted
	require.Contains(t, body, "12,345")       // Network event count via humanInt
	require.Contains(t, body, "[100, 1,233]") // Seq range
	require.Contains(t, body, "2026-05-24")
	require.Contains(t, body, "Indexed range")
	require.Contains(t, body, "overflow-wrap: anywhere")
	require.Contains(t, body, "Segment Files")
	require.NotContains(t, body, "Top failing hosts")
	require.NotContains(t, body, `<h2>Collections</h2>`)
}

func TestHandler_RendersUnknownBackfillDurationForOldSteadyStateData(t *testing.T) {
	t.Parallel()
	s := newFixtureSnap()
	s.Backfill.StartedAt = time.Time{}
	s.Backfill.CompletedAt = time.Time{}
	s.Backfill.Duration = 0

	h, err := web.New(web.Options{Snapshotter: &fakeSnapshotter{snap: s}})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	require.Contains(t, body, "Duration")
	require.Contains(t, body, "unknown")
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
	require.NotContains(t, body, `class="bar"`)
	require.NotContains(t, body, "80.00%")
	require.NotContains(t, body, "Progress")
	require.Contains(t, body, "Discovered")
	require.Contains(t, body, "100 repos")
	require.Contains(t, body, "Downloaded")
	require.Contains(t, body, "80 repos")
	require.Contains(t, body, "Errored")
	require.Contains(t, body, "10 repos")
	require.Contains(t, body, "Unavailable")
	require.NotContains(t, body, "enumerating repos")
}

func TestHandler_RendersSegmentFilesWithMissingSegmentTrees(t *testing.T) {
	t.Parallel()
	s := newFixtureSnap()
	s.Request = status.Request{Tab: "segments"}
	s.SegmentAggregate.Trees = nil
	h, err := web.New(web.Options{Snapshotter: &fakeSnapshotter{snap: s}})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status?tab=segments", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Contains(t, rr.Body.String(), "Segment aggregates are not available")
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

func TestHandler_RendersHostsTab(t *testing.T) {
	t.Parallel()
	s := newFixtureSnap()
	s.Request = status.Request{Tab: "hosts", HostSort: "largest"}
	s.Hosts = status.HostDiagnostics{Rows: []status.HostRow{{
		Host:             "pds.example.com",
		Total:            10,
		Active:           9,
		Complete:         7,
		Failed:           2,
		LatestError:      `<script>alert("x")</script>`,
		LatestErrorClass: "http_5xx",
		RecentErrors: []status.HostErrorRow{{
			DID:   "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
			Class: "http_5xx",
			Error: `<script>alert("x")</script>`,
		}},
	}}}
	src := &fakeSnapshotter{snap: s}
	h, err := web.New(web.Options{
		Snapshotter: src,
		Now:         func() time.Time { return time.Date(2026, 5, 25, 12, 0, 5, 0, time.UTC) },
	})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status?tab=hosts", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, status.Request{Tab: "hosts"}, src.lastReq)
	body := rr.Body.String()
	require.Contains(t, body, "Hosts")
	require.Contains(t, body, "host-filter")
	require.Contains(t, body, "Largest hosts")
	require.Contains(t, body, "Failing hosts")
	require.Contains(t, body, `href="/status?tab=hosts&amp;sort=largest"`)
	require.Contains(t, body, `href="/status?tab=hosts&amp;sort=failing"`)
	require.Contains(t, body, "Latest error")
	require.Contains(t, body, "Recent failing repos")
	require.Contains(t, body, `class="host-errors-row"`)
	require.Contains(t, body, `class="host-error-item"`)
	require.Contains(t, body, "sample-did")
	require.Contains(t, body, "account-link")
	require.Contains(t, body, `href="/status?tab=accounts&amp;account=did%3aplc%3aaaaaaaaaaaaaaaaaaaaaaaaa"`)
	require.Contains(t, body, `target="_blank"`)
	require.Contains(t, body, "pds.example.com")
	require.Contains(t, body, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	require.NotContains(t, body, `<script>alert("x")</script>`)
	require.Contains(t, body, "&lt;script&gt;")
}

func TestHandler_RendersAccountsTabWithHandleQuery(t *testing.T) {
	t.Parallel()
	s := newFixtureSnap()
	s.Request = status.Request{Tab: "accounts", Account: "alice.test"}
	s.Account = status.AccountLookup{
		Query:     "alice.test",
		QueryKind: "handle",
		Found:     true,
		DID:       "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
		Handle:    "alice.test",
		Host:      "pds.example.com",
		PDS:       "https://pds.example.com",
		Backfill:  "failed",
		Attempts:  3,
		LastError: "boom",
	}
	src := &fakeSnapshotter{snap: s}
	h, err := web.New(web.Options{Snapshotter: src})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status?tab=accounts&account=alice.test", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, status.Request{Tab: "accounts", Account: "alice.test"}, src.lastReq)
	body := rr.Body.String()
	require.Contains(t, body, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	require.Contains(t, body, `name="account"`)
	require.Contains(t, body, `placeholder="DID or handle"`)
	require.NotContains(t, body, `name="did" class="lookup-input"`)
	require.NotContains(t, body, `name="handle" class="lookup-input"`)
	require.Contains(t, body, "declared handle")
	require.Contains(t, body, "pds.example.com")
	require.NotContains(t, body, "Host context")
	require.Contains(t, body, "boom")
}

func TestHandler_AutoVerifiesFoundAccount(t *testing.T) {
	t.Parallel()
	s := newFixtureSnap()
	s.Request = status.Request{Tab: "accounts", DID: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"}
	s.Account = status.AccountLookup{
		Query:    "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
		Found:    true,
		DID:      "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
		Backfill: "complete",
	}
	actions := &fakeRepoActions{
		verifyReport: repoexport.VerifyReport{
			DID:               "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
			Match:             true,
			AuthoritativeRev:  "3abc",
			AuthoritativeRoot: "bafy-authoritative",
			LocalLatestRev:    "3abc",
			LocalRoot:         "bafy-authoritative",
			LocalRecordCount:  42,
		},
	}
	h, err := web.New(web.Options{Snapshotter: &fakeSnapshotter{snap: s}, RepoActions: actions})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status?tab=accounts&did=did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	require.NotContains(t, body, `name="repo_action" value="verify"`)
	require.NotContains(t, body, ">Verify<")
	require.NotContains(t, body, "Export CAR")
	require.NotContains(t, body, "/status/repo/export")
	require.Equal(t, 1, actions.verifyCalls)
	require.Equal(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", actions.verifyDID)
	require.Contains(t, body, "Repo verification")
	require.Contains(t, body, "Match")
	require.Contains(t, body, "match-ok")
	require.Contains(t, body, "authoritative rev")
	require.Contains(t, body, "3abc")
	require.Contains(t, body, "local records")
	require.Contains(t, body, "42")
}

func TestHandler_RendersRepoVerificationMismatch(t *testing.T) {
	t.Parallel()
	s := newFixtureSnap()
	s.Request = status.Request{Tab: "accounts", DID: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"}
	s.Account = status.AccountLookup{
		Query:    "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
		Found:    true,
		DID:      "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
		Backfill: "complete",
	}
	actions := &fakeRepoActions{
		verifyReport: repoexport.VerifyReport{
			DID:              "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
			Match:            false,
			AuthoritativeRev: "3abc",
			LocalLatestRev:   "3def",
			Message:          "root mismatch",
		},
	}
	h, err := web.New(web.Options{Snapshotter: &fakeSnapshotter{snap: s}, RepoActions: actions})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status?tab=accounts&did=did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	require.Contains(t, body, "Match failure")
	require.Contains(t, body, "match-fail")
	require.Contains(t, body, "root mismatch")
}

func TestHandler_RateLimitsAutomaticRepoVerificationBySourceIP(t *testing.T) {
	t.Parallel()
	s := newFixtureSnap()
	s.Request = status.Request{Tab: "accounts", DID: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"}
	s.Account = status.AccountLookup{
		Query:    "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
		Found:    true,
		DID:      "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
		Backfill: "complete",
	}
	actions := &fakeRepoActions{}
	h, err := web.New(web.Options{
		Snapshotter: &fakeSnapshotter{snap: s},
		RepoActions: actions,
		RepoActionRateLimit: web.RateLimit{
			Limit:  1,
			Window: time.Hour,
		},
	})
	require.NoError(t, err)

	first := httptest.NewRecorder()
	req1 := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status?tab=accounts&did=did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", nil)
	req1.RemoteAddr = "203.0.113.10:5555"
	h.ServeHTTP(first, req1)
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, 1, actions.verifyCalls)

	verify := httptest.NewRecorder()
	req2 := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status?tab=accounts&did=did:plc:bbbbbbbbbbbbbbbbbbbbbbbb", nil)
	req2.RemoteAddr = "203.0.113.10:5556"
	h.ServeHTTP(verify, req2)
	require.Equal(t, http.StatusOK, verify.Code)
	require.Contains(t, verify.Body.String(), "repo action rate limit exceeded")
	require.Equal(t, 1, actions.verifyCalls)

	otherIP := httptest.NewRecorder()
	req3 := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status?tab=accounts&did=did:plc:cccccccccccccccccccccccc", nil)
	req3.RemoteAddr = "203.0.113.11:5555"
	h.ServeHTTP(otherIP, req3)
	require.Equal(t, http.StatusOK, otherIP.Code)
	require.Equal(t, 2, actions.verifyCalls)
}

func TestHandler_CanDisableRepoActionRateLimit(t *testing.T) {
	t.Parallel()
	s := newFixtureSnap()
	s.Request = status.Request{Tab: "accounts", DID: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"}
	s.Account = status.AccountLookup{
		Query:    "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
		Found:    true,
		DID:      "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
		Backfill: "complete",
	}
	actions := &fakeRepoActions{}
	h, err := web.New(web.Options{
		Snapshotter: &fakeSnapshotter{snap: s},
		RepoActions: actions,
		RepoActionRateLimit: web.RateLimit{
			Limit:  1,
			Window: time.Hour,
		},
		DisableRepoActionRateLimit: true,
	})
	require.NoError(t, err)

	first := httptest.NewRecorder()
	req1 := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status?tab=accounts&did=did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", nil)
	req1.RemoteAddr = "203.0.113.10:5555"
	h.ServeHTTP(first, req1)
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, 1, actions.verifyCalls)

	second := httptest.NewRecorder()
	req2 := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status?tab=accounts&did=did:plc:bbbbbbbbbbbbbbbbbbbbbbbb", nil)
	req2.RemoteAddr = "203.0.113.10:5556"
	h.ServeHTTP(second, req2)
	require.Equal(t, http.StatusOK, second.Code)
	require.NotContains(t, second.Body.String(), "repo action rate limit exceeded")
	require.Equal(t, 2, actions.verifyCalls)
}

func TestHandler_DoesNotVerifyMissingAccount(t *testing.T) {
	t.Parallel()
	s := newFixtureSnap()
	s.Request = status.Request{Tab: "accounts", DID: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"}
	s.Account = status.AccountLookup{
		Query: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
		Found: false,
	}
	actions := &fakeRepoActions{}
	h, err := web.New(web.Options{Snapshotter: &fakeSnapshotter{snap: s}, RepoActions: actions})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status?tab=accounts&did=did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Zero(t, actions.verifyCalls)
	require.NotContains(t, rr.Body.String(), "Repo verification")
}

func TestHandler_RendersSegmentFilesTab(t *testing.T) {
	t.Parallel()
	s := newFixtureSnap()
	s.Request = status.Request{Tab: "segments"}
	h, err := web.New(web.Options{Snapshotter: &fakeSnapshotter{snap: s}})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status?tab=segments", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	require.Contains(t, body, "Network segment files")
	require.Contains(t, body, "Latest segment")
	require.Contains(t, body, "1,234") // Latest segment EventCount via humanInt
	require.Contains(t, body, "567")   // UniqueDIDCount via humanInt64Cast
	require.NotContains(t, body, "Cursor lookback")
	require.NotContains(t, body, "<h2>Phase</h2>")
}

func TestHandler_RendersCollectionsTab(t *testing.T) {
	t.Parallel()
	s := newFixtureSnap()
	s.Request = status.Request{Tab: "collections"}
	h, err := web.New(web.Options{Snapshotter: &fakeSnapshotter{snap: s}})
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status?tab=collections", nil)
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	require.Contains(t, body, "<h2>Collections</h2>")
	require.Contains(t, body, "app.bsky.feed.post")
	require.NotContains(t, body, "<h2>Phase</h2>")
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
