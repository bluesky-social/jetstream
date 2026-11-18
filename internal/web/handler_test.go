package web_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
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
	snap          *status.Snapshot
	err           error
	snapshotCalls int
	lastReq       status.Request
	lastCtx       context.Context
	snapshotFn    func(context.Context, status.Request) (*status.Snapshot, error)
}

func (f *fakeSnapshotter) SnapshotForRequest(ctx context.Context, req status.Request) (*status.Snapshot, error) {
	f.snapshotCalls++
	f.lastReq = req
	f.lastCtx = ctx
	if f.snapshotFn != nil {
		return f.snapshotFn(ctx, req)
	}
	return f.snap, f.err
}

type fakeRepoActions struct {
	verifyReport repoexport.VerifyReport
	verifyErr    error
	verifyCalls  int
	verifyDID    string
	verifyCtx    context.Context
	verifyFn     func(context.Context, string) (repoexport.VerifyReport, error)
}

func (f *fakeRepoActions) VerifyRepo(ctx context.Context, did string) (repoexport.VerifyReport, error) {
	f.verifyCalls++
	f.verifyDID = did
	f.verifyCtx = ctx
	if f.verifyFn != nil {
		return f.verifyFn(ctx, did)
	}
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
			HostsTotal:      1, HostsInFlight: 1, RelayAccountFloor: 120, EnumeratedAccounts: 100,
			TopRemainingHosts: []status.BackfillHostRow{{
				Hostname: "<script>alert('xss')</script>", State: "running",
				RelayAccounts: 120, EnumeratedAccounts: 100, RemainingEstimate: 20,
			}},
			StartedAt:   time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC),
			CompletedAt: time.Date(2026, 5, 23, 7, 5, 0, 0, time.UTC),
			Duration:    3*24*time.Hour + 7*time.Hour + 5*time.Minute,
		},
		Live: status.LiveStats{
			UpstreamCursor:           1234567,
			NextSeq:                  999,
			BootstrapSeq:             0,
			LastSeenUpstreamEventAt:  time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC),
			LastSeenUpstreamEventAge: 5 * time.Second,
		},
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
						MinWitnessedAt:  time.Date(2026, 5, 24, 0, 0, 0, 0, time.UTC),
						MaxWitnessedAt:  time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC),
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
				MinWitnessedAt:    time.Date(2026, 5, 24, 0, 0, 0, 0, time.UTC),
				MaxWitnessedAt:    time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC),
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

func newAccountSnapshot(did string) *status.Snapshot {
	snap := newFixtureSnap()
	snap.Request = status.Request{Tab: "accounts", Account: did}
	snap.Account = status.AccountLookup{Query: did, QueryKind: "did", Found: true, DID: did}
	return snap
}

func foundAccountSnapshotter() *fakeSnapshotter {
	return &fakeSnapshotter{
		snapshotFn: func(_ context.Context, req status.Request) (*status.Snapshot, error) {
			snap := newAccountSnapshot(req.Account)
			snap.Request = req
			return snap, nil
		},
	}
}

func newFixtureSnapBackfilling() *status.Snapshot {
	s := newFixtureSnap()
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
	require.Contains(t, body, "v1.2.3 (commit abcdef0)")
	require.Contains(t, body, "steady_state")
	require.Contains(t, body, "Backfill")
	require.Contains(t, body, "Backfill PDS roster")
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
	require.Contains(t, body, "Last upstream event")
	require.Contains(t, body, "5s ago")
	require.Contains(t, body, "2026-05-24")
	require.Contains(t, body, "Witnessed range")
	require.Contains(t, body, "overflow-wrap: anywhere")
	require.Contains(t, body, "Segment Files")
	require.NotContains(t, strings.ToLower(body), "free bytes")
	require.NotContains(t, strings.ToLower(body), "disk free")
	require.NotContains(t, strings.ToLower(body), "data_dir_free")
	require.NotContains(t, strings.ToLower(body), "capacity")
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

func TestHandler_AccountPageDefersRepositoryVerification(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		method          string
		path            string
		snap            *status.Snapshot
		wantPlaceholder bool
	}{
		{
			name:            "eligible account page",
			method:          http.MethodGet,
			path:            "/status?tab=accounts&account=did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
			snap:            newAccountSnapshot("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"),
			wantPlaceholder: true,
		},
		{name: "overview page", method: http.MethodGet, path: "/status", snap: newFixtureSnap()},
		{
			name:   "missing account",
			method: http.MethodGet,
			path:   "/status?tab=accounts&account=did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
			snap: func() *status.Snapshot {
				snap := newAccountSnapshot("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
				snap.Account.Found = false
				return snap
			}(),
		},
		{
			name:   "HEAD account page",
			method: http.MethodHead,
			path:   "/status?tab=accounts&account=did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
			snap:   newAccountSnapshot("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			verifyStarted := make(chan struct{})
			releaseVerification := make(chan struct{})
			actions := &fakeRepoActions{verifyFn: func(context.Context, string) (repoexport.VerifyReport, error) {
				close(verifyStarted)
				<-releaseVerification
				return repoexport.VerifyReport{}, nil
			}}
			h, err := web.New(web.Options{
				Snapshotter: &fakeSnapshotter{snap: test.snap},
				RepoActions: actions,
			})
			require.NoError(t, err)

			recorder := httptest.NewRecorder()
			request := httptest.NewRequestWithContext(t.Context(), test.method, test.path, nil)
			done := make(chan struct{})
			go func() {
				h.ServeHTTP(recorder, request)
				close(done)
			}()
			select {
			case <-done:
			case <-verifyStarted:
				close(releaseVerification)
				<-done
				t.Fatal("status page waited for slow repository verification")
			}
			require.Equal(t, http.StatusOK, recorder.Code)
			require.Zero(t, actions.verifyCalls, "page rendering must not run repository verification")
			if test.method == http.MethodHead {
				require.Empty(t, recorder.Body.String())
				return
			}
			if test.wantPlaceholder {
				require.Contains(t, recorder.Body.String(), `id="repo-verification"`)
				require.Contains(t, recorder.Body.String(), "Loading…")
				require.Contains(t, recorder.Body.String(), `data-did="did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"`)
			} else {
				require.NotContains(t, recorder.Body.String(), `id="repo-verification"`)
				require.NotContains(t, recorder.Body.String(), "Loading…")
			}
		})
	}
}

func TestHandler_RepoVerificationEndpointRendersOutcomes(t *testing.T) {
	t.Parallel()
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	tests := []struct {
		name         string
		report       repoexport.VerifyReport
		verifyErr    error
		wantStatus   int
		wantContains []string
		wantAbsent   []string
	}{
		{
			name: "matching report",
			report: repoexport.VerifyReport{
				DID: did, Match: true, AuthoritativeRev: "3abc", AuthoritativeRoot: "bafy-authoritative",
				LocalLatestRev: "3abc", LocalRoot: "bafy-authoritative", LocalRecordCount: 42,
			},
			wantStatus:   http.StatusOK,
			wantContains: []string{"match-ok", "authoritative rev", "bafy-authoritative", "local records", "42"},
		},
		{
			name: "mismatch report",
			report: repoexport.VerifyReport{
				DID: did, Match: false, AuthoritativeRev: "3abc", LocalLatestRev: "3def", Message: "root mismatch",
			},
			wantStatus:   http.StatusOK,
			wantContains: []string{"Match failure", "match-fail", "root mismatch"},
		},
		{
			name:         "verification error is escaped",
			verifyErr:    errors.New(`<script>alert("pds failed")</script>`),
			wantStatus:   http.StatusBadGateway,
			wantContains: []string{"Repo verification", "&lt;script&gt;", "pds failed"},
			wantAbsent:   []string{"<script>alert"},
		},
		{
			name:         "verification deadline",
			verifyErr:    context.DeadlineExceeded,
			wantStatus:   http.StatusGatewayTimeout,
			wantContains: []string{"timed out", "Please try again later"},
			wantAbsent:   []string{"match-ok", "match-fail"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			src := foundAccountSnapshotter()
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			actions := &fakeRepoActions{verifyReport: test.report, verifyErr: test.verifyErr}
			h, err := web.New(web.Options{Snapshotter: src, RepoActions: actions, Logger: logger})
			require.NoError(t, err)

			startedAt := time.Now()
			recorder := httptest.NewRecorder()
			request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status/verify_repo?did="+did, nil)
			request.RemoteAddr = "203.0.113.10:1234"
			h.ServeHTTP(recorder, request)

			require.Equal(t, test.wantStatus, recorder.Code)
			require.Equal(t, "text/html; charset=utf-8", recorder.Header().Get("Content-Type"))
			require.Equal(t, "repo-verification", recorder.Header().Get("X-Jetstream-Status-Fragment"))
			require.Equal(t, "no-store", recorder.Header().Get("Cache-Control"))
			body := recorder.Body.String()
			for _, want := range test.wantContains {
				require.Contains(t, body, want)
			}
			for _, absent := range test.wantAbsent {
				require.NotContains(t, body, absent)
			}
			if test.verifyErr != nil {
				require.Contains(t, logs.String(), "status: repository verification failed")
			}
			require.Equal(t, 1, actions.verifyCalls)
			require.Equal(t, did, actions.verifyDID)
			deadline, ok := actions.verifyCtx.Deadline()
			require.True(t, ok, "verification must receive an explicit context deadline")
			require.True(t, deadline.After(startedAt) && deadline.Before(startedAt.Add(30*time.Second)), "deadline %v must be below Headwind's 30s header timeout", deadline)
			snapshotDeadline, ok := src.lastCtx.Deadline()
			require.True(t, ok)
			require.Equal(t, deadline, snapshotDeadline)
		})
	}
}

func TestHandler_RepoVerificationHidesSnapshotError(t *testing.T) {
	t.Parallel()
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	internalError := errors.New("pebble: failed reading /data/private")
	src := &fakeSnapshotter{err: internalError}
	actions := &fakeRepoActions{}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	h, err := web.New(web.Options{Snapshotter: src, RepoActions: actions, Logger: logger})
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status/verify_repo?did="+did, nil)
	request.RemoteAddr = "203.0.113.10:1234"
	h.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Contains(t, recorder.Body.String(), "Repository status is temporarily unavailable")
	require.NotContains(t, recorder.Body.String(), internalError.Error())
	require.Contains(t, logs.String(), internalError.Error())
	require.Equal(t, "repo-verification", recorder.Header().Get("X-Jetstream-Status-Fragment"))
	require.Zero(t, actions.verifyCalls)
}

func TestHandler_RateLimitsRepositoryVerificationRequestsBySourceIP(t *testing.T) {
	t.Parallel()
	const didA = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	const didB = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
	src := foundAccountSnapshotter()
	actions := &fakeRepoActions{verifyReport: repoexport.VerifyReport{Match: true}}
	h, err := web.New(web.Options{
		Snapshotter:         src,
		RepoActions:         actions,
		RepoActionRateLimit: web.RateLimit{Limit: 1, Window: time.Hour},
	})
	require.NoError(t, err)
	tests := []struct {
		name          string
		path          string
		remoteAddr    string
		wantStatus    int
		verifyCalls   int
		snapshotCalls int
		wantText      string
	}{
		{
			name: "account page does not spend verification limit", path: "/status?tab=accounts&account=" + didA,
			remoteAddr: "203.0.113.10:5555", wantStatus: http.StatusOK, verifyCalls: 0, snapshotCalls: 1, wantText: "Loading…",
		},
		{
			name: "first endpoint request is allowed", path: "/status/verify_repo?did=" + didA,
			remoteAddr: "203.0.113.10:5556", wantStatus: http.StatusOK, verifyCalls: 1, snapshotCalls: 2, wantText: "match-ok",
		},
		{
			name: "same source is rate limited", path: "/status/verify_repo?did=" + didB,
			remoteAddr: "203.0.113.10:5557", wantStatus: http.StatusTooManyRequests, verifyCalls: 1, snapshotCalls: 2, wantText: "rate-limited",
		},
		{
			name: "another source remains eligible", path: "/status/verify_repo?did=" + didB,
			remoteAddr: "203.0.113.11:5555", wantStatus: http.StatusOK, verifyCalls: 2, snapshotCalls: 3, wantText: "match-ok",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, test.path, nil)
			request.RemoteAddr = test.remoteAddr
			h.ServeHTTP(recorder, request)
			require.Equal(t, test.wantStatus, recorder.Code)
			require.Contains(t, recorder.Body.String(), test.wantText)
			require.Equal(t, test.verifyCalls, actions.verifyCalls)
			require.Equal(t, test.snapshotCalls, src.snapshotCalls)
		})
	}
}

func TestHandler_CanDisableRepoActionRateLimit(t *testing.T) {
	t.Parallel()
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	actions := &fakeRepoActions{verifyReport: repoexport.VerifyReport{Match: true}}
	h, err := web.New(web.Options{
		Snapshotter:                foundAccountSnapshotter(),
		RepoActions:                actions,
		RepoActionRateLimit:        web.RateLimit{Limit: 1, Window: time.Hour},
		DisableRepoActionRateLimit: true,
	})
	require.NoError(t, err)
	tests := []struct {
		name       string
		remoteAddr string
		wantCalls  int
	}{
		{name: "first request", remoteAddr: "203.0.113.10:5555", wantCalls: 1},
		{name: "repeated request", remoteAddr: "203.0.113.10:5556", wantCalls: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/status/verify_repo?did="+did, nil)
			request.RemoteAddr = test.remoteAddr
			h.ServeHTTP(recorder, request)
			require.Equal(t, http.StatusOK, recorder.Code)
			require.Contains(t, recorder.Body.String(), "match-ok")
			require.Equal(t, test.wantCalls, actions.verifyCalls)
		})
	}
}

func TestHandler_RepoVerificationEndpointValidatesRequests(t *testing.T) {
	t.Parallel()
	const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	tests := []struct {
		name              string
		method            string
		path              string
		actionsConfigured bool
		accountFound      bool
		wantStatus        int
		wantAllow         string
		wantSnapshotCalls int
		wantVerifyCalls   int
		wantText          string
	}{
		{name: "missing DID", method: http.MethodGet, path: "/status/verify_repo", actionsConfigured: true, accountFound: true, wantStatus: http.StatusBadRequest, wantText: "single repository DID"},
		{name: "invalid DID", method: http.MethodGet, path: "/status/verify_repo?did=not-a-did", actionsConfigured: true, accountFound: true, wantStatus: http.StatusBadRequest, wantText: "invalid did"},
		{name: "duplicate DID", method: http.MethodGet, path: "/status/verify_repo?did=" + did + "&did=" + did, actionsConfigured: true, accountFound: true, wantStatus: http.StatusBadRequest, wantText: "single repository DID"},
		{name: "missing local account", method: http.MethodGet, path: "/status/verify_repo?did=" + did, actionsConfigured: true, accountFound: false, wantStatus: http.StatusNotFound, wantSnapshotCalls: 1, wantText: "No local repository"},
		{name: "HEAD never verifies", method: http.MethodHead, path: "/status/verify_repo?did=" + did, actionsConfigured: true, accountFound: true, wantStatus: http.StatusMethodNotAllowed, wantAllow: http.MethodGet},
		{name: "POST is rejected", method: http.MethodPost, path: "/status/verify_repo?did=" + did, actionsConfigured: true, accountFound: true, wantStatus: http.StatusMethodNotAllowed, wantAllow: http.MethodGet},
		{name: "missing verifier", method: http.MethodGet, path: "/status/verify_repo?did=" + did, accountFound: true, wantStatus: http.StatusServiceUnavailable, wantText: "unavailable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshotCalls := 0
			src := &fakeSnapshotter{snapshotFn: func(_ context.Context, req status.Request) (*status.Snapshot, error) {
				snapshotCalls++
				snap := newAccountSnapshot(req.Account)
				snap.Request = req
				snap.Account.Found = test.accountFound
				return snap, nil
			}}
			var actions *fakeRepoActions
			var repoActions web.RepoActions
			if test.actionsConfigured {
				actions = &fakeRepoActions{verifyReport: repoexport.VerifyReport{Match: true}}
				repoActions = actions
			}
			h, err := web.New(web.Options{Snapshotter: src, RepoActions: repoActions})
			require.NoError(t, err)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequestWithContext(t.Context(), test.method, test.path, nil)
			h.ServeHTTP(recorder, request)
			require.Equal(t, test.wantStatus, recorder.Code)
			require.Equal(t, test.wantAllow, recorder.Header().Get("Allow"))
			if test.wantText != "" {
				require.Contains(t, recorder.Body.String(), test.wantText)
			}
			require.Equal(t, test.wantSnapshotCalls, snapshotCalls)
			if actions != nil {
				require.Equal(t, test.wantVerifyCalls, actions.verifyCalls)
			}
		})
	}
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
