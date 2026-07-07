package backfill

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	metastore "github.com/bluesky-social/jetstream/internal/store"
	"github.com/jcalabro/atmos"
	atmosidentity "github.com/jcalabro/atmos/identity"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/gt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func newBackfillAdversitySimulator(t *testing.T, accounts, initialRecords int, faults *simhttp.FaultPlan) (*world.World, *httptest.Server) {
	t.Helper()

	cfg := world.DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Accounts = accounts
	cfg.InitialRecords = initialRecords
	cfg.InitialRecordsMin = initialRecords
	cfg.InitialRecordsMax = initialRecords
	w, err := world.New(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil))))

	srv := httptest.NewServer(simhttp.NewHandlerWithOptions(w, "http://example.test", simhttp.HandlerOptions{
		Faults:             faults,
		ListReposPageLimit: 2,
	}))
	t.Cleanup(srv.Close)
	return w, srv
}

func runBackfillAgainstSimulator(t *testing.T, st *metastore.Store, w *ingest.Writer, srv *httptest.Server, metrics *Metrics) error {
	t.Helper()

	client := srv.Client()
	client.Timeout = 5 * time.Second
	return Run(t.Context(), Config{
		Store:             st,
		Writer:            w,
		HTTPClient:        client,
		RelayURL:          srv.URL,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           metrics,
		BackfillWorkers:   1,
		BackfillBatchSize: 2,
		RetryBaseDelay:    time.Millisecond,
		RetryMaxDelay:     5 * time.Millisecond,
	})
}

func TestRun_GetRepoRepoNotFoundCompletesTerminalFromSimulator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		fault simhttp.GetRepoResponseFault
	}{
		{
			name: "canonical",
			fault: simhttp.GetRepoResponseFault{
				Status:  http.StatusBadRequest,
				Error:   "RepoNotFound",
				Message: "Could not find repo for DID",
			},
		},
		{
			name: "non_canonical",
			fault: simhttp.GetRepoResponseFault{
				Status:  http.StatusBadRequest,
				Error:   "NotFound",
				Message: "Repo not found",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			faults := simhttp.NewFaultPlan()
			w, srv := newBackfillAdversitySimulator(t, 2, 1, faults)
			acct, err := w.LoadAccount(0)
			require.NoError(t, err)
			faults.AddGetRepoResponseFault(string(acct.DID), tc.fault, 1)

			st, writer, _ := newRetryTestWriter(t)
			metrics := NewMetrics(prometheus.NewRegistry())
			require.NoError(t, runBackfillAgainstSimulator(t, st, writer, srv, metrics))

			bs := NewStore(st, metrics)
			rs, err := bs.readRepoStatus(acct.DID)
			require.NoError(t, err)
			require.Equal(t, StatusComplete, rs.Backfill.Status)
			require.Empty(t, rs.Backfill.LastError)
			require.Equal(t, 0, rs.Backfill.Attempts)
			require.Equal(t, 0, rs.Backfill.RetryCount)
			require.True(t, rs.Backfill.NextAttemptAt.IsZero())
			require.False(t, rs.Backfill.CompletedAt.IsZero())
			require.Equal(t, mustURLHost(t, srv.URL), rs.Host)

			hs, ok, err := loadHostStatus(st, mustURLHost(t, srv.URL))
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, uint64(2), hs.Total)
			require.Equal(t, uint64(2), hs.Complete)
			require.Equal(t, uint64(0), hs.Failed)
			require.Empty(t, hs.RecentErrors)
			require.Empty(t, hs.ErrorClassCounts)
			require.Equal(t, 1, faults.GetRepoResponseFaultsFired(string(acct.DID)))
		})
	}
}

func TestRetryRunner_FailedRepoNotFoundCompletesTerminalFromSimulator(t *testing.T) {
	t.Parallel()

	faults := simhttp.NewFaultPlan()
	w, srv := newBackfillAdversitySimulator(t, 1, 1, faults)
	acct, err := w.LoadAccount(0)
	require.NoError(t, err)
	faults.AddGetRepoResponseFault(string(acct.DID), simhttp.GetRepoResponseFault{
		Status:  http.StatusBadRequest,
		Error:   "RepoNotFound",
		Message: "Could not find repo for DID",
	}, 1)

	st, writer, _ := newRetryTestWriter(t)
	metrics := NewMetrics(prometheus.NewRegistry())
	bs := NewStore(st, metrics)
	ctx := context.Background()
	host := mustURLHost(t, srv.URL)
	require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: acct.DID, Active: true}))
	require.NoError(t, bs.OnFail(ctx, acct.DID, host, fmt.Errorf("xrpc: HTTP 503: bootstrap unavailable"), 1))

	r, err := newRetryRunner(RetryConfig{
		Store:         st,
		BackfillStore: bs,
		Writer:        writer,
		HTTPClient:    srv.Client(),
		RelayURL:      srv.URL,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:       metrics,
		Interval:      time.Hour,
		Workers:       1,
		HostWorkers:   1,
		MaxDelay:      24 * time.Hour,
		now:           func() time.Time { return time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC) },
	})
	require.NoError(t, err)
	require.NoError(t, r.processCandidate(ctx, retryCandidate{DID: acct.DID, Host: host, Retry: 0}))

	rs, err := bs.readRepoStatus(acct.DID)
	require.NoError(t, err)
	require.Equal(t, StatusComplete, rs.Backfill.Status)
	require.Empty(t, rs.Backfill.LastError)
	require.Equal(t, 0, rs.Backfill.Attempts)
	require.Equal(t, 0, rs.Backfill.RetryCount)
	require.True(t, rs.Backfill.NextAttemptAt.IsZero())
	require.Equal(t, host, rs.Host)

	hs, ok, err := loadHostStatus(st, host)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), hs.Total)
	require.Equal(t, uint64(1), hs.Complete)
	require.Equal(t, uint64(0), hs.Failed)
	require.NotEmpty(t, hs.RecentErrors, "successful retry must not erase historical bootstrap diagnostics")
	require.NotEmpty(t, hs.ErrorClassCounts)
	require.Equal(t, 1, faults.GetRepoResponseFaultsFired(string(acct.DID)))
}

func TestRetryRunner_RateLimitFromSimulatorParksClampsAndRecovers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		reset     func(time.Time, time.Duration) time.Time
		wantNext  func(time.Time, time.Duration) time.Time
		wantClamp bool
	}{
		{
			name:      "near_future",
			reset:     func(now time.Time, _ time.Duration) time.Time { return now.Add(2 * time.Hour) },
			wantNext:  func(now time.Time, _ time.Duration) time.Time { return now.Add(2 * time.Hour) },
			wantClamp: false,
		},
		{
			name:      "hostile_far_future",
			reset:     func(now time.Time, _ time.Duration) time.Time { return now.Add(1000 * 24 * time.Hour) },
			wantNext:  func(now time.Time, maxDelay time.Duration) time.Time { return now.Add(maxDelay) },
			wantClamp: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			now := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
			maxDelay := 7 * 24 * time.Hour
			reset := tc.reset(now, maxDelay)
			wantNext := tc.wantNext(now, maxDelay).UTC()

			faults := simhttp.NewFaultPlan()
			w, srv := newBackfillAdversitySimulator(t, 2, 1, faults)
			first, err := w.LoadAccount(0)
			require.NoError(t, err)
			second, err := w.LoadAccount(1)
			require.NoError(t, err)
			faults.AddGetRepoResponseFault(string(first.DID), simhttp.GetRepoResponseFault{
				Status:  http.StatusTooManyRequests,
				Error:   "RateLimitExceeded",
				Message: "slow down",
				Headers: map[string]string{
					"RateLimit-Limit":     "100",
					"RateLimit-Remaining": "0",
					"RateLimit-Reset":     fmt.Sprintf("%d", reset.Unix()),
				},
			}, 1)

			st, writer, _ := newRetryTestWriter(t)
			metrics := NewMetrics(prometheus.NewRegistry())
			bs := NewStore(st, metrics)
			host := mustURLHost(t, srv.URL)
			for _, did := range []atmos.DID{first.DID, second.DID} {
				require.NoError(t, bs.OnDiscover(context.Background(), atmossync.ListReposEntry{DID: did, Active: true}))
				require.NoError(t, bs.OnFail(context.Background(), did, host, fmt.Errorf("xrpc: HTTP 503: bootstrap unavailable"), 1))
			}

			currentNow := now
			r, err := newRetryRunner(RetryConfig{
				Store:         st,
				BackfillStore: bs,
				Writer:        writer,
				HTTPClient:    srv.Client(),
				RelayURL:      srv.URL,
				Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
				Metrics:       metrics,
				Interval:      time.Hour,
				Workers:       1,
				HostWorkers:   1,
				MaxDelay:      maxDelay,
				now:           func() time.Time { return currentNow },
			})
			require.NoError(t, err)

			require.NoError(t, r.processCandidate(context.Background(), retryCandidate{DID: first.DID, Host: host, Retry: 0}))
			rs, err := bs.readRepoStatus(first.DID)
			require.NoError(t, err)
			require.Equal(t, StatusFailed, rs.Backfill.Status)
			require.Equal(t, wantNext, rs.Backfill.NextAttemptAt)
			require.Equal(t, 1, faults.GetRepoResponseFaultsFired(string(first.DID)))
			require.True(t, r.isHostParked(host, currentNow))

			hs, ok, err := loadHostStatus(st, host)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, ErrorClassHTTP429, hs.LatestErrorClass)
			require.Equal(t, uint64(1), hs.ErrorClassCounts[ErrorClassHTTP429])
			require.NotEmpty(t, hs.RecentErrors)
			require.Equal(t, ErrorClassHTTP429, hs.RecentErrors[0].Class)

			require.NoError(t, r.processCandidate(context.Background(), retryCandidate{DID: second.DID, Host: host, Retry: 0}))
			secondRS, err := bs.readRepoStatus(second.DID)
			require.NoError(t, err)
			require.Equal(t, StatusFailed, secondRS.Backfill.Status)
			require.Equal(t, wantNext, secondRS.Backfill.NextAttemptAt)
			require.InDelta(t, 1.0, testutil.ToFloat64(metrics.RetrySkippedHostParked), 0)

			currentNow = wantNext.Add(time.Millisecond)
			require.NoError(t, r.processCandidate(context.Background(), retryCandidate{DID: first.DID, Host: host, Retry: rs.Backfill.RetryCount}))
			rs, err = bs.readRepoStatus(first.DID)
			require.NoError(t, err)
			require.Equal(t, StatusComplete, rs.Backfill.Status)
			require.True(t, rs.Backfill.NextAttemptAt.IsZero())
			require.Empty(t, rs.Backfill.LastError)
			require.Equal(t, tc.wantClamp, reset.After(now.Add(maxDelay)))
		})
	}
}

func TestRun_GetRepoRedirectRecordsFinalHostFromSimulator(t *testing.T) {
	t.Parallel()

	faults := simhttp.NewFaultPlan()
	w, source := newBackfillAdversitySimulator(t, 1, 1, faults)
	target := httptest.NewServer(simhttp.NewHandler(w, "http://example.test"))
	t.Cleanup(target.Close)

	acct, err := w.LoadAccount(0)
	require.NoError(t, err)
	faults.AddGetRepoResponseFault(string(acct.DID), simhttp.GetRepoResponseFault{
		Status:           http.StatusFound,
		RedirectLocation: target.URL + "/xrpc/com.atproto.sync.getRepo?did=" + string(acct.DID),
	}, 1)

	st, writer, _ := newRetryTestWriter(t)
	metrics := NewMetrics(prometheus.NewRegistry())
	require.NoError(t, runBackfillAgainstSimulator(t, st, writer, source, metrics))

	bs := NewStore(st, metrics)
	rs, err := bs.readRepoStatus(acct.DID)
	require.NoError(t, err)
	require.Equal(t, StatusComplete, rs.Backfill.Status)
	require.Equal(t, mustURLHost(t, target.URL), rs.Host)
	require.NotEqual(t, mustURLHost(t, source.URL), rs.Host)
	require.Equal(t, 1, faults.GetRepoResponseFaultsFired(string(acct.DID)))

	targetHS, ok, err := loadHostStatus(st, mustURLHost(t, target.URL))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), targetHS.Complete)
}

func TestRun_ListReposDuplicateAndShrinkPagesConvergeFromSimulator(t *testing.T) {
	t.Parallel()

	faults := simhttp.NewFaultPlan()
	// BackfillBatchSize is 2 in runBackfillAgainstSimulator. The first
	// request is shrunk to one entry and returns cursor=1. The next normal
	// page returns DIDs 1 and 2 with cursor=3. The duplicate_previous_page
	// fault on cursor=3 serves DIDs 1 and 2 again but repeats cursor=3, so the
	// following healed request serves DIDs 3 and 4. This proves duplicate
	// discovery is idempotent without skipping the requested range.
	faults.AddListReposFault("", simhttp.ListReposFaultShrinkPage, 1)
	faults.AddListReposFault("3", simhttp.ListReposFaultDuplicatePreviousPage, 1)
	w, srv := newBackfillAdversitySimulator(t, 5, 1, faults)

	st, writer, _ := newRetryTestWriter(t)
	metrics := NewMetrics(prometheus.NewRegistry())
	require.NoError(t, runBackfillAgainstSimulator(t, st, writer, srv, metrics))

	bs := NewStore(st, metrics)
	for i := 0; i < 5; i++ {
		acct, err := w.LoadAccount(i)
		require.NoError(t, err)
		rs, err := bs.readRepoStatus(acct.DID)
		require.NoError(t, err)
		require.Equalf(t, StatusComplete, rs.Backfill.Status, "account %d", i)
	}
	counts, ok, err := LoadCounts(st)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(5), counts.Total)
	require.Equal(t, uint64(5), counts.Complete)
	require.Equal(t, 1, faults.ListReposFaultsFired(""))
	require.Equal(t, 1, faults.ListReposFaultsFired("3"))
}

func TestRun_ListReposBoundedCursorLoopConvergesFromSimulator(t *testing.T) {
	t.Parallel()

	faults := simhttp.NewFaultPlan()
	// The cursor loop fires once on the second page. Because the fault budget
	// heals, the next request for cursor=2 receives the same page with a
	// terminal cursor. The duplicate entries must not double-count discovery.
	faults.AddListReposFault("2", simhttp.ListReposFaultCursorLoop, 1)
	w, srv := newBackfillAdversitySimulator(t, 4, 1, faults)

	st, writer, _ := newRetryTestWriter(t)
	metrics := NewMetrics(prometheus.NewRegistry())
	require.NoError(t, runBackfillAgainstSimulator(t, st, writer, srv, metrics))

	bs := NewStore(st, metrics)
	for i := 0; i < 4; i++ {
		acct, err := w.LoadAccount(i)
		require.NoError(t, err)
		rs, err := bs.readRepoStatus(acct.DID)
		require.NoError(t, err)
		require.Equalf(t, StatusComplete, rs.Backfill.Status, "account %d", i)
	}
	counts, ok, err := LoadCounts(st)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(4), counts.Total)
	require.Equal(t, uint64(4), counts.Complete)
	require.Equal(t, 1, faults.ListReposFaultsFired("2"))
}

func TestSelectedRepoIdentityMetadataPLCFaultsFromSimulator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		mode          simhttp.PLCFaultMode
		wantHost      string
		wantHandle    string
		wantError     bool
		wantNoRepoRow bool
	}{
		{
			name:       "missing_pds_endpoint",
			mode:       simhttp.PLCFaultMissingPDSEndpoint,
			wantHost:   HostBucketInvalidPDS,
			wantHandle: "user-0.test",
		},
		{
			name:       "malformed_handle",
			mode:       simhttp.PLCFaultMalformedHandle,
			wantHandle: "",
		},
		{
			name:       "malformed_pds_endpoint",
			mode:       simhttp.PLCFaultMalformedPDSEndpoint,
			wantHost:   HostBucketInvalidPDS,
			wantHandle: "user-0.test",
		},
		{
			name:          "resolution_failure",
			mode:          simhttp.PLCFaultResolutionFailure,
			wantError:     true,
			wantNoRepoRow: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := world.DefaultConfig()
			cfg.DataDir = t.TempDir()
			cfg.Accounts = 1
			cfg.InitialRecords = 1
			cfg.InitialRecordsMin = 1
			cfg.InitialRecordsMax = 1
			w, err := world.New(t.Context(), cfg)
			require.NoError(t, err)
			t.Cleanup(func() { _ = w.Close() })
			_, err = w.EnsureSeed()
			require.NoError(t, err)
			require.NoError(t, w.Bootstrap(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil))))
			acct, err := w.LoadAccount(0)
			require.NoError(t, err)

			faults := simhttp.NewFaultPlan()
			faults.AddPLCFault(string(acct.DID), tc.mode, 1)
			srv := httptest.NewServer(nil)
			srv.Config.Handler = simhttp.NewHandlerWithOptions(w, srv.URL, simhttp.HandlerOptions{Faults: faults})
			t.Cleanup(srv.Close)

			st, _, _ := newRetryTestWriter(t)
			bs := NewStore(st, nil)
			var gotErrors []error
			r := selectedRunner{cfg: selectedReposConfig{
				Store: bs,
				IdentityResolver: &atmosidentity.DefaultResolver{
					PLCURL:     gt.Some(srv.URL),
					HTTPClient: gt.Some(http.DefaultClient),
				},
				OnError: func(_ atmos.DID, err error) {
					gotErrors = append(gotErrors, err)
				},
			}}

			require.NoError(t, r.recordIdentityMetadata(t.Context(), acct.DID))
			require.Equal(t, 1, faults.PLCFaultsFired(string(acct.DID)))
			if tc.wantError {
				require.NotEmpty(t, gotErrors)
			} else {
				require.Empty(t, gotErrors)
			}
			if tc.wantNoRepoRow {
				_, ok, err := LoadRepoStatus(st, acct.DID)
				require.NoError(t, err)
				require.False(t, ok)
				return
			}

			rs, ok, err := LoadRepoStatus(st, acct.DID)
			require.NoError(t, err)
			require.True(t, ok)
			wantHost := tc.wantHost
			if wantHost == "" {
				wantHost = mustURLHost(t, srv.URL)
			}
			require.Equal(t, wantHost, rs.Host)
			require.Equal(t, tc.wantHandle, rs.Handle)
			require.Equal(t, StatusNotStarted, rs.Backfill.Status)
		})
	}
}
