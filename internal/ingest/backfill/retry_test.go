package backfill

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	metastore "github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/stretchr/testify/require"
)

func newRetryTestWriter(t *testing.T) (*metastore.Store, *ingest.Writer, string) {
	t.Helper()
	dir := t.TempDir()
	st, err := metastore.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	segmentsDir := filepath.Join(dir, "segments")
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       segmentsDir,
		Store:             st,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return st, w, segmentsDir
}

func TestRetryRunner_ScanDueFiltersCandidates(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)

	rows := map[atmos.DID]*RepoStatus{
		"did:plc:due": {
			Backfill: RepoBackfillStatus{Status: StatusFailed, RetryCount: 2, NextAttemptAt: now.Add(-time.Minute)},
			Host:     "pds-a.example.com",
			Active:   true,
		},
		"did:plc:unset": {
			Backfill: RepoBackfillStatus{Status: StatusFailed},
			Host:     "pds-b.example.com",
			Active:   true,
		},
		"did:plc:future": {
			Backfill: RepoBackfillStatus{Status: StatusFailed, NextAttemptAt: now.Add(time.Hour)},
			Host:     "pds-c.example.com",
			Active:   true,
		},
		"did:plc:inactive": {
			Backfill: RepoBackfillStatus{Status: StatusFailed},
			Host:     "pds-d.example.com",
			Active:   false,
		},
		"did:plc:complete": {
			Backfill: RepoBackfillStatus{Status: StatusComplete},
			Host:     "pds-e.example.com",
			Active:   true,
		},
	}
	for did, rs := range rows {
		require.NoError(t, s.putRepoStatus(did, rs))
	}

	r := &retryRunner{cfg: RetryConfig{Store: s.db}}
	var got []retryCandidate
	require.NoError(t, r.scanDue(context.Background(), now, func(c retryCandidate) error {
		got = append(got, c)
		return nil
	}))
	slices.SortFunc(got, func(a, b retryCandidate) int {
		return cmp.Compare(string(a.DID), string(b.DID))
	})

	require.Equal(t, []retryCandidate{
		{DID: "did:plc:due", Host: "pds-a.example.com", Retry: 2},
		{DID: "did:plc:unset", Host: "pds-b.example.com", Retry: 0},
	}, got)
}

func TestRetryRunner_ScanDueRejectsCorruptRows(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	require.NoError(t, s.db.Set(repoKey("did:plc:corrupt"), []byte("not json"), pebble.Sync))

	r := &retryRunner{cfg: RetryConfig{Store: s.db}}
	err := r.scanDue(context.Background(), time.Now(), func(retryCandidate) error { return nil })
	require.ErrorContains(t, err, "decode RepoStatus")
}

func TestRetryRunner_ScanDueSkipsLegacyPending(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)

	rows := map[atmos.DID]*RepoStatus{
		"did:plc:pending-due": {
			Backfill: RepoBackfillStatus{Status: StatusPending},
			Host:     "pds-a.example.com",
			Active:   true,
		},
		"did:plc:pending-inactive": {
			Backfill: RepoBackfillStatus{Status: StatusPending},
			Host:     "pds-b.example.com",
			Active:   false,
		},
		"did:plc:pending-future": {
			Backfill: RepoBackfillStatus{Status: StatusPending, NextAttemptAt: now.Add(time.Hour)},
			Host:     "pds-c.example.com",
			Active:   true,
		},
	}
	for did, rs := range rows {
		require.NoError(t, s.putRepoStatus(did, rs))
	}

	r := &retryRunner{cfg: RetryConfig{Store: s.db}}
	var got []retryCandidate
	require.NoError(t, r.scanDue(context.Background(), now, func(c retryCandidate) error {
		got = append(got, c)
		return nil
	}))

	require.Empty(t, got, "pending rows must not trigger the steady-state failed-repo retry loop")
}

func TestRunPendingRepoRetryPassProcessesOnlyPending(t *testing.T) {
	t.Parallel()
	st, w, segmentsDir := newRetryTestWriter(t)
	bs := NewStore(st, nil)
	ctx := context.Background()
	pending := atmos.DID("did:plc:pending-only")
	failed := atmos.DID("did:plc:failed-skip")
	fixtures := map[atmos.DID]repoFixture{
		pending: buildRepoFixture(t, pending),
		failed:  buildRepoFixture(t, failed),
	}
	srv := newStubServer(t, fixtures)

	require.NoError(t, bs.putRepoStatus(pending, &RepoStatus{
		Backfill: RepoBackfillStatus{Status: StatusPending},
		Active:   true,
	}))
	require.NoError(t, bs.putRepoStatus(failed, &RepoStatus{
		Backfill: RepoBackfillStatus{Status: StatusFailed},
		Active:   true,
	}))

	require.NoError(t, RunPendingRepoRetryPass(ctx, RetryConfig{
		Store:       st,
		Writer:      w,
		HTTPClient:  srv.srv.Client(),
		RelayURL:    srv.srv.URL,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Workers:     1,
		HostWorkers: 1,
		MaxDelay:    24 * time.Hour,
	}))

	events := collectActiveEvents(t, filepath.Join(segmentsDir, ingest.SegmentFilename(0)))
	require.Len(t, events, 2)
	require.Equal(t, segment.KindSync, events[0].Kind)
	require.Equal(t, segment.KindCreateResync, events[1].Kind)
	require.Equal(t, string(pending), events[0].DID)
	require.Equal(t, string(pending), events[1].DID)

	rs, err := bs.readRepoStatus(pending)
	require.NoError(t, err)
	require.Equal(t, StatusComplete, rs.Backfill.Status)
	rs, err = bs.readRepoStatus(failed)
	require.NoError(t, err)
	require.Equal(t, StatusFailed, rs.Backfill.Status)
}

// TestRunPendingRepoRetryPassReschedulesOnConfiguredInterval guards the merge
// crash-recovery call site (orchestrator/merge.go): RunPendingRepoRetryPass must
// forward Interval so a transient getRepo failure reschedules the interrupted
// bootstrap repo on the configured failed-repo cadence. If Interval is omitted,
// selectedBackoffDelay(base=0) falls straight through to MaxDelay, deferring a
// crash-recovery repo ~7 days instead of hours (#262).
func TestRunPendingRepoRetryPassReschedulesOnConfiguredInterval(t *testing.T) {
	t.Parallel()
	st, w, _ := newRetryTestWriter(t)
	bs := NewStore(st, nil)
	ctx := context.Background()
	did := atmos.DID("did:plc:pending-transient-fail")
	now := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)

	srv := newStubServer(t, map[atmos.DID]repoFixture{})
	srv.failGetRepo = map[atmos.DID]bool{did: true}
	srv.failGetRepoCode = http.StatusServiceUnavailable

	require.NoError(t, bs.putRepoStatus(did, &RepoStatus{
		Backfill: RepoBackfillStatus{Status: StatusPending},
		Active:   true,
	}))

	const interval = time.Hour
	const maxDelay = 7 * 24 * time.Hour
	require.NoError(t, RunPendingRepoRetryPass(ctx, RetryConfig{
		Store:       st,
		Writer:      w,
		HTTPClient:  srv.srv.Client(),
		RelayURL:    srv.srv.URL,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Interval:    interval,
		Workers:     1,
		HostWorkers: 1,
		MaxDelay:    maxDelay,
		now:         func() time.Time { return now },
		jitter:      func(int64) int64 { return 0 },
	}))

	rs, err := bs.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, StatusFailed, rs.Backfill.Status)
	// retryCount==0 => base<<0 == Interval, jitter pinned to 0.
	require.Equal(t, now.Add(interval).UTC(), rs.Backfill.NextAttemptAt,
		"transient failure must reschedule on the configured interval, not MaxDelay")
	require.True(t, rs.Backfill.NextAttemptAt.Before(now.Add(maxDelay)),
		"reschedule must be well short of MaxDelay")
}

func TestRetryRunner_SuccessAppendsResyncAndCompletes(t *testing.T) {
	t.Parallel()
	st, w, segmentsDir := newRetryTestWriter(t)
	bs := NewStore(st, nil)
	ctx := context.Background()
	did := atmos.DID("did:plc:retryok")
	fixture := buildRepoFixture(t, did)
	srv := newStubServer(t, map[atmos.DID]repoFixture{did: fixture})
	host := mustURLHost(t, srv.srv.URL)
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)

	require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
	require.NoError(t, bs.OnFail(ctx, did, host, errors.New("xrpc: HTTP 503: bootstrap unavailable"), 1))
	require.NoError(t, bs.RecordRetryFailure(ctx, did, host, errors.New("xrpc 503 unavailable"), now.Add(-time.Hour)))

	r, err := newRetryRunner(RetryConfig{
		Store:       st,
		Writer:      w,
		HTTPClient:  srv.srv.Client(),
		RelayURL:    srv.srv.URL,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Interval:    time.Hour,
		Workers:     1,
		HostWorkers: 1,
		MaxDelay:    24 * time.Hour,
		now:         func() time.Time { return now },
	})
	require.NoError(t, err)
	require.NoError(t, r.runPass(ctx))

	events := collectActiveEvents(t, filepath.Join(segmentsDir, ingest.SegmentFilename(0)))
	require.Len(t, events, 2)
	require.Equal(t, segment.KindSync, events[0].Kind)
	require.Equal(t, segment.KindCreateResync, events[1].Kind)
	require.Equal(t, string(did), events[0].DID)
	require.Equal(t, string(did), events[1].DID)
	require.Equal(t, fixture.did, atmos.DID(events[1].DID))

	rs, err := bs.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, StatusComplete, rs.Backfill.Status)
	require.Equal(t, 0, rs.Backfill.Attempts)
	require.Equal(t, 0, rs.Backfill.RetryCount)
	require.True(t, rs.Backfill.NextAttemptAt.IsZero())
	require.Empty(t, rs.Backfill.LastError)
}

func TestRetryRunner_RateLimitParksHost(t *testing.T) {
	t.Parallel()
	st, w, _ := newRetryTestWriter(t)
	bs := NewStore(st, nil)
	ctx := context.Background()
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	reset := now.Add(2 * time.Hour)
	var hits atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/xrpc/com.atproto.sync.getRepo", r.URL.Path)
		hits.Add(1)
		w.Header().Set("RateLimit-Limit", "100")
		w.Header().Set("RateLimit-Remaining", "0")
		w.Header().Set("RateLimit-Reset", fmt.Sprintf("%d", reset.Unix()))
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"RateLimitExceeded"}`))
	}))
	t.Cleanup(srv.Close)
	host := mustURLHost(t, srv.URL)

	first := atmos.DID("did:plc:ratelimitone")
	second := atmos.DID("did:plc:ratelimittwo")
	for _, did := range []atmos.DID{first, second} {
		require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
		require.NoError(t, bs.OnFail(ctx, did, host, errors.New("xrpc: HTTP 503: bootstrap unavailable"), 1))
		require.NoError(t, bs.RecordRetryFailure(ctx, did, host, errors.New("xrpc 503 unavailable"), now.Add(-time.Hour)))
	}

	r, err := newRetryRunner(RetryConfig{
		Store:       st,
		Writer:      w,
		HTTPClient:  srv.Client(),
		RelayURL:    srv.URL,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Interval:    time.Hour,
		Workers:     1,
		HostWorkers: 1,
		MaxDelay:    24 * time.Hour,
		now:         func() time.Time { return now },
	})
	require.NoError(t, err)

	require.NoError(t, r.processCandidate(ctx, retryCandidate{DID: first, Host: host, Retry: 1}))
	require.True(t, r.isHostParked(host, now))
	require.Equal(t, int64(1), hits.Load())

	rs, err := bs.readRepoStatus(first)
	require.NoError(t, err)
	require.Equal(t, reset, rs.Backfill.NextAttemptAt)

	require.NoError(t, r.processCandidate(ctx, retryCandidate{DID: second, Host: host, Retry: 1}))
	require.Equal(t, int64(1), hits.Load(), "parked host should suppress additional same-host attempts")

	rs, err = bs.readRepoStatus(second)
	require.NoError(t, err)
	require.Equal(t, reset, rs.Backfill.NextAttemptAt)
	require.Equal(t, 1, rs.Backfill.RetryCount, "a parked skip is not a failed attempt")
}

func TestRetryRunner_NextAttemptAtClampsRetryAfter(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	maxDelay := 7 * 24 * time.Hour
	r := &retryRunner{cfg: RetryConfig{
		Interval: 4 * time.Hour,
		MaxDelay: maxDelay,
		now:      func() time.Time { return now },
	}}

	// A hostile/buggy upstream reports a reset 1000 days out. parkHost
	// would otherwise suppress the whole host until then; the schedule
	// must be clamped to MaxDelay.
	farReset := now.Add(1000 * 24 * time.Hour)
	rlErr := &xrpc.Error{
		StatusCode: http.StatusTooManyRequests,
		Name:       "RateLimitExceeded",
		RateLimit:  &xrpc.RateLimit{Reset: farReset},
	}
	got := r.nextAttemptAt(rlErr, 0)
	require.Equal(t, now.Add(maxDelay).UTC(), got, "far-future Retry-After must clamp to MaxDelay")

	// A near reset within MaxDelay is honored verbatim.
	nearReset := now.Add(2 * time.Hour)
	rlErr.RateLimit.Reset = nearReset
	got = r.nextAttemptAt(rlErr, 0)
	require.Equal(t, nearReset.UTC(), got, "Retry-After within MaxDelay is honored")
}

func mustURLHost(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return u.Host
}

// TestRetryRunner_DownloadTimeoutBoundsStalledFetch: a getRepo whose
// body stalls mid-transfer must be cut off by the per-attempt download
// budget and recorded as a normal retry failure (backoff scheduled),
// not hang the retry worker until the transport's 30-minute backstop.
// Mirrors atmos backfill.Engine's DownloadTimeout posture
// (bluesky-social/jetstream#299 follow-up: the retry runner bypasses
// the engine, so it needs its own bound).
func TestRetryRunner_DownloadTimeoutBoundsStalledFetch(t *testing.T) {
	t.Parallel()
	st, w, _ := newRetryTestWriter(t)
	bs := NewStore(st, nil)
	ctx := context.Background()
	did := atmos.DID("did:plc:retrystall")
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)

	stalled := make(chan struct{})
	t.Cleanup(func() { close(stalled) })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/xrpc/com.atproto.sync.getRepo", r.URL.Path)
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-stalled:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	host := mustURLHost(t, srv.URL)

	require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
	require.NoError(t, bs.OnFail(ctx, did, host, errors.New("xrpc: HTTP 503: bootstrap unavailable"), 1))
	require.NoError(t, bs.RecordRetryFailure(ctx, did, host, errors.New("xrpc 503 unavailable"), now.Add(-time.Hour)))

	r, err := newRetryRunner(RetryConfig{
		Store:           st,
		Writer:          w,
		HTTPClient:      srv.Client(),
		RelayURL:        srv.URL,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		Interval:        time.Hour,
		Workers:         1,
		HostWorkers:     1,
		MaxDelay:        24 * time.Hour,
		DownloadTimeout: 200 * time.Millisecond,
		now:             func() time.Time { return now },
	})
	require.NoError(t, err)

	start := time.Now()
	require.NoError(t, r.processCandidate(ctx, retryCandidate{DID: did, Host: host, Retry: 1}),
		"a timed-out download is a recordable failure, not a pass-fatal error")
	require.Less(t, time.Since(start), 10*time.Second,
		"attempt must be bounded by DownloadTimeout, not the transport backstop")

	rs, err := bs.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, StatusFailed, rs.Backfill.Status)
	require.Equal(t, 2, rs.Backfill.RetryCount, "timeout must be recorded as a failed attempt")
	require.True(t, rs.Backfill.NextAttemptAt.After(now), "backoff must be scheduled")
}
