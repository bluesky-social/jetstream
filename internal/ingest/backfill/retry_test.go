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
