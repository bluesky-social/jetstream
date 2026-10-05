package backfill

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/jcalabro/atmos"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// newPendingPassWriter is newRetryTestWriter over a commit-counting store,
// with blocks big enough that a block commit carries many repos.
func newPendingPassWriter(t *testing.T) (*countingStore, *ingest.Writer, string) {
	t.Helper()
	dir := t.TempDir()
	raw, err := pebblestore.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	db := &countingStore{Store: raw}
	segmentsDir := filepath.Join(dir, "segments")
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       segmentsDir,
		Store:             db,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxEventsPerBlock: 64,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return db, w, segmentsDir
}

// rateLimitedPDS serves getRepo from a stub, answering 429 to the first
// limited requests for each DID (every request when limited < 0).
type rateLimitedPDS struct {
	stub      *stubServer
	srv       *httptest.Server
	limited   int
	pastReset bool

	mu   sync.Mutex
	hits map[string]int
}

func newRateLimitedPDS(t *testing.T, fixtures map[atmos.DID]repoFixture, limited int, pastReset bool) *rateLimitedPDS {
	t.Helper()
	p := &rateLimitedPDS{stub: newStubServer(t, fixtures), limited: limited, pastReset: pastReset, hits: map[string]int{}}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		did := r.URL.Query().Get("did")
		p.mu.Lock()
		p.hits[did]++
		n := p.hits[did]
		p.mu.Unlock()
		if p.limited < 0 || n <= p.limited {
			if p.pastReset {
				w.Header().Set("RateLimit-Remaining", "0")
				w.Header().Set("RateLimit-Reset", fmt.Sprint(time.Now().Add(-time.Minute).Unix()))
			}
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"RateLimitExceeded"}`))
			return
		}
		p.stub.handle(w, r)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func scanRepoRows(t *testing.T, db metastore.Store) map[atmos.DID]*RepoStatus {
	t.Helper()
	prefix := []byte(repoKeyPrefix)
	it, err := db.NewIter(t.Context(), prefix, metastore.PrefixUpperBound(prefix))
	require.NoError(t, err)
	defer func() { _ = it.Close() }()
	out := map[atmos.DID]*RepoStatus{}
	for it.Next() {
		rs, err := decodeRepoStatus(it.Value())
		require.NoError(t, err)
		out[atmos.DID(strings.TrimPrefix(string(it.Key()), repoKeyPrefix))] = rs
	}
	require.NoError(t, it.Err())
	return out
}

// The pending pass waits out rate limits in memory and returns with no row
// left pending: repos behind a limiter that clears complete, and those
// behind one that never does are recorded failed for the steady loop.
func TestRunPendingRepoRetryPass_WaitsOutRateLimits(t *testing.T) {
	t.Parallel()
	for _, pastReset := range []bool{false, true} {
		t.Run(fmt.Sprintf("past_reset=%v", pastReset), func(t *testing.T) {
			t.Parallel()
			db, w, segmentsDir := newPendingPassWriter(t)
			bs := newSeededStore(t, db, nil)
			start := time.Now()

			// h0 and h1 answer each DID's first request with a 429; h2
			// rate limits forever.
			hosts := []string{"h0.example.test", "h1.example.test", "h2.example.test"}
			limits := []int{1, 1, -1}
			servers := map[string]*rateLimitedPDS{}
			hostOf := map[atmos.DID]string{}
			for h, host := range hosts {
				fixtures := map[atmos.DID]repoFixture{}
				for i := range 8 {
					did := atmos.DID(fmt.Sprintf("did:plc:h%d-%02d", h, i))
					fixtures[did] = buildRepoFixture(t, did)
					hostOf[did] = host
					rs := &RepoStatus{Backfill: RepoBackfillStatus{Status: StatusPending}, Host: host, PDS: host, Active: true}
					if i == 0 {
						// Deferred by an earlier pass: the pending pass
						// must not wait for it.
						rs.Backfill.NextAttemptAt = start.Add(24 * time.Hour)
					}
					require.NoError(t, bs.putRepoStatus(did, rs))
				}
				servers[host] = newRateLimitedPDS(t, fixtures, limits[h], pastReset)
			}
			relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "stamped repos must not use the relay", http.StatusInternalServerError)
			}))
			t.Cleanup(relay.Close)

			metrics := NewMetrics(prometheus.NewRegistry())
			db.reset()
			require.NoError(t, RunPendingRepoRetryPass(t.Context(), RetryConfig{
				Store: db, Writer: w, HTTPClient: relay.Client(), RelayURL: relay.URL,
				Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: metrics,
				Interval: time.Hour, Workers: 16, HostWorkers: 2, MaxDelay: 24 * time.Hour,
				parkBase: time.Millisecond, parkMax: 4 * time.Millisecond,
				NewHostClient: func(hostname string) (*atmossync.Client, error) {
					srv := servers[hostname].srv
					return atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{
						Host: srv.URL, HTTPClient: gt.Some(srv.Client()), Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
					}}), nil
				},
			}))
			commits := db.commits.Load()

			events := collectActiveEvents(t, filepath.Join(segmentsDir, ingest.SegmentFilename(0)))
			perDID := map[atmos.DID]int{}
			for _, ev := range events {
				perDID[atmos.DID(ev.DID)]++
			}
			completed := 0
			for did, rs := range scanRepoRows(t, db) {
				switch hostOf[did] {
				case "h2.example.test":
					require.Equal(t, StatusFailed, rs.Backfill.Status, did)
					require.True(t, rs.Backfill.NextAttemptAt.After(start), did)
					require.NotEmpty(t, rs.Backfill.LastError, did)
					require.Zero(t, perDID[did], did)
				default:
					require.Equal(t, StatusComplete, rs.Backfill.Status, did)
					require.Equal(t, hostOf[did], rs.PDS, did)
					require.Equal(t, 2, perDID[did], "%s: one sync and one record", did)
					completed++
				}
			}
			require.Equal(t, 16, completed)
			require.Less(t, commits, int64(completed), "completions must ride the writer's block commits, not a transaction each")
			require.Positive(t, testutil.ToFloat64(metrics.RetryRequeued))
			require.InDelta(t, 1, testutil.ToFloat64(metrics.RetryHostsAbandoned), 0)
			require.Zero(t, testutil.ToFloat64(metrics.RetryQueueRemaining))
		})
	}
}

// Failures commit in batches, not a transaction per repo.
func TestRunPendingRepoRetryPass_BatchesFailures(t *testing.T) {
	t.Parallel()
	db, w, _ := newPendingPassWriter(t)
	bs := newSeededStore(t, db, nil)
	const n = 600
	srv := newStubServer(t, map[atmos.DID]repoFixture{})
	srv.failGetRepo = map[atmos.DID]bool{}
	srv.failGetRepoCode = http.StatusServiceUnavailable
	for i := range n {
		did := atmos.DID(fmt.Sprintf("did:plc:fail%04d", i))
		srv.failGetRepo[did] = true
		require.NoError(t, bs.putRepoStatus(did, &RepoStatus{Backfill: RepoBackfillStatus{Status: StatusPending}, Active: true}))
	}

	db.reset()
	require.NoError(t, RunPendingRepoRetryPass(t.Context(), RetryConfig{
		Store: db, Writer: w, HTTPClient: srv.srv.Client(), RelayURL: srv.srv.URL,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Interval: time.Hour, Workers: 32, HostWorkers: 32, MaxDelay: 24 * time.Hour,
	}))
	require.LessOrEqual(t, db.commits.Load(), int64(n/retryWriteBatch+3), "failure writes must be batched")
	rows := scanRepoRows(t, db)
	require.Len(t, rows, n)
	for did, rs := range rows {
		require.Equal(t, StatusFailed, rs.Backfill.Status, did)
		require.Equal(t, 1, rs.Backfill.RetryCount, did)
	}
	counts, ok, err := LoadCounts(db)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(n), counts.Failed)
	require.Zero(t, counts.Pending)
}

// A batch of retry failures leaves the keyspace that recording them one at
// a time does, terminal answers and raced completions included.
func TestStore_RecordRetryFailuresMatchesSequential(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	var fails []retryFailure
	seed := func(t *testing.T) (*Store, metastore.Store) {
		db := memstore.New()
		st := NewStore(db, nil)
		for i := range 40 {
			did := atmos.DID(fmt.Sprintf("did:plc:rf%03d", i))
			status := StatusPending
			switch i % 5 {
			case 1:
				status = StatusFailed
			case 4:
				status = StatusComplete // a completion raced the attempt
			}
			require.NoError(t, st.putRepoStatus(did, &RepoStatus{
				Backfill: RepoBackfillStatus{Status: status, RetryCount: i % 3},
				Host:     fmt.Sprintf("old%d.example.test", i%4),
				Active:   true,
			}))
		}
		require.NoError(t, st.SeedCounts(t.Context()))
		return st, db
	}
	errs := []error{
		&xrpc.Error{StatusCode: http.StatusServiceUnavailable, Message: "unavailable"},
		&xrpc.Error{StatusCode: http.StatusBadRequest, Name: "RepoNotFound"},
		&xrpc.Error{StatusCode: http.StatusBadRequest, Name: "RepoDeactivated"},
		errors.New("connection reset"),
	}
	for i := range 40 {
		fails = append(fails, retryFailure{
			did:  atmos.DID(fmt.Sprintf("did:plc:rf%03d", i)),
			host: []string{"", "new.example.test", "old1.example.test"}[i%3],
			err:  errs[i%len(errs)],
			next: now.Add(time.Duration(i) * time.Minute),
		})
	}

	ref, refDB := seed(t)
	for _, f := range fails {
		require.NoError(t, ref.RecordRetryFailure(t.Context(), f.did, f.host, f.err, f.next))
	}
	got, gotDB := seed(t)
	require.NoError(t, got.recordRetryFailures(t.Context(), fails[:17]))
	require.NoError(t, got.recordRetryFailures(t.Context(), fails[17:]))
	require.Equal(t, normalizedKeyspace(t, refDB), normalizedKeyspace(t, gotDB))

	// A missing row fails the whole batch and writes nothing.
	before := normalizedKeyspace(t, gotDB)
	err := got.recordRetryFailures(t.Context(), []retryFailure{fails[0], {did: "did:plc:nope", err: errs[0], next: now}})
	require.ErrorContains(t, err, "missing row")
	require.Equal(t, before, normalizedKeyspace(t, gotDB))
}

// A rate limit with no reset parks its host briefly, backing off while
// parks repeat, rather than for the retry interval; a server reset is
// honored up to the mode's ceiling.
func TestRetryRunner_RateLimitParkBackoff(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	newRunner := func(pending bool) *retryRunner {
		_, w, _ := newRetryTestWriter(t)
		r, err := newRetryRunner(RetryConfig{
			Store: memstore.New(), Writer: w, HTTPClient: http.DefaultClient, RelayURL: "http://relay.invalid",
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			Interval: 4 * time.Hour, MaxDelay: 7 * 24 * time.Hour,
			now: func() time.Time { return now }, jitter: func(int64) int64 { return 0 },
			pendingPass: pending,
		})
		require.NoError(t, err)
		return r
	}
	noReset := &xrpc.Error{StatusCode: http.StatusTooManyRequests}
	const host = "pds.example.test"

	r := newRunner(false)
	var got []time.Duration
	for range 9 {
		until := r.parkRateLimited(host, noReset)
		got = append(got, until.Sub(now))
		// A worker already in flight doesn't lengthen the park.
		require.Equal(t, until, r.parkRateLimited(host, noReset))
		now = until
	}
	require.Equal(t, []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second,
		32 * time.Second, time.Minute, time.Minute, time.Minute,
	}, got)
	r.hostSucceeded(host)
	require.Equal(t, time.Second, r.parkRateLimited(host, noReset).Sub(now), "a success resets the backoff")

	farReset := &xrpc.Error{StatusCode: http.StatusTooManyRequests, RateLimit: &xrpc.RateLimit{Reset: now.Add(48 * time.Hour)}}
	require.Equal(t, now.Add(48*time.Hour), newRunner(false).parkRateLimited(host, farReset), "the steady loop honors a reset within MaxDelay")
	require.Equal(t, now.Add(pendingPassMaxPark), newRunner(true).parkRateLimited(host, farReset), "the pending pass clamps a reset")
}
