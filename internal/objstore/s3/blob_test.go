package s3_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/objstore/blobtest"
	"github.com/bluesky-social/jetstream/internal/objstore/s3"
	"github.com/bluesky-social/jetstream/internal/objstore/s3/s3test"
)

// fault holds a Fault's settings; Fault itself is not copyable.
type fault struct {
	op     s3test.Op
	kind   s3test.FaultKind
	status int
	code   string
}

func fakeConfig(rt http.RoundTripper) s3.Config {
	return s3.Config{
		Endpoint:        s3test.FakeEndpoint,
		Region:          "us-east-1",
		Bucket:          "bucket",
		Prefix:          "pfx/",
		PathStyle:       true,
		AccessKeyID:     "AKIDTEST",
		SecretAccessKey: "not-a-real-secret",
		RetryTimeout:    2 * time.Second,
		AttemptTimeout:  500 * time.Millisecond,
		Transport:       rt,
	}
}

func wrongBytesFaults(key string) []*s3test.Fault {
	return []*s3test.Fault{
		{Op: s3test.OpGet, Key: key, Ordinal: 1, Kind: s3test.FaultWrongBytes},
		{Op: s3test.OpGetRange, Key: key, Ordinal: 1, Kind: s3test.FaultWrongBytes},
	}
}

// TestContractFake runs the Blob contract through the real SDK against the
// in-memory S3, with 404 and with AWS's 403 for missing keys.
func TestContractFake(t *testing.T) {
	t.Parallel()
	for _, as403 := range []bool{false, true} {
		name := "404"
		if as403 {
			name = "403"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			blobtest.Run(t, blobtest.Config{
				New: func(t *testing.T) objstore.Blob {
					f := s3test.NewFake()
					f.MissingAs403 = as403
					return s3test.New(t, fakeConfig(f))
				},
				NewWrongBytes: func(t *testing.T, key string) objstore.Blob {
					return s3test.New(t, fakeConfig(&s3test.FaultTransport{Base: s3test.NewFake(), Faults: wrongBytesFaults(key)}))
				},
			})
		})
	}
}

// TestContractReal runs the contract against the object store named by
// JETSTREAM_TEST_S3_* (SeaweedFS and MinIO under `just test-storage`).
func TestContractReal(t *testing.T) {
	t.Parallel()
	cfg := s3test.Env(t)
	blobtest.Run(t, blobtest.Config{
		New: func(t *testing.T) objstore.Blob { return s3test.New(t, cfg) },
		NewWrongBytes: func(t *testing.T, key string) objstore.Blob {
			c := cfg
			c.Transport = &s3test.FaultTransport{Faults: wrongBytesFaults(key)}
			return s3test.New(t, c)
		},
	})
}

// TestRealFaults drives the retry path through a real object store.
func TestRealFaults(t *testing.T) {
	t.Parallel()
	cfg := s3test.Env(t)
	ft := &s3test.FaultTransport{Faults: []*s3test.Fault{
		{Op: s3test.OpPut, Ordinal: 1, Kind: s3test.FaultStatus},
		{Op: s3test.OpPut, Ordinal: 2, Kind: s3test.FaultErrorAfter},
		{Op: s3test.OpGet, Ordinal: 1, Kind: s3test.FaultTruncate},
		{Op: s3test.OpGet, Ordinal: 2, Kind: s3test.FaultTimeout},
	}}
	cfg.Transport = ft
	cfg.AttemptTimeout = 500 * time.Millisecond
	b := s3test.New(t, cfg)
	ctx := t.Context()
	data := []byte(strings.Repeat("jetstream ", 100))
	require.NoError(t, b.PutKey(ctx, "k", data))
	got, err := b.GetKey(ctx, "k")
	require.NoError(t, err)
	require.Equal(t, data, got)
	require.Empty(t, ft.Unfired())
}

func newMetrics(t *testing.T) *s3.Metrics {
	t.Helper()
	return s3.NewMetrics(prometheus.NewRegistry())
}

func requests(m *s3.Metrics, op, result string) float64 {
	return testutil.ToFloat64(m.Requests.WithLabelValues(op, result))
}

// TestTransientFaultsRetry covers each transient failure the design names:
// 5xx, a stalled connection, a truncated body, and a PUT whose response was
// lost. Each is retried and the call succeeds with the right bytes.
func TestTransientFaultsRetry(t *testing.T) {
	t.Parallel()
	data := []byte(strings.Repeat("0123456789", 50))
	cases := []struct {
		name   string
		fault  fault
		op     string
		result string // the failed attempt's result label
	}{
		{"503 on put", fault{op: s3test.OpPut, kind: s3test.FaultStatus}, "put", "error"},
		{"500 on get", fault{op: s3test.OpGet, kind: s3test.FaultStatus, status: 500}, "get", "error"},
		{"slow down", fault{op: s3test.OpGet, kind: s3test.FaultStatus, status: 503, code: "SlowDown"}, "get", "error"},
		{"throttled", fault{op: s3test.OpGetRange, kind: s3test.FaultStatus, status: 429, code: "TooManyRequests"}, "get_range", "error"},
		{"timeout on get", fault{op: s3test.OpGet, kind: s3test.FaultTimeout}, "get", "timeout"},
		{"truncated get", fault{op: s3test.OpGet, kind: s3test.FaultTruncate}, "get", "error"},
		{"truncated range", fault{op: s3test.OpGetRange, kind: s3test.FaultTruncate}, "get_range", "error"},
		{"put response lost", fault{op: s3test.OpPut, kind: s3test.FaultErrorAfter}, "put", "error"},
		{"delete 503", fault{op: s3test.OpDelete, kind: s3test.FaultStatus}, "delete", "error"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				f := s3test.Fault{Op: c.fault.op, Kind: c.fault.kind, Status: c.fault.status, Code: c.fault.code, Ordinal: 1}
				ft := &s3test.FaultTransport{Base: s3test.NewFake(), Faults: []*s3test.Fault{&f}}
				cfg := fakeConfig(ft)
				cfg.Metrics = newMetrics(t)
				b := s3test.New(t, cfg)
				ctx := t.Context()

				require.NoError(t, b.PutKey(ctx, "k", data))
				got, err := b.GetKey(ctx, "k")
				require.NoError(t, err)
				require.Equal(t, data, got)
				got, err = b.GetKeyRange(ctx, "k", 10, 20)
				require.NoError(t, err)
				require.Equal(t, data[10:30], got)
				require.NoError(t, b.DeleteKey(ctx, "k"))

				require.EqualValues(t, 1, f.Fired())
				require.InDelta(t, 1, requests(cfg.Metrics, c.op, c.result), 0)
				require.InDelta(t, 1, requests(cfg.Metrics, c.op, "ok"), 0)
				require.Positive(t, testutil.ToFloat64(cfg.Metrics.Bytes.WithLabelValues("get")))
			})
		})
	}
}

// TestRetryTimeout checks that a backend that never recovers fails the call
// once RetryTimeout has passed, not before and not never.
func TestRetryTimeout(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ft := &s3test.FaultTransport{Base: s3test.NewFake(), Faults: []*s3test.Fault{
			{Kind: s3test.FaultStatus, Status: 503},
		}}
		cfg := fakeConfig(ft)
		cfg.RetryTimeout = 30 * time.Second
		cfg.Metrics = newMetrics(t)
		b := s3test.New(t, cfg)
		start := time.Now()
		err := b.PutKey(t.Context(), "k", []byte("x"))
		require.ErrorContains(t, err, "gave up after")
		elapsed := time.Since(start)
		require.LessOrEqual(t, elapsed, 30*time.Second)
		require.Greater(t, elapsed, 25*time.Second, "backoff is capped, so attempts continue until near the deadline")
		require.Greater(t, requests(cfg.Metrics, "put", "error"), 10.0)
	})
}

// TestAttemptTimeoutBoundedByRetryTimeout checks that a connection that
// hangs on every attempt still fails at RetryTimeout.
func TestAttemptTimeoutBoundedByRetryTimeout(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ft := &s3test.FaultTransport{Base: s3test.NewFake(), Faults: []*s3test.Fault{{Kind: s3test.FaultTimeout}}}
		cfg := fakeConfig(ft)
		cfg.RetryTimeout = 5 * time.Second
		cfg.AttemptTimeout = 2 * time.Second
		b := s3test.New(t, cfg)
		start := time.Now()
		_, err := b.GetKey(t.Context(), "k")
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.LessOrEqual(t, time.Since(start), 5*time.Second)
		require.GreaterOrEqual(t, ft.Faults[0].Fired(), int64(2))
	})
}

// TestFinalErrorsNotRetried checks the answers that another attempt cannot
// change: missing objects, bad ranges, and client errors.
func TestFinalErrorsNotRetried(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		fault    *s3test.Fault
		notFound bool
	}{
		{"missing 404", nil, true},
		{"access denied 403", &s3test.Fault{Kind: s3test.FaultStatus, Status: 403, Code: "AccessDenied"}, true},
		{"no such bucket", &s3test.Fault{Kind: s3test.FaultStatus, Status: 404, Code: "NoSuchBucket"}, false},
		{"bad request", &s3test.Fault{Kind: s3test.FaultStatus, Status: 400, Code: "InvalidArgument"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			var faults []*s3test.Fault
			if c.fault != nil {
				faults = append(faults, c.fault)
			}
			ft := &s3test.FaultTransport{Base: s3test.NewFake(), Faults: faults}
			cfg := fakeConfig(ft)
			cfg.Metrics = newMetrics(t)
			b := s3test.New(t, cfg)
			_, err := b.GetKey(t.Context(), "missing")
			require.Error(t, err)
			require.Equal(t, c.notFound, errors.Is(err, objstore.ErrNotFound), "%v", err)
			total := 0.0
			for _, r := range []string{"ok", "not_found", "error", "timeout", "canceled", "invalid_range"} {
				total += requests(cfg.Metrics, "get", r)
			}
			require.InDelta(t, 1, total, 0, "exactly one attempt")
		})
	}
}

// TestCredentialErrorsAreNotMissing pins that a 403 for broken credentials
// never reads as a missing object: the protocol would call that corruption.
func TestCredentialErrorsAreNotMissing(t *testing.T) {
	t.Parallel()
	for _, code := range []string{"InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken", "RequestTimeTooSkewed"} {
		t.Run(code, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ft := &s3test.FaultTransport{Base: s3test.NewFake(), Faults: []*s3test.Fault{
					{Kind: s3test.FaultStatus, Status: 403, Code: code},
				}}
				b := s3test.New(t, fakeConfig(ft))
				_, err := b.GetKey(t.Context(), "k")
				require.Error(t, err)
				require.NotErrorIs(t, err, objstore.ErrNotFound)
				require.NotContains(t, err.Error(), "not-a-real-secret")
			})
		})
	}
}

// TestWriteDenialIsNotMissing pins that a 403 on a write is an error:
// DeleteKey maps a missing key to success, so reading a denied DELETE as
// "missing" would silently leak the object.
func TestWriteDenialIsNotMissing(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ft := &s3test.FaultTransport{Base: s3test.NewFake(), Faults: []*s3test.Fault{
			{Op: s3test.OpDelete, Kind: s3test.FaultStatus, Status: 403, Code: "AccessDenied"},
			{Op: s3test.OpPut, Kind: s3test.FaultStatus, Status: 403, Code: "AccessDenied"},
		}}
		b := s3test.New(t, fakeConfig(ft))
		err := b.DeleteKey(t.Context(), "k")
		require.Error(t, err)
		require.NotErrorIs(t, err, objstore.ErrNotFound)
		err = b.PutKey(t.Context(), "k", []byte("x"))
		require.Error(t, err)
		require.NotErrorIs(t, err, objstore.ErrNotFound)
	})
}

func TestInvalidRangeFromServer(t *testing.T) {
	t.Parallel()
	b := s3test.New(t, fakeConfig(s3test.NewFake()))
	ctx := t.Context()
	require.NoError(t, b.PutKey(ctx, "k", []byte("abc")))
	_, err := b.GetKeyRange(ctx, "k", 3, 1)
	require.ErrorIs(t, err, objstore.ErrInvalidRange)
	got, err := b.GetKeyRange(ctx, "k", 1, 1<<62)
	require.NoError(t, err)
	require.Equal(t, []byte("bc"), got)
}

func TestCanceledDuringBackoff(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ft := &s3test.FaultTransport{Base: s3test.NewFake(), Faults: []*s3test.Fault{{Kind: s3test.FaultStatus}}}
		cfg := fakeConfig(ft)
		cfg.RetryTimeout = time.Minute
		b := s3test.New(t, cfg)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		err := b.PutKey(ctx, "k", []byte("x"))
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

// recorder records request paths and the peak number in flight.
type recorder struct {
	base http.RoundTripper
	gate chan struct{}

	mu       sync.Mutex
	paths    []string
	inflight atomic.Int64
	peak     atomic.Int64
}

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.paths = append(r.paths, req.URL.Path)
	r.mu.Unlock()
	n := r.inflight.Add(1)
	defer r.inflight.Add(-1)
	for {
		p := r.peak.Load()
		if n <= p || r.peak.CompareAndSwap(p, n) {
			break
		}
	}
	if r.gate != nil {
		<-r.gate
	}
	return r.base.RoundTrip(req)
}

func TestPrefixAndPathStyle(t *testing.T) {
	t.Parallel()
	rec := &recorder{base: s3test.NewFake()}
	b := s3test.New(t, fakeConfig(rec))
	key := objstore.Key([16]byte{1}, [16]byte{2})
	require.NoError(t, b.PutKey(t.Context(), key, []byte("x")))
	require.Equal(t, []string{"/bucket/pfx/" + key}, rec.paths)
}

func TestUploadConcurrencyLimit(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{base: s3test.NewFake(), gate: make(chan struct{})}
		cfg := fakeConfig(rec)
		cfg.UploadConcurrency = 2
		cfg.AttemptTimeout = time.Hour
		cfg.RetryTimeout = time.Hour
		b := s3test.New(t, cfg)
		var wg sync.WaitGroup
		for i := range 6 {
			wg.Go(func() {
				require.NoError(t, b.PutKey(t.Context(), string(rune('a'+i)), []byte("x")))
			})
		}
		synctest.Wait()
		require.EqualValues(t, 2, rec.inflight.Load())
		close(rec.gate)
		wg.Wait()
		require.EqualValues(t, 2, rec.peak.Load())
	})
}

func TestConfigRedactsCredentials(t *testing.T) {
	t.Parallel()
	cfg := fakeConfig(nil)
	var buf strings.Builder
	slog.New(slog.NewTextHandler(&buf, nil)).Info("cfg", "s3", cfg)
	for _, s := range []string{fmt.Sprint(cfg), fmt.Sprintf("%+v", cfg), fmt.Sprintf("%#v", cfg), buf.String()} {
		require.NotContains(t, s, cfg.SecretAccessKey)
		require.NotContains(t, s, cfg.AccessKeyID)
		require.Contains(t, s, "bucket")
	}
}

func TestNewValidates(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	_, err := s3.New(ctx, s3.Config{Region: "us-east-1"})
	require.ErrorContains(t, err, "bucket")
	_, err = s3.New(ctx, s3.Config{Bucket: "b"})
	require.ErrorContains(t, err, "region")
	_, err = s3.New(ctx, s3.Config{Bucket: "b", Region: "r", AccessKeyID: "id"})
	require.ErrorContains(t, err, "both")
}
