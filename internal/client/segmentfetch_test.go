package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
)

// segETag derives a deterministic strong-ETag value for a test segment's
// bytes, standing in for production's sealed-header checksum.
func segETag(raw []byte) string {
	return strconv.FormatUint(xxh3.Hash(raw), 16)
}

// stripedDownloader shrinks the part size so small test fixtures exercise
// multi-part striping (probe + parts) instead of completing in the probe, and
// opts into 4-way striping (the default is the single resumable stream).
func stripedDownloader(host string, concurrency int, partSize int64) *Downloader {
	d := singleStreamDownloader(host, concurrency, partSize)
	d.SetSegmentStripes(4)
	return d
}

// singleStreamDownloader keeps the default stripes=1 resumable-stream mode,
// with a small part size so the probe/remainder split is exercised.
func singleStreamDownloader(host string, concurrency int, partSize int64) *Downloader {
	d := NewDownloader(&xrpc.Client{Host: host}, concurrency, nil)
	d.segPartSize = partSize
	d.partRetryDelay = time.Millisecond
	return d
}

// segServer is a minimal ServeContent-based segment server with per-request
// interception, for tests that need to inject failures or ETag swaps without
// the full archiveServer fixture.
type segServer struct {
	mu   sync.Mutex
	body []byte
	etag string
	// intercept, when non-nil, runs first and reports whether it handled the
	// request entirely.
	intercept func(w http.ResponseWriter, r *http.Request) bool
	reqs      atomic.Int64
	rangeReqs atomic.Int64
}

func (s *segServer) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.reqs.Add(1)
		if r.Header.Get("Range") != "" {
			s.rangeReqs.Add(1)
		}
		s.mu.Lock()
		body, etag, intercept := s.body, s.etag, s.intercept
		s.mu.Unlock()
		if intercept != nil && intercept(w, r) {
			return
		}
		if etag != "" {
			w.Header().Set("ETag", etag)
		}
		http.ServeContent(w, r, "seg.jss", time.Unix(1_730_000_000, 0), bytes.NewReader(body))
	}
}

func newSegServer(t *testing.T, body []byte) (*segServer, string) {
	t.Helper()
	s := &segServer{body: body, etag: `"` + segETag(body) + `"`}
	srv := httptest.NewServer(s.handler())
	t.Cleanup(srv.Close)
	return s, srv.URL
}

func patternBody(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i * 7)
	}
	return b
}

func TestFetchSegmentStripedReassemblesExactly(t *testing.T) {
	t.Parallel()
	body := patternBody(1<<20 + 12345) // deliberately not part-aligned
	s, url := newSegServer(t, body)

	d := stripedDownloader(url, 4, 64<<10) // 64 KiB parts -> 17 parts
	got, err := d.fetchSegment(context.Background(), "seg.jss")
	require.NoError(t, err)
	require.Equal(t, body, got, "striped download must reassemble byte-identical content")
	require.Greater(t, s.rangeReqs.Load(), int64(2), "multi-part fetch expected")
}

func TestFetchSegmentSingleProbeWhenSmall(t *testing.T) {
	t.Parallel()
	body := patternBody(10 << 10) // smaller than one part
	s, url := newSegServer(t, body)

	d := stripedDownloader(url, 4, 64<<10)
	got, err := d.fetchSegment(context.Background(), "seg.jss")
	require.NoError(t, err)
	require.Equal(t, body, got)
	require.Equal(t, int64(1), s.reqs.Load(), "a segment smaller than one part completes in the probe")
}

func TestFetchSegmentFallsBackWithoutRangeSupport(t *testing.T) {
	t.Parallel()
	body := patternBody(300 << 10)
	s := &segServer{body: body}
	// Plain handler that ignores Range entirely (no ServeContent, no ETag).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.reqs.Add(1)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	d := stripedDownloader(srv.URL, 4, 64<<10)
	got, err := d.fetchSegment(context.Background(), "seg.jss")
	require.NoError(t, err)
	require.Equal(t, body, got, "a 200-to-the-probe server degrades to the whole-body path")
}

// TestFetchSegmentRangeIgnoring200MidBodyCutIsRetried: when the server ignores
// Range entirely (200 to the probe) and that stream dies mid-body, the retry
// budget must still apply — the 200 path must not be the one read without
// retry protection.
func TestFetchSegmentRangeIgnoring200MidBodyCutIsRetried(t *testing.T) {
	t.Parallel()
	body := patternBody(300 << 10)
	var reqs atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if reqs.Add(1) == 1 { // the probe's 200: die mid-body
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			_, _ = w.Write(body[:100<<10])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			panic(http.ErrAbortHandler)
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	d := stripedDownloader(srv.URL, 4, 64<<10)
	got, err := d.fetchSegment(context.Background(), "seg.jss")
	require.NoError(t, err, "a mid-body cut on the 200 path must be retried")
	require.Equal(t, body, got)
	require.GreaterOrEqual(t, reqs.Load(), int64(2), "cut probe + full-body retry")
}

// TestFetchSegmentRangeIgnoring200DebitsAttemptBudget: on the 200 path the
// failed probe read consumes one attempt, so attempts=2 against a
// persistently-cutting server means exactly 2 requests total.
func TestFetchSegmentRangeIgnoring200DebitsAttemptBudget(t *testing.T) {
	t.Parallel()
	body := patternBody(300 << 10)
	var reqs atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body[:100<<10])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		panic(http.ErrAbortHandler)
	}))
	t.Cleanup(srv.Close)

	d := stripedDownloader(srv.URL, 4, 64<<10)
	d.xc.Retry = gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(2)})
	_, err := d.fetchSegment(context.Background(), "seg.jss")
	require.Error(t, err)
	require.Equal(t, int64(2), reqs.Load(),
		"attempts=2 means probe + one fallback, the probe attempt is debited")
}

// TestFetchSegmentRangeIgnoring200SingleAttemptFailsFast: with attempts=1 the
// failed probe body read IS the one attempt — no second full-file request.
func TestFetchSegmentRangeIgnoring200SingleAttemptFailsFast(t *testing.T) {
	t.Parallel()
	body := patternBody(300 << 10)
	var reqs atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body[:100<<10])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		panic(http.ErrAbortHandler)
	}))
	t.Cleanup(srv.Close)

	d := stripedDownloader(srv.URL, 4, 64<<10)
	d.xc.Retry = gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})
	_, err := d.fetchSegment(context.Background(), "seg.jss")
	require.Error(t, err, "attempts=1 must fail fast on the 200 path")
	require.Equal(t, int64(1), reqs.Load(), "exactly one request with attempts=1")
}

func TestFetchSegmentFallsBackWithoutETag(t *testing.T) {
	t.Parallel()
	body := patternBody(300 << 10)
	s := &segServer{body: body} // etag empty: ServeContent honors Range but sends no validator
	srv := httptest.NewServer(s.handler())
	t.Cleanup(srv.Close)

	d := stripedDownloader(srv.URL, 4, 64<<10)
	got, err := d.fetchSegment(context.Background(), "seg.jss")
	require.NoError(t, err)
	require.Equal(t, body, got, "no ETag -> no splice protection -> single-stream fallback")
}

// TestFetchSegmentFallbackRetriesTransientFailure guards the no-ETag/no-Range
// fallback path's retry budget: the bulk transport is WithNoRetries, so a
// transient 500 mid-download must be retried by fetchWholeFallback itself,
// matching the pre-#296 QueryRaw behavior.
func TestFetchSegmentFallbackRetriesTransientFailure(t *testing.T) {
	t.Parallel()
	body := patternBody(300 << 10)
	s := &segServer{body: body} // no ETag → probe 206 routes to the fallback
	var fallbackReqs atomic.Int64
	s.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		// First full-body (Range-less) request 500s; the retry succeeds.
		if r.Header.Get("Range") == "" && fallbackReqs.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return true
		}
		return false
	}
	srv := httptest.NewServer(s.handler())
	t.Cleanup(srv.Close)

	d := stripedDownloader(srv.URL, 4, 64<<10)
	got, err := d.fetchSegment(context.Background(), "seg.jss")
	require.NoError(t, err, "a transient fallback failure must be retried")
	require.Equal(t, body, got)
	require.Equal(t, int64(2), fallbackReqs.Load(), "failed attempt + successful retry")
}

// TestFetchSegmentFallback4xxIsPermanent: a 4xx on the fallback path (e.g. a
// SegmentNotFound XRPC envelope) is not retryable and must surface after one
// attempt, not burn the whole retry budget.
func TestFetchSegmentFallback4xxIsPermanent(t *testing.T) {
	t.Parallel()
	body := patternBody(300 << 10)
	s := &segServer{body: body} // no ETag → probe 206 routes to the fallback
	var fallbackReqs atomic.Int64
	s.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Header.Get("Range") == "" { // the fallback's full-body request
			fallbackReqs.Add(1)
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"SegmentNotFound"}`))
			return true
		}
		return false
	}
	srv := httptest.NewServer(s.handler())
	t.Cleanup(srv.Close)

	d := stripedDownloader(srv.URL, 4, 64<<10)
	_, err := d.fetchSegment(context.Background(), "seg.jss")
	require.Error(t, err)
	require.ErrorContains(t, err, "SegmentNotFound")
	require.Equal(t, int64(1), fallbackReqs.Load(), "a 4xx must not be retried")
	// Parity with the pre-#296 JetstreamGetSegment error surface: consumers
	// match on *xrpc.Error to classify failures.
	var xerr *xrpc.Error
	require.True(t, errors.As(err, &xerr), "segment errors must expose *xrpc.Error")
	require.Equal(t, http.StatusNotFound, xerr.StatusCode)
	require.Equal(t, "SegmentNotFound", xerr.Name)
	require.NotEmpty(t, xerr.Host, "host attribution must survive, matching xrpc.parseError")
}

// TestFetchSegmentProbe429IsRetried: 429 throttling is retryable (matching
// the xrpc retry policy the pre-#296 path used); a transient rate-limit blip
// on the probe must not fail the segment.
func TestFetchSegmentProbe429IsRetried(t *testing.T) {
	t.Parallel()
	body := patternBody(300 << 10)
	s, url := newSegServer(t, body)
	var throttled atomic.Bool
	s.mu.Lock()
	s.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if throttled.CompareAndSwap(false, true) { // 429 the first (probe) request
			w.WriteHeader(http.StatusTooManyRequests)
			return true
		}
		return false
	}
	s.mu.Unlock()

	d := stripedDownloader(url, 4, 64<<10)
	got, err := d.fetchSegment(context.Background(), "seg.jss")
	require.NoError(t, err, "a transient 429 on the probe must be retried")
	require.Equal(t, body, got)
	require.True(t, throttled.Load())
}

func TestFetchSegmentPartRetryIsPartScoped(t *testing.T) {
	t.Parallel()
	body := patternBody(1 << 20)
	s, url := newSegServer(t, body)

	// Fail the first attempt of exactly one non-probe part with a 500.
	var failed atomic.Bool
	s.mu.Lock()
	s.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		rng := r.Header.Get("Range")
		if rng != "" && rng != "bytes=0-65535" && failed.CompareAndSwap(false, true) {
			w.WriteHeader(http.StatusInternalServerError)
			return true
		}
		return false
	}
	s.mu.Unlock()

	d := stripedDownloader(url, 4, 64<<10)
	got, err := d.fetchSegment(context.Background(), "seg.jss")
	require.NoError(t, err, "a transient part failure must be retried, not fail the segment")
	require.Equal(t, body, got)
	require.True(t, failed.Load(), "the injected failure must have fired")
}

func TestFetchSegmentGenerationSwapRestartsCleanly(t *testing.T) {
	t.Parallel()
	genA := patternBody(1 << 20)
	genB := make([]byte, 1<<20+777) // different size AND content
	for i := range genB {
		genB[i] = byte(i * 13)
	}
	s, url := newSegServer(t, genA)

	// After the first probe completes, swap the file to generation B —
	// simulating a compaction rewrite mid-download. If-Range with the stale
	// ETag then returns 200/416, which must trigger a clean restart, never a
	// splice of A-parts and B-parts.
	var swapped atomic.Bool
	s.mu.Lock()
	s.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Header.Get("If-Range") != "" && swapped.CompareAndSwap(false, true) {
			s.mu.Lock()
			s.body = genB
			s.etag = `"` + segETag(genB) + `"`
			s.mu.Unlock()
		}
		return false
	}
	s.mu.Unlock()

	d := stripedDownloader(url, 4, 64<<10)
	got, err := d.fetchSegment(context.Background(), "seg.jss")
	require.NoError(t, err)
	require.Equal(t, genB, got, "after a generation swap the client must deliver the NEW generation intact")
}

// TestFetchSegmentPart4xxIsPermanent extends the fallback path's 4xx
// classification to the part/remainder paths: a part answered with a 4xx XRPC
// envelope (not 429) must fail after exactly one attempt on that part, not
// burn the retry budget re-asking a question with a permanent answer.
func TestFetchSegmentPart4xxIsPermanent(t *testing.T) {
	t.Parallel()
	body := patternBody(1 << 20)
	s, url := newSegServer(t, body)
	var partReqs atomic.Int64
	s.mu.Lock()
	s.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Header.Get("Range") == "bytes=131072-196607" {
			partReqs.Add(1)
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":"Forbidden"}`))
			return true
		}
		return false
	}
	s.mu.Unlock()

	d := stripedDownloader(url, 4, 64<<10)
	_, err := d.fetchSegment(context.Background(), "seg.jss")
	require.Error(t, err)
	require.ErrorContains(t, err, "Forbidden")
	require.Equal(t, int64(1), partReqs.Load(), "a 4xx part response must not be retried")
	var xerr *xrpc.Error
	require.True(t, errors.As(err, &xerr), "part errors must expose *xrpc.Error")
	require.Equal(t, http.StatusForbidden, xerr.StatusCode)
}

func TestFetchSegmentPersistentPartFailureSurfacesError(t *testing.T) {
	t.Parallel()
	body := patternBody(1 << 20)
	s, url := newSegServer(t, body)
	s.mu.Lock()
	s.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Header.Get("Range") == "bytes=131072-196607" { // one specific part, every attempt
			w.WriteHeader(http.StatusInternalServerError)
			return true
		}
		return false
	}
	s.mu.Unlock()

	d := stripedDownloader(url, 4, 64<<10)
	_, err := d.fetchSegment(context.Background(), "seg.jss")
	require.Error(t, err, "a persistently failing part must fail the segment after the attempt budget")
	require.ErrorContains(t, err, "part 131072-196607")
}

func TestFetchSegmentCancellation(t *testing.T) {
	t.Parallel()
	body := patternBody(1 << 20)
	s, url := newSegServer(t, body)
	release := make(chan struct{})
	s.mu.Lock()
	s.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Header.Get("Range") != "bytes=0-65535" { // park all non-probe parts
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
		return false
	}
	s.mu.Unlock()
	t.Cleanup(func() { close(release) })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	d := stripedDownloader(url, 4, 64<<10)
	go func() {
		_, err := d.fetchSegment(ctx, "seg.jss")
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.Error(t, err, "cancellation must abort the striped fetch")
	case <-time.After(10 * time.Second):
		t.Fatal("striped fetch did not unwind on cancellation")
	}
}

// TestFetchSegmentSingleStreamResumesMidBody is the core guard for the
// default (stripes=1) mode's headline property: when the remainder stream dies
// mid-body, the retry must resume from the exact failure offset with a Range
// request — never re-download from byte 0 (#296).
func TestFetchSegmentSingleStreamResumesMidBody(t *testing.T) {
	t.Parallel()
	body := patternBody(1 << 20)
	etag := `"` + segETag(body) + `"`

	const partSize = 64 << 10
	const cutAt = 300 << 10 // absolute offset where the remainder stream dies
	var cut atomic.Bool
	var resumedFrom atomic.Int64
	resumedFrom.Store(-1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rng := r.Header.Get("Range")
		var off, last int64
		_, err := fmt.Sscanf(rng, "bytes=%d-%d", &off, &last)
		require.NoError(t, err, "every request in range mode carries a Range header")
		if off > int64(partSize) && cut.Load() && resumedFrom.Load() < 0 {
			resumedFrom.Store(off) // first request after the injected cut
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, last, len(body)))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusPartialContent)
		if off == int64(partSize) && cut.CompareAndSwap(false, true) {
			// The remainder stream: write only up to cutAt, then die mid-body.
			_, _ = w.Write(body[off:cutAt])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			panic(http.ErrAbortHandler) // hard connection reset
		}
		_, _ = w.Write(body[off : last+1])
	}))
	t.Cleanup(srv.Close)

	d := singleStreamDownloader(srv.URL, 2, partSize)
	got, err := d.fetchSegment(context.Background(), "seg.jss")
	require.NoError(t, err, "a mid-body cut must be resumed, not failed")
	require.Equal(t, body, got, "resumed download must be byte-identical")
	require.Equal(t, int64(cutAt), resumedFrom.Load(),
		"the retry must resume from the exact cut offset, not restart the remainder")
}

// TestFetchSegmentResumeDribbleIsBounded guards the retry budget against a
// hostile server that delivers a trickle of bytes then cuts every connection:
// tiny progress must NOT refund the attempt budget, so the fetch fails after
// the configured attempts instead of issuing O(total) range requests.
func TestFetchSegmentResumeDribbleIsBounded(t *testing.T) {
	t.Parallel()
	body := patternBody(1 << 20)
	etag := `"` + segETag(body) + `"`
	const partSize = 64 << 10
	var reqs atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
		var off, last int64
		_, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &off, &last)
		require.NoError(t, err)
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, last, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		if off == 0 { // let the probe complete so we reach the remainder stream
			_, _ = w.Write(body[:partSize])
			return
		}
		_, _ = w.Write(body[off : off+1]) // dribble one byte, then die
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		panic(http.ErrAbortHandler)
	}))
	t.Cleanup(srv.Close)

	d := singleStreamDownloader(srv.URL, 2, partSize)
	_, err := d.fetchSegment(context.Background(), "seg.jss")
	require.Error(t, err, "a dribbling server must exhaust the budget, not be resumed forever")
	require.ErrorContains(t, err, "resume stalled")
	require.LessOrEqual(t, reqs.Load(), int64(1+d.partAttempts()),
		"1-byte progress must not refund the attempt budget")
}

// TestFetchSegmentSingleAttemptDoesNotResume: WithMaxDownloadAttempts(1) is
// documented as "disables retries entirely" — one request, and a mid-body cut
// fails rather than resuming.
func TestFetchSegmentSingleAttemptDoesNotResume(t *testing.T) {
	t.Parallel()
	body := patternBody(1 << 20)
	etag := `"` + segETag(body) + `"`
	const partSize = 64 << 10
	var remainderReqs atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var off, last int64
		_, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &off, &last)
		require.NoError(t, err)
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, last, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		if off == 0 {
			_, _ = w.Write(body[:partSize])
			return
		}
		remainderReqs.Add(1)
		_, _ = w.Write(body[off : 300<<10]) // substantial progress, then die
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		panic(http.ErrAbortHandler)
	}))
	t.Cleanup(srv.Close)

	d := singleStreamDownloader(srv.URL, 2, partSize)
	d.xc.Retry = gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})
	_, err := d.fetchSegment(context.Background(), "seg.jss")
	require.Error(t, err, "attempts=1 must fail fast, not resume")
	require.Equal(t, int64(1), remainderReqs.Load(),
		"attempts=1 means exactly one remainder request, even after progress")
}

// TestFetchSegmentContinuationWithoutETagIsRejected guards the splice-safety
// invariant: once range mode is chosen (probe carried an ETag), a continuation
// 206 that arrives WITHOUT an ETag cannot prove it's the same generation and
// must be treated as a generation change — never spliced into the buffer.
func TestFetchSegmentContinuationWithoutETagIsRejected(t *testing.T) {
	t.Parallel()
	body := patternBody(1 << 20)
	s, url := newSegServer(t, body)

	// Strip the ETag from every non-probe (If-Range-carrying) response while
	// still honoring the range — a misbehaving proxy that ignores If-Range.
	s.mu.Lock()
	s.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Header.Get("If-Range") != "" {
			var off, last int64
			_, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &off, &last)
			require.NoError(t, err)
			last = min(last, int64(len(body)-1))
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, last, len(body)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[off : last+1])
			return true
		}
		return false
	}
	s.mu.Unlock()

	d := stripedDownloader(url, 4, 64<<10)
	_, err := d.fetchSegment(context.Background(), "seg.jss")
	require.Error(t, err, "ETag-less continuations must not be spliced")
	require.ErrorIs(t, err, errSegmentGenerationChanged)
}

// TestFetchSegmentProbeBodyCutDebitsAttemptBudget: the failed probe body read
// consumes one attempt, so with attempts=2 a persistently-cutting server gets
// exactly 2 requests total (probe + one gap-fill), not 1 + a fresh budget.
func TestFetchSegmentProbeBodyCutDebitsAttemptBudget(t *testing.T) {
	t.Parallel()
	body := patternBody(48 << 10)
	etag := `"` + segETag(body) + `"`
	var reqs atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
		var off, last int64
		_, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &off, &last)
		require.NoError(t, err)
		last = min(last, int64(len(body)-1))
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, last, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[off : off+(10<<10)]) // always cut mid-body
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		panic(http.ErrAbortHandler)
	}))
	t.Cleanup(srv.Close)

	d := singleStreamDownloader(srv.URL, 2, 64<<10)
	d.xc.Retry = gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(2)})
	_, err := d.fetchSegment(context.Background(), "seg.jss")
	require.Error(t, err)
	require.Equal(t, int64(2), reqs.Load(),
		"attempts=2 means probe + one gap-fill, the probe attempt is debited")
}

// TestFetchSegmentProbeBodyCutSingleAttemptFailsFast: with attempts=1 a probe
// body cut on the 206 path must not trigger a gap-fill request.
func TestFetchSegmentProbeBodyCutSingleAttemptFailsFast(t *testing.T) {
	t.Parallel()
	body := patternBody(48 << 10)
	etag := `"` + segETag(body) + `"`
	var reqs atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
		var off, last int64
		_, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &off, &last)
		require.NoError(t, err)
		last = min(last, int64(len(body)-1))
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, last, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[:20<<10])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		panic(http.ErrAbortHandler)
	}))
	t.Cleanup(srv.Close)

	d := singleStreamDownloader(srv.URL, 2, 64<<10)
	d.xc.Retry = gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})
	_, err := d.fetchSegment(context.Background(), "seg.jss")
	require.Error(t, err, "attempts=1 must fail fast on a probe body cut")
	require.Equal(t, int64(1), reqs.Load(), "exactly one request with attempts=1")
}

// TestFetchSegmentProbeBodyCutIsRetried guards the probe's body read: when the
// first range response's body dies mid-read, the missing suffix must be
// re-fetched with bounded retries rather than failing the segment. Small
// segments complete entirely in the probe, so without this the "resumable"
// path never gets a chance to help them.
func TestFetchSegmentProbeBodyCutIsRetried(t *testing.T) {
	t.Parallel()
	body := patternBody(48 << 10) // fits in one 64 KiB probe part
	etag := `"` + segETag(body) + `"`
	const cutAt = 20 << 10
	var cut atomic.Bool
	var refetched atomic.Int64
	refetched.Store(-1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var off, last int64
		_, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &off, &last)
		require.NoError(t, err)
		last = min(last, int64(len(body)-1))
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, last, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		if off == 0 && cut.CompareAndSwap(false, true) {
			_, _ = w.Write(body[:cutAt]) // die mid-probe-body
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			panic(http.ErrAbortHandler)
		}
		if off > 0 {
			refetched.CompareAndSwap(-1, off)
		}
		_, _ = w.Write(body[off : last+1])
	}))
	t.Cleanup(srv.Close)

	d := singleStreamDownloader(srv.URL, 2, 64<<10)
	got, err := d.fetchSegment(context.Background(), "seg.jss")
	require.NoError(t, err, "a probe body cut must be retried, not fail the segment")
	require.Equal(t, body, got, "retried download must be byte-identical")
	require.Equal(t, int64(cutAt), refetched.Load(),
		"the gap-fill must resume from the exact cut offset")
}

// TestFetchSegmentCarriesAuthHeaders: the raw range path must send the same
// identity headers as the xrpc QueryRaw path (Authorization, User-Agent),
// or authenticated archives break for whole segments while getBlock works.
func TestFetchSegmentCarriesAuthHeaders(t *testing.T) {
	t.Parallel()
	body := patternBody(300 << 10)
	s, url := newSegServer(t, body)
	var missing atomic.Int64
	s.mu.Lock()
	s.intercept = func(w http.ResponseWriter, r *http.Request) bool {
		if r.Header.Get("Authorization") != "Bearer test-jwt" || r.Header.Get("User-Agent") == "" {
			missing.Add(1)
		}
		return false
	}
	s.mu.Unlock()

	d := stripedDownloader(url, 4, 64<<10)
	d.xc.SetAuth(&xrpc.AuthInfo{AccessJwt: "test-jwt"})
	got, err := d.fetchSegment(context.Background(), "seg.jss")
	require.NoError(t, err)
	require.Equal(t, body, got)
	require.Equal(t, int64(0), missing.Load(), "every segment request must carry auth + UA headers")
	require.Greater(t, s.reqs.Load(), int64(1), "multi-request fetch expected")
}

// TestPartAttemptsClampsToOne: xrpc treats MaxAttempts=0 as one attempt; the
// segment path must too, or a valid retry policy disables downloads entirely.
func TestPartAttemptsClampsToOne(t *testing.T) {
	t.Parallel()
	body := patternBody(300 << 10)
	_, url := newSegServer(t, body)
	d := stripedDownloader(url, 4, 64<<10)
	d.xc.Retry = gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(0)})
	require.Equal(t, 1, d.partAttempts())
	got, err := d.fetchSegment(context.Background(), "seg.jss")
	require.NoError(t, err, "MaxAttempts=0 must mean one attempt, not zero")
	require.Equal(t, body, got)
}

// TestParseRetryAfterAndBackoff: server-supplied retry deferrals must be
// honored (clamped), preferred over exponential backoff only when longer, and
// malformed/hostile values must never stall the client.
func TestParseRetryAfterAndBackoff(t *testing.T) {
	t.Parallel()
	h := func(k, v string) http.Header {
		hdr := http.Header{}
		hdr.Set(k, v)
		return hdr
	}
	require.Equal(t, 5*time.Second, parseRetryAfter(h("Retry-After", "5")))
	require.Equal(t, maxRetryAfterWait, parseRetryAfter(h("Retry-After", "3600")), "clamped to the cap")
	require.Equal(t, time.Duration(0), parseRetryAfter(h("Retry-After", "-10")), "negative → 0")
	require.Equal(t, time.Duration(0), parseRetryAfter(h("Retry-After", "garbage")))
	require.Equal(t, time.Duration(0), parseRetryAfter(http.Header{}))
	reset := strconv.FormatInt(time.Now().Add(4*time.Second).Unix(), 10)
	got := parseRetryAfter(h("RateLimit-Reset", reset))
	require.InDelta(t, float64(4*time.Second), float64(got), float64(time.Second))

	// Backoff: the server hint wins only when longer than the schedule.
	long := &statusError{err: &xrpc.Error{StatusCode: 429}, retryAfter: 5 * time.Second}
	require.Equal(t, 5*time.Second, retryBackoff(time.Second, 1, long))
	require.Equal(t, 8*time.Second, retryBackoff(time.Second, 4, long), "exponential wins when longer")
	require.Equal(t, 2*time.Second, retryBackoff(time.Second, 2, errors.New("plain")), "no hint → schedule")
	// permanentError wrapping must not hide the hint.
	require.Equal(t, 5*time.Second, retryBackoff(time.Second, 1, &permanentError{err: long}))
	// Large attempt counts clamp to the cap instead of overflowing.
	require.Equal(t, maxRetryAfterWait, retryBackoff(time.Second, 80, errors.New("plain")))
	require.Equal(t, maxRetryAfterWait, retryBackoff(500*time.Millisecond, 40, nil))
}

func TestParseContentRange(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in                string
		start, end, total int64
		ok                bool
	}{
		{"bytes 0-99/1000", 0, 99, 1000, true},
		{"bytes 900-999/1000", 900, 999, 1000, true},
		{"bytes 0-0/1", 0, 0, 1, true},
		{"bytes 0-99/*", 0, 0, 0, false},
		{"bytes 99-0/1000", 0, 0, 0, false},
		{"bytes 0-1000/1000", 0, 0, 0, false}, // end >= total
		{"items 0-99/1000", 0, 0, 0, false},
		{"bytes 0-99", 0, 0, 0, false},
		{"", 0, 0, 0, false},
		{fmt.Sprintf("bytes 0-99/%d", int64(1)<<62), 0, 99, 1 << 62, true},
	}
	for _, c := range cases {
		start, end, total, err := parseContentRange(c.in)
		if !c.ok {
			require.Error(t, err, "input %q", c.in)
			continue
		}
		require.NoError(t, err, "input %q", c.in)
		require.Equal(t, []int64{c.start, c.end, c.total}, []int64{start, end, total}, "input %q", c.in)
	}
}
