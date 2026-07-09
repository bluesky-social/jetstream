package client

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/atmos/xrpc"
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
