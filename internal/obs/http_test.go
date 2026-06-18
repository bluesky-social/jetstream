package obs_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
)

// requireHistogramHasObservation asserts that the given label
// combination on m.HTTPRequestDuration has at least one
// observation recorded. We scrape the live registry through the
// promhttp handler we already use in production so the test
// exercises the same exposition path operators see.
func requireHistogramHasObservation(t *testing.T, m *obs.Metrics, handler, method, code string) {
	t.Helper()

	rec := httptest.NewRecorder()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	require.NoError(t, err)
	promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}).ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)

	want := fmt.Sprintf(
		`jetstream_http_request_duration_seconds_count{code=%q,handler=%q,method=%q} `,
		code, handler, method,
	)
	for line := range strings.SplitSeq(string(body), "\n") {
		if strings.HasPrefix(line, want) {
			return
		}
	}
	t.Fatalf(
		"no observation for handler=%q method=%q code=%q\nfull text:\n%s",
		handler, method, code, body,
	)
}

// getCtx is the noctx-clean test helper for issuing a GET. Returns
// the response with body intact; caller closes.
func getCtx(t *testing.T, url string) *http.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// TestMiddleware_RecordsStatusCode is the regression test for
// statusRecorder.WriteHeader: any non-200 response must propagate into
// the `code` histogram label. Without this, a silent regression that
// always labels code="200" would slip past the existing integration
// test (which only checks that a histogram bucket line exists).
func TestMiddleware_RecordsStatusCode(t *testing.T) {
	t.Parallel()

	m := obs.NewMetrics()
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	ts := httptest.NewServer(m.Middleware("teapot", handler))
	t.Cleanup(ts.Close)

	resp := getCtx(t, ts.URL)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusTeapot, resp.StatusCode)

	requireHistogramHasObservation(t, m, "teapot", http.MethodGet, "418")
}

// TestMiddleware_DefaultStatusIs200 covers the path where a
// handler writes a body without an explicit WriteHeader call: the
// recorder's default status (200) must be the one observed.
func TestMiddleware_DefaultStatusIs200(t *testing.T) {
	t.Parallel()

	m := obs.NewMetrics()
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("hello"))
	})

	ts := httptest.NewServer(m.Middleware("hello", handler))
	t.Cleanup(ts.Close)

	resp := getCtx(t, ts.URL)
	require.NoError(t, resp.Body.Close())

	requireHistogramHasObservation(t, m, "hello", http.MethodGet, "200")
}

// TestMiddleware_PreservesHijacker is the regression test for
// the websocket-upgrade footgun. A statusRecorder that doesn't expose
// http.Hijacker breaks every websocket handler in surprising ways at
// production-traffic time. We assert by checking that the handler
// can type-assert the wrapped writer to http.Hijacker and use it.
func TestMiddleware_PreservesHijacker(t *testing.T) {
	t.Parallel()

	m := obs.NewMetrics()
	hijacked := make(chan error, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		h, ok := w.(http.Hijacker)
		if !ok {
			hijacked <- errors.New("statusRecorder does not implement http.Hijacker")
			return
		}
		conn, _, err := h.Hijack()
		if err != nil {
			hijacked <- err
			return
		}
		hijacked <- nil
		_ = conn.Close()
	})

	ts := httptest.NewServer(m.Middleware("hijack", handler))
	t.Cleanup(ts.Close)

	dialCtx, dialCancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer dialCancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(dialCtx, "tcp", strings.TrimPrefix(ts.URL, "http://"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"))
	require.NoError(t, err)

	select {
	case err := <-hijacked:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("handler never reported hijack outcome")
	}
}

// TestStatusRecorder_HijackUnsupported asserts the graceful failure
// mode when statusRecorder wraps a writer that itself doesn't
// implement Hijacker. We surface a clear error rather than panicking.
func TestStatusRecorder_HijackUnsupported(t *testing.T) {
	t.Parallel()

	m := obs.NewMetrics()
	captured := make(chan error, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		h, ok := w.(http.Hijacker)
		require.True(t, ok, "wrapper must always expose Hijacker, even if delegate fails")
		_, _, err := h.Hijack()
		captured <- err
	})

	// httptest.NewRecorder's ResponseWriter doesn't implement Hijacker.
	rec := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodGet, "/", nil) //nolint:noctx // synthesized request
	require.NoError(t, err)
	m.Middleware("hijack-fail", handler).ServeHTTP(rec, req)

	select {
	case err := <-captured:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("Hijack call never returned")
	}
}

// readerFromRecorder is a minimal ResponseWriter that also implements
// io.ReaderFrom, standing in for the real *net/http response which uses
// ReadFrom to trigger sendfile(2).
type readerFromRecorder struct {
	http.ResponseWriter
	readFromCalled bool
}

func (r *readerFromRecorder) ReadFrom(src io.Reader) (int64, error) {
	r.readFromCalled = true
	return io.Copy(r.ResponseWriter, src)
}

// TestStatusRecorder_PreservesReaderFrom is the regression test for
// sendfile(2) performance. http.ServeContent triggers zero-copy transfer
// when the ResponseWriter implements io.ReaderFrom; a statusRecorder that
// doesn't expose it forces slow userspace copies for large file downloads.
func TestStatusRecorder_PreservesReaderFrom(t *testing.T) {
	t.Parallel()

	m := obs.NewMetrics()
	inner := &readerFromRecorder{ResponseWriter: httptest.NewRecorder()}
	readFromCalled := false

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		rf, ok := w.(io.ReaderFrom)
		require.True(t, ok, "statusRecorder should implement io.ReaderFrom")

		n, err := rf.ReadFrom(strings.NewReader("hello"))
		require.NoError(t, err)
		require.EqualValues(t, 5, n)
		readFromCalled = inner.readFromCalled
	})

	req, err := http.NewRequest(http.MethodGet, "/", nil) //nolint:noctx // synthesized request
	require.NoError(t, err)
	m.Middleware("readfrom", handler).ServeHTTP(inner, req)

	require.True(t, readFromCalled, "ReadFrom must delegate to the inner writer (sendfile path)")
}
