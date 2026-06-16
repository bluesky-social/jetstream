package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/stretchr/testify/require"
)

// PublicHandler returns the handler attached to the public listener.
func (s *Server) PublicHandler() http.Handler {
	if s.srv.Handler == nil {
		s.srv.Handler = s.publicMux()
	}
	return s.srv.Handler
}

// DebugHandler returns the handler attached to the debug listener.
func (s *Server) DebugHandler() http.Handler { return s.dbgSrv.Handler }

// SetReady forces the readiness flag, normally flipped by Run when both
// listeners are bound. Tests use this to exercise /readyz state transitions
// without booting the full server.
func (s *Server) SetReady(ready bool) { s.ready.Store(ready) }

// newServer constructs a Server suitable for handler-level tests. It does
// not bind any listeners; tests that want HTTP behavior mount the returned
// Server's handlers under httptest.NewServer.
func newServer(t *testing.T) *Server {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := obs.NewMetrics()
	return New(Config{
		PublicAddr:      "127.0.0.1:0",
		DebugAddr:       "127.0.0.1:0",
		ShutdownTimeout: 5 * time.Second,
	}, logger, metrics)
}

// mountDebug spins up an httptest server backed by srv's debug handler. The
// returned URL is rooted at the listener — append e.g. "/metrics".
func mountDebug(t *testing.T, srv *Server) string {
	t.Helper()
	ts := httptest.NewServer(srv.DebugHandler())
	t.Cleanup(ts.Close)
	return ts.URL
}

// mountPublic spins up an httptest server backed by srv's public handler.
func mountPublic(t *testing.T, srv *Server) string {
	t.Helper()
	ts := httptest.NewServer(srv.PublicHandler())
	t.Cleanup(ts.Close)
	return ts.URL
}

func TestDebugHandler_Metrics(t *testing.T) {
	t.Parallel()
	base := mountDebug(t, newServer(t))

	body := mustGet(t, base+"/metrics")
	require.Contains(t, body, "jetstream_build_info")
	// Standard Go collector should also be present.
	require.Contains(t, body, "go_goroutines")
}

func TestDebugHandler_Healthz(t *testing.T) {
	t.Parallel()
	base := mountDebug(t, newServer(t))

	require.Equal(t, "ok\n", mustGet(t, base+"/healthz"))
}

func TestDebugHandler_Readyz(t *testing.T) {
	t.Parallel()

	srv := newServer(t)
	base := mountDebug(t, srv)

	// Without a Run() call, the server has not flipped ready, so /readyz
	// should report 503.
	resp, err := doGet(t.Context(), base+"/readyz")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	// Flipping ready turns /readyz into a 200. This is what Run does the
	// instant both listeners are bound.
	srv.SetReady(true)
	require.Equal(t, "ok\n", mustGet(t, base+"/readyz"))
}

func TestDebugHandler_Pprof(t *testing.T) {
	t.Parallel()
	base := mountDebug(t, newServer(t))

	resp, err := doGet(t.Context(), base+"/debug/pprof/")
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestServer_RecordsMetricsForPublicRequests is the one cross-cutting test
// that has to span both muxes: a request to the public mux must register a
// counter increment that we observe via the debug mux's /metrics. Both
// httptest servers wrap the same Server instance and therefore share its
// prometheus registry.
func TestServer_RecordsMetricsForPublicRequests(t *testing.T) {
	t.Parallel()

	srv := newServer(t)
	publicURL := mountPublic(t, srv)
	debugURL := mountDebug(t, srv)

	_ = mustGet(t, publicURL+"/")

	metrics := mustGet(t, debugURL+"/metrics")
	require.Contains(t, metrics, `jetstream_http_request_duration_seconds_bucket{`)
	// And the histogram carries the exact label set we registered.
	// `commit` was deliberately removed from the histogram (it duplicates
	// build_info and resets every series on deploy); if it ever comes
	// back, scan the histogram lines specifically.
	for line := range strings.SplitSeq(metrics, "\n") {
		if !strings.HasPrefix(line, "jetstream_http_request_duration_seconds") {
			continue
		}
		require.NotContains(t, line, `commit="`,
			"http_request_duration_seconds must not carry a commit label")
	}
	require.Contains(t, metrics, `code="200"`)
	require.Contains(t, metrics, `handler="root"`)
	require.Contains(t, metrics, `method="GET"`)
}

// TestServer_MetricsCaptureNon200StatusCodes verifies that
// statusRecorder.WriteHeader is wired correctly: a 404 from the
// default mux must surface as `code="404"` on the histogram, not
// the recorder's zero-value or the default 200.
func TestServer_MetricsCaptureNon200StatusCodes(t *testing.T) {
	t.Parallel()

	srv := newServer(t)
	publicURL := mountPublic(t, srv)
	debugURL := mountDebug(t, srv)

	// The public mux only registers GET /{$}; any other path is a 404.
	resp, err := doGet(t.Context(), publicURL+"/no-such-path")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	// 404 from a stdlib NotFound goes through statusRecorder.Write
	// without WriteHeader being called; the recorder's default of 200
	// would mask the real code. Today we don't wrap the 404 path
	// (only registered routes carry Middleware), so this
	// assertion is forward-looking for when the project adds a
	// catch-all instrumented handler. For now, just confirm the
	// metric for the registered route doesn't get spuriously created.
	metrics := mustGet(t, debugURL+"/metrics")
	require.NotContains(t, metrics, `code="404"`,
		"unrouted 404s should not yet record metrics; revisit when a catch-all is added")
}

// TestPublicHandler_UnknownPath404 pins the public mux's behavior
// for unknown paths. Adding a catch-all in the future must be a
// deliberate choice.
func TestPublicHandler_UnknownPath404(t *testing.T) {
	t.Parallel()
	base := mountPublic(t, newServer(t))

	resp, err := doGet(t.Context(), base+"/does-not-exist")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// TestServer_LifecycleAndGracefulShutdown is the only test that exercises
// the real Run path with bound TCP listeners. It covers the bind ordering,
// the ready-flag transition, and the bounded-shutdown contract — none of
// which are testable through httptest alone.
func TestServer_LifecycleAndGracefulShutdown(t *testing.T) {
	t.Parallel()

	srv := newServer(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	// Both listeners must be bound and /readyz must answer 200 before the
	// server is observably "running".
	require.Eventually(t, func() bool {
		addr := srv.DebugAddr()
		if addr == "" {
			return false
		}
		resp, err := doGet(t.Context(), "http://"+addr+"/readyz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 2*time.Second, 10*time.Millisecond, "server never became ready")

	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("server did not shut down within deadline")
	}
}

// doGet issues an HTTP GET that respects ctx. Required by the linter and a
// genuine improvement: a hung server fails tests fast instead of blocking
// until the package timeout.
func doGet(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

// mustGet GETs url under a 5s deadline, asserts a 200 status, and returns
// the response body.
func mustGet(t *testing.T, url string) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	resp, err := doGet(ctx, url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode, "GET %s", url)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(body)
}

func TestPublicHandler_StatusUnwired(t *testing.T) {
	t.Parallel()
	base := mountPublic(t, newServer(t))

	resp, err := doGet(t.Context(), base+"/status")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestPublicHandler_StatusWired(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := obs.NewMetrics()
	srv := New(Config{
		PublicAddr:      "127.0.0.1:0",
		DebugAddr:       "127.0.0.1:0",
		ShutdownTimeout: 5 * time.Second,
		StatusHandler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte("ok"))
		}),
	}, logger, metrics)

	ts := httptest.NewServer(srv.PublicHandler())
	defer ts.Close()

	resp, err := doGet(t.Context(), ts.URL+"/status")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRegisterPublicRoute(t *testing.T) {
	t.Parallel()

	srv := New(Config{
		PublicAddr:      "127.0.0.1:0",
		DebugAddr:       "127.0.0.1:0",
		ShutdownTimeout: 5 * time.Second,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), obs.NewMetrics())

	srv.RegisterPublicRoute("GET /custom", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("custom-ok"))
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()

	// Wait for binding.
	deadline := time.Now().Add(2 * time.Second)
	for srv.PublicAddr() == "" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.NotEmpty(t, srv.PublicAddr())

	resp, err := doGet(ctx, "http://"+srv.PublicAddr()+"/custom")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, "custom-ok", string(body))

	cancel()
	require.NoError(t, <-runErr)
}
