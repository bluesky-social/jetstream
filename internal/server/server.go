// Package server owns the HTTP listeners for the jetstream process.
//
// We expose two listeners:
//
//   - Public (default :8080): the protocol surface.
//
//   - Debug (default :6060): operations endpoints — /metrics, /healthz,
//     /debug/pprof, etc. Should not be exposed to the public internet
//     when the system is deployed.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/bluesky-social/jetstream-v2/internal/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
)

// Config controls the listeners. Zero values are not valid; callers should
// populate explicitly from CLI flags.
type Config struct {
	// PublicAddr is the bind address for the public listener (e.g. ":8080").
	PublicAddr string

	// DebugAddr is the bind address for the metrics/pprof listener (e.g. ":6060").
	DebugAddr string

	// ShutdownTimeout bounds how long graceful shutdown is allowed to take.
	// After this elapses, in-flight requests are abandoned.
	ShutdownTimeout time.Duration

	// StatusHandler, if non-nil, is mounted at GET /status and HEAD
	// /status on the public listener. cmd/jetstream constructs this via
	// the web package; tests can pass any http.Handler.
	StatusHandler http.Handler
}

type publicRoute struct {
	pattern string
	handler http.Handler
}

// Server bundles the public and debug HTTP servers and the readiness flag
// they share. It is constructed via New and driven via Run.
type Server struct {
	cfg Config

	logger  *slog.Logger
	metrics *obs.Metrics

	srv    *http.Server
	dbgSrv *http.Server

	statusHandler http.Handler

	// publicAddr and debugAddr hold the bound addresses once Run starts.
	// They're stored as atomic pointers because they're written by the Run
	// goroutine and read by external callers (tests, /readyz observers).
	publicAddr atomic.Pointer[string]
	debugAddr  atomic.Pointer[string]

	// ready is flipped to true once both listeners are bound and serving.
	// /readyz reads it atomically.
	ready atomic.Bool

	// extraPublicRoutes is appended-to by RegisterPublicRoute. It's
	// drained once when Run builds the public mux, so registrations
	// after Run starts are not observed.
	extraPublicRoutes []publicRoute
}

// New wires up the muxes for both listeners. It does not bind any sockets;
// that happens in Run.
func New(cfg Config, logger *slog.Logger, metrics *obs.Metrics) *Server {
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	logger = logger.With(slog.String("component", "server"))

	s := &Server{cfg: cfg, logger: logger, metrics: metrics, statusHandler: cfg.StatusHandler}

	// Timeout policy:
	//
	//   ReadHeaderTimeout: 10s. Bounds slowloris-style attacks where a
	//     malicious client trickles request headers byte-by-byte. Cheap and
	//     universally safe — well-behaved clients send headers in one packet.
	//
	//   IdleTimeout: 2m. Bounds keep-alive connections that are sitting open
	//     between requests. Does NOT affect in-flight requests, so it's safe
	//     to apply globally. Closes leaked fds from clients that forgot to
	//     hang up.
	//
	//   ReadTimeout / WriteTimeout: deliberately UNSET on the public server.
	//     The public surface will serve large segment-file downloads (~256 MB
	//     each) and long-lived websocket streams; a global WriteTimeout would
	//     kill both. ReadTimeout would similarly cap legitimate streaming
	//     uploads (e.g. the bulk-timestamp import CSV in DESIGN.md §8).
	//     Per-handler deadlines via r.Context() are the right tool when an
	//     individual route needs a bound.
	//
	//     The debug server inherits the same omissions for symmetry: pprof's
	//     /debug/pprof/profile and /trace endpoints accept a `seconds` query
	//     param and intentionally hold the response open for that long, so
	//     a WriteTimeout here would silently truncate profiles.
	// Route http.Server's internal error log through slog so panic
	// recovery, TLS handshake failures, and other server-internal
	// messages share the same JSON-friendly pipeline as everything
	// else. Without this they go through the standard log package
	// to stderr as unstructured text and bypass log shipping.
	stdErrLog := slog.NewLogLogger(logger.Handler(), slog.LevelError)

	s.srv = &http.Server{
		Addr:              cfg.PublicAddr,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          stdErrLog,
	}

	s.dbgSrv = &http.Server{
		Addr:              cfg.DebugAddr,
		Handler:           s.debugMux(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          stdErrLog,
	}

	return s
}

// RegisterPublicRoute attaches an additional handler to the public mux.
// Must be called before Run; routes registered after Run starts are not
// observed. Pattern uses Go 1.22+ ServeMux syntax (e.g. "GET /subscribe").
func (s *Server) RegisterPublicRoute(pattern string, h http.Handler) {
	s.extraPublicRoutes = append(s.extraPublicRoutes, publicRoute{
		pattern: pattern,
		handler: h,
	})
}

// Run binds both listeners and serves until ctx is cancelled, at which point
// it triggers graceful shutdown bounded by ShutdownTimeout. Run returns nil
// if shutdown completed cleanly, or the first error encountered.
//
// Both listeners are bound synchronously before serve goroutines start, so
// callers can rely on PublicAddr/DebugAddr having concrete addresses (if
// they used :0) by the time Run is observable to be running.
func (s *Server) Run(ctx context.Context) error {
	// Build the public mux now so RegisterPublicRoute calls between
	// New and Run are observed.
	s.srv.Handler = s.publicMux()

	// A zero-value ListenConfig matches the behavior of the package-level
	// net.Listen but lets the bind respect ctx (e.g. cancelled while a
	// reverse-resolved DNS lookup is in flight).
	var lc net.ListenConfig
	publicLn, err := lc.Listen(ctx, "tcp", s.cfg.PublicAddr)
	if err != nil {
		return fmt.Errorf("bind public listener %q: %w", s.cfg.PublicAddr, err)
	}

	debugLn, err := lc.Listen(ctx, "tcp", s.cfg.DebugAddr)
	if err != nil {
		_ = publicLn.Close()
		return fmt.Errorf("bind debug listener %q: %w", s.cfg.DebugAddr, err)
	}

	// Publish the bound addresses so callers (tests, log lines, /readyz
	// observers) see the resolved port — relevant when binding to :0.
	publicAddr := publicLn.Addr().String()
	s.publicAddr.Store(&publicAddr)
	debugAddr := debugLn.Addr().String()
	s.debugAddr.Store(&debugAddr)

	s.logger.Info("listening",
		"public", publicAddr,
		"debug", debugAddr,
	)

	// Buffered so a fast-failing Serve doesn't block the goroutine forever
	// if the parent has already moved on.
	errs := make(chan error, 2)
	go func() { errs <- serveOrIgnoreClosed(s.srv, publicLn) }()
	go func() { errs <- serveOrIgnoreClosed(s.dbgSrv, debugLn) }()

	// Best-effort: if either Serve has already failed by the time we
	// reach this line, don't claim readiness even for the brief
	// window before the select below catches the error. This closes
	// a race where /readyz could answer 200 to a probe scheduled
	// between Store(true) and the err-channel branch firing.
	select {
	case err := <-errs:
		s.logger.Error("failed during startup", "err", err)
		_ = s.shutdown()
		// Drain the second goroutine.
		<-errs
		return err
	default:
	}

	s.ready.Store(true)

	// Wait for either a serve error or context cancellation. Either path
	// proceeds to graceful shutdown.
	var firstErr error
	receivedFromErrs := 0
	select {
	case <-ctx.Done():
		s.logger.Info("shutdown requested", "timeout", s.cfg.ShutdownTimeout)
	case err := <-errs:
		receivedFromErrs = 1
		// One server failed; treat as fatal and tear the other down.
		firstErr = err
		s.logger.Error("exited unexpectedly", "err", err)
	}

	s.ready.Store(false)
	if shutdownErr := s.shutdown(); shutdownErr != nil && firstErr == nil {
		firstErr = shutdownErr
	}

	// Drain the remaining serve goroutine results. Both goroutines
	// always send exactly one value, so we always read exactly two
	// total — otherwise a second concurrent failure during shutdown
	// would be silently dropped on the floor.
	for receivedFromErrs < 2 {
		if err := <-errs; err != nil && firstErr == nil {
			firstErr = err
		}
		receivedFromErrs++
	}

	return firstErr
}

// shutdown terminates both servers within ShutdownTimeout
func (s *Server) shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()

	s.srv.SetKeepAlivesEnabled(false)
	s.dbgSrv.SetKeepAlivesEnabled(false)

	errs := errgroup.Group{}
	errs.Go(func() error { return s.srv.Shutdown(ctx) })
	errs.Go(func() error { return s.dbgSrv.Shutdown(ctx) })

	if err := errs.Wait(); err != nil {
		return fmt.Errorf("failed to shutdown server: %w", err)
	}

	return nil
}

// PublicAddr returns the bound public listener address, or "" if Run has not
// yet bound it.
func (s *Server) PublicAddr() string {
	if p := s.publicAddr.Load(); p != nil {
		return *p
	}
	return ""
}

// DebugAddr returns the bound debug listener address, or "" if Run has not
// yet bound it.
func (s *Server) DebugAddr() string {
	if p := s.debugAddr.Load(); p != nil {
		return *p
	}
	return ""
}

// publicMux builds the user-facing routes. Each route is wrapped with the
// prom + OTEL middleware via Metrics.InstrumentHandler.
func (s *Server) publicMux() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /{$}", s.metrics.InstrumentHandler("root", http.HandlerFunc(s.handleRoot)))
	if s.statusHandler != nil {
		instrumented := s.metrics.InstrumentHandler("status", s.statusHandler)
		mux.Handle("GET /status", instrumented)
		mux.Handle("HEAD /status", instrumented)
	}
	for _, r := range s.extraPublicRoutes {
		// Wrap each registered route in the same prom + OTEL middleware
		// as the built-in / route, with a metric label derived from the
		// pattern (last path segment).
		mux.Handle(r.pattern, s.metrics.InstrumentHandler(routeLabel(r.pattern), r.handler))
	}
	return mux
}

// routeLabel derives a stable, low-cardinality metric label from a
// servemux pattern such as "GET /subscribe" -> "subscribe".
func routeLabel(pattern string) string {
	if i := strings.IndexByte(pattern, ' '); i >= 0 {
		pattern = pattern[i+1:]
	}
	pattern = strings.TrimPrefix(pattern, "/")
	if pattern == "" || pattern == "{$}" {
		return "root"
	}
	return pattern
}

// debugMux builds the operator-facing routes.
//
// We deliberately use a fresh ServeMux rather than http.DefaultServeMux:
// DefaultServeMux is a process-wide singleton, so reusing it would (a) panic
// the second time New() runs in the same process — including across tests —
// and (b) silently expose any other library's init()-time DefaultServeMux
// registrations on our debug listener.
//
// pprof's handlers are registered explicitly here. We import net/http/pprof
// for those handler functions, not for its init()-time side effect.
func (s *Server) debugMux() http.Handler {
	mux := http.NewServeMux()

	mux.Handle("GET /metrics", promhttp.HandlerFor(s.metrics.Registry, promhttp.HandlerOpts{
		Registry: s.metrics.Registry,
	}))

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	// pprof. Index dispatches to the per-profile handlers based on path, so
	// the trailing-slash route covers /debug/pprof/heap, /debug/pprof/goroutine,
	// etc. The four explicit routes below are the special-cased non-profile
	// endpoints that need their own handler functions. We deliberately do
	// not method-restrict any of these: pprof.Index uses its own path
	// matching for sub-profile dispatch, and pprof.Symbol legitimately
	// accepts POST for large symbol resolution requests from go tool pprof.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return mux
}

// handleRoot returns a small JSON identity payload. It exists primarily to
// give us something real to instrument while the substantive endpoints are
// being designed.
func (s *Server) handleRoot(w http.ResponseWriter, _ *http.Request) {
	resp := map[string]string{
		"name":    "jetstream",
		"version": version.Get().Version,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// serveOrIgnoreClosed runs srv.Serve and treats ErrServerClosed as nil so
// graceful shutdown doesn't surface as a startup error.
func serveOrIgnoreClosed(srv *http.Server, ln net.Listener) error {
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}
