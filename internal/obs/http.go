package obs

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// InstrumentHandler wraps h with prometheus and OTEL HTTP instrumentation.
// `name` is the logical handler label used as the `handler` metric label and
// the otelhttp span name; it should be a short identifier like "root" or
// "segments_download", not the raw path (which would explode label
// cardinality).
func (m *Metrics) InstrumentHandler(name string, h http.Handler) http.Handler {
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		h.ServeHTTP(rec, r)

		code := strconv.Itoa(rec.status)
		m.HTTPRequestDuration.WithLabelValues(name, r.Method, code).Observe(time.Since(start).Seconds())
	})

	return otelhttp.NewHandler(wrapped, name)
}

// Middleware is the canonical name AGENTS.md uses for the
// instrumentation wrapper above. Kept as a thin alias so callers
// can write the documented form.
func (m *Metrics) Middleware(name string, h http.Handler) http.Handler {
	return m.InstrumentHandler(name, h)
}

// statusRecorder captures the status code so we can label metrics
// with it. It transparently delegates the optional ResponseWriter
// interfaces (Hijacker, Flusher, Pusher) so middleware does not
// silently break websocket upgrades or response streaming.
//
// Wrappers that fully break Hijacker are a notorious bug class
// (the websocket library returns "response writer does not support
// hijacking" the first time someone adds a websocket route), so we
// pay the small cost of the assertions up front rather than
// discover it at production-traffic time.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.status = code
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.wroteHeader = true
	}
	return r.ResponseWriter.Write(b)
}

// Flush forwards to the underlying writer if it supports it. Used
// by streaming responses (chunked transfer, server-sent events).
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack forwards to the underlying writer if it supports it.
// Required for websocket upgrades.
func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("statusRecorder: %w", errHijackUnsupported)
	}
	return h.Hijack()
}

// Push forwards HTTP/2 server push to the underlying writer.
// Returns http.ErrNotSupported when the underlying writer doesn't
// implement it, matching the contract callers already expect from
// http.Pusher.
func (r *statusRecorder) Push(target string, opts *http.PushOptions) error {
	if p, ok := r.ResponseWriter.(http.Pusher); ok {
		return p.Push(target, opts)
	}
	return http.ErrNotSupported
}

var errHijackUnsupported = errors.New("underlying ResponseWriter does not implement http.Hijacker")
