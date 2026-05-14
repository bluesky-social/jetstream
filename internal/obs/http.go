package obs

import (
	"net/http"
	"strconv"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/version"
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
		m.HTTPRequestDuration.WithLabelValues(version.Get().Commit, name, r.Method, code).Observe(time.Since(start).Seconds())
	})

	return otelhttp.NewHandler(wrapped, name)
}

// statusRecorder captures the status code so we can label metrics with it.
// It deliberately doesn't implement Hijacker/Flusher/etc. yet — when we add a
// websocket handler, we'll either skip this middleware for that route or
// promote this to a more thorough wrapper.
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
