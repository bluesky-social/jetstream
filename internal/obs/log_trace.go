package obs

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// traceContextHandler wraps a slog.Handler and injects trace_id /
// span_id record attributes when the record is emitted via the
// *Context slog variants and an active span is found in ctx. Logs
// outside an active span pass through unchanged.
//
// The wrapper is a thin delegate: WithAttrs / WithGroup re-wrap the
// inner so caller-provided attributes stack normally.
//
// WithGroup scoping: trace_id and span_id are added to the record
// before it is forwarded to the inner handler, which means any
// WithGroup-active group at the call site will scope these IDs under
// its prefix. We accept this — group-scoped IDs are arguably more
// useful for grepping per-request logs in JSON — and document it
// here so it isn't a surprise.
type traceContextHandler struct {
	inner slog.Handler
}

func newTraceContextHandler(inner slog.Handler) slog.Handler {
	return &traceContextHandler{inner: inner}
}

func (h *traceContextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *traceContextHandler) Handle(ctx context.Context, r slog.Record) error {
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.inner.Handle(ctx, r)
}

func (h *traceContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceContextHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *traceContextHandler) WithGroup(name string) slog.Handler {
	return &traceContextHandler{inner: h.inner.WithGroup(name)}
}
