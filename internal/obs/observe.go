// Package obs: observe.go provides the canonical tracing helper used
// throughout jetstream. Observe opens a span named after the calling
// function (via gt.Caller) and returns the derived ctx, the span,
// and a done(err) closure that finalizes the span exactly once.
//
// Discipline: Observe must be called directly from the function it
// represents. Wrapping it in another helper changes the caller depth
// and produces wrong span names. The helper is intentionally trace-
// only — latency histograms remain explicit, deliberate, per-
// subsystem decisions.
//
// Hot-path rule: Observe must NOT be used from per-record / per-event
// code paths (e.g. ingest.Writer.Append, live.ConvertEvent's inner op
// loop). Per-event spans would balloon to billions/day at full
// network scale and overwhelm any trace exporter. Use it at per-
// batch, per-block, per-repo, per-seal, per-phase-transition
// granularity.
package obs

import (
	"context"
	"sync"

	"github.com/jcalabro/gt"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Observe starts a span named after the calling function. Returns the
// derived ctx, the span (so callers can attach attributes), and a
// done(err) closure that ends the span and sets status. done is
// idempotent: subsequent calls are no-ops.
//
// HOT PATH: do not call from per-record/per-event code paths; use at
// per-batch / per-block / per-phase granularity. See package doc.
//
// Status mapping:
//   - err == nil → codes.Ok
//   - any other err (including context.Canceled) → codes.Error +
//     span.RecordError(err)
func Observe(ctx context.Context, opts ...trace.SpanStartOption) (context.Context, trace.Span, func(error)) {
	// skip=2: gt.Caller(0)=Caller itself, (1)=Observe, (2)=user code.
	info := gt.Caller(2)
	ctx, span := tracerForCallerInfo(info).Start(ctx, info.Func, opts...)

	var once sync.Once
	return ctx, span, func(err error) {
		once.Do(func() {
			defer span.End()
			if err == nil {
				span.SetStatus(codes.Ok, "")
				return
			}
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		})
	}
}
