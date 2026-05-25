// Package obs: observe.go provides the canonical tracing helpers used
// throughout jetstream. Span (and Span1 for one-value-plus-error
// returns) opens a span named after the calling function (via
// gt.Caller), runs the supplied body, and finalizes the span based on
// the body's returned error. Span attributes can be set from inside
// the body via trace.SpanFromContext(ctx).
//
// Discipline: Span must be called directly from the function it
// represents. Wrapping it in another helper changes the caller depth
// and produces wrong span names. The helpers are intentionally trace-
// only — latency histograms remain explicit, deliberate, per-
// subsystem decisions.
//
// Hot-path rule: Span must NOT be used from per-record / per-event
// code paths (e.g. ingest.Writer.Append, live.ConvertEvent's inner op
// loop). Per-event spans would balloon to billions/day at full
// network scale and overwhelm any trace exporter. Use it at per-
// batch, per-block, per-repo, per-seal, per-phase-transition
// granularity.
package obs

import (
	"context"

	"github.com/jcalabro/gt"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Span starts a span named after the calling function, runs fn with
// the span-derived context, and finalizes the span based on fn's
// returned error. The returned error is fn's error, unmodified.
//
// To attach span attributes from inside fn, use
// trace.SpanFromContext(ctx).
//
// HOT PATH: do not call from per-record/per-event code paths; use at
// per-batch / per-block / per-phase granularity. See package doc.
//
// Status mapping:
//   - fn returns nil → codes.Ok
//   - fn returns any other err (including context.Canceled) →
//     codes.Error + span.RecordError(err)
//
// Panics propagate. The span will be left un-Ended in that case;
// the process is dying anyway and crashing is preferred over
// papering over the failure.
func Span(ctx context.Context, fn func(context.Context) error, opts ...trace.SpanStartOption) error {
	ctx, span := startCallerSpan(ctx, opts)
	err := fn(ctx)
	finishSpan(span, err)
	return err
}

// Span1 is Span for functions that return one value plus an error.
// The value is returned unmodified alongside fn's error.
//
// Same caller-depth and hot-path rules as Span.
func Span1[T any](ctx context.Context, fn func(context.Context) (T, error), opts ...trace.SpanStartOption) (T, error) {
	ctx, span := startCallerSpan(ctx, opts)
	v, err := fn(ctx)
	finishSpan(span, err)
	return v, err
}

// startCallerSpan opens a span named after the function that called
// Span / Span1. skip=3: gt.Caller(0)=Caller, (1)=startCallerSpan,
// (2)=Span/Span1, (3)=user code.
func startCallerSpan(ctx context.Context, opts []trace.SpanStartOption) (context.Context, trace.Span) {
	info := gt.Caller(3)
	return tracerForCallerInfo(info).Start(ctx, info.Func, opts...)
}

func finishSpan(span trace.Span, err error) {
	if err == nil {
		span.SetStatus(codes.Ok, "")
	} else {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
