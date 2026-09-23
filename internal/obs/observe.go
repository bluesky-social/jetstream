// Span and Span2 name traces after their caller using gt.Caller. Call them
// directly from the function being traced; wrappers change the name.
// Attributes can be set with trace.SpanFromContext(ctx). Histograms remain
// the subsystem's responsibility.
//
// Trace batches, blocks, repos, seals, and phase transitions. Per-event spans
// would overwhelm exporters at network scale.
package obs

import (
	"context"

	"github.com/jcalabro/gt"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Span names a span after the caller, runs fn with its context, and returns
// fn's error unchanged. Use trace.SpanFromContext(ctx) to attach attributes.
//
// Use at batch, block, or phase granularity, never per record. A nil error
// sets codes.Ok; any error, including cancellation, sets codes.Error and
// records it. Panics propagate without ending the span.
func Span(ctx context.Context, fn func(context.Context) error, opts ...trace.SpanStartOption) error {
	ctx, span := startCallerSpan(ctx, opts)
	err := fn(ctx)
	finishSpan(span, err)
	return err
}

// Span2 is Span for functions that return one value plus an error.
// The value is returned unmodified alongside fn's error.
//
// Same caller-depth and hot-path rules as Span.
func Span2[T any](ctx context.Context, fn func(context.Context) (T, error), opts ...trace.SpanStartOption) (T, error) {
	ctx, span := startCallerSpan(ctx, opts)
	v, err := fn(ctx)
	finishSpan(span, err)
	return v, err
}

// startCallerSpan opens a span named after the function that called
// Span / Span2. skip=3: gt.Caller(0)=Caller, (1)=startCallerSpan,
// (2)=Span/Span2, (3)=user code.
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
