package obs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/obs"
	observe_innertest "github.com/bluesky-social/jetstream-v2/internal/obs/observe_innertest"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// installRecorder swaps in a span-recording TracerProvider for the
// duration of the test and restores the previous one on cleanup.
func installRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	prev := otel.GetTracerProvider()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	return rec
}

func spanHappyPath(ctx context.Context) error {
	return obs.Span(ctx, func(_ context.Context) error {
		return nil
	})
}

// These tests cannot run in parallel: installRecorder mutates the
// global otel.TracerProvider, so concurrent tests would race for it
// and steal each other's recorded spans.

//nolint:paralleltest // mutates global TracerProvider via installRecorder
func TestSpan_HappyPathSetsOk(t *testing.T) {
	rec := installRecorder(t)
	require.NoError(t, spanHappyPath(context.Background()))

	spans := rec.Ended()
	require.Len(t, spans, 1)
	span := spans[0]
	require.Equal(t, "spanHappyPath", span.Name())
	require.Equal(t, codes.Ok, span.Status().Code)
}

func spanErrorPath(ctx context.Context) error {
	return obs.Span(ctx, func(_ context.Context) error {
		return errors.New("boom")
	})
}

//nolint:paralleltest // mutates global TracerProvider via installRecorder
func TestSpan_ErrorPathSetsErrorAndRecords(t *testing.T) {
	rec := installRecorder(t)
	require.Error(t, spanErrorPath(context.Background()))

	spans := rec.Ended()
	require.Len(t, spans, 1)
	span := spans[0]
	require.Equal(t, "spanErrorPath", span.Name())
	require.Equal(t, codes.Error, span.Status().Code)
	require.NotEmpty(t, span.Events(), "RecordError should attach an exception event")
}

func spanContextCanceled(ctx context.Context) error {
	return obs.Span(ctx, func(_ context.Context) error {
		return context.Canceled
	})
}

// Per spec section 5: context.Canceled is treated as a real error so
// operators see it in span status.
//
//nolint:paralleltest // mutates global TracerProvider via installRecorder
func TestSpan_ContextCanceledIsError(t *testing.T) {
	rec := installRecorder(t)
	require.ErrorIs(t, spanContextCanceled(context.Background()), context.Canceled)
	require.Equal(t, codes.Error, rec.Ended()[0].Status().Code)
}

//nolint:paralleltest // mutates global TracerProvider via installRecorder
func TestSpan_TracerScopeFromCallerPackage(t *testing.T) {
	rec := installRecorder(t)
	require.NoError(t, spanHappyPath(context.Background()))
	spans := rec.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, "jetstream/obs_test", spans[0].InstrumentationScope().Name)
}

//nolint:paralleltest // Mutates global TracerProvider.
func TestSpan_TracerScopeFromForeignPackage(t *testing.T) {
	rec := installRecorder(t)
	require.NoError(t, observe_innertest.Inner(context.Background()))
	spans := rec.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, "jetstream/obs/observe_innertest", spans[0].InstrumentationScope().Name)
}

func spanReturnsValue(ctx context.Context) (int, error) {
	return obs.Span2(ctx, func(_ context.Context) (int, error) {
		return 42, nil
	})
}

//nolint:paralleltest // mutates global TracerProvider via installRecorder
func TestSpan1_ReturnsValueAndSetsOk(t *testing.T) {
	rec := installRecorder(t)
	v, err := spanReturnsValue(context.Background())
	require.NoError(t, err)
	require.Equal(t, 42, v)

	spans := rec.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, "spanReturnsValue", spans[0].Name())
	require.Equal(t, codes.Ok, spans[0].Status().Code)
}

func spanReturnsValueErr(ctx context.Context) (string, error) {
	return obs.Span2(ctx, func(_ context.Context) (string, error) {
		return "partial", errors.New("oops")
	})
}

// Span1 returns the value alongside the error unmodified, even on the
// error path — the tracing wrapper must not swallow either.
//
//nolint:paralleltest // mutates global TracerProvider via installRecorder
func TestSpan1_ErrorPathReturnsValueAndError(t *testing.T) {
	rec := installRecorder(t)
	v, err := spanReturnsValueErr(context.Background())
	require.Error(t, err)
	require.Equal(t, "partial", v)

	spans := rec.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, codes.Error, spans[0].Status().Code)
}
