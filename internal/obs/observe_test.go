package obs_test

import (
	"context"
	"errors"
	"sync"
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

func observeHappyPath(ctx context.Context) error {
	_, _, done := obs.Observe(ctx)
	done(nil)
	return nil
}

// These tests cannot run in parallel: installRecorder mutates the
// global otel.TracerProvider, so concurrent tests would race for it
// and steal each other's recorded spans.

//nolint:paralleltest // mutates global TracerProvider via installRecorder
func TestObserve_HappyPathSetsOk(t *testing.T) {
	rec := installRecorder(t)
	require.NoError(t, observeHappyPath(context.Background()))

	spans := rec.Ended()
	require.Len(t, spans, 1)
	span := spans[0]
	require.Equal(t, "observeHappyPath", span.Name())
	require.Equal(t, codes.Ok, span.Status().Code)
}

func observeErrorPath(ctx context.Context) error {
	_, _, done := obs.Observe(ctx)
	err := errors.New("boom")
	done(err)
	return err
}

//nolint:paralleltest // mutates global TracerProvider via installRecorder
func TestObserve_ErrorPathSetsErrorAndRecords(t *testing.T) {
	rec := installRecorder(t)
	_ = observeErrorPath(context.Background())

	spans := rec.Ended()
	require.Len(t, spans, 1)
	span := spans[0]
	require.Equal(t, "observeErrorPath", span.Name())
	require.Equal(t, codes.Error, span.Status().Code)
	require.NotEmpty(t, span.Events(), "RecordError should attach an exception event")
}

func observeContextCanceled(ctx context.Context) {
	_, _, done := obs.Observe(ctx)
	done(context.Canceled)
}

// Per spec section 5: context.Canceled is treated as a real error so
// operators see it in span status.
//
//nolint:paralleltest // mutates global TracerProvider via installRecorder
func TestObserve_ContextCanceledIsError(t *testing.T) {
	rec := installRecorder(t)
	observeContextCanceled(context.Background())
	require.Equal(t, codes.Error, rec.Ended()[0].Status().Code)
}

func observeIdempotent(ctx context.Context) {
	_, _, done := obs.Observe(ctx)
	done(nil)
	done(errors.New("would be wrong"))
}

//nolint:paralleltest // mutates global TracerProvider via installRecorder
func TestObserve_DoneIsIdempotent(t *testing.T) {
	rec := installRecorder(t)
	observeIdempotent(context.Background())
	spans := rec.Ended()
	require.Len(t, spans, 1, "second done() must not re-end the span")
	require.Equal(t, codes.Ok, spans[0].Status().Code, "second done() must not flip status")
}

//nolint:paralleltest // mutates global TracerProvider via installRecorder
func TestObserve_TracerScopeFromCallerPackage(t *testing.T) {
	rec := installRecorder(t)
	require.NoError(t, observeHappyPath(context.Background()))
	spans := rec.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, "jetstream/obs_test", spans[0].InstrumentationScope().Name)
}

//nolint:paralleltest // Mutates global TracerProvider.
func TestObserve_TracerScopeFromForeignPackage(t *testing.T) {
	rec := installRecorder(t)
	observe_innertest.Inner(context.Background())
	spans := rec.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, "jetstream/obs/observe_innertest", spans[0].InstrumentationScope().Name)
}

//nolint:paralleltest // Mutates global TracerProvider.
func TestObserve_DoneIsConcurrencySafe(t *testing.T) {
	rec := installRecorder(t)
	_, _, done := obs.Observe(context.Background())

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			done(nil)
		}()
	}
	wg.Wait()

	spans := rec.Ended()
	require.Len(t, spans, 1, "concurrent done() must end the span exactly once")
}
