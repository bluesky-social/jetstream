package obs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TestLogger_InjectsTraceIDFromContext confirms that when a logger
// built via NewLogger is invoked through *Context variants inside an
// active span, trace_id and span_id appear as record attributes.
//
//nolint:paralleltest // mutates global TracerProvider
func TestLogger_InjectsTraceIDFromContext(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	var buf bytes.Buffer
	logger := obs.NewLogger(&buf, slog.LevelInfo, obs.LogFormatJSON)

	ctx, span := otel.Tracer("test").Start(context.Background(), "op")
	defer span.End()

	logger.InfoContext(ctx, "hello")

	var rec map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec))
	require.Equal(t, span.SpanContext().TraceID().String(), rec["trace_id"])
	require.Equal(t, span.SpanContext().SpanID().String(), rec["span_id"])
}

// TestLogger_NoTraceIDOutsideSpan confirms the decorator is a no-op
// when there is no active span.
//
//nolint:paralleltest // mutates global TracerProvider via sibling tests
func TestLogger_NoTraceIDOutsideSpan(t *testing.T) {
	var buf bytes.Buffer
	logger := obs.NewLogger(&buf, slog.LevelInfo, obs.LogFormatJSON)

	logger.InfoContext(context.Background(), "hello")

	var rec map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec))
	_, hasTrace := rec["trace_id"]
	require.False(t, hasTrace)
}

// TestLogger_InjectsTraceIDThroughWithChain confirms that attributes
// attached via logger.With(...) survive the trace-injector wrap and
// that trace_id / span_id injection still fires through the chain.
//
//nolint:paralleltest // Mutates global TracerProvider.
func TestLogger_InjectsTraceIDThroughWithChain(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	var buf bytes.Buffer
	logger := obs.NewLogger(&buf, slog.LevelInfo, obs.LogFormatJSON).
		With(slog.String("user", "jim"))

	ctx, span := otel.Tracer("test").Start(context.Background(), "op")
	defer span.End()

	logger.InfoContext(ctx, "hello")

	var rec map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec))
	require.Equal(t, "jim", rec["user"], "user attr from With() must survive the wrap")
	require.Equal(t, span.SpanContext().TraceID().String(), rec["trace_id"])
	require.Equal(t, span.SpanContext().SpanID().String(), rec["span_id"])
}

// TestLogger_TraceIDScopedUnderActiveWithGroup locks in the documented
// WithGroup scoping behavior: trace_id / span_id are added to the
// record before it is forwarded to the inner handler, so they end up
// nested inside any active WithGroup group.
//
//nolint:paralleltest // Mutates global TracerProvider.
func TestLogger_TraceIDScopedUnderActiveWithGroup(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	var buf bytes.Buffer
	logger := obs.NewLogger(&buf, slog.LevelInfo, obs.LogFormatJSON).
		WithGroup("req")

	ctx, span := otel.Tracer("test").Start(context.Background(), "op")
	defer span.End()

	logger.InfoContext(ctx, "hello")

	var rec map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec))

	// Top level should NOT have trace_id — it's scoped under the group.
	_, atTop := rec["trace_id"]
	require.False(t, atTop, "trace_id must be scoped inside the active group")

	group, ok := rec["req"].(map[string]any)
	require.True(t, ok, "req group should exist")
	require.Equal(t, span.SpanContext().TraceID().String(), group["trace_id"])
	require.Equal(t, span.SpanContext().SpanID().String(), group["span_id"])
}
