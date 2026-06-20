package obs_test

import (
	"context"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/stretchr/testify/require"
)

// TestSetupTracing_NoExporterInstallsNoop is the contract test for the
// "no OTLP env vars set" path. It must:
//
//  1. Succeed without error.
//  2. Install something that makes obs.Tracer return a working tracer
//     (so callers can blindly StartSpan without nil checks).
//  3. Return a shutdown closure that returns nil promptly.
func TestSetupTracing_NoExporterInstallsNoop(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	shutdown, err := obs.SetupTracing(t.Context(), obs.TracingConfig{ServiceName: "test"})
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	// Tracer creation must not panic and must return a non-nil tracer.
	tr := obs.Tracer("test")
	require.NotNil(t, tr)

	_, span := tr.Start(t.Context(), "noop-span")
	require.NotNil(t, span)
	span.End()

	// Shutdown of the no-op provider must not block or error.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, shutdown(ctx))
}
