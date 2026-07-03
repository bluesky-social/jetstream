package obs

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/bluesky-social/jetstream/internal/version"
	"github.com/jcalabro/gt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// TracingConfig captures the small surface area we need to bootstrap tracing.
// Endpoint and headers are read from the standard OTEL_EXPORTER_OTLP_* env
// vars by the exporter itself; we only carry the service name explicitly so
// callers can override it from a CLI flag.
type TracingConfig struct {
	ServiceName string
}

// TracerShutdown shuts down the active tracer provider, flushing any pending
// spans. Always call this on process exit.
type TracerShutdown func(context.Context) error

// SetupTracing installs a global TracerProvider. If no OTLP endpoint is
// configured via the standard env vars, we install a no-op provider so
// otel.Tracer(...) calls remain free.
//
// Returns a shutdown function that the caller is responsible for invoking
// during graceful shutdown.
func SetupTracing(ctx context.Context, cfg TracingConfig) (TracerShutdown, error) {
	if !otlpConfigured() {
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
		// Propagators are still useful even without an exporter so incoming
		// trace headers don't get dropped if a real exporter is added later.
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create OTLP HTTP exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(version.Get().Version),
		),
	)
	if err != nil {
		// Exporter has already started its background batch
		// processor and TCP pool; tear it down before bailing so
		// we don't leak goroutines/fds on a config error.
		_ = exp.Shutdown(ctx)
		return nil, fmt.Errorf("build OTEL resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// otlpConfigured reports whether an OTLP endpoint env var is set. The OTLP
// spec lets either the generic or the traces-specific var configure traces.
func otlpConfigured() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
}

// Tracer returns a tracer scoped to this codebase. Centralizing the name
// keeps span library labels consistent across packages.
func Tracer(name string) trace.Tracer {
	return otel.Tracer("jetstream/" + name)
}

// repoPathPrefix is stripped from gt.CallerInfo.Package so tracer scopes
// read as "ingest/live" rather than the full import path.
const repoPathPrefix = "github.com/bluesky-social/jetstream/"

// fallbackTracerName is used when frame inspection produces an
// unusable label. It is intentionally short so it shows up clearly in
// trace UIs as "the path that lost provenance".
const fallbackTracerName = "observe"

// tracerForCallerInfo turns a gt.CallerInfo into the project-scoped
// tracer for that frame. We trim the repo prefix and a leading
// "internal/" so a span emitted from
// github.com/bluesky-social/jetstream/internal/ingest/live lands
// under jetstream/ingest/live.
func tracerForCallerInfo(info gt.CallerInfo) trace.Tracer {
	pkg := strings.TrimPrefix(info.Package, repoPathPrefix)
	pkg = strings.TrimPrefix(pkg, "internal/")
	if pkg == "" || pkg == "unknown" {
		pkg = fallbackTracerName
	}
	return Tracer(pkg)
}
