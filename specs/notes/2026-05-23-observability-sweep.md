# Observability Sweep Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make jetstream's observability rigorous, surgical, and consistent — adding a single `obs.Observe` tracing helper, normalizing logging conventions, filling latency histograms at clear operator-interest boundaries, and trimming redundant log lines.

**Architecture:** Trace-only `obs.Observe` helper (caller-name-derived spans, no metric coupled). Per-subsystem `Metrics` structs grow with targeted additions: pebble-store latency histogram, segment seal histogram, backfill handle_repo histogram, verifier failure counter. Logging normalizes to `slog.With("component", "...")` and trims ~18 redundant lines. All hot paths (per-record / per-event) remain free of spans.

**Tech Stack:** Go, `log/slog`, `go.opentelemetry.io/otel`, `github.com/prometheus/client_golang`, `github.com/jcalabro/gt` (for `CallerName`), `github.com/cockroachdb/pebble`.

---

## Reference notes for the implementer

- `just test` runs `gotestsum -short`; `just test-race` runs the full suite under race. Use `just test ./internal/obs` to run a single package.
- Spec lives at `docs/superpowers/specs/2026-05-23-observability-sweep-design.md` and is the source of truth if any task here is ambiguous.
- All `Metrics` structs in this codebase follow a nil-safe pattern: every method first does `if m != nil`. Preserve this so tests can pass `nil` and skip metric registration.
- `*store.Store` embeds `*pebble.DB`. Existing callers use `s.Get(...)`, `s.Set(...)` via the promoted methods. Defining matching methods on `*Store` shadows the promoted ones — that's the integration seam used in Task 5.
- `internal/store/sync_unix.go` and `sync_darwin.go` define `store.SyncWrites` (a `*pebble.WriteOptions`). Don't change that.
- Span name discipline: `obs.Observe` MUST be called directly from the function it represents — never wrapped — because span name comes from `gt.CallerName(2)`.

---

## Task 1: `obs.Observe` helper + buckets + tests

**Files:**
- Create: `internal/obs/observe.go`
- Create: `internal/obs/observe_test.go`
- Create: `internal/obs/buckets.go`
- Modify: `internal/obs/tracing.go` (add `tracerForCaller`)

- [ ] **Step 1: Write the failing test for `obs.Observe`**

Create `internal/obs/observe_test.go`:

```go
package obs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
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

func TestObserve_DoneIsIdempotent(t *testing.T) {
	rec := installRecorder(t)
	observeIdempotent(context.Background())
	spans := rec.Ended()
	require.Len(t, spans, 1, "second done() must not re-end the span")
	require.Equal(t, codes.Ok, spans[0].Status().Code, "second done() must not flip status")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test ./internal/obs -run TestObserve`
Expected: FAIL with "undefined: obs.Observe"

- [ ] **Step 3: Implement `internal/obs/observe.go`**

```go
// Package obs: observe.go provides the canonical tracing helper used
// throughout jetstream. Observe opens a span named after the calling
// function (via gt.CallerName) and returns the derived ctx, the span,
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

	"github.com/jcalabro/gt"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Observe starts a span named after the calling function. Returns the
// derived ctx, the span (so callers can attach attributes), and a
// done(err) closure that ends the span and sets status. done is
// idempotent: subsequent calls are no-ops.
//
// Status mapping:
//   - err == nil → codes.Ok
//   - any other err (including context.Canceled) → codes.Error +
//     span.RecordError(err)
func Observe(ctx context.Context, opts ...trace.SpanStartOption) (context.Context, trace.Span, func(error)) {
	name := gt.CallerName(2)
	// skip=3: runtime.Caller → tracerForCaller → Observe → user code.
	ctx, span := tracerForCaller(3).Start(ctx, name, opts...)

	var called bool
	return ctx, span, func(err error) {
		if called {
			return
		}
		called = true
		defer span.End()
		if err == nil {
			span.SetStatus(codes.Ok, "")
			return
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}
```

- [ ] **Step 4: Add `tracerForCaller` to `internal/obs/tracing.go`**

Append to `internal/obs/tracing.go`:

```go
import (
	"runtime"
	"strings"
	"sync"
)

// repoPathPrefix is stripped from runtime function names so tracer
// scopes read as "ingest/live" rather than "github.com/.../internal/ingest/live".
const repoPathPrefix = "github.com/bluesky-social/jetstream-v2/"

// pkgCache memoizes pkg-name extraction per-PC. The runtime returns a
// stable function pointer for a given Go function, so caching by PC is
// safe and avoids repeated string parsing on every Observe call.
var pkgCache sync.Map // map[uintptr]string

// tracerForCaller returns the package-scoped tracer for the function
// at skip frames above the caller of Observe. The package portion of
// the runtime frame becomes the tracer name, so spans live under
// jetstream/<pkg> (e.g. jetstream/ingest/live, jetstream/segment).
//
// skip is relative to tracerForCaller; pass the same skip value used
// for gt.CallerName so the two agree on the frame.
func tracerForCaller(skip int) trace.Tracer {
	pc, _, _, ok := runtime.Caller(skip)
	if !ok {
		return Tracer("observe")
	}
	if cached, hit := pkgCache.Load(pc); hit {
		return Tracer(cached.(string))
	}
	pkg := pkgFromPC(pc)
	pkgCache.Store(pc, pkg)
	return Tracer(pkg)
}

// pkgFromPC turns a PC into a short package label like "ingest/live".
// runtime.FuncForPC returns names like
// "github.com/bluesky-social/jetstream-v2/internal/ingest/live.(*Consumer).processBatch".
// We slice off the function suffix, strip the repo prefix, and strip
// a leading "internal/" so the tracer name reads naturally.
func pkgFromPC(pc uintptr) string {
	f := runtime.FuncForPC(pc)
	if f == nil {
		return "observe"
	}
	full := f.Name()
	// Find the package/function boundary: the last "/" then the next "."
	// after it (function names may contain "." for receivers/closures).
	lastSlash := strings.LastIndex(full, "/")
	dot := strings.Index(full[lastSlash+1:], ".")
	if dot < 0 {
		return "observe"
	}
	pkg := full[:lastSlash+1+dot]
	pkg = strings.TrimPrefix(pkg, repoPathPrefix)
	pkg = strings.TrimPrefix(pkg, "internal/")
	if pkg == "" {
		return "observe"
	}
	return pkg
}
```

And update `Observe` (in `observe.go`) to pass `skip=3`: the frame walk goes user-code → Observe → tracerForCaller → runtime.Caller, so we want to skip three frames to land back on user-code.

```go
func Observe(ctx context.Context, opts ...trace.SpanStartOption) (context.Context, trace.Span, func(error)) {
	name := gt.CallerName(2)
	ctx, span := tracerForCaller(3).Start(ctx, name, opts...)
	// ... rest unchanged
}
```

Add a unit test pinning that two Observe calls in different packages produce different tracer scopes. Append to `internal/obs/observe_test.go`:

```go
func TestObserve_TracerScopeFromCallerPackage(t *testing.T) {
	rec := installRecorder(t)
	observeHappyPath(context.Background())
	spans := rec.Ended()
	require.Len(t, spans, 1)
	// observeHappyPath lives in package obs_test, which after stripping
	// becomes "internal/obs_test"; we just assert the scope is non-empty
	// and not the fallback "observe".
	require.NotEqual(t, "observe", spans[0].InstrumentationScope().Name)
}
```

- [ ] **Step 5: Run the test**

Run: `just test ./internal/obs -run TestObserve`
Expected: PASS, all four subtests.

- [ ] **Step 6: Add bucket presets file**

Create `internal/obs/buckets.go`:

```go
package obs

import "github.com/prometheus/client_golang/prometheus"

// LatencyBucketsFast covers ~0.1 ms → ~1.6 s in 14 exponential
// buckets. Use for hot-ish operations: pebble Get/Set, identity
// cache, anything that should normally complete in microseconds.
var LatencyBucketsFast = prometheus.ExponentialBuckets(0.0001, 2, 14)

// LatencyBucketsSlow covers ~10 ms → ~160 s in 14 exponential
// buckets. Use for heavyweight operations: repo download, segment
// seal, phase transitions. Matches the existing
// jetstream_orchestrator_state_duration_seconds bucket layout so
// dashboards can reuse the same axes.
var LatencyBucketsSlow = prometheus.ExponentialBuckets(0.01, 2, 14)
```

- [ ] **Step 7: Run lint and full test pass for `obs`**

Run: `just lint && just test ./internal/obs`
Expected: PASS, no lint warnings.

- [ ] **Step 8: Commit**

```bash
git add internal/obs/observe.go internal/obs/observe_test.go internal/obs/buckets.go internal/obs/tracing.go
git commit -m "obs: add Observe helper and latency bucket presets"
```

---

## Task 2: slog→trace context handler decorator

**Files:**
- Create: `internal/obs/log_trace.go`
- Create: `internal/obs/log_trace_test.go`
- Modify: `internal/obs/logger.go` (wrap handler in `NewLogger`)

- [ ] **Step 1: Write the failing test**

Create `internal/obs/log_trace_test.go`:

```go
package obs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel"
)

// TestLogger_InjectsTraceIDFromContext confirms that when a logger
// built via NewLogger is invoked through *Context variants inside an
// active span, trace_id and span_id appear as record attributes.
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
func TestLogger_NoTraceIDOutsideSpan(t *testing.T) {
	var buf bytes.Buffer
	logger := obs.NewLogger(&buf, slog.LevelInfo, obs.LogFormatJSON)

	logger.InfoContext(context.Background(), "hello")

	var rec map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec))
	_, hasTrace := rec["trace_id"]
	require.False(t, hasTrace)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `just test ./internal/obs -run TestLogger`
Expected: FAIL — `trace_id`/`span_id` keys missing.

- [ ] **Step 3: Implement the decorator**

Create `internal/obs/log_trace.go`:

```go
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
// inner so caller-provided attribute groups stack normally.
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
```

- [ ] **Step 4: Wire the decorator into `NewLogger`**

In `internal/obs/logger.go`, change `NewLogger` to wrap the handler:

```go
func NewLogger(w io.Writer, level slog.Level, format LogFormat) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}

	var h slog.Handler
	switch format {
	case LogFormatJSON:
		h = slog.NewJSONHandler(w, opts)
	default:
		h = slog.NewTextHandler(w, opts)
	}

	return slog.New(newTraceContextHandler(h))
}
```

- [ ] **Step 5: Run the test**

Run: `just test ./internal/obs -run TestLogger`
Expected: PASS.

- [ ] **Step 6: Run the full obs package test**

Run: `just test ./internal/obs`
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/obs/log_trace.go internal/obs/log_trace_test.go internal/obs/logger.go
git commit -m "obs: inject trace_id/span_id into slog records via context"
```

---

## Task 3: `verifier.Classify` + `verifier.Metrics`

**Files:**
- Create: `internal/obs/verifier.go`
- Create: `internal/obs/verifier_test.go`

- [ ] **Step 1: Write the failing test for Classify**

Create `internal/obs/verifier_test.go`:

```go
package obs_test

import (
	"errors"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestClassify_FallbackOther(t *testing.T) {
	t.Parallel()
	require.Equal(t, "other", obs.Classify(errors.New("totally unrecognized")))
}

func TestClassify_NilIsOther(t *testing.T) {
	t.Parallel()
	// A nil error reaching Classify is itself a caller bug; we map to
	// "other" rather than panic so a single misuse can't crash the
	// process.
	require.Equal(t, "other", obs.Classify(nil))
}

func TestClassify_Signature(t *testing.T) {
	t.Parallel()
	require.Equal(t, "signature", obs.Classify(errors.New("signature verification failed")))
}

func TestClassify_Chain(t *testing.T) {
	t.Parallel()
	require.Equal(t, "chain", obs.Classify(errors.New("chain state mismatch")))
}

func TestClassify_Hosting(t *testing.T) {
	t.Parallel()
	require.Equal(t, "hosting", obs.Classify(errors.New("hosting state invalid")))
}

func TestClassify_Resolve(t *testing.T) {
	t.Parallel()
	require.Equal(t, "resolve", obs.Classify(errors.New("could not resolve did")))
}

func TestVerifierMetrics_NilSafeAndIncrement(t *testing.T) {
	t.Parallel()
	// nil receiver must be a no-op (matches the codebase convention).
	var m *obs.VerifierMetrics
	m.IncFailure("signature")

	reg := prometheus.NewRegistry()
	m = obs.NewVerifierMetrics(reg)

	m.IncFailure("signature")
	m.IncFailure("signature")
	m.IncFailure("chain")

	require.Equal(t, float64(2), testutil.ToFloat64(m.Failures.WithLabelValues("signature")))
	require.Equal(t, float64(1), testutil.ToFloat64(m.Failures.WithLabelValues("chain")))
}
```

- [ ] **Step 2: Run to verify failure**

Run: `just test ./internal/obs -run "TestClassify|TestVerifier"`
Expected: FAIL — undefined symbols.

- [ ] **Step 3: Implement `verifier.go`**

Create `internal/obs/verifier.go`:

```go
package obs

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// VerifierMetrics owns prometheus state for the firehose verifier.
// A nil *VerifierMetrics is a valid zero-value: every method is a
// no-op so tests can skip metric registration entirely.
type VerifierMetrics struct {
	Failures *prometheus.CounterVec
}

// NewVerifierMetrics registers verifier counters against reg.
func NewVerifierMetrics(reg prometheus.Registerer) *VerifierMetrics {
	m := &VerifierMetrics{
		Failures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "verifier",
			Name:      "failures_total",
			Help:      "Number of verifier rejections, by error class. kind=other is the catch-all and should be near-zero in steady state; sustained kind=other is a signal that a new error class needs categorizing.",
		}, []string{"kind"}),
	}
	reg.MustRegister(m.Failures)
	return m
}

// IncFailure increments the counter for the given kind. nil-safe.
func (m *VerifierMetrics) IncFailure(kind string) {
	if m != nil {
		m.Failures.WithLabelValues(kind).Inc()
	}
}

// Classify maps a verifier error to one of a small closed enum of
// kinds. Unknown errors map to "other" — bounded cardinality matters
// more than per-error specificity; kind="other" being non-zero is
// itself an operator signal.
//
// Classification is a substring match on the error message rather
// than errors.Is/errors.As because atmos's verifier surfaces many
// errors as fmt.Errorf strings without sentinel types. If atmos
// later exports sentinel errors, switch this to errors.Is — the
// signature is unchanged.
func Classify(err error) string {
	if err == nil {
		return "other"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "signature"):
		return "signature"
	case strings.Contains(msg, "chain"):
		return "chain"
	case strings.Contains(msg, "hosting"):
		return "hosting"
	case strings.Contains(msg, "resolve"):
		return "resolve"
	default:
		return "other"
	}
}
```

- [ ] **Step 4: Run the tests**

Run: `just test ./internal/obs -run "TestClassify|TestVerifier"`
Expected: all PASS.

- [ ] **Step 5: Run lint**

Run: `just lint`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add internal/obs/verifier.go internal/obs/verifier_test.go
git commit -m "obs: add verifier failure metric and Classify helper"
```

---

## Task 4: `store.Metrics` (struct, registration, observers)

**Files:**
- Create: `internal/store/metrics.go`
- Create: `internal/store/metrics_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/store/metrics_test.go`:

```go
package store_test

import (
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestMetrics_NilSafe pins the codebase-wide nil-receiver convention.
func TestMetrics_NilSafe(t *testing.T) {
	t.Parallel()
	var m *store.Metrics
	m.ObserveGet(0, nil)
	m.ObserveSet(0, nil)
	m.ObserveDelete(0, nil)
	m.ObserveBatchCommit(0, nil)
}

// TestMetrics_Registration confirms NewMetrics registers exactly the
// documented histogram with bounded label cardinality.
func TestMetrics_Registration(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := store.NewMetrics(reg)
	require.NotNil(t, m)

	mfs, err := reg.Gather()
	require.NoError(t, err)

	var found bool
	for _, mf := range mfs {
		if mf.GetName() == "jetstream_store_op_duration_seconds" {
			found = true
		}
	}
	require.True(t, found, "histogram must be registered")
}

// TestMetrics_StatusLabels exercises the four observe methods with
// every status branch and confirms the sample counts land on the
// correct {op,status} series.
func TestMetrics_StatusLabels(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := store.NewMetrics(reg)

	m.ObserveGet(0, nil)
	m.ObserveGet(0, store.ErrNotFound)
	m.ObserveGet(0, errSomeIO())
	m.ObserveSet(0, nil)
	m.ObserveSet(0, errSomeIO())
	m.ObserveDelete(0, nil)
	m.ObserveBatchCommit(0, nil)

	require.Equal(t, uint64(1), histCount(t, reg, "get", "ok"))
	require.Equal(t, uint64(1), histCount(t, reg, "get", "notfound"))
	require.Equal(t, uint64(1), histCount(t, reg, "get", "error"))
	require.Equal(t, uint64(1), histCount(t, reg, "set", "ok"))
	require.Equal(t, uint64(1), histCount(t, reg, "set", "error"))
	require.Equal(t, uint64(1), histCount(t, reg, "delete", "ok"))
	require.Equal(t, uint64(1), histCount(t, reg, "batch_commit", "ok"))

	// Sanity: a series we did not touch must remain at zero.
	require.Equal(t, uint64(0), histCount(t, reg, "delete", "error"))

	// testutil import-only guard.
	_ = testutil.ToFloat64
}

func errSomeIO() error { return &simpleErr{"io fail"} }

type simpleErr struct{ s string }

func (e *simpleErr) Error() string { return e.s }

// histCount returns the cumulative observation count for the
// {op, status} histogram series, or 0 if the series isn't present.
func histCount(t *testing.T, reg *prometheus.Registry, op, status string) uint64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "jetstream_store_op_duration_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["op"] == op && labels["status"] == status {
				return m.GetHistogram().GetSampleCount()
			}
		}
	}
	return 0
}
```

- [ ] **Step 2: Run to verify failure**

Run: `just test ./internal/store -run TestMetrics`
Expected: FAIL — undefined.

- [ ] **Step 3: Implement `store/metrics.go`**

Create `internal/store/metrics.go`:

```go
package store

import (
	"errors"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/cockroachdb/pebble"
	"github.com/prometheus/client_golang/prometheus"
)

// ErrNotFound is re-exported for callers that want a stable handle
// on the "key absent" outcome without importing pebble. It aliases
// pebble.ErrNotFound so errors.Is keeps working through the wrapper.
var ErrNotFound = pebble.ErrNotFound

const (
	statusOK       = "ok"
	statusNotFound = "notfound"
	statusError    = "error"
)

// Metrics owns prometheus state for the metadata pebble store. A
// nil *Metrics is a valid zero-value: every observe* method is a
// no-op, so tests can skip metric registration.
type Metrics struct {
	OpDuration *prometheus.HistogramVec

	// Pre-computed observers. We pay the WithLabelValues cost once at
	// registration time so per-call observe doesn't allocate.
	getOk       prometheus.Observer
	getNotFound prometheus.Observer
	getError    prometheus.Observer
	setOk       prometheus.Observer
	setError    prometheus.Observer
	deleteOk    prometheus.Observer
	deleteError prometheus.Observer
	commitOk    prometheus.Observer
	commitError prometheus.Observer
}

// NewMetrics registers the store histogram against reg and pre-binds
// the per-{op,status} observers.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	hist := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "jetstream",
		Subsystem: "store",
		Name:      "op_duration_seconds",
		Help:      "Duration of pebble metadata-store operations by op and outcome.",
		Buckets:   obs.LatencyBucketsFast,
	}, []string{"op", "status"})
	reg.MustRegister(hist)

	return &Metrics{
		OpDuration:  hist,
		getOk:       hist.WithLabelValues("get", statusOK),
		getNotFound: hist.WithLabelValues("get", statusNotFound),
		getError:    hist.WithLabelValues("get", statusError),
		setOk:       hist.WithLabelValues("set", statusOK),
		setError:    hist.WithLabelValues("set", statusError),
		deleteOk:    hist.WithLabelValues("delete", statusOK),
		deleteError: hist.WithLabelValues("delete", statusError),
		commitOk:    hist.WithLabelValues("batch_commit", statusOK),
		commitError: hist.WithLabelValues("batch_commit", statusError),
	}
}

// ObserveGet records a Get duration tagged by outcome.
func (m *Metrics) ObserveGet(start time.Time, err error) {
	if m == nil {
		return
	}
	d := time.Since(start).Seconds()
	switch {
	case err == nil:
		m.getOk.Observe(d)
	case errors.Is(err, pebble.ErrNotFound):
		m.getNotFound.Observe(d)
	default:
		m.getError.Observe(d)
	}
}

// ObserveSet records a Set duration tagged by outcome.
func (m *Metrics) ObserveSet(start time.Time, err error) {
	if m == nil {
		return
	}
	d := time.Since(start).Seconds()
	if err != nil {
		m.setError.Observe(d)
		return
	}
	m.setOk.Observe(d)
}

// ObserveDelete records a Delete duration tagged by outcome.
func (m *Metrics) ObserveDelete(start time.Time, err error) {
	if m == nil {
		return
	}
	d := time.Since(start).Seconds()
	if err != nil {
		m.deleteError.Observe(d)
		return
	}
	m.deleteOk.Observe(d)
}

// ObserveBatchCommit records a Batch.Commit duration tagged by outcome.
func (m *Metrics) ObserveBatchCommit(start time.Time, err error) {
	if m == nil {
		return
	}
	d := time.Since(start).Seconds()
	if err != nil {
		m.commitError.Observe(d)
		return
	}
	m.commitOk.Observe(d)
}
```

Note: the `start time.Time` argument to each observer is intentional — the call sites in Task 5 will capture `start := time.Now()` before the pebble call and pass it in. This avoids the closure-allocation overhead a `defer m.ObserveGet(time.Now(), err)` pattern would incur.

- [ ] **Step 4: Run the test**

Run: `just test ./internal/store -run TestMetrics`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store/metrics.go internal/store/metrics_test.go
git commit -m "store: add Metrics with pre-bound op_duration observers"
```

---

## Task 5: Wire `store.Metrics` into `*store.Store` and update `Open`

**Files:**
- Modify: `internal/store/store.go`
- Modify: `internal/store/store_test.go`
- Modify: every caller of `store.Open`

- [ ] **Step 1: Write the failing test**

Append to `internal/store/store_test.go`:

```go
// TestOpen_RecordsMetricsThroughInstrumentedMethods exercises the
// happy path of every instrumented method against a real pebble db
// and confirms each lands a sample on the matching {op,status}
// histogram series.
func TestOpen_RecordsMetricsThroughInstrumentedMethods(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := store.NewMetrics(reg)

	s, err := store.Open(t.TempDir(), m)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.NoError(t, s.Set([]byte("k"), []byte("v"), pebble.Sync))

	val, closer, err := s.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, "v", string(val))
	require.NoError(t, closer.Close())

	_, _, err = s.Get([]byte("missing"))
	require.ErrorIs(t, err, pebble.ErrNotFound)

	require.NoError(t, s.Delete([]byte("k"), pebble.Sync))

	mfs, err := reg.Gather()
	require.NoError(t, err)
	getCounts := map[string]uint64{}
	setCounts := map[string]uint64{}
	deleteCounts := map[string]uint64{}
	for _, mf := range mfs {
		if mf.GetName() != "jetstream_store_op_duration_seconds" {
			continue
		}
		for _, mm := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range mm.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			c := mm.GetHistogram().GetSampleCount()
			switch labels["op"] {
			case "get":
				getCounts[labels["status"]] = c
			case "set":
				setCounts[labels["status"]] = c
			case "delete":
				deleteCounts[labels["status"]] = c
			}
		}
	}
	require.Equal(t, uint64(1), getCounts["ok"])
	require.Equal(t, uint64(1), getCounts["notfound"])
	require.Equal(t, uint64(1), setCounts["ok"])
	require.Equal(t, uint64(1), deleteCounts["ok"])
}
```

- [ ] **Step 2: Update existing tests for the new `Open` signature**

Change every `store.Open(t.TempDir())` to `store.Open(t.TempDir(), nil)` in `internal/store/store_test.go`.

```go
func TestOpen_RequiresDataDir(t *testing.T) {
	t.Parallel()
	_, err := store.Open("", nil)
	require.ErrorContains(t, err, "data dir is required")
}

func TestOpen_CreatesPebbleSubdir(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()

	s, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	_, err = filepath.EvalSymlinks(filepath.Join(dataDir, store.PebbleSubdir, "LOCK"))
	require.NoError(t, err, "pebble LOCK file should exist after Open")
}

func TestStore_RoundTripViaEmbeddedDB(t *testing.T) {
	t.Parallel()
	s, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.NoError(t, s.Set([]byte("hello"), []byte("world"), pebble.Sync))

	val, closer, err := s.Get([]byte("hello"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = closer.Close() })
	require.Equal(t, "world", string(val))
}

func TestStore_CloseIsIdempotent(t *testing.T) {
	t.Parallel()
	s, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
}
```

Don't forget the `prometheus` import:

```go
import (
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)
```

- [ ] **Step 3: Run to verify the new test fails and old tests fail to compile**

Run: `just test ./internal/store`
Expected: compile errors (Open signature mismatch).

- [ ] **Step 4: Implement the new `Store` API in `internal/store/store.go`**

Replace the existing file content with:

```go
// Package store ... (unchanged docstring)
package store

import (
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

const PebbleSubdir = "meta.pebble"

// Store is the typed handle to the metadata pebble database. Its
// instrumented Get/Set/Delete/Commit methods shadow the embedded
// pebble methods of the same name; callers picking up *Store
// automatically get the histogram. Operations not on the hot path
// (NewBatch, NewIter, Snapshot) come through as plain promoted
// methods.
type Store struct {
	*pebble.DB
	metrics *Metrics
}

// Open opens (creating if necessary) the metadata pebble database
// at <dataDir>/meta.pebble. m may be nil, in which case the store
// records no metrics.
func Open(dataDir string, m *Metrics) (*Store, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("store: Open: data dir is required")
	}

	path := filepath.Join(dataDir, PebbleSubdir)

	opts := &pebble.Options{}
	opts.EnsureDefaults()
	for i := range opts.Levels {
		opts.Levels[i].FilterPolicy = bloom.FilterPolicy(10)
	}

	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, fmt.Errorf("store: open pebble at %s: %w", path, err)
	}

	return &Store{DB: db, metrics: m}, nil
}

func (s *Store) Close() error {
	if s.DB == nil {
		return nil
	}
	err := s.DB.Close()
	s.DB = nil
	if err != nil {
		return fmt.Errorf("store: close pebble: %w", err)
	}
	return nil
}

// Get is the instrumented version of *pebble.DB.Get. It shadows the
// promoted method so callers automatically observe.
func (s *Store) Get(key []byte) ([]byte, io.Closer, error) {
	start := time.Now()
	val, closer, err := s.DB.Get(key)
	s.metrics.ObserveGet(start, err)
	return val, closer, err
}

// Set is the instrumented version of *pebble.DB.Set.
func (s *Store) Set(key, value []byte, opts *pebble.WriteOptions) error {
	start := time.Now()
	err := s.DB.Set(key, value, opts)
	s.metrics.ObserveSet(start, err)
	return err
}

// Delete is the instrumented version of *pebble.DB.Delete.
func (s *Store) Delete(key []byte, opts *pebble.WriteOptions) error {
	start := time.Now()
	err := s.DB.Delete(key, opts)
	s.metrics.ObserveDelete(start, err)
	return err
}

// Commit is the instrumented version of pebble.Batch.Commit. Use it
// in place of b.Commit(opts) so the duration histogram captures
// batch commits alongside single-key writes.
func (s *Store) Commit(b *pebble.Batch, opts *pebble.WriteOptions) error {
	start := time.Now()
	err := b.Commit(opts)
	s.metrics.ObserveBatchCommit(start, err)
	return err
}
```

- [ ] **Step 5: Update `internal/ingest/syncstate/store.go` `Delete` to use `s.s.Commit`**

Replace `b.Commit(store.SyncWrites)` (line 110) with `p.s.Commit(b, store.SyncWrites)`:

```go
func (p *PebbleStateStore) Delete(_ context.Context, did atmos.DID) error {
	b := p.s.NewBatch()
	defer func() { _ = b.Close() }()

	if err := b.Delete(chainKey(did), nil); err != nil {
		return fmt.Errorf("syncstate: delete chain %s: %w", did, err)
	}
	if err := b.Delete(hostKey(did), nil); err != nil {
		return fmt.Errorf("syncstate: delete hosting %s: %w", did, err)
	}
	if err := p.s.Commit(b, store.SyncWrites); err != nil {
		return fmt.Errorf("syncstate: delete %s: %w", did, err)
	}
	return nil
}
```

- [ ] **Step 6: Update every `store.Open(...)` call site**

```bash
# Find them
grep -rn "store\.Open(" --include="*.go" .
```

Edit each call site to add the second argument:

- `cmd/jetstream/main.go:236`: `store.Open(dataDir)` → `store.Open(dataDir, storeMetrics)` (will define `storeMetrics` in Task 11)
- `internal/ingest/backfill/store_test.go`, `internal/ingest/backfill/cursor_test.go`, etc.: every `store.Open(t.TempDir())` → `store.Open(t.TempDir(), nil)`
- Any helper test file in `orchestrator/testfixtures_test.go`, `live/consumer_test.go`, `lifecycle/phase_test.go`, `identity/cache_test.go`, `syncstate/store_test.go`, `ingest/writer_test.go` — all gain the second arg.

For now, every non-`cmd/jetstream` caller passes `nil`. The production wiring lands in Task 11.

Quick sed pass (verify before running):

```bash
# Preview
grep -rn "store\.Open(" --include="*.go" .
```

For each test file, update by hand or via:

```bash
# Update test files
find . -name "*_test.go" -print0 | xargs -0 perl -pi -e 's/store\.Open\(([^,)]+)\)/store.Open($1, nil)/g'
```

Then confirm the grep shows `store.Open(..., nil)` everywhere except `cmd/jetstream/main.go`.

- [ ] **Step 7: Run the full test suite**

Run: `just test`
Expected: all PASS. The new `TestOpen_RecordsMetricsThroughInstrumentedMethods` passes; existing tests keep passing through `nil`.

- [ ] **Step 8: Run lint**

Run: `just lint`
Expected: clean.

- [ ] **Step 9: Commit**

```bash
git add internal/store/ internal/ingest/syncstate/store.go
git add -- $(git status --porcelain | awk '/_test\.go$/ {print $2}')
git commit -m "store: instrument Get/Set/Delete/Commit with op_duration histogram"
```

---

## Task 6: `segment.Metrics` and Seal instrumentation

**Files:**
- Create: `segment/metrics.go`
- Create: `segment/metrics_test.go`
- Modify: `segment/writer.go` (add `Metrics` field on `Config`)
- Modify: `segment/seal.go` (record histogram on success path)

- [ ] **Step 1: Write the failing test**

Create `segment/metrics_test.go`:

```go
package segment_test

import (
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestMetrics_NilSafe pins the codebase nil-receiver convention.
func TestMetrics_NilSafe(t *testing.T) {
	t.Parallel()
	var m *segment.Metrics
	m.ObserveSeal(0, nil)
}

// TestSeal_RecordsHistogram drives a real Writer through Seal with a
// configured Metrics and confirms the histogram landed exactly one
// observation. We intentionally don't pin the bucket; just sample
// count.
func TestSeal_RecordsHistogram(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := segment.NewMetrics(reg)

	path := filepath.Join(t.TempDir(), "seg_0.jss")
	w, err := segment.New(segment.Config{
		Path:    path,
		Metrics: m,
	})
	require.NoError(t, err)

	require.NoError(t, w.Append(segment.Event{
		IndexedAt: 1,
		Kind:      segment.KindCreate,
		DID:       "did:plc:test",
		Seq:       1,
	}))

	_, err = w.Seal()
	require.NoError(t, err)

	mfs, err := reg.Gather()
	require.NoError(t, err)
	var count uint64
	for _, mf := range mfs {
		if mf.GetName() == "jetstream_segment_seal_duration_seconds" {
			for _, mm := range mf.GetMetric() {
				count += mm.GetHistogram().GetSampleCount()
			}
		}
	}
	require.Equal(t, uint64(1), count)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `just test ./segment -run "TestMetrics_NilSafe|TestSeal_RecordsHistogram"`
Expected: FAIL — undefined.

- [ ] **Step 3: Implement `segment/metrics.go`**

```go
package segment

import (
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics owns prometheus state for the segment package. nil is a
// valid zero-value: every method is a no-op.
type Metrics struct {
	SealDuration prometheus.Histogram
}

// NewMetrics registers segment metrics against reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		SealDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "jetstream",
			Subsystem: "segment",
			Name:      "seal_duration_seconds",
			Help:      "End-to-end duration of segment.Writer.Seal: flush + footer + header pwrite + fsyncs.",
			Buckets:   obs.LatencyBucketsSlow,
		}),
	}
	reg.MustRegister(m.SealDuration)
	return m
}

// ObserveSeal records a successful seal duration. Failed seals are
// not recorded — operators chase failures through error logs and
// trace status, not the success-time histogram.
func (m *Metrics) ObserveSeal(start time.Time, err error) {
	if m == nil || err != nil {
		return
	}
	m.SealDuration.Observe(time.Since(start).Seconds())
}
```

- [ ] **Step 4: Add `Metrics` field to `segment.Config`**

In `segment/writer.go`, modify `Config`:

```go
type Config struct {
	Path string

	MaxEventsPerBlock int

	// Metrics is optional; nil disables segment-package metrics
	// (e.g. seal duration). The zero-value Metrics is a no-op.
	Metrics *Metrics
}
```

And modify `Writer` to capture the metrics handle:

```go
type Writer struct {
	cfg     Config
	file    *os.File
	pending pendingBlock
	closed  bool

	bodyScratch []byte
	wireScratch []byte
	stickyErr   error
}
```

(no struct change actually required if `cfg.Metrics` is read directly. Confirm `cfg` is preserved in the constructor — it already is.)

- [ ] **Step 5: Wire seal observation into `segment/seal.go`**

In `segment/seal.go`, change `Seal`:

```go
func (w *Writer) Seal() (SealResult, error) {
	if w.closed {
		return SealResult{}, ErrClosed
	}
	if w.stickyErr != nil {
		return SealResult{}, w.stickyErr
	}
	start := time.Now()
	if err := w.flushLocked(); err != nil {
		return SealResult{}, err
	}
	res, err := w.sealAfterFlush()
	if err != nil {
		return SealResult{}, err
	}
	w.cfg.Metrics.ObserveSeal(start, nil)
	w.closed = true
	return res, nil
}
```

Add `"time"` to the imports if not already there.

- [ ] **Step 6: Run the test**

Run: `just test ./segment -run "TestMetrics_NilSafe|TestSeal_RecordsHistogram"`
Expected: PASS.

- [ ] **Step 7: Run the segment package test suite to confirm no regressions**

Run: `just test ./segment`
Expected: all PASS (existing seal tests should not care about the metrics field — `Metrics: nil` is the zero value).

- [ ] **Step 8: Commit**

```bash
git add segment/metrics.go segment/metrics_test.go segment/writer.go segment/seal.go
git commit -m "segment: add Metrics with seal_duration_seconds histogram"
```

---

## Task 7: Migrate existing trace sites to `obs.Observe`

**Files:**
- Modify: `internal/ingest/writer.go` (`flushAndRotateLocked` rotation path)
- Modify: `internal/ingest/backfill/handler.go` (`HandleRepo`)
- Modify: `internal/ingest/live/consumer.go` (`processBatch`)

This task is mechanical — replace each `tracer.Start` block with `obs.Observe`. The new spans are named after the Go function (per `gt.CallerName`) — that means `flushAndRotateLocked`, `HandleRepo`, `processBatch`. Listed in spec section 5 risks: dashboards keying on the old names need updating.

- [ ] **Step 1: Migrate `internal/ingest/writer.go`**

Replace the `flushAndRotateLocked` body's tracing setup:

```go
func (w *Writer) flushAndRotateLocked(ctx context.Context) error {
	ctx, span, done := obs.Observe(ctx)
	var err error
	defer func() { done(err) }()

	if err = w.active.Flush(); err != nil {
		return fmt.Errorf("ingest: flush block: %w", err)
	}
	w.cfg.Metrics.incBlocksFlushed()

	if err = saveNextSeq(w.cfg.Store, w.cfg.SeqKey, w.nextSeq); err != nil {
		return err
	}

	if w.cfg.OnAfterFlush != nil {
		if err = w.cfg.OnAfterFlush(ctx); err != nil {
			return fmt.Errorf("ingest: on_after_flush: %w", err)
		}
	}

	path := filepath.Join(w.cfg.SegmentsDir, segmentFilename(w.activeIdx))
	info, statErr := os.Stat(path)
	if statErr != nil {
		err = statErr
		return fmt.Errorf("ingest: stat active segment: %w", err)
	}
	w.activeBytes = info.Size() - int64(segment.ReservedHeaderBytes)
	w.cfg.Metrics.setActiveSegBytes(w.activeBytes)

	if w.activeBytes < w.cfg.MaxSegmentBytes {
		return nil
	}

	if err = w.rotateLocked(ctx); err != nil {
		return err
	}
	return nil
}

// rotateLocked is split out so its span (rotateLocked) is a child of
// flushAndRotateLocked rather than a sibling buried inside it.
func (w *Writer) rotateLocked(ctx context.Context) (err error) {
	_, span, done := obs.Observe(ctx)
	defer func() { done(err) }()
	_ = span // attributes attached below

	if _, err = w.active.Seal(); err != nil {
		return fmt.Errorf("ingest: seal segment %d: %w", w.activeIdx, err)
	}

	w.activeIdx++
	span.SetAttributes(attribute.Int64("active_idx", int64(w.activeIdx)))

	nextPath := filepath.Join(w.cfg.SegmentsDir, segmentFilename(w.activeIdx))
	next, err := segment.New(segment.Config{
		Path:              nextPath,
		MaxEventsPerBlock: w.cfg.MaxEventsPerBlock,
	})
	if err != nil {
		return fmt.Errorf("ingest: open new active segment %s: %w", nextPath, err)
	}
	w.active = next
	w.activeBytes = 0
	w.cfg.Metrics.setActiveSegBytes(0)
	w.cfg.Metrics.incSegmentsRotated()

	w.cfg.Logger.Info("ingest: rotated segment", "new_index", w.activeIdx)
	return nil
}
```

Add to imports:
```go
"github.com/bluesky-social/jetstream-v2/internal/obs"
"go.opentelemetry.io/otel/attribute"
```

Drop the existing `obs.Tracer("ingest")` line. The unused `tracer` variable in the original is gone.

- [ ] **Step 2: Migrate `internal/ingest/backfill/handler.go`**

Replace `HandleRepo`:

```go
func (h *SegmentHandler) HandleRepo(ctx context.Context, did atmos.DID, r *repo.Repo, commit *repo.Commit) (retErr error) {
	ctx, span, done := obs.Observe(ctx)
	defer func() { done(retErr) }()

	span.SetAttributes(attribute.String("did", string(did)))
	start := time.Now()
	defer func() {
		h.metrics.observeHandleRepo(start)
	}()

	indexedAt := h.now().UnixMicro()

	walkErr := r.Tree.Walk(func(key string, cid cbor.CID) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		collection, rkey, err := splitMSTKey(key)
		if err != nil {
			return fmt.Errorf("backfill: did=%s: %w", did, err)
		}
		payload, err := r.Store.GetBlock(cid)
		if err != nil {
			return fmt.Errorf("backfill: did=%s get block %s/%s: %w", did, collection, rkey, err)
		}

		ev := segment.Event{
			IndexedAt:  indexedAt,
			Kind:       segment.KindCreate,
			DID:        string(did),
			Collection: collection,
			Rkey:       rkey,
			Rev:        commit.Rev,
			Payload:    payload,
		}
		if err := h.writer.Append(ctx, &ev); err != nil {
			return fmt.Errorf("backfill: did=%s append %s/%s: %w", did, collection, rkey, err)
		}
		return nil
	})
	if walkErr != nil {
		retErr = walkErr
		return walkErr
	}
	return nil
}
```

Add to `SegmentHandler`:

```go
type SegmentHandler struct {
	writer  *ingest.Writer
	logger  *slog.Logger
	now     func() time.Time
	metrics *Metrics
}
```

And update `NewSegmentHandler`:

```go
func NewSegmentHandler(writer *ingest.Writer, logger *slog.Logger, m *Metrics) *SegmentHandler {
	if writer == nil {
		panic("backfill: NewSegmentHandler: writer is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &SegmentHandler{
		writer:  writer,
		logger:  logger,
		now:     time.Now,
		metrics: m,
	}
}
```

Add to imports:
```go
"github.com/bluesky-social/jetstream-v2/internal/obs"
"go.opentelemetry.io/otel/attribute"
```

Update the only call site `internal/ingest/backfill/run.go:84`:

```go
handler := NewSegmentHandler(cfg.Writer, cfg.Logger, cfg.Metrics)
```

- [ ] **Step 3: Add `observeHandleRepo` and the histogram to backfill metrics**

Edit `internal/ingest/backfill/metrics.go`:

```go
type Metrics struct {
	Discovered          prometheus.Counter
	Completed           prometheus.Counter
	Failed              prometheus.Counter
	ActiveFlips         prometheus.Counter
	OnFailErrors        prometheus.Counter
	HandleRepoDuration  prometheus.Histogram
	ProgressCompleted   prometheus.Gauge
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		// ... existing counters unchanged ...
		HandleRepoDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name:    "handle_repo_duration_seconds",
			Help:    "Duration of SegmentHandler.HandleRepo per repo.",
			Buckets: obs.LatencyBucketsSlow,
		}),
		ProgressCompleted: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "progress_completed",
			Help: "Number of repos the engine has reported complete in the current Run.",
		}),
	}
	reg.MustRegister(
		m.Discovered, m.Completed, m.Failed, m.ActiveFlips, m.OnFailErrors,
		m.HandleRepoDuration, m.ProgressCompleted,
	)
	return m
}

func (m *Metrics) observeHandleRepo(start time.Time) {
	if m != nil {
		m.HandleRepoDuration.Observe(time.Since(start).Seconds())
	}
}

func (m *Metrics) setProgressCompleted(v int) {
	if m != nil {
		m.ProgressCompleted.Set(float64(v))
	}
}
```

Add imports `"time"` and `"github.com/bluesky-social/jetstream-v2/internal/obs"`.

- [ ] **Step 4: Migrate `internal/ingest/live/consumer.go`'s `processBatch` span**

Replace the inline `tracer.Start` block in `Run`:

```go
// Old:
//   tracer := obs.Tracer("livestream")
//   for batch, err := range client.Events(ctx) {
//       ...
//       batchCtx, span := tracer.Start(ctx, "livestream.batch")
//       if perr := c.processBatch(batchCtx, batch); perr != nil {
//           span.RecordError(perr)
//           span.End()
//           return perr
//       }
//       span.End()
//   }
```

With:

```go
for batch, err := range client.Events(ctx) {
	if err != nil {
		c.cfg.Metrics.incDecodeErrors()
		c.cfg.Logger.Warn("stream error", "err", err)
		continue
	}
	if perr := c.processBatchObserved(ctx, batch); perr != nil {
		return perr
	}
}
```

Add `processBatchObserved` (the inner function gets the span name `processBatchObserved`, which matches the body it's wrapping; the existing `processBatch` keeps its hot inner loop unspanned):

```go
func (c *Consumer) processBatchObserved(ctx context.Context, batch []streaming.Event) (retErr error) {
	ctx, _, done := obs.Observe(ctx)
	defer func() { done(retErr) }()
	return c.processBatch(ctx, batch)
}
```

Drop the `tracer := obs.Tracer("livestream")` line.

- [ ] **Step 5: Run the full suite**

Run: `just test`
Expected: all PASS. Existing tests don't pin span names, so the renames don't break them.

- [ ] **Step 6: Run lint**

Run: `just lint`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add internal/ingest/writer.go internal/ingest/backfill/ internal/ingest/live/consumer.go
git commit -m "ingest: migrate existing tracer.Start sites to obs.Observe"
```

---

## Task 8: Add new spans (Seal, runMerge, runBootstrap, runSteadyState, finishBootstrap, backfill.Run)

**Files:**
- Modify: `segment/seal.go`
- Modify: `internal/ingest/orchestrator/merge.go`
- Modify: `internal/ingest/orchestrator/bootstrap.go`
- Modify: `internal/ingest/orchestrator/steady.go`
- Modify: `internal/ingest/orchestrator/orchestrator.go`
- Modify: `internal/ingest/backfill/run.go`

- [ ] **Step 1: Wrap `Seal` in `Observe`**

In `segment/seal.go`, modify `Seal`:

```go
func (w *Writer) Seal() (res SealResult, retErr error) {
	ctx := context.Background()
	_, _, done := obs.Observe(ctx)
	defer func() { done(retErr) }()

	if w.closed {
		retErr = ErrClosed
		return SealResult{}, retErr
	}
	if w.stickyErr != nil {
		retErr = w.stickyErr
		return SealResult{}, retErr
	}
	start := time.Now()
	if err := w.flushLocked(); err != nil {
		retErr = err
		return SealResult{}, err
	}
	res, err := w.sealAfterFlush()
	if err != nil {
		retErr = err
		return SealResult{}, err
	}
	w.cfg.Metrics.ObserveSeal(start, nil)
	w.closed = true
	return res, nil
}
```

Add `"context"` and `"github.com/bluesky-social/jetstream-v2/internal/obs"` to imports.

(Why a fresh `context.Background()`? `Seal` is sync and synchronous from the caller's stack — no upstream context. The span will be parentless, which is fine: this is a top-level operator-interest operation. Any future caller wishing to thread a parent context can switch to a `SealContext(ctx)` overload.)

- [ ] **Step 2: Wrap `runMerge`**

```go
func (o *Orchestrator) runMerge(ctx context.Context) (retErr error) {
	ctx, _, done := obs.Observe(ctx)
	defer func() { done(retErr) }()

	if err := ctx.Err(); err != nil {
		retErr = err
		return err
	}
	return nil
}
```

Drop the two `Info` log lines per spec.

Add `"github.com/bluesky-social/jetstream-v2/internal/obs"` to imports.

- [ ] **Step 3: Wrap `runBootstrap` and `finishBootstrap`**

In `bootstrap.go`, top of `runBootstrap`:

```go
func (o *Orchestrator) runBootstrap(ctx context.Context) (retErr error) {
	ctx, _, done := obs.Observe(ctx)
	defer func() { done(retErr) }()

	// ... rest of body unchanged ...
}
```

Same for `finishBootstrap`:

```go
func (o *Orchestrator) finishBootstrap(bootstrapLive *live.Consumer, bw *ingest.Writer, liveSegmentsDir string) (retErr error) {
	ctx := context.Background()
	_, _, done := obs.Observe(ctx)
	defer func() { done(retErr) }()

	// ... rest of body unchanged ...
}
```

Note: `finishBootstrap` doesn't currently take a context. Adding `context.Background()` here (rather than threading one) keeps the call signature unchanged and the span correctly parentless from the orchestrator's perspective; `finishBootstrap` runs synchronously after the errgroup returns and has no cancellation contract anyway.

Add `"github.com/bluesky-social/jetstream-v2/internal/obs"` and (for `finishBootstrap`) `"context"` to imports.

- [ ] **Step 4: Wrap `runSteadyState`**

```go
func (o *Orchestrator) runSteadyState(ctx context.Context) (retErr error) {
	ctx, _, done := obs.Observe(ctx)
	defer func() { done(retErr) }()

	o.cfg.Metrics.setPhase(PhaseGaugeSteadyState)
	// ... rest of body unchanged ...
}
```

Add `obs` import.

- [ ] **Step 5: Wrap `Orchestrator.Run`**

```go
func (o *Orchestrator) Run(ctx context.Context) (retErr error) {
	ctx, _, done := obs.Observe(ctx)
	defer func() { done(retErr) }()

	// ... rest of body unchanged ...
}
```

- [ ] **Step 6: Wrap `backfill.Run`**

```go
func Run(ctx context.Context, cfg Config) (retErr error) {
	if err := cfg.validate(); err != nil {
		return err
	}
	ctx, _, done := obs.Observe(ctx)
	defer func() { done(retErr) }()

	// ... rest of body unchanged ...
}
```

Add `obs` import.

- [ ] **Step 7: Run the full suite**

Run: `just test`
Expected: all PASS.

- [ ] **Step 8: Run lint**

Run: `just lint`
Expected: clean.

- [ ] **Step 9: Commit**

```bash
git add segment/seal.go internal/ingest/orchestrator/ internal/ingest/backfill/run.go
git commit -m "obs: wrap Seal, orchestrator phases, and backfill.Run with Observe"
```

---

## Task 9: Logging style normalization (component attribute, message cleanup)

**Files:**
- Modify: `internal/ingest/writer.go` (logger init)
- Modify: `internal/ingest/live/consumer.go` (logger init at construction; message cleanup)
- Modify: `internal/ingest/backfill/run.go` (component attribute; message cleanup)
- Modify: `internal/ingest/backfill/handler.go` (component attribute)
- Modify: `internal/ingest/orchestrator/orchestrator.go` (component attribute on root logger)
- Modify: `internal/server/server.go` (component attribute)
- Modify: `cmd/jetstream/main.go` (component=main on top-level logger)

- [ ] **Step 1: `cmd/jetstream/main.go`**

After `slog.SetDefault(logger)` in `runServe`:

```go
logger = logger.With(slog.String("component", "main"))
slog.SetDefault(logger)
```

Then the existing `logger.Info("starting jetstream", ...)` becomes message-clean:

```go
logger.Info("startup",
    "version", info.Version,
    "commit", info.Commit,
    "built", info.Date,
)
```

- [ ] **Step 2: `internal/server/server.go`**

In `New`:

```go
func New(cfg Config, logger *slog.Logger, metrics *obs.Metrics) *Server {
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	logger = logger.With(slog.String("component", "server"))

	s := &Server{cfg: cfg, logger: logger, metrics: metrics}
	// ... rest unchanged ...
}
```

Drop the `"server"` prefix from log messages — they become `"listening"`, `"failed during startup"`, `"shutdown requested"`, `"exited unexpectedly"`.

- [ ] **Step 3: `internal/ingest/writer.go` logger init**

Modify the `Writer` so its logger gets the component on construction. In `Open`:

```go
logger := cfg.Logger.With(slog.String("component", "ingest/writer"))
w.cfg.Logger = logger
```

(Actually simpler — store the component-attributed logger in `cfg.Logger` directly, since `applyDefaults` already mutates `cfg`.) Then strip `"ingest:"` prefixes from log messages: `"opened"`, `"close after failed seal"`, `"rotated segment"`.

- [ ] **Step 4: `internal/ingest/live/consumer.go`**

In `Open`, replace the writer-only `slog.With` with one for the consumer itself:

```go
func Open(cfg Config) (*Consumer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()
	cfg.Logger = cfg.Logger.With(slog.String("component", "livestream/consumer"))

	c := &Consumer{cfg: cfg}
	// ... existing ingest.Open call now passes cfg.Logger.With("component", "livestream/ingest") ...
}
```

In the existing `ingest.Open` call, change `Logger: cfg.Logger.With(slog.String("component", "livestream/ingest"))` (already correctly named).

Strip `"livestream:"` prefixes from messages: `"subscribing"`, `"reconnecting"`, `"client close"`, `"stream error"`, `"unknown event kind"`.

Drop the `"livestream: stopped"` log line entirely (per spec trim list).

- [ ] **Step 5: `internal/ingest/backfill/run.go`**

```go
func Run(ctx context.Context, cfg Config) (retErr error) {
	if err := cfg.validate(); err != nil {
		return err
	}
	ctx, _, done := obs.Observe(ctx)
	defer func() { done(retErr) }()

	logger := cfg.Logger.With(slog.String("component", "backfill/run"))
	// ... rest of body uses `logger` directly; strip "backfill:" prefixes from messages.
}
```

Strip `"backfill:"` prefixes: `"resuming from saved cursor"`, `"repo failed"`, `"starting"`, `"engine returned error"`, `"engine drained"`.

Drop the `"backfill: progress"` log line. Replace with the gauge:

```go
OnProgress: gt.Some(func(stats atmosbackfill.Stats) {
    cfg.Metrics.setProgressCompleted(stats.Completed)
}),
```

- [ ] **Step 6: `internal/ingest/backfill/handler.go`**

In `NewSegmentHandler`:

```go
return &SegmentHandler{
    writer:  writer,
    logger:  logger.With(slog.String("component", "backfill/handler")),
    now:     time.Now,
    metrics: m,
}
```

(no log calls in handler.go today, so no message changes; the attributed logger is in place for future use.)

- [ ] **Step 7: `internal/ingest/orchestrator/orchestrator.go`**

In `New`:

```go
func New(cfg Config) (*Orchestrator, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.Logger = cfg.Logger.With(slog.String("component", "orchestrator"))
	return &Orchestrator{cfg: cfg}, nil
}
```

Strip `"orchestrator:"` prefixes from every log call across `bootstrap.go`, `merge.go`, `states.go`, `steady.go`, and `orchestrator.go`. Examples:

- `"orchestrator: starting"` → `"starting"`
- `"orchestrator: cutover begin"` → `"cutover begin"`
- `"orchestrator: bootstrap-live close after error"` → `"bootstrap-live close after error"`
- etc.

Per spec trim list, also DELETE these log lines entirely (covered by metrics):
- `o.cfg.Logger.Info("orchestrator: phase=merging")` (states.go:21)
- `o.cfg.Logger.Info("orchestrator: phase=steady_state")` (states.go:35)
- `o.cfg.Logger.Info("orchestrator: merge begin (stub no-op)")` (merge.go:42)
- `o.cfg.Logger.Info("orchestrator: merge complete")` (merge.go:46)
- `o.cfg.Logger.Info("orchestrator: bootstrap consumer drained")` (bootstrap.go:138)
- `o.cfg.Logger.Info("orchestrator: backfill writer closed")` (bootstrap.go:182)
- `o.cfg.Logger.Info("orchestrator: bootstrap segment sealed")` (bootstrap.go:218)

- [ ] **Step 8: Run the full suite**

Run: `just test`
Expected: all PASS. No tests assert on log message text in this codebase.

- [ ] **Step 9: Run lint**

Run: `just lint`
Expected: clean.

- [ ] **Step 10: Commit**

```bash
git add cmd/jetstream/main.go internal/server/server.go internal/ingest/
git commit -m "logging: normalize to component attribute, drop subsystem prefixes, trim redundant lines"
```

---

## Task 10: Per-record / hot-path documentation guard

**Files:**
- Modify: `internal/ingest/writer.go` (Append docstring)
- Modify: `internal/ingest/live/consumer.go` (processBatch docstring)
- Modify: `internal/ingest/live/events.go` (ConvertEvent docstring)

- [ ] **Step 1: Add the hot-path note**

In each of the three functions, append to its existing docstring (or add a new line if missing):

```go
// HOT-PATH: must NOT call obs.Observe — per-event spans would balloon
// to billions/day at full network scale. See internal/obs/observe.go.
```

Specifically:
- `internal/ingest/writer.go` — `Append` (line 215).
- `internal/ingest/live/consumer.go` — `processBatch` (line 239).
- `internal/ingest/live/events.go` — `ConvertEvent` (line 25).

- [ ] **Step 2: Run lint**

Run: `just lint`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add internal/ingest/writer.go internal/ingest/live/consumer.go internal/ingest/live/events.go
git commit -m "ingest: document hot-path no-Observe rule on Append/processBatch/ConvertEvent"
```

---

## Task 11: Wire `verifier.Metrics`, `store.Metrics`, `segment.Metrics` in `cmd/jetstream`

**Files:**
- Modify: `cmd/jetstream/main.go`

- [ ] **Step 1: Construct the new metrics and pass them through**

In `runServe`, after `metrics := obs.NewMetrics()`:

```go
storeMetrics := store.NewMetrics(metrics.Registry)
segmentMetrics := segment.NewMetrics(metrics.Registry)
verifierMetrics := obs.NewVerifierMetrics(metrics.Registry)
```

Update the `store.Open` call to pass `storeMetrics`:

```go
metaStore, err := store.Open(dataDir, storeMetrics)
```

In the `OnVerificationFailure` callback:

```go
OnVerificationFailure: gt.Some(func(did atmos.DID, vErr error) {
    verifierMetrics.IncFailure(obs.Classify(vErr))
    logger.Warn("verifier failure",
        "did", did,
        "err", vErr,
    )
}),
```

Pass `segmentMetrics` to the orchestrator config:

```go
orch, err := orchestrator.New(orchestrator.Config{
    DataDir:         dataDir,
    Store:           metaStore,
    RelayURL:        cmd.String("relay-url"),
    HTTPClient:      xrpcClient.HTTPClient.Val(),
    Directory:       directory,
    Verifier:        verifier,
    Logger:          logger,
    Metrics:         orchestrator.NewMetrics(metrics.Registry),
    IngestMetrics:   ingest.NewMetrics(metrics.Registry),
    LiveMetrics:     live.NewMetrics(metrics.Registry),
    BackfillMetrics: backfill.NewMetrics(metrics.Registry),
    SegmentMetrics:  segmentMetrics,
})
```

- [ ] **Step 2: Plumb `SegmentMetrics` into `orchestrator.Config` and through to writer construction**

`internal/ingest/orchestrator/config.go` adds:

```go
type Config struct {
    // ... existing ...
    SegmentMetrics *segment.Metrics
}
```

Add `"github.com/bluesky-social/jetstream-v2/segment"` to imports.

In `internal/ingest/orchestrator/bootstrap.go` and `steady.go`, where `live.Open(...)` is called and where `ingest.Open(...)` is called for the backfill writer and the bootstrap-seal writer, plumb `SegmentMetrics` through `live.Config` and `ingest.Config`.

`internal/ingest/config.go` gains:

```go
type Config struct {
    // ... existing ...
    SegmentMetrics *segment.Metrics
}
```

In `internal/ingest/writer.go`, where `segment.New(...)` is called (currently in `Open` and in `flushAndRotateLocked`'s rotation path), pass through:

```go
seg, segErr := segment.New(segment.Config{
    Path:              path,
    MaxEventsPerBlock: cfg.MaxEventsPerBlock,
    Metrics:           cfg.SegmentMetrics,
})
```

`internal/ingest/live/config.go` gains the same field; `live.Open` plumbs it into the `ingest.Open` call.

- [ ] **Step 3: Run the full suite**

Run: `just test`
Expected: all PASS. Tests that use `live.Config` / `ingest.Config` without setting `SegmentMetrics` get nil — fine, segment.Metrics is nil-safe.

- [ ] **Step 4: Run lint**

Run: `just lint`
Expected: clean.

- [ ] **Step 5: Run the binary briefly**

Run:
```bash
just build
./bin/jetstream version
```
Expected: prints version. (We're not running `serve` against a real relay here; smoke testing happens in Task 13.)

- [ ] **Step 6: Commit**

```bash
git add cmd/jetstream/main.go internal/ingest/ internal/ingest/orchestrator/
git commit -m "cmd: wire store/segment/verifier metrics into the registry"
```

---

## Task 12: Migrate lifecycle log calls to `*Context` variants

**Files:**
- Modify: `internal/ingest/writer.go`
- Modify: `internal/ingest/live/consumer.go`
- Modify: `internal/ingest/orchestrator/*.go`
- Modify: `internal/ingest/backfill/run.go`
- Modify: `internal/server/server.go`

The trace-context handler decorator (Task 2) injects `trace_id`/`span_id` only when log calls receive a `ctx`. Migrate the log lines that fall inside an `Observe`-spanned function to the `*Context` form.

- [ ] **Step 1: Find candidates**

```bash
grep -rn '\.cfg\.Logger\.Info\|\.cfg\.Logger\.Warn\|\.cfg\.Logger\.Error' --include="*.go" internal/ cmd/ | grep -v "_test.go"
```

For each call site that runs *inside* a function with an active `ctx` (every function modified in Tasks 7 and 8), change `.Info(` → `.InfoContext(ctx,`, `.Warn(` → `.WarnContext(ctx,`, `.Error(` → `.ErrorContext(ctx,`.

Concrete sites:
- `internal/ingest/writer.go`: in `flushAndRotateLocked`/`rotateLocked` (the `"rotated segment"` line).
- `internal/ingest/live/consumer.go`: in `Run` (the `"subscribing"`, `"reconnecting"`, `"client close"`, `"stream error"`, `"unknown event kind"` lines).
- `internal/ingest/backfill/run.go`: in `Run` (every `logger.Info`/`logger.Warn`/`logger.Error`).
- `internal/ingest/backfill/handler.go`: no log calls today; skip.
- `internal/ingest/orchestrator/bootstrap.go`: every `o.cfg.Logger.…` inside `runBootstrap`/`finishBootstrap` (the surviving lines after Task 9's trim).
- `internal/ingest/orchestrator/orchestrator.go`: the `"starting"` line in `Run`.
- `internal/ingest/orchestrator/steady.go`: `"steady-state consumer running"`, the deferred Error log.

Lines that run OUTSIDE any spanned context (e.g. `cmd/jetstream/main.go`'s top-level startup line, `server.go`'s `"listening"` and shutdown lines, `Open`-time lines in `writer.go`'s `"opened"`) stay as-is — there's no useful trace context for them.

- [ ] **Step 2: Run the full suite**

Run: `just test`
Expected: all PASS.

- [ ] **Step 3: Run lint**

Run: `just lint`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add internal/ingest/ internal/server/server.go
git commit -m "logging: use *Context slog variants inside spanned scopes"
```

---

## Task 13: Manual smoke test and PR-ready summary

**Files:**
- Create: short `.md` note in this plan's "PR description draft" section below; not a tracked file.

- [ ] **Step 1: Build and run against a local relay**

```bash
just build
JETSTREAM_LOG_FORMAT=text JETSTREAM_LOG_LEVEL=info \
    OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
    ./bin/jetstream serve --data-dir /tmp/jss-smoke
```

Wait ~30 seconds, then in another shell:

```bash
curl -s localhost:6060/metrics | grep -E '^jetstream_' | sort > /tmp/metrics.out
wc -l /tmp/metrics.out
```

- [ ] **Step 2: Verify the new series exist**

```bash
grep -E 'jetstream_store_op_duration_seconds_(count|sum|bucket)' /tmp/metrics.out | head
grep -E 'jetstream_segment_seal_duration_seconds_' /tmp/metrics.out | head
grep -E 'jetstream_backfill_handle_repo_duration_seconds_' /tmp/metrics.out | head
grep -E 'jetstream_backfill_progress_completed' /tmp/metrics.out | head
grep -E 'jetstream_verifier_failures_total' /tmp/metrics.out | head
```

Expected: every grep returns at least one line (some series may be zero, but the metric must be exposed).

- [ ] **Step 3: Verify cardinality budget**

```bash
awk -F'{' '{print $1}' /tmp/metrics.out | sort -u | wc -l
```

Expected: well under 100 distinct metric families.

```bash
# Worst-case: store_op_duration_seconds buckets × 4 ops × 3 statuses ≈ 168 series + sum + count.
grep -c '^jetstream_store_op_duration_seconds' /tmp/metrics.out
```

Expected: ~190-200 lines (14 buckets + sum + count = 16 lines per {op,status}, max 12 series → ~192). Confirms cardinality is bounded.

- [ ] **Step 4: Verify trace export**

If a local OTEL collector is running on `:4318`, traces should arrive containing spans named `Run`, `runBootstrap`, `processBatchObserved`, `flushAndRotateLocked`, `HandleRepo`, `Seal`, `runMerge`, `runSteadyState`. If no collector, this step is skipped — the no-op tracer provider is in effect.

- [ ] **Step 5: Stop the binary and confirm clean shutdown**

`Ctrl-C`. Expected: `INFO shutdown requested ...` then process exit 0.

- [ ] **Step 6: PR description draft (kept in this task's body, not committed)**

```markdown
## Observability sweep

Adds `obs.Observe` (caller-name-derived span helper, modeled on tango's
`observe`), normalizes logging to `slog.With("component", …)`, fills
latency-histogram gaps at clear operator-interest boundaries, and trims
~7 redundant log lines.

### New metrics
- `jetstream_store_op_duration_seconds{op,status}` — pebble I/O.
- `jetstream_segment_seal_duration_seconds` — segment seal end-to-end.
- `jetstream_backfill_handle_repo_duration_seconds` — per-repo download.
- `jetstream_backfill_progress_completed` — replaces the chatty progress
  log line.
- `jetstream_verifier_failures_total{kind}` — verifier rejections.

### Span renamings
Tracer scope is the calling package (e.g. `ingest/live`,
`ingest/orchestrator`, `segment`); span names come from the calling
function:

| Old | New |
|---|---|
| `livestream.batch` | `processBatchObserved` |
| `ingest.flush_block` | `flushAndRotateLocked` |
| `ingest.rotate_segment` | `rotateLocked` |
| `backfill.handle_repo` | `HandleRepo` |

New spans: `Seal`, `runMerge`, `runBootstrap`, `runSteadyState`,
`finishBootstrap`, `Run` (orchestrator), `Run` (backfill).

### Removed log lines (covered by metrics or spans)
- `orchestrator: phase=merging` / `phase=steady_state`
- `orchestrator: merge begin (stub no-op)` / `merge complete`
- `orchestrator: bootstrap consumer drained`
- `orchestrator: backfill writer closed`
- `orchestrator: bootstrap segment sealed`
- `livestream: stopped`
- `backfill: progress`

### Operator dashboards
The `livestream.batch` / `ingest.flush_block` etc. span-name renames
mean dashboards keying on the old names need updating. All metric names
are unchanged or new.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

(No commit for this step — the PR description is drafted in the PR body when the branch is opened.)

---

## Self-review notes

- Task 1 establishes `obs.Observe` so every later task can build on it.
- Task 2 is independent of Task 1 (different file, different functionality) and could in principle land first; we keep them ordered for narrative clarity.
- Task 5's `Open` signature change cascades to every test file — handled in one mechanical sed pass.
- Task 7 (migrate existing trace sites) precedes Task 8 (add new ones) so the codebase is uniform on the helper before new sites land.
- Task 9 trims log lines AFTER Task 8 has covered them with spans, so there's never a moment in the history where an event has neither a log nor a span.
- Task 11 wires production metrics; Task 12 adds the `*Context` log decoration last because it's the most mechanical and only adds value once spans exist (Task 8).
- Task 13 is verification-only.

If a task fails its tests after implementation, the failure is contained to that task's commit — bisect-friendly.
