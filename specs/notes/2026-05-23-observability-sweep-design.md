# Observability Sweep Design

Date: 2026-05-23
Author: jcalabro (with Claude)
Status: spec — pending implementation plan

## Goal

Make jetstream's observability rigorous, surgical, and consistent. Operators should have a small, clearly-named set of metrics with bounded cardinality; spans should be present at every operator-meaningful boundary and absent on hot paths; logs should be sparse and structured.

This spec covers the full sweep: a new tracing helper, normalized logging conventions, targeted metric additions, gap-filling spans, and the file-by-file delta.

## Non-goals

- No log sampling.
- No metric exemplars (trace↔metric drilldown stays a future evolution).
- No span events emitted from log calls.
- No changes to `inspect` CLI subcommand's printf I/O.
- No new metric for log-line counts.
- No replacement of pebble's own metrics — we add a thin layer alongside.

## Guiding principles

1. **Trace-only `Observe` helper.** Spans get a single ergonomic helper modeled on tango's `observe`. Latency metrics remain explicit, deliberate, per-subsystem.
2. **Caller-derived span names.** `gt.CallerName(2)` produces the span name. No bikeshedding, no drift.
3. **Closed-enum labels only.** Every metric label comes from a fixed set we control. No DIDs, collections, hosts, or paths in labels — those are span attributes.
4. **Hot paths are span-free.** Per-record / per-event spans (writer.Append, ConvertEvent op-loops) are forbidden; this is documented in the helper's docstring.
5. **Component attribute on every logger.** Messages drop subsystem prefixes; `slog.With("component", "...")` carries it.
6. **Simple and consistent over clever.** When two patterns produce the same observability surface, pick the one already in use.

## Architecture

### `obs.Observe` helper

```go
// internal/obs/observe.go
func Observe(ctx context.Context, opts ...trace.SpanStartOption) (context.Context, trace.Span, func(error))
```

- Span name: `gt.CallerName(2)` (e.g. `flushAndRotateLocked`, `HandleRepo`, `processBatch`).
- Tracer: `tracerForCaller(name)` extracts the package portion of the runtime frame and calls `obs.Tracer(pkg)`, so spans live under `jetstream/livestream`, `jetstream/backfill`, etc.
- `done(err)` is idempotent (safe to call from a defer that runs after an explicit done on the happy path).
- Status mapping:
  - `err == nil` → `codes.Ok`
  - any other err (including `context.Canceled`) → `codes.Error` + `span.RecordError(err)`
- The helper itself does NOT call `Observe` recursively or accept a metric; latency histograms are wired explicitly per call site.

### Logging conventions

Every constructable component creates its logger at startup with a `component` attribute:

```go
logger := baseLogger.With(slog.String("component", "livestream/consumer"))
```

Messages drop `"livestream:"` / `"orchestrator:"` prefixes. Errors use `slog.Any("err", err)`.

Component naming table (canonical, applied uniformly):

| Package | Component value |
|---|---|
| `cmd/jetstream` | `main` |
| `internal/server` | `server` |
| `internal/ingest` (writer) | `ingest/writer` |
| `internal/ingest/live` | `livestream/consumer` |
| `internal/ingest/backfill` (run) | `backfill/run` |
| `internal/ingest/backfill` (handler) | `backfill/handler` |
| `internal/ingest/orchestrator` (root) | `orchestrator` |
| `internal/ingest/orchestrator` (children) | `orchestrator/bootstrap-live`, `orchestrator/steady-live`, `orchestrator/backfill-ingest`, `orchestrator/bootstrap-seal` (already in place) |

A small slog handler decorator in `obs/log_trace.go` injects `trace_id` and `span_id` into log records when the logger is invoked via the `*Context` variants. Lifecycle-relevant call sites migrate to `InfoContext` / `WarnContext` / `ErrorContext`.

### Trim list — log lines to delete (covered by metrics or spans)

- `orchestrator: phase=merging` (states.go:21) — phase gauge + transition counter cover this.
- `orchestrator: phase=steady_state` (states.go:35) — same.
- `backfill: progress` (run.go:110) — replaced by a new `jetstream_backfill_progress_completed` gauge.
- `orchestrator: merge begin (stub no-op)` and `merge complete` (merge.go:42,46) — promoted to a single `Observe` span on `runMerge`.
- `orchestrator: bootstrap consumer drained` (bootstrap.go:138) — covered by `state_duration{state="drain_bootstrap"}`.
- `orchestrator: backfill writer closed` (bootstrap.go:182) — covered by `state_duration{state="close_backfill"}`.
- `orchestrator: bootstrap segment sealed` (bootstrap.go:218) — covered by `state_duration{state="seal_bootstrap"}` and the new `segment.SealDuration` histogram.
- `livestream: stopped` (consumer.go:228) — process-shutdown is logged once at top level; per-consumer line is noise.

### Keep list

All `Warn` / `Error` lines, all process-level startup/shutdown lines in `cmd/jetstream`, `ingest: opened`, `ingest: rotated segment`, `livestream: subscribing`, `orchestrator: starting`, `backfill: resuming from saved cursor`, `backfill: starting`, `backfill: engine drained`.

Net change: ~44 → ~26 sites.

### Metric conventions

- Naming: `jetstream_<subsystem>_<noun>_<unit>`. `_total` for counters, `_seconds` for histograms, no suffix for gauges.
- Per-subsystem `Metrics` struct. Nil-safe inc/set helpers. `NewMetrics(reg)` registers everything.
- Two histogram bucket presets in `obs/buckets.go`:
  - `LatencyBucketsFast = ExponentialBuckets(0.0001, 2, 14)` — pebble, identity cache.
  - `LatencyBucketsSlow = ExponentialBuckets(0.01, 2, 14)` — repo download, segment seal, phase transitions. Matches the existing `orchestrator.state_duration` shape.
- Pre-computed `prometheus.Observer` instances at registration time (the tango `memcacheObservers` pattern) for hot-ish paths like `store.Metrics`.

### New metrics

| Metric | Type | Labels | Lives in |
|---|---|---|---|
| `jetstream_backfill_handle_repo_duration_seconds` | histogram (slow) | none | `backfill.Metrics` |
| `jetstream_backfill_progress_completed` | gauge | none | `backfill.Metrics` (replaces progress log line) |
| `jetstream_segment_seal_duration_seconds` | histogram (slow) | none | new `segment.Metrics` |
| `jetstream_store_op_duration_seconds` | histogram (fast) | `{op, status}` — `op∈{get,set,delete,batch_commit}`, `status∈{ok,notfound,error}` | new `store.Metrics` |
| `jetstream_verifier_failures_total` | counter | `{kind}` — `kind∈{signature,chain,hosting,resolve,other}` | new `verifier.Metrics` (under `internal/obs/`) |

### Verifier failure classifier

```go
// internal/obs/verifier.go
func Classify(err error) string
```

Returns one of `signature`, `chain`, `hosting`, `resolve`, `other`. Unrecognized errors map to `"other"` rather than crashing — `kind="other"` being non-zero is itself an operator signal that a new error class needs categorizing.

### `store.Metrics` wiring

`store.Store` gains `Get`, `Set`, `Delete`, and `Commit(b)` methods that `time.Since`-instrument and forward to the embedded `*pebble.DB`. Embedded DB stays exposed for `NewBatch` / `NewIter` / `Snapshot` (these aren't latency-interesting in the same way; iterators are caller-driven).

`store.Open` signature changes from `Open(dataDir string)` to `Open(dataDir string, m *Metrics)`. `nil` is the supported zero value and produces no-op observers (same posture as every other `Metrics` struct in the codebase). No options pattern — this is a single optional dependency, and an extra parameter is simpler than introducing functional options for one knob. All callers that today touch `s.DB.Get(...)` / `s.DB.Set(...)` switch to `s.Get(...)` / `s.Set(...)`. The migration is mechanical — all consumer call sites already do this idiomatically (e.g. `live/cursor.go:49,97`, `identity/cache.go:64,114`).

### New spans (using `Observe`)

Migrated from manual `tracer.Start`:
- `flushAndRotateLocked` (was `ingest.flush_block`)
- `HandleRepo` (was `backfill.handle_repo`)
- `processBatch` (was `livestream.batch`)

New:
- `Seal` in `segment/seal.go` — multi-step file I/O, today opaque.
- `runMerge` — replaces the begin/complete log pair.
- `runBootstrap` and `runSteadyState` — phase-level latency as a single trace tree.
- `finishBootstrap` — child of `runBootstrap`.
- `Run` in `backfill` — parents per-`HandleRepo` spans.

Stays untraced: `writer.Append`, `ConvertEvent` and its inner per-op loop, `store.Get/Set` (the histogram covers them).

### Span attributes

- `attribute.String("did", string(did))` only on `HandleRepo`-level spans.
- `attribute.Int64("seq", …)` / `attribute.String("phase", string(phase))` / `attribute.Int64("active_idx", int64(idx))` on relevant lifecycle spans.
- Set inside the function body via `span.SetAttributes(...)`. The helper signature stays minimal.

## File-by-file delta

### New files
- `internal/obs/observe.go`
- `internal/obs/buckets.go`
- `internal/obs/log_trace.go`
- `internal/obs/verifier.go`
- `internal/store/metrics.go`
- `internal/segment/metrics.go` (or `segment/metrics.go` if the package layout doesn't have an internal split — it's `segment/`)

### Modified files
- `internal/obs/tracing.go` — adds `tracerForCaller(name)`.
- `internal/obs/logger.go` — wraps the handler with the trace-ID injector.
- `internal/store/store.go` — `Get/Set/Delete/Commit` methods, optional `*Metrics`, embedded `*pebble.DB` retained.
- `segment/seal.go` — wrap `Seal` body in `Observe`, record histogram.
- `segment/writer.go` — accept optional `*Metrics`.
- `internal/ingest/writer.go` — `Observe` migration; `slog.With("component", "ingest/writer")`; rotation span gets `active_idx`.
- `internal/ingest/live/consumer.go` — `Observe` migration; component logger; drop `livestream: stopped`.
- `internal/ingest/backfill/handler.go` — `Observe` + `did` attribute; observe `handle_repo_duration_seconds`; component logger.
- `internal/ingest/backfill/run.go` — `Observe` around engine drive; replace progress log with gauge; component logger.
- `internal/ingest/orchestrator/orchestrator.go` — `Observe` on `Run`; component logger.
- `internal/ingest/orchestrator/bootstrap.go` — `Observe` on `runBootstrap` and `finishBootstrap`; drop redundant lifecycle logs.
- `internal/ingest/orchestrator/states.go` — drop redundant `phase=…` info logs.
- `internal/ingest/orchestrator/merge.go` — `Observe` on `runMerge`; drop the two info logs.
- `internal/ingest/orchestrator/steady.go` — `Observe` on `runSteadyState`; component logger.
- `internal/server/server.go` — `*Context` slog variants where request context exists; component logger.
- `internal/identity/cache.go` — route through `s.Get`/`s.Set`/`s.Delete`.
- `internal/ingest/syncstate/store.go` — same.
- `internal/ingest/backfill/store.go` — same.
- `internal/ingest/live/cursor.go` — same.
- `internal/lifecycle/phase.go` — same.
- `internal/ingest/writer.go` (seq save) — same.
- `cmd/jetstream/main.go` — register `verifier.Metrics`, `store.Metrics`, `segment.Metrics`. Wire `OnVerificationFailure` to `Classify(err)` + counter increment in addition to the warn log. Pass `store.Metrics` into `store.Open`.

### Test additions
- `internal/obs/observe_test.go` — span name from caller, idempotent done, error mapping (nil → Ok, real err → Error+RecordError, `context.Canceled` → Error).
- `internal/obs/verifier_test.go` — table test for `Classify`, including the `"other"` fallback.
- `internal/store/metrics_test.go` — `op=get,status=ok|notfound|error` and `op=set,status=ok|error` via real pebble I/O.
- `segment/metrics_test.go` — successful Seal records once; pre-flush failure records nothing.

Existing tests passing nil `*Metrics` keep working — the nil-safe pattern is preserved.

## Error handling

- `obs.Observe` cannot fail; OTEL misconfiguration falls through to a no-op tracer (existing behavior in `tracing.go`).
- `store.Metrics` callbacks don't return errors; histograms are recorded on every path including failure.
- `verifier.Classify` returns `"other"` for unknown errors — bounded cardinality is preserved by construction.
- Log-line removals are irreversible from an operator-runbook standpoint. The PR description must list every removed line and the metric/span that replaces it.

## Risks and mitigations

1. **slog handler wrapper double-wraps on `With`**: implement as a delegating handler whose `WithAttrs`/`WithGroup` re-wrap once. Standard pattern.
2. **`gt.CallerName(2)` returns wrong frame depth if helper is nested**: helper has no internal wrapper; docstring forbids wrapping; unit test catches a regression.
3. **Per-call histogram overhead on hot pebble paths**: pre-compute `prometheus.Observer` instances at registration time so `WithLabelValues` is paid once.
4. **Span name renamings break operator dashboards**: tracer name still encodes the package (`jetstream/livestream`); document renamings in the PR body. Old: `livestream.batch`, `ingest.flush_block`, `ingest.rotate_segment`, `backfill.handle_repo`. New: `processBatch` under `jetstream/livestream`, `flushAndRotateLocked` under `jetstream/ingest`, etc.

## Smoke test (manual, not in CI)

Run `just run serve` against a local relay; `curl localhost:6060/metrics`; confirm:
- All new series appear with their declared labels and bounded cardinality.
- `jetstream_store_op_duration_seconds_bucket` rows are populated for both `op=get` and `op=set`.
- `jetstream_verifier_failures_total` increments under simulated bad input.
- Trace export (when `OTEL_EXPORTER_OTLP_ENDPOINT` is set) shows the new span names with package-scoped tracers.
