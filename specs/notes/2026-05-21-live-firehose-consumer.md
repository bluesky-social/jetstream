# Live Firehose Consumer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run an atmos firehose consumer concurrently with the bootstrap-phase backfill engine, writing events into `data/backfill/live_segments/` and persisting the upstream relay cursor in pebble for crash-safe resume. Adds a `phase` lifecycle key so the server refuses to start the live_segments consumer once the merge step (future PR) flips us to steady state.

**Architecture:** A new `internal/livestream` package owns a generic firehose-to-segments consumer that wraps a dedicated `*ingest.Writer` pointed at a configured directory. Two small additions to `internal/ingest.Config` (`SeqKey`, `OnAfterFlush`) let the consumer share writer machinery without forking it. A new `internal/lifecycle` package owns the `phase` pebble key. `cmd/jetstream/main.go` reads the phase, persists it on fresh dirs, and starts the live consumer as a third sibling under the existing errgroup. The consumer-side code never names "live_segments" — that lives in `cmd/jetstream` wiring so the same `Consumer` type can be redeployed against `data/segments/` once the merge step lands.

**Tech Stack:** Go 1.26, `github.com/jcalabro/atmos/streaming` v0.0.16 (websocket client over `coder/websocket`), pebble, prometheus, `coder/websocket` for the test fake firehose, OTel via `internal/obs`.

**Spec:** [docs/superpowers/specs/2026-05-21-live-firehose-consumer-design.md](../specs/2026-05-21-live-firehose-consumer-design.md)

---

## File Map

### New files

```
internal/lifecycle/
  doc.go               package overview
  phase.go             Phase type + ReadPhase / WritePhase
  phase_test.go        round-trip + unknown-value rejection

internal/livestream/
  doc.go               package overview, DESIGN.md §4.1 reference
  errors.go            sentinel errors (ErrInvalidConfig)
  config.go            Config + validate()
  metrics.go           Prometheus counters/gauges, nil-safe
  cursor.go            LoadUpstreamCursor / SaveUpstreamCursor
  events.go            ConvertEvent: streaming.Event → []segment.Event
  consumer.go          Consumer type (Open/Run/Close/LastUpstreamSeq)
  url.go               deriveSubscribeReposURL helper

  cursor_test.go       round-trip + missing-key
  events_test.go       table-driven cases per event type
  events_swarm_test.go random sequences, panic/invariant checks
  url_test.go          URL derivation cases
  metrics_test.go      registration round-trip
  consumer_test.go     integration: scripted ws server + tempdir + real pebble
```

### Modified files

```
internal/ingest/config.go   add SeqKey, OnAfterFlush; defaults preserve current callers
internal/ingest/writer.go   route SeqKey through load/save; call OnAfterFlush after seq save
internal/ingest/writer_test.go   new tests for SeqKey override + OnAfterFlush hook
cmd/jetstream/main.go       read phase, write bootstrap on fresh dir, start liveConsumer
cmd/jetstream/serve_test.go new sub-tests for phase gate behavior
```

### Untouched

`internal/backfill/*` (only used by reference; the backfill writer's behavior must not change). `segment/*`. `internal/server/*`.

---

## Conventions for this Plan

- **Test framework:** standard library `testing` + `github.com/stretchr/testify/require`. Match existing style in `internal/ingest/writer_test.go`.
- **Logging:** `*slog.Logger` injected via Config; tests use `slog.New(slog.NewTextHandler(io.Discard, nil))`.
- **Prometheus:** `prometheus.NewRegistry()` per test, never the global default.
- **Pebble:** `t.TempDir()` + `store.Open(dir)`, with `t.Cleanup` to close.
- **Builds/runs:** prefer `just test ./internal/livestream` over raw `go test`. The `just test` recipe forwards `-short` and uses `gotestsum`.
- **Commits:** one commit per task (each task is one logical chunk that compiles + tests). Commit messages use the same conventional style as the existing repo (`feat:`, `test:`, `refactor:`).
- **Co-author footer on every commit** (matches `git log`):
  ```
  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

---

## Task 1: Add the `lifecycle` package — `Phase` type and round-trip

Adds the smallest piece of state the rest of this plan depends on: a single pebble key indicating which lifecycle phase the data dir is in.

**Files:**
- Create: `internal/lifecycle/doc.go`
- Create: `internal/lifecycle/phase.go`
- Create: `internal/lifecycle/phase_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/lifecycle/phase_test.go`:

```go
package lifecycle

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestReadPhase_Empty(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	got, err := ReadPhase(st)
	require.NoError(t, err)
	require.Equal(t, Phase(""), got)
}

func TestPhase_RoundTrip(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	for _, p := range []Phase{PhaseBootstrap, PhaseSteadyState} {
		require.NoError(t, WritePhase(st, p))
		got, err := ReadPhase(st)
		require.NoError(t, err)
		require.Equal(t, p, got)
	}
}

func TestReadPhase_UnknownValueRejected(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, st.Set([]byte("phase"), []byte("banana"), store.SyncWrites))

	_, err := ReadPhase(st)
	require.Error(t, err)
	require.Contains(t, err.Error(), "banana")
}

func TestWritePhase_RejectsUnknown(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	err := WritePhase(st, Phase("banana"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "banana")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test ./internal/lifecycle`
Expected: FAIL with "no Go files" / package not found.

- [ ] **Step 3: Write the package doc**

Create `internal/lifecycle/doc.go`:

```go
// Package lifecycle owns the persistent process-lifecycle markers
// that gate which subsystems jetstream starts on a given run.
//
// Today there is exactly one marker: "phase", a string-valued pebble
// key whose value tells cmd/jetstream whether we are still in the
// bootstrap phase (running the backfill engine + live_segments
// consumer) or in steady state (running only the live consumer
// against data/segments). Future PRs may add further markers
// (e.g. backfill_done) here so cmd/jetstream's phase decisions stay
// in one place.
//
// We deliberately keep this outside internal/store, whose package
// doc reserves that package for keyspace-agnostic database lifecycle.
package lifecycle
```

- [ ] **Step 4: Implement `phase.go`**

Create `internal/lifecycle/phase.go`:

```go
package lifecycle

import (
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
)

// Phase names a single jetstream-process lifecycle state.
type Phase string

const (
	// PhaseBootstrap means the backfill engine has not yet finished
	// initial repo download. Both the backfill engine and the
	// live_segments consumer run in this phase.
	PhaseBootstrap Phase = "bootstrap"

	// PhaseSteadyState means backfill is complete and the merge step
	// has folded live_segments into segments. Only the steady-state
	// live consumer runs here. Setting this value is a future PR; for
	// now cmd/jetstream refuses to start when it observes this phase.
	PhaseSteadyState Phase = "steady_state"
)

// phaseKey is the pebble key holding the persisted phase value.
const phaseKey = "phase"

// ReadPhase returns the persisted phase. An empty value (no key
// stored) is reported as "" with nil error so callers can decide
// what to do on a fresh data dir. An unknown value crashes the read
// rather than silently mapping to a default — DESIGN.md and
// PRACTICES.md prefer crashing over data corruption.
func ReadPhase(s *store.Store) (Phase, error) {
	val, closer, err := s.Get([]byte(phaseKey))
	if errors.Is(err, pebble.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("lifecycle: read phase: %w", err)
	}
	defer func() { _ = closer.Close() }()

	p := Phase(string(val))
	if !p.valid() {
		return "", fmt.Errorf("lifecycle: unrecognized phase value %q in pebble", string(val))
	}
	return p, nil
}

// WritePhase persists p with pebble.Sync. Rejects unknown values so
// callers cannot accidentally write garbage that ReadPhase will
// later reject.
func WritePhase(s *store.Store, p Phase) error {
	if !p.valid() {
		return fmt.Errorf("lifecycle: refuse to write unrecognized phase %q", string(p))
	}
	if err := s.Set([]byte(phaseKey), []byte(p), store.SyncWrites); err != nil {
		return fmt.Errorf("lifecycle: write phase: %w", err)
	}
	return nil
}

func (p Phase) valid() bool {
	switch p {
	case PhaseBootstrap, PhaseSteadyState:
		return true
	default:
		return false
	}
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `just test ./internal/lifecycle`
Expected: PASS, all four tests.

- [ ] **Step 6: Run lint**

Run: `just lint`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/lifecycle/
git commit -m "$(cat <<'EOF'
feat(lifecycle): add Phase pebble key with round-trip

Introduces internal/lifecycle, owning a single "phase" pebble key that
gates which subsystems jetstream starts. ReadPhase returns "" for a
fresh data dir; unknown values are rejected rather than silently
mapped, matching the project's no-silent-fallback rule. The merge-
step PR will flip the value from bootstrap to steady_state.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Extend `ingest.Config` with `SeqKey` (default-preserving)

The live consumer needs its own seq counter so the two writers don't collide. We add a `SeqKey` config field whose default is the existing `"seq/next"` literal — the backfill writer's behavior is unchanged.

**Files:**
- Modify: `internal/ingest/config.go`
- Modify: `internal/ingest/writer.go`
- Modify: `internal/ingest/writer_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/ingest/writer_test.go`:

```go
// TestOpen_HonorsCustomSeqKey verifies that two Writers with
// different SeqKey values maintain independent counters in the same
// pebble store. This is what enables the live_segments consumer to
// share a metadata db with the backfill writer without their seq
// counters colliding.
func TestOpen_HonorsCustomSeqKey(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	mkWriter := func(subdir, key string) *Writer {
		w, err := Open(Config{
			SegmentsDir: filepath.Join(t.TempDir(), subdir),
			Store:       st,
			SeqKey:      key,
			Logger:      logger,
			Metrics:     NewMetrics(prometheus.NewRegistry()),
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = w.Close() })
		return w
	}

	wA := mkWriter("a", "seq/next")
	wB := mkWriter("b", "live_segments/seq/next")

	for i := 0; i < 5; i++ {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "x.y", Rkey: "r", Rev: "1", Payload: []byte{0x01}}
		require.NoError(t, wA.Append(t.Context(), &ev))
		require.Equal(t, uint64(i), ev.Seq)
	}
	for i := 0; i < 3; i++ {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:b", Collection: "x.y", Rkey: "r", Rev: "1", Payload: []byte{0x02}}
		require.NoError(t, wB.Append(t.Context(), &ev))
		require.Equal(t, uint64(i), ev.Seq, "live writer's seq is independent of backfill writer's")
	}
}

// TestOpen_DefaultSeqKey pins back-compat: zero-value SeqKey resolves
// to "seq/next", which is what every existing caller relies on.
func TestOpen_DefaultSeqKey(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{}) // SeqKey left zero
	require.Equal(t, "seq/next", w.cfg.SeqKey)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test ./internal/ingest -run "TestOpen_HonorsCustomSeqKey|TestOpen_DefaultSeqKey"`
Expected: FAIL — `Config` has no field `SeqKey`.

- [ ] **Step 3: Add `SeqKey` to Config and apply defaults**

Modify `internal/ingest/config.go`:

```go
// SeqKey is the pebble key holding the writer's seq counter.
// Default "seq/next" preserves backfill-writer behavior. The
// live_segments consumer overrides this with
// "live_segments/seq/next" so the two counters do not collide
// when a single pebble store is shared between multiple writers.
SeqKey string
```

Inside `applyDefaults`:

```go
if c.SeqKey == "" {
    c.SeqKey = "seq/next"
}
```

(Add after the existing `MaxEventsPerBlock` default.)

- [ ] **Step 4: Route SeqKey through the writer**

Modify `internal/ingest/writer.go`:

Replace the `seqNextKey` constant declaration:

```go
// seqNextKey is the legacy default value for Config.SeqKey. Kept
// as a constant to anchor the back-compat behavior of fresh data
// dirs created before SeqKey existed.
const seqNextKey = "seq/next"
```

Replace `loadNextSeq` and `saveNextSeq` to take a key:

```go
// loadNextSeq reads the persisted seq/next counter for key. A missing
// key is not an error; it means "fresh data dir" and reads as zero.
func loadNextSeq(st *store.Store, key string) (uint64, error) {
	val, closer, err := st.Get([]byte(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("ingest: load %s: %w", key, err)
	}
	defer func() { _ = closer.Close() }()

	if len(val) != 8 {
		return 0, fmt.Errorf("ingest: %s has wrong length %d (want 8)", key, len(val))
	}
	return binary.LittleEndian.Uint64(val), nil
}

// saveNextSeq durably persists the seq counter for key via pebble.Sync.
func saveNextSeq(st *store.Store, key string, v uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	if err := st.Set([]byte(key), buf[:], store.SyncWrites); err != nil {
		return fmt.Errorf("ingest: save %s: %w", key, err)
	}
	return nil
}
```

Update the three call sites in `writer.go` (the call inside `Open`, `Close`, and `flushAndRotateLocked`) to pass `w.cfg.SeqKey`. Concretely:

In `Open`, replace:
```go
pebbleSeq, err := loadNextSeq(cfg.Store)
```
with:
```go
pebbleSeq, err := loadNextSeq(cfg.Store, cfg.SeqKey)
```

And similarly `saveNextSeq(cfg.Store, reconciled)` → `saveNextSeq(cfg.Store, cfg.SeqKey, reconciled)`.

In `Close`:
```go
if err := saveNextSeq(w.cfg.Store, w.cfg.SeqKey, w.nextSeq); err != nil {
```

In `flushAndRotateLocked`:
```go
if err := saveNextSeq(w.cfg.Store, w.cfg.SeqKey, w.nextSeq); err != nil {
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `just test ./internal/ingest`
Expected: PASS, all existing tests still pass plus the two new ones.

- [ ] **Step 6: Run lint**

Run: `just lint`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/ingest/
git commit -m "$(cat <<'EOF'
feat(ingest): make seq counter pebble key configurable

Adds Config.SeqKey, defaulting to the existing "seq/next" literal.
This unblocks running two ingest.Writer instances against the same
pebble store with disjoint counters — needed for the upcoming
live_segments consumer.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Extend `ingest.Config` with `OnAfterFlush` hook

The live consumer needs to advance `relay/cursor` *after* a block has been fsynced and `seq/next` has been pebble.Sync'd. We add a per-flush callback hook so the consumer can do this without forking the writer.

**Files:**
- Modify: `internal/ingest/config.go`
- Modify: `internal/ingest/writer.go`
- Modify: `internal/ingest/writer_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/ingest/writer_test.go`:

```go
// TestFlush_InvokesOnAfterFlushHook pins the durability hook contract:
// after each block flush the writer calls OnAfterFlush exactly once,
// AFTER segment.Flush has fsynced and AFTER saveNextSeq has been
// pebble.Sync'd. The live consumer uses this to durably advance the
// upstream relay cursor with the same per-block cadence as seq/next.
func TestFlush_InvokesOnAfterFlushHook(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	w, err := Open(Config{
		SegmentsDir:       filepath.Join(t.TempDir(), "segments"),
		Store:             newTestStore(t),
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           NewMetrics(prometheus.NewRegistry()),
		MaxEventsPerBlock: 2,
		OnAfterFlush: func(_ context.Context) error {
			calls.Add(1)
			return nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	// Two events fill the block, triggering one flush.
	for i := 0; i < 2; i++ {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "x.y", Rkey: "r", Rev: "1", Payload: []byte{0x01}}
		require.NoError(t, w.Append(t.Context(), &ev))
	}
	require.Equal(t, int32(1), calls.Load(), "exactly one flush hook fired")

	// Two more events trigger a second flush.
	for i := 0; i < 2; i++ {
		ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "x.y", Rkey: "r", Rev: "1", Payload: []byte{0x02}}
		require.NoError(t, w.Append(t.Context(), &ev))
	}
	require.Equal(t, int32(2), calls.Load())
}

// TestFlush_OnAfterFlushErrorPropagates verifies that an error from
// the hook surfaces back through Append so the errgroup can tear
// the process down. PRACTICES.md: crashing > silent corruption.
func TestFlush_OnAfterFlushErrorPropagates(t *testing.T) {
	t.Parallel()

	want := errors.New("hook boom")
	w, err := Open(Config{
		SegmentsDir:       filepath.Join(t.TempDir(), "segments"),
		Store:             newTestStore(t),
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics:           NewMetrics(prometheus.NewRegistry()),
		MaxEventsPerBlock: 1,
		OnAfterFlush:      func(_ context.Context) error { return want },
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "x.y", Rkey: "r", Rev: "1", Payload: []byte{0xab}}
	err = w.Append(t.Context(), &ev)
	require.ErrorIs(t, err, want)
}
```

You will also need new imports: `"context"`, `"errors"`, `"sync/atomic"`. Add them to the existing import block.

- [ ] **Step 2: Run test to verify it fails**

Run: `just test ./internal/ingest -run "TestFlush_InvokesOnAfterFlushHook|TestFlush_OnAfterFlushErrorPropagates"`
Expected: FAIL — `Config` has no field `OnAfterFlush`.

- [ ] **Step 3: Add the field**

Modify `internal/ingest/config.go`. After the `SeqKey` field added in Task 2:

```go
// OnAfterFlush, if non-nil, runs after each block flush has
// completed: segment.Flush has fsynced and SeqKey has been
// pebble.Sync'd. Errors propagate up through Append. A nil hook
// is a no-op. Used by the live consumer to advance "relay/cursor"
// with the same per-block cadence as seq/next.
//
// Hooks must not call back into the Writer (that would deadlock
// on the writer mutex) or perform unbounded I/O (that would stall
// every Append in the active worker pool).
OnAfterFlush func(ctx context.Context) error
```

The `config.go` file will need a new import: `"context"`.

- [ ] **Step 4: Invoke the hook in `flushAndRotateLocked`**

Modify `internal/ingest/writer.go`. Locate `flushAndRotateLocked`. After the existing `saveNextSeq` call (which is now `saveNextSeq(w.cfg.Store, w.cfg.SeqKey, w.nextSeq)`) and before the `os.Stat` block:

```go
if w.cfg.OnAfterFlush != nil {
    if err := w.cfg.OnAfterFlush(ctx); err != nil {
        span.RecordError(err)
        return fmt.Errorf("ingest: on_after_flush: %w", err)
    }
}
```

The hook receives the same context the Append call did. The mutex is still held; that's intentional — the hook is part of the per-block durability sequence. The hook must not block.

- [ ] **Step 5: Run tests to verify they pass**

Run: `just test ./internal/ingest`
Expected: PASS, all existing + two new.

- [ ] **Step 6: Run lint**

Run: `just lint`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/ingest/
git commit -m "$(cat <<'EOF'
feat(ingest): add OnAfterFlush callback for downstream durability

After each block flush, the writer now invokes Config.OnAfterFlush
with the caller's context, after segment.Flush has fsynced and
SeqKey has been pebble.Sync'd. The live_segments consumer will use
this to advance relay/cursor with the same per-block cadence the
DESIGN.md §3.1.1 invariant requires.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: `livestream` package skeleton — doc, errors, metrics

Land the empty package shell with no behavior. Subsequent tasks fill it in incrementally.

**Files:**
- Create: `internal/livestream/doc.go`
- Create: `internal/livestream/errors.go`
- Create: `internal/livestream/metrics.go`
- Create: `internal/livestream/metrics_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/livestream/metrics_test.go`:

```go
package livestream

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestNewMetrics_RegistersAllSeries pins that the constructor
// registers every series on the provided registry exactly once.
// We catch double-registration via reg.Register, which returns
// AlreadyRegisteredError on collision.
func TestNewMetrics_RegistersAllSeries(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	require.NotNil(t, m)

	// Re-registering the same collectors must collide.
	require.Panics(t, func() { _ = NewMetrics(reg) })
}

// TestMetrics_NilSafe pins that a nil *Metrics receiver tolerates
// every increment / set helper. This lets tests skip metric
// registration entirely by passing nil.
func TestMetrics_NilSafe(t *testing.T) {
	t.Parallel()
	var m *Metrics
	m.incEventsReceived()
	m.incEventsConverted()
	m.incReconnects()
	m.incDecodeErrors()
	m.setUpstreamCursor(42)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test ./internal/livestream`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write the package doc**

Create `internal/livestream/doc.go`:

```go
// Package livestream owns the consumer that pumps the upstream
// relay's com.atproto.sync.subscribeRepos firehose into a
// directory of segment files. The package is deliberately generic:
// it is used during the bootstrap phase to populate
// data/backfill/live_segments (DESIGN.md §4.1 step 1), and the
// same Consumer type will be reused after the merge step lands
// to populate data/segments in steady state (DESIGN.md §4.3).
//
// The Consumer wraps a dedicated *ingest.Writer. The mapping from
// upstream firehose events to segment.Events lives in events.go
// as a pure function so it is straightforward to unit-test
// against arbitrary input. Cursor durability is delegated to the
// writer's OnAfterFlush hook so persisted cursor ≤ durable events
// holds for free, as DESIGN.md §3.1.1 requires.
//
// This package does not yet act on #sync events (atmos v0.0.16
// does not implement full sync 1.1). #sync frames are archived
// into the segment file as KindSync but no resync is triggered.
// The opt-out is one line and will be removed when the atmos
// dependency is upgraded.
package livestream
```

- [ ] **Step 4: Write `errors.go`**

Create `internal/livestream/errors.go`:

```go
package livestream

import "errors"

// Sentinel errors. Callers compare with errors.Is.
var (
	// ErrInvalidConfig is returned by Open when Config has unusable
	// values.
	ErrInvalidConfig = errors.New("livestream: invalid config")

	// ErrClosed is returned by Run / Close after the Consumer has
	// already been closed.
	ErrClosed = errors.New("livestream: consumer is closed")
)
```

- [ ] **Step 5: Write `metrics.go`**

Create `internal/livestream/metrics.go`:

```go
package livestream

import "github.com/prometheus/client_golang/prometheus"

const (
	metricsNamespace = "jetstream"
	metricsSubsystem = "livestream"
)

// Metrics owns the prometheus counters and gauges for the livestream
// consumer. A nil *Metrics is a valid zero-value: every method is a
// no-op, so tests can skip metric registration entirely.
type Metrics struct {
	EventsReceived  prometheus.Counter
	EventsConverted prometheus.Counter
	Reconnects      prometheus.Counter
	DecodeErrors    prometheus.Counter
	UpstreamCursor  prometheus.Gauge
}

// NewMetrics registers the livestream counters/gauges against reg.
// Calls reg.MustRegister, which panics if these are already
// registered. Construct exactly once per process.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		EventsReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "events_received_total",
			Help: "Number of upstream firehose events the consumer decoded successfully.",
		}),
		EventsConverted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "events_converted_total",
			Help: "Number of segment.Events emitted by the converter (one per record op for commits, one per non-commit event).",
		}),
		Reconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "reconnects_total",
			Help: "Number of websocket reconnect attempts the atmos client has made.",
		}),
		DecodeErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "decode_errors_total",
			Help: "Number of upstream frames that failed to decode.",
		}),
		UpstreamCursor: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "upstream_cursor",
			Help: "Last persisted upstream relay cursor.",
		}),
	}
	reg.MustRegister(
		m.EventsReceived, m.EventsConverted, m.Reconnects,
		m.DecodeErrors, m.UpstreamCursor,
	)
	return m
}

func (m *Metrics) incEventsReceived() {
	if m != nil {
		m.EventsReceived.Inc()
	}
}

func (m *Metrics) incEventsConverted() {
	if m != nil {
		m.EventsConverted.Inc()
	}
}

func (m *Metrics) incReconnects() {
	if m != nil {
		m.Reconnects.Inc()
	}
}

func (m *Metrics) incDecodeErrors() {
	if m != nil {
		m.DecodeErrors.Inc()
	}
}

func (m *Metrics) setUpstreamCursor(v int64) {
	if m != nil {
		m.UpstreamCursor.Set(float64(v))
	}
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `just test ./internal/livestream`
Expected: PASS, both tests.

- [ ] **Step 7: Run lint**

Run: `just lint`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/livestream/
git commit -m "$(cat <<'EOF'
feat(livestream): scaffold package with doc, errors, metrics

Empty package shell that the next several tasks fill in. Metrics
mirror the existing internal/ingest pattern: nil-safe receivers,
namespace=jetstream, subsystem=livestream.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Upstream cursor — load + save

A small file with two pure functions on a `*store.Store`. Mirrors `internal/backfill/cursor.go`.

**Files:**
- Create: `internal/livestream/cursor.go`
- Create: `internal/livestream/cursor_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/livestream/cursor_test.go`:

```go
package livestream

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestLoadUpstreamCursor_Empty(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	got, err := LoadUpstreamCursor(st, "relay/cursor")
	require.NoError(t, err)
	require.Equal(t, int64(0), got)
}

func TestUpstreamCursor_RoundTrip(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	require.NoError(t, SaveUpstreamCursor(st, "relay/cursor", 12345))
	got, err := LoadUpstreamCursor(st, "relay/cursor")
	require.NoError(t, err)
	require.Equal(t, int64(12345), got)
}

func TestUpstreamCursor_DistinctKeys(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	require.NoError(t, SaveUpstreamCursor(st, "relay/cursor", 10))
	require.NoError(t, SaveUpstreamCursor(st, "replica/upstream_cursor", 20))

	a, err := LoadUpstreamCursor(st, "relay/cursor")
	require.NoError(t, err)
	require.Equal(t, int64(10), a)

	b, err := LoadUpstreamCursor(st, "replica/upstream_cursor")
	require.NoError(t, err)
	require.Equal(t, int64(20), b)
}

func TestLoadUpstreamCursor_RejectsCorruptValue(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	require.NoError(t, st.Set([]byte("relay/cursor"), []byte{0x01, 0x02, 0x03}, store.SyncWrites))

	_, err := LoadUpstreamCursor(st, "relay/cursor")
	require.Error(t, err)
	require.Contains(t, err.Error(), "wrong length")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test ./internal/livestream`
Expected: FAIL — `LoadUpstreamCursor` undefined.

- [ ] **Step 3: Implement `cursor.go`**

Create `internal/livestream/cursor.go`:

```go
// Package livestream: cursor.go persists the upstream relay firehose
// cursor in pebble so a process restart resumes from the last
// durably-flushed block. DESIGN.md §3.1.1: persisted cursor must be
// less than or equal to the latest durable event in the segment file.
//
// The encoding is little-endian uint64 bytes — the same shape used
// by ingest.Writer for seq/next, so operators inspecting pebble see
// a consistent layout. atmos exposes the cursor as int64; we cast
// at the boundary and document the implicit non-negativity
// constraint (atmos relays only emit positive seq values).
package livestream

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
)

// LoadUpstreamCursor reads the persisted relay cursor for key.
// A missing key returns 0 with nil error so a fresh data dir
// starts the firehose at "live" (atmos's "no cursor" semantics).
func LoadUpstreamCursor(s *store.Store, key string) (int64, error) {
	val, closer, err := s.Get([]byte(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("livestream: load %s: %w", key, err)
	}
	defer func() { _ = closer.Close() }()

	if len(val) != 8 {
		return 0, fmt.Errorf("livestream: %s has wrong length %d (want 8)", key, len(val))
	}
	return int64(binary.LittleEndian.Uint64(val)), nil
}

// SaveUpstreamCursor durably persists v under key with pebble.Sync.
// Used inside ingest.Writer's OnAfterFlush so the cursor advance
// is ordered after the per-block fsync.
func SaveUpstreamCursor(s *store.Store, key string, v int64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	if err := s.Set([]byte(key), buf[:], store.SyncWrites); err != nil {
		return fmt.Errorf("livestream: save %s: %w", key, err)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `just test ./internal/livestream`
Expected: PASS, all four tests.

- [ ] **Step 5: Commit**

```bash
git add internal/livestream/cursor.go internal/livestream/cursor_test.go
git commit -m "$(cat <<'EOF'
feat(livestream): persist upstream relay cursor in pebble

LoadUpstreamCursor / SaveUpstreamCursor mirror internal/backfill/
cursor.go: little-endian uint64 encoding, pebble.Sync on write,
ErrNotFound mapped to zero so a fresh data dir starts at the
firehose tip.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: WebSocket URL helper

A tiny pure function that turns the existing `--relay-url` HTTP base into the firehose WebSocket URL. Pure + tested in isolation so the consumer doesn't need to relitigate URL parsing.

**Files:**
- Create: `internal/livestream/url.go`
- Create: `internal/livestream/url_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/livestream/url_test.go`:

```go
package livestream

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeriveSubscribeReposURL(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want string
		err  bool
	}{
		{
			name: "https",
			in:   "https://bsky.network",
			want: "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos",
		},
		{
			name: "http",
			in:   "http://localhost:2470",
			want: "ws://localhost:2470/xrpc/com.atproto.sync.subscribeRepos",
		},
		{
			name: "trailing slash stripped",
			in:   "https://bsky.network/",
			want: "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos",
		},
		{
			name: "with path discarded",
			in:   "https://bsky.network/some/path",
			want: "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos",
		},
		{
			name: "missing scheme is an error",
			in:   "bsky.network",
			err:  true,
		},
		{
			name: "unsupported scheme is an error",
			in:   "ftp://bsky.network",
			err:  true,
		},
		{
			name: "empty is an error",
			in:   "",
			err:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := deriveSubscribeReposURL(tc.in)
			if tc.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test ./internal/livestream -run TestDeriveSubscribeReposURL`
Expected: FAIL — undefined.

- [ ] **Step 3: Implement `url.go`**

Create `internal/livestream/url.go`:

```go
package livestream

import (
	"fmt"
	"net/url"
)

// subscribeReposPath is the relay XRPC path that delivers the firehose.
const subscribeReposPath = "/xrpc/com.atproto.sync.subscribeRepos"

// deriveSubscribeReposURL converts an HTTP relay base URL (the same
// value the operator passes via --relay-url) into the WebSocket URL
// the atmos streaming client needs.
//
// Scheme mapping: https → wss, http → ws. Any path / query the
// caller might have on the input is discarded — the firehose path
// is fixed by the protocol.
func deriveSubscribeReposURL(relayURL string) (string, error) {
	if relayURL == "" {
		return "", fmt.Errorf("livestream: relay URL is empty")
	}
	parsed, err := url.Parse(relayURL)
	if err != nil {
		return "", fmt.Errorf("livestream: parse relay URL: %w", err)
	}
	switch parsed.Scheme {
	case "https":
		parsed.Scheme = "wss"
	case "http":
		parsed.Scheme = "ws"
	default:
		return "", fmt.Errorf("livestream: unsupported relay scheme %q (want http or https)", parsed.Scheme)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("livestream: relay URL %q is missing a host", relayURL)
	}
	parsed.Path = subscribeReposPath
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `just test ./internal/livestream`
Expected: PASS, all url cases plus prior tests.

- [ ] **Step 5: Commit**

```bash
git add internal/livestream/url.go internal/livestream/url_test.go
git commit -m "$(cat <<'EOF'
feat(livestream): URL helper that converts --relay-url to wss firehose

deriveSubscribeReposURL maps https→wss, http→ws, and rejects empty,
missing-host, or unsupported-scheme inputs. The Consumer reuses
this so cmd/jetstream does not need a new flag for the WebSocket
URL.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: `ConvertEvent` — pure mapper from upstream events to segment.Events

This is the substantive pure function. It takes an `atmos/streaming.Event` and an `indexedAt` timestamp, returns `[]segment.Event`. Tests use real atmos types so the mapping is verified end-to-end.

**Files:**
- Create: `internal/livestream/events.go`
- Create: `internal/livestream/events_test.go`

- [ ] **Step 1: Write the failing test (commit-only first)**

Create `internal/livestream/events_test.go` with the commit-event happy-path case. We will extend it across subsequent steps before implementation.

```go
package livestream

import (
	"bytes"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

const testIndexedAt int64 = 1_700_000_000_000_000

// buildCommit constructs a real #commit event with the given
// (collection, rkey) record creates. Each create writes a tiny CBOR
// map {"v": i}. The returned event is shaped exactly like one
// atmos's streaming decoder would emit.
func buildCommit(t *testing.T, did, rev string, recs ...struct{ Coll, Rkey string }) (streaming.Event, [][]byte) {
	t.Helper()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	mstore := mst.NewMemBlockStore()
	r := &atmosrepo.Repo{
		DID:   atmosDIDFromString(t, did),
		Clock: nil, // not needed for this test
		Store: mstore,
		Tree:  mst.NewTree(mstore),
	}

	payloads := make([][]byte, 0, len(recs))
	ops := make([]*comatproto.SyncSubscribeRepos_RepoOp, 0, len(recs))
	for i, rc := range recs {
		val := map[string]any{"v": i}
		require.NoError(t, r.Create(rc.Coll, rc.Rkey, val))
		// Capture the encoded record bytes from the repo. atmos's
		// Repo.Get returns the CID and the raw block bytes that
		// will land in the CAR — exactly what atmos's streaming
		// decoder will see on the other side.
		cid, blk, err := r.Get(rc.Coll, rc.Rkey)
		require.NoError(t, err)
		payloads = append(payloads, append([]byte(nil), blk...))

		ops = append(ops, &comatproto.SyncSubscribeRepos_RepoOp{
			Action: "create",
			Path:   rc.Coll + "/" + rc.Rkey,
			CID:    gt.Some(lextypes.LexCIDLink{Link: cid.String()}),
		})
	}

	var carBuf bytes.Buffer
	require.NoError(t, r.ExportCAR(&carBuf, key))

	return streaming.Event{
		Seq: 42,
		Commit: &comatproto.SyncSubscribeRepos_Commit{
			Repo:   did,
			Rev:    rev,
			Ops:    ops,
			Blocks: carBuf.Bytes(),
		},
	}, payloads
}

// atmosDIDFromString is a tiny helper because atmos.DID is a string
// alias but its constructor enforces some validation.
func atmosDIDFromString(t *testing.T, s string) atmos.DID {
	t.Helper()
	d, err := atmos.ParseDID(s)
	require.NoError(t, err)
	return d
}
```

Then add the first table-driven test:

```go
func TestConvertEvent_CommitCreate(t *testing.T) {
	t.Parallel()

	did := "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	evt, payloads := buildCommit(t, did, "3l3qo2vutsw2b",
		struct{ Coll, Rkey string }{"app.bsky.feed.post", "rec0"},
		struct{ Coll, Rkey string }{"app.bsky.feed.like", "rec1"},
	)

	got, err := ConvertEvent(evt, testIndexedAt)
	require.NoError(t, err)
	require.Len(t, got, 2)

	for i, want := range []struct {
		coll, rkey string
		payload    []byte
	}{
		{"app.bsky.feed.post", "rec0", payloads[0]},
		{"app.bsky.feed.like", "rec1", payloads[1]},
	} {
		ev := got[i]
		require.Equal(t, segment.KindCreate, ev.Kind)
		require.Equal(t, did, ev.DID)
		require.Equal(t, want.coll, ev.Collection)
		require.Equal(t, want.rkey, ev.Rkey)
		require.Equal(t, "3l3qo2vutsw2b", ev.Rev)
		require.Equal(t, testIndexedAt, ev.IndexedAt)
		require.Equal(t, uint64(0), ev.Seq, "Seq is allocated downstream by ingest.Writer")
		require.Equal(t, want.payload, ev.Payload)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test ./internal/livestream -run TestConvertEvent_CommitCreate`
Expected: FAIL — `ConvertEvent` undefined.

- [ ] **Step 3: Implement the commit-only path**

Create `internal/livestream/events.go`:

```go
// Package livestream: events.go is the pure converter from atmos's
// upstream streaming event shape to the segment.Event shape jetstream
// writes to disk. No I/O, no allocation beyond the result slice and
// CBOR marshalling. Safe to fuzz against arbitrary input — every
// branch returns an error rather than panicking on malformed bytes.
//
// All segment.Events derived from a single upstream event share the
// same indexedAt timestamp. Per-record timestamps would imply false
// ordering (DESIGN.md §3.4 requires per-DID ingest order is preserved).
//
// Seq is left zero on the returned events — ingest.Writer.Append
// allocates the value at write time.
package livestream

import (
	"fmt"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/streaming"
)

// ConvertEvent translates one atmos streaming.Event into zero or
// more segment.Events. See the per-kind mapping in the spec
// (§4.3 of the design doc).
func ConvertEvent(evt streaming.Event, indexedAt int64) ([]segment.Event, error) {
	switch {
	case evt.Commit != nil:
		return convertCommit(evt, indexedAt)
	case evt.Identity != nil:
		return convertIdentity(evt, indexedAt)
	case evt.Account != nil:
		return convertAccount(evt, indexedAt)
	case evt.Sync != nil:
		return convertSync(evt, indexedAt)
	case evt.Info != nil:
		return nil, nil // archival no-op; #info is informational
	default:
		return nil, nil // forward-compat: unknown frames already filtered upstream
	}
}

func convertCommit(evt streaming.Event, indexedAt int64) ([]segment.Event, error) {
	commit := evt.Commit
	ops := make([]segment.Event, 0, len(commit.Ops))

	for op, err := range evt.Operations() {
		if err != nil {
			return nil, fmt.Errorf("livestream: decode commit ops for did=%s: %w", commit.Repo, err)
		}

		kind, err := actionKind(op.Action)
		if err != nil {
			return nil, fmt.Errorf("livestream: did=%s: %w", commit.Repo, err)
		}

		segEv := segment.Event{
			IndexedAt:  indexedAt,
			Kind:       kind,
			DID:        commit.Repo,
			Collection: op.Collection,
			Rkey:       op.RKey,
			Rev:        commit.Rev,
		}
		// Deletes have no record bytes; everything else carries the
		// raw CBOR record block exactly as atmos extracted it.
		if kind != segment.KindDelete {
			segEv.Payload = append([]byte(nil), op.BlockData()...)
		}
		ops = append(ops, segEv)
	}
	return ops, nil
}

func actionKind(a streaming.Action) (segment.Kind, error) {
	switch a {
	case streaming.ActionCreate:
		return segment.KindCreate, nil
	case streaming.ActionUpdate:
		return segment.KindUpdate, nil
	case streaming.ActionDelete:
		return segment.KindDelete, nil
	case streaming.ActionResync:
		// #sync resyncs synthesize ops with this action. We don't
		// drive resyncs in this PR (atmos v0.0.16 doesn't support
		// full sync 1.1), so reaching this path is a programming
		// error rather than a data error.
		return 0, fmt.Errorf("livestream: unexpected resync op (sync handling is disabled)")
	default:
		return 0, fmt.Errorf("livestream: unknown commit action %q", a)
	}
}

func convertIdentity(evt streaming.Event, indexedAt int64) ([]segment.Event, error) {
	payload, err := evt.Identity.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("livestream: marshal identity: %w", err)
	}
	return []segment.Event{{
		IndexedAt: indexedAt,
		Kind:      segment.KindIdentity,
		DID:       evt.Identity.DID,
		Payload:   payload,
	}}, nil
}

func convertAccount(evt streaming.Event, indexedAt int64) ([]segment.Event, error) {
	payload, err := evt.Account.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("livestream: marshal account: %w", err)
	}
	return []segment.Event{{
		IndexedAt: indexedAt,
		Kind:      segment.KindAccount,
		DID:       evt.Account.DID,
		Payload:   payload,
	}}, nil
}

func convertSync(evt streaming.Event, indexedAt int64) ([]segment.Event, error) {
	payload, err := evt.Sync.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("livestream: marshal sync: %w", err)
	}
	return []segment.Event{{
		IndexedAt: indexedAt,
		Kind:      segment.KindSync,
		DID:       evt.Sync.DID,
		Rev:       evt.Sync.Rev,
		Payload:   payload,
	}}, nil
}
```

- [ ] **Step 4: Run the commit test to verify it passes**

Run: `just test ./internal/livestream -run TestConvertEvent_CommitCreate`
Expected: PASS.

- [ ] **Step 5: Add tests for non-commit and edge cases**

Append to `internal/livestream/events_test.go`:

```go
func TestConvertEvent_Identity(t *testing.T) {
	t.Parallel()

	id := &comatproto.SyncSubscribeRepos_Identity{
		DID:    "did:plc:bbb",
		Handle: gt.Some("bob.test"),
		Seq:    99,
		Time:   "2026-05-21T00:00:00Z",
	}
	evt := streaming.Event{Seq: 99, Identity: id}

	got, err := ConvertEvent(evt, testIndexedAt)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, segment.KindIdentity, got[0].Kind)
	require.Equal(t, "did:plc:bbb", got[0].DID)
	require.Equal(t, testIndexedAt, got[0].IndexedAt)

	// Round-trip the payload to confirm faithful serialization.
	var roundTrip comatproto.SyncSubscribeRepos_Identity
	require.NoError(t, roundTrip.UnmarshalCBOR(got[0].Payload))
	require.Equal(t, id.DID, roundTrip.DID)
	require.Equal(t, id.Seq, roundTrip.Seq)
	require.Equal(t, id.Time, roundTrip.Time)
	require.True(t, roundTrip.Handle.HasVal())
	require.Equal(t, "bob.test", roundTrip.Handle.Val())
}

func TestConvertEvent_Account(t *testing.T) {
	t.Parallel()

	acc := &comatproto.SyncSubscribeRepos_Account{
		DID:    "did:plc:ccc",
		Active: false,
		Status: gt.Some("takendown"),
		Seq:    100,
		Time:   "2026-05-21T00:00:00Z",
	}
	evt := streaming.Event{Seq: 100, Account: acc}

	got, err := ConvertEvent(evt, testIndexedAt)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, segment.KindAccount, got[0].Kind)
	require.Equal(t, "did:plc:ccc", got[0].DID)

	var roundTrip comatproto.SyncSubscribeRepos_Account
	require.NoError(t, roundTrip.UnmarshalCBOR(got[0].Payload))
	require.Equal(t, acc.DID, roundTrip.DID)
	require.Equal(t, acc.Active, roundTrip.Active)
	require.True(t, roundTrip.Status.HasVal())
	require.Equal(t, "takendown", roundTrip.Status.Val())
}

func TestConvertEvent_Sync(t *testing.T) {
	t.Parallel()

	sync := &comatproto.SyncSubscribeRepos_Sync{
		DID:    "did:plc:ddd",
		Rev:    "rev-xyz",
		Blocks: []byte{0x01, 0x02, 0x03},
		Seq:    101,
		Time:   "2026-05-21T00:00:00Z",
	}
	evt := streaming.Event{Seq: 101, Sync: sync}

	got, err := ConvertEvent(evt, testIndexedAt)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, segment.KindSync, got[0].Kind)
	require.Equal(t, "did:plc:ddd", got[0].DID)
	require.Equal(t, "rev-xyz", got[0].Rev)

	var roundTrip comatproto.SyncSubscribeRepos_Sync
	require.NoError(t, roundTrip.UnmarshalCBOR(got[0].Payload))
	require.Equal(t, sync.DID, roundTrip.DID)
	require.Equal(t, sync.Rev, roundTrip.Rev)
	require.Equal(t, sync.Blocks, roundTrip.Blocks)
}

func TestConvertEvent_InfoEmits_Nothing(t *testing.T) {
	t.Parallel()
	evt := streaming.Event{Info: &comatproto.SyncSubscribeRepos_Info{}}
	got, err := ConvertEvent(evt, testIndexedAt)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestConvertEvent_EmptyEvent_Nothing(t *testing.T) {
	t.Parallel()
	got, err := ConvertEvent(streaming.Event{}, testIndexedAt)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestConvertEvent_CommitDelete_PayloadNil(t *testing.T) {
	t.Parallel()

	did := "did:plc:eee"
	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo: did,
		Rev:  "rev-del",
		Ops: []*comatproto.SyncSubscribeRepos_RepoOp{
			{Action: "delete", Path: "app.bsky.feed.post/rec0", CID: gt.None[lextypes.LexCIDLink]()},
		},
		Blocks: nil,
	}
	got, err := ConvertEvent(streaming.Event{Seq: 5, Commit: commit}, testIndexedAt)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, segment.KindDelete, got[0].Kind)
	require.Equal(t, "app.bsky.feed.post", got[0].Collection)
	require.Equal(t, "rec0", got[0].Rkey)
	require.Nil(t, got[0].Payload)
}

func TestConvertEvent_CommitUnknownAction_Errors(t *testing.T) {
	t.Parallel()

	commit := &comatproto.SyncSubscribeRepos_Commit{
		Repo: "did:plc:fff",
		Rev:  "rev-bad",
		Ops: []*comatproto.SyncSubscribeRepos_RepoOp{
			{Action: "lol-no", Path: "x.y/r"},
		},
	}
	_, err := ConvertEvent(streaming.Event{Commit: commit}, testIndexedAt)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown commit action")
}
```

All required imports for these tests are already in the import block at the top of the file. If goimports flags any unused, delete them.

- [ ] **Step 6: Run tests**

Run: `just test ./internal/livestream`
Expected: PASS, all events tests.

- [ ] **Step 7: Run lint**

Run: `just lint`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/livestream/events.go internal/livestream/events_test.go
git commit -m "$(cat <<'EOF'
feat(livestream): pure ConvertEvent from streaming.Event to segment.Event

Implements the per-kind mapping from atmos's upstream firehose event
shape into the segment.Event rows we write to disk. Commits fan out
into one event per record op; identity / account / sync each
produce a single event whose Payload is the re-serialized CBOR.
Seq is left zero — ingest.Writer.Append allocates it.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Swarm test for `ConvertEvent`

Random sequences of mixed events. Asserts no panics and that every returned event satisfies on-disk column-width invariants.

**Files:**
- Create: `internal/livestream/events_swarm_test.go`

- [ ] **Step 1: Write the swarm test**

Create `internal/livestream/events_swarm_test.go`:

```go
package livestream

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/gt"
)

// iterations are the number of swarm events to generate. The default
// `just test -short` invocation should be quick; the long test run
// gets a much larger count to surface invariant violations.
func swarmIterations(t *testing.T) int {
	t.Helper()
	if testing.Short() {
		return 50
	}
	return 1000
}

// TestConvertEvent_Swarm generates random events of every supported
// kind and asserts ConvertEvent never panics, never returns events
// that would later fail segment encoding (invalid column widths,
// missing DID), and never silently drops a malformed input.
func TestConvertEvent_Swarm(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(0xfeed, 0xface))

	for i := 0; i < swarmIterations(t); i++ {
		evt := randomEvent(r)

		got, err := ConvertEvent(evt, int64(i))
		if err != nil {
			// Errors are allowed for adversarial / unknown-action
			// inputs; they must not panic. The require below will
			// only be visible when err is nil.
			continue
		}

		for _, ev := range got {
			if ev.DID == "" && ev.Kind != 0 {
				t.Fatalf("iter %d: empty DID for kind %d", i, ev.Kind)
			}
			if ev.Kind < segment.KindCreate || ev.Kind > segment.KindSync {
				t.Fatalf("iter %d: invalid Kind %d", i, ev.Kind)
			}
			if len(ev.DID) > math.MaxUint16 {
				t.Fatalf("iter %d: DID too long (%d > uint16 max)", i, len(ev.DID))
			}
			if len(ev.Collection) > math.MaxUint8 {
				t.Fatalf("iter %d: Collection too long (%d > uint8 max)", i, len(ev.Collection))
			}
			if len(ev.Rkey) > math.MaxUint8 {
				t.Fatalf("iter %d: Rkey too long (%d > uint8 max)", i, len(ev.Rkey))
			}
			if len(ev.Rev) > math.MaxUint8 {
				t.Fatalf("iter %d: Rev too long (%d > uint8 max)", i, len(ev.Rev))
			}
		}
	}
}

func randomEvent(r *rand.Rand) streaming.Event {
	switch r.IntN(5) {
	case 0:
		return streaming.Event{Identity: &comatproto.SyncSubscribeRepos_Identity{
			DID:    randomDID(r),
			Handle: gt.Some("h.test"),
			Time:   "2026-05-21T00:00:00Z",
		}}
	case 1:
		return streaming.Event{Account: &comatproto.SyncSubscribeRepos_Account{
			DID:    randomDID(r),
			Active: r.IntN(2) == 0,
			Time:   "2026-05-21T00:00:00Z",
		}}
	case 2:
		return streaming.Event{Sync: &comatproto.SyncSubscribeRepos_Sync{
			DID:  randomDID(r),
			Rev:  "rev",
			Time: "2026-05-21T00:00:00Z",
		}}
	case 3:
		return streaming.Event{Info: &comatproto.SyncSubscribeRepos_Info{}}
	default:
		// Empty / malformed-commit fallback. ConvertEvent must
		// either succeed or return an error, never panic.
		return streaming.Event{Commit: &comatproto.SyncSubscribeRepos_Commit{
			Repo: randomDID(r),
			Rev:  "rev",
			Ops:  nil,
		}}
	}
}

func randomDID(r *rand.Rand) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	buf := make([]byte, 24)
	for i := range buf {
		buf[i] = alphabet[r.IntN(len(alphabet))]
	}
	return "did:plc:" + string(buf)
}
```

- [ ] **Step 2: Run the swarm test**

Run: `just test ./internal/livestream -run TestConvertEvent_Swarm`
Expected: PASS.

- [ ] **Step 3: Run the long-form swarm**

Run: `just test-long ./internal/livestream -run TestConvertEvent_Swarm`
Expected: PASS, 1000 iterations.

- [ ] **Step 4: Run the race detector against just this test**

Run: `just test-race ./internal/livestream -run TestConvertEvent_Swarm`
Expected: PASS (function is pure; race detector should never fire).

- [ ] **Step 5: Commit**

```bash
git add internal/livestream/events_swarm_test.go
git commit -m "$(cat <<'EOF'
test(livestream): swarm test for ConvertEvent invariants

Random sequences of mixed events confirm ConvertEvent never panics
on adversarial input and that every emitted segment.Event satisfies
the on-disk column-width invariants.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: `livestream.Config` + validation

The Config struct and a `validate()` matching the existing pattern in `internal/ingest/config.go`. Tests cover required-field rejection.

**Files:**
- Create: `internal/livestream/config.go`
- Create: `internal/livestream/config_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/livestream/config_test.go`:

```go
package livestream

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	st := newTestStore(t)
	good := Config{
		SegmentsDir: t.TempDir(),
		Store:       st,
		SeqKey:      "live_segments/seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    "https://bsky.network",
		Logger:      logger,
	}

	t.Run("happy", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, good.validate())
	})

	cases := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"missing SegmentsDir", func(c *Config) { c.SegmentsDir = "" }, "SegmentsDir"},
		{"missing Store", func(c *Config) { c.Store = nil }, "Store"},
		{"missing SeqKey", func(c *Config) { c.SeqKey = "" }, "SeqKey"},
		{"missing CursorKey", func(c *Config) { c.CursorKey = "" }, "CursorKey"},
		{"missing RelayURL", func(c *Config) { c.RelayURL = "" }, "RelayURL"},
		{"missing Logger", func(c *Config) { c.Logger = nil }, "Logger"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := good
			c.Store = st // share, since *store.Store is fine across tests
			tc.mutate(&c)
			err := c.validate()
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrInvalidConfig))
			require.Contains(t, err.Error(), tc.want)
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test ./internal/livestream -run TestConfig_Validate`
Expected: FAIL — `Config` undefined.

- [ ] **Step 3: Implement `config.go`**

Create `internal/livestream/config.go`:

```go
package livestream

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/jetstream/internal/store"
)

// Config controls Consumer behavior.
type Config struct {
	// SegmentsDir is where the consumer writes seg_*.jss files.
	// For bootstrap phase this is "<data-dir>/backfill/live_segments".
	// Once the merge step lands, steady-state callers point this at
	// "<data-dir>/segments".
	SegmentsDir string

	// Store is the shared metadata pebble db.
	Store *store.Store

	// SeqKey is the pebble key used by the underlying ingest.Writer
	// for its seq counter. Bootstrap uses "live_segments/seq/next";
	// steady state uses "seq/next".
	SeqKey string

	// CursorKey is the pebble key for the upstream relay cursor.
	// Both phases use "relay/cursor" (the merge step will hand
	// cursor ownership over without renaming the key).
	CursorKey string

	// RelayURL is the upstream relay HTTP base URL — the same value
	// the operator passes via --relay-url. The consumer derives the
	// WebSocket URL from this internally.
	RelayURL string

	// Logger is required.
	Logger *slog.Logger

	// Metrics is optional; nil means no /metrics counters incrementing.
	Metrics *Metrics

	// MaxSegmentBytes / MaxEventsPerBlock forward to ingest.Config.
	// Zero means use ingest defaults.
	MaxSegmentBytes   int64
	MaxEventsPerBlock int

	// now is overridable for tests; production uses time.Now.
	now func() time.Time
}

func (c *Config) validate() error {
	if c.SegmentsDir == "" {
		return fmt.Errorf("%w: SegmentsDir is required", ErrInvalidConfig)
	}
	if c.Store == nil {
		return fmt.Errorf("%w: Store is required", ErrInvalidConfig)
	}
	if c.SeqKey == "" {
		return fmt.Errorf("%w: SeqKey is required", ErrInvalidConfig)
	}
	if c.CursorKey == "" {
		return fmt.Errorf("%w: CursorKey is required", ErrInvalidConfig)
	}
	if c.RelayURL == "" {
		return fmt.Errorf("%w: RelayURL is required", ErrInvalidConfig)
	}
	if c.Logger == nil {
		return fmt.Errorf("%w: Logger is required", ErrInvalidConfig)
	}
	return nil
}

func (c *Config) applyDefaults() {
	if c.now == nil {
		c.now = time.Now
	}
}
```

- [ ] **Step 4: Run tests**

Run: `just test ./internal/livestream`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/livestream/config.go internal/livestream/config_test.go
git commit -m "$(cat <<'EOF'
feat(livestream): add Config with required-field validation

Required fields are rejected with %w: ErrInvalidConfig so callers
can errors.Is. SegmentsDir / SeqKey / CursorKey are explicit so the
same Consumer type is trivially repointable from live_segments to
segments once the merge step lands.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: `Consumer` Open + Close (no Run yet)

Open creates the underlying `ingest.Writer` with the right config and stores its handle. Close flushes and tears it down. Run is a separate task — keeping this commit small and reviewable.

**Files:**
- Create: `internal/livestream/consumer.go`
- Modify: `internal/livestream/config_test.go` (add Open coverage)

- [ ] **Step 1: Write the failing test**

Append to `internal/livestream/config_test.go`:

```go
import (
	// ... existing imports ...
	"path/filepath"
)

func TestOpen_RejectsBadConfig(t *testing.T) {
	t.Parallel()
	_, err := Open(Config{})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidConfig))
}

func TestOpen_CreatesSegmentsDirAndPersistsSeqKey(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")

	c, err := Open(Config{
		SegmentsDir: dir,
		Store:       st,
		SeqKey:      "live_segments/seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    "https://bsky.network",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	// Underlying writer must have created seg_0000000000.jss in dir.
	matches, err := filepath.Glob(filepath.Join(dir, "seg_*.jss"))
	require.NoError(t, err)
	require.Len(t, matches, 1)
}

func TestClose_Idempotent(t *testing.T) {
	t.Parallel()
	c, err := Open(Config{
		SegmentsDir: filepath.Join(t.TempDir(), "live_segments"),
		Store:       newTestStore(t),
		SeqKey:      "live_segments/seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    "https://bsky.network",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	require.NoError(t, c.Close())
	require.NoError(t, c.Close()) // second call is a no-op
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test ./internal/livestream -run "TestOpen|TestClose_Idempotent"`
Expected: FAIL — `Open` undefined.

- [ ] **Step 3: Implement `consumer.go` (Open + Close only)**

Create `internal/livestream/consumer.go`:

```go
// Package livestream: consumer.go owns Consumer, the firehose-to-
// segments pump. Open builds the underlying *ingest.Writer with
// the live-cursor advance hook wired in. Run subscribes to the
// upstream firehose and pushes events through ConvertEvent into
// the writer. Close flushes and tears everything down.
package livestream

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/bluesky-social/jetstream/internal/ingest"
)

// Consumer drives the upstream firehose into a directory of
// segment files. Goroutine-safe to construct; Run is a
// single-producer loop.
type Consumer struct {
	cfg    Config
	writer *ingest.Writer

	// lastUpstream holds the highest upstream seq whose ops have ALL
	// been buffered into the active segment via writer.Append. It is
	// read by the OnAfterFlush hook to advance relay/cursor.
	// atomic.Int64 because OnAfterFlush is invoked from the writer's
	// internal goroutine (the caller of Append, but the writer holds
	// the mutex during the hook); making it atomic future-proofs us
	// for any later refactor that decouples them.
	lastUpstream atomic.Int64

	closeMu sync.Mutex
	closed  bool
}

// Open initializes the consumer's writer and validates config.
// Does not subscribe to the firehose; that happens in Run.
func Open(cfg Config) (*Consumer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()

	c := &Consumer{cfg: cfg}

	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       cfg.SegmentsDir,
		Store:             cfg.Store,
		SeqKey:            cfg.SeqKey,
		MaxSegmentBytes:   cfg.MaxSegmentBytes,
		MaxEventsPerBlock: cfg.MaxEventsPerBlock,
		Logger:            cfg.Logger.With(slog.String("component", "livestream/ingest")),
		// Metrics intentionally nil: per-writer ingest metrics for
		// the live writer are not registered to avoid colliding with
		// the backfill writer's series. The livestream-level Metrics
		// (events received / converted, decode errors, reconnects,
		// upstream cursor) live in cfg.Metrics.
		Metrics:      nil,
		OnAfterFlush: c.onAfterFlush,
	})
	if err != nil {
		return nil, fmt.Errorf("livestream: open writer: %w", err)
	}
	c.writer = w

	return c, nil
}

// Close flushes any pending block, persists the cursor, and closes
// the underlying writer. Idempotent.
func (c *Consumer) Close() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.writer == nil {
		return nil
	}
	if err := c.writer.Close(); err != nil {
		return fmt.Errorf("livestream: close: %w", err)
	}
	return nil
}

// LastUpstreamSeq returns the highest upstream seq whose ops have
// all been buffered into the active segment. This is the in-memory
// value, NOT the persisted relay/cursor — the persisted cursor
// lags by at most one in-flight block.
//
// Used by tests and the future merge orchestrator that needs to
// know where to resume the steady-state consumer from.
func (c *Consumer) LastUpstreamSeq() int64 {
	return c.lastUpstream.Load()
}

// onAfterFlush is the ingest.Writer hook that runs after every
// block flush. Persists the highest fully-buffered upstream seq
// to relay/cursor with pebble.Sync. The placement of
// lastUpstream.Store in Run guarantees the value read here is
// always less than or equal to the latest durable event in the
// just-flushed block (DESIGN.md §3.1.1).
func (c *Consumer) onAfterFlush(ctx context.Context) error {
	cur := c.lastUpstream.Load()
	if cur == 0 {
		// Block flushed before any upstream event was fully
		// processed (only possible during very early startup if
		// the writer has pre-existing state). Skip the save —
		// nothing to persist.
		return nil
	}
	if err := SaveUpstreamCursor(c.cfg.Store, c.cfg.CursorKey, cur); err != nil {
		return err
	}
	c.cfg.Metrics.setUpstreamCursor(cur)
	return nil
}
```

- [ ] **Step 4: Run tests**

Run: `just test ./internal/livestream`
Expected: PASS, all current tests including new Open/Close coverage.

- [ ] **Step 5: Commit**

```bash
git add internal/livestream/consumer.go internal/livestream/config_test.go
git commit -m "$(cat <<'EOF'
feat(livestream): Consumer Open / Close with cursor advance hook

Wraps a dedicated *ingest.Writer pointed at SegmentsDir. The writer's
OnAfterFlush hook reads Consumer.lastUpstream and pebble.Sync's it
to relay/cursor; lastUpstream is updated by Run (next task) only
after every op of an upstream event has been buffered, satisfying
the DESIGN.md §3.1.1 invariant that persisted cursor ≤ durable
events.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 11: `Consumer.Run` and integration test against a fake firehose

The biggest task. Adds `Run`, plus an integration test that scripts a real WebSocket firehose server using `coder/websocket` (already a transitive dep via atmos). The test exercises the full pipeline: subscribe → decode → convert → append → flush → cursor save → reopen-and-resume.

**Files:**
- Modify: `internal/livestream/consumer.go` (add `Run`)
- Create: `internal/livestream/consumer_test.go`

- [ ] **Step 1: Implement `Run`**

Append to `internal/livestream/consumer.go`:

```go
import (
	// ... existing ...
	"time"

	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/gt"
)
```

Then append the methods:

```go
// Run subscribes to the upstream relay's subscribeRepos firehose
// and pumps events into the underlying writer. Returns nil on
// clean context cancellation; returns the underlying error on a
// fatal write or pebble failure (so the errgroup can tear the
// process down).
//
// Reconnects with exponential backoff are handled internally by
// atmos's streaming.Client; Run does not see transient network
// errors as terminal.
func (c *Consumer) Run(ctx context.Context) error {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return ErrClosed
	}
	c.closeMu.Unlock()

	wsURL, err := deriveSubscribeReposURL(c.cfg.RelayURL)
	if err != nil {
		return fmt.Errorf("livestream: %w", err)
	}

	startCursor, err := LoadUpstreamCursor(c.cfg.Store, c.cfg.CursorKey)
	if err != nil {
		return fmt.Errorf("livestream: load start cursor: %w", err)
	}

	c.cfg.Logger.Info("livestream: subscribing",
		"url", wsURL,
		"start_cursor", startCursor,
	)

	opts := streaming.Options{
		URL:        wsURL,
		Cursor:     gt.Some(startCursor),
		SyncClient: gt.Some[*sync.Client](nil), // disable auto-resync; out of scope
		OnReconnect: gt.Some(func(attempt int, delay time.Duration) {
			c.cfg.Metrics.incReconnects()
			c.cfg.Logger.Warn("livestream: reconnecting",
				"attempt", attempt,
				"delay", delay,
			)
		}),
	}

	client, err := streaming.NewClient(opts)
	if err != nil {
		return fmt.Errorf("livestream: new client: %w", err)
	}
	defer func() {
		if cerr := client.Close(); cerr != nil {
			c.cfg.Logger.Warn("livestream: client close", "err", cerr)
		}
	}()

	tracer := obs.Tracer("livestream")

	for batch, err := range client.Events(ctx) {
		if err != nil {
			// Decode / sequence-gap errors flow through here;
			// atmos has already flushed the partial batch as nil
			// + err. Log and continue — the next iteration will
			// either reconnect or yield the next batch.
			c.cfg.Metrics.incDecodeErrors()
			c.cfg.Logger.Warn("livestream: stream error", "err", err)
			continue
		}

		batchCtx, span := tracer.Start(ctx, "livestream.batch")
		if perr := c.processBatch(batchCtx, batch); perr != nil {
			span.RecordError(perr)
			span.End()
			return perr
		}
		span.End()
	}

	c.cfg.Logger.Info("livestream: stopped",
		"last_upstream_seq", c.lastUpstream.Load(),
	)
	return ctx.Err()
}

// processBatch writes one batch of decoded events into the writer.
// Crucially, lastUpstream is updated only AFTER all ops of an event
// have been Append'd, so a flush triggered mid-event reads the
// previous fully-buffered upstream seq and the persisted cursor
// can never get ahead of the durable events.
func (c *Consumer) processBatch(ctx context.Context, batch []streaming.Event) error {
	indexedAt := c.cfg.now().UnixMicro()

	for _, evt := range batch {
		c.cfg.Metrics.incEventsReceived()

		segEvts, err := ConvertEvent(evt, indexedAt)
		if err != nil {
			c.cfg.Metrics.incDecodeErrors()
			// Bad shape from upstream is a data-integrity issue;
			// we surface it rather than silently dropping events.
			return fmt.Errorf("livestream: convert: %w", err)
		}

		for i := range segEvts {
			if err := c.writer.Append(ctx, &segEvts[i]); err != nil {
				return fmt.Errorf("livestream: append: %w", err)
			}
			c.cfg.Metrics.incEventsConverted()
		}

		// Only AFTER every op for this upstream event is buffered.
		if evt.Seq > 0 {
			c.lastUpstream.Store(evt.Seq)
		}
	}

	return nil
}
```

- [ ] **Step 2: Write the integration test (happy path)**

Create `internal/livestream/consumer_test.go`:

```go
package livestream

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// fakeFirehose is a minimal subscribeRepos server: it upgrades to
// a WebSocket and writes a scripted sequence of CBOR frames with
// {op:1, t:"<type>"} headers, exactly the wire format atmos's
// decoder consumes.
type fakeFirehose struct {
	t        *testing.T
	frames   [][]byte         // pre-encoded frames to send
	connWG   atomic.Int32     // tracks live connections
	receivedCursors []string  // cursors observed across reconnects
	cursorsMu sync.Mutex
}

func (f *fakeFirehose) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/com.atproto.sync.subscribeRepos") {
			http.NotFound(w, r)
			return
		}
		f.cursorsMu.Lock()
		f.receivedCursors = append(f.receivedCursors, r.URL.Query().Get("cursor"))
		f.cursorsMu.Unlock()

		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			f.t.Logf("fake firehose accept: %v", err)
			return
		}
		f.connWG.Add(1)
		defer f.connWG.Add(-1)
		defer conn.CloseNow()

		ctx := r.Context()
		for _, frame := range f.frames {
			if err := conn.Write(ctx, websocket.MessageBinary, frame); err != nil {
				return
			}
		}
		// Hold open until client closes.
		<-ctx.Done()
	})
}

// encodeFrame builds the CBOR frame format atmos expects:
// {op:1, t:"<type>"} concatenated with the body CBOR.
func encodeFrame(t *testing.T, typ string, body []byte) []byte {
	t.Helper()
	hdr := cbor.AppendMapHeader(nil, 2)
	hdr = append(hdr, cbor.AppendTextKey(nil, "op")...)
	hdr = cbor.AppendInt(hdr, 1)
	hdr = append(hdr, cbor.AppendTextKey(nil, "t")...)
	hdr = cbor.AppendText(hdr, typ)
	return append(hdr, body...)
}

func encodeIdentityFrame(t *testing.T, did string, seq int64) []byte {
	t.Helper()
	id := &comatproto.SyncSubscribeRepos_Identity{
		DID:    did,
		Handle: gt.Some("h.test"),
		Seq:    seq,
		Time:   "2026-05-21T00:00:00Z",
	}
	body, err := id.MarshalCBOR()
	require.NoError(t, err)
	return encodeFrame(t, "#identity", body)
}

func encodeAccountFrame(t *testing.T, did string, seq int64) []byte {
	t.Helper()
	acc := &comatproto.SyncSubscribeRepos_Account{
		DID:    did,
		Active: true,
		Seq:    seq,
		Time:   "2026-05-21T00:00:00Z",
	}
	body, err := acc.MarshalCBOR()
	require.NoError(t, err)
	return encodeFrame(t, "#account", body)
}

func TestConsumer_Run_HappyPath(t *testing.T) {
	t.Parallel()

	f := &fakeFirehose{
		t: t,
		frames: [][]byte{
			encodeIdentityFrame(t, "did:plc:aaa", 1),
			encodeAccountFrame(t, "did:plc:aaa", 2),
			encodeIdentityFrame(t, "did:plc:bbb", 3),
		},
	}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")

	c, err := Open(Config{
		SegmentsDir:       dir,
		Store:             st,
		SeqKey:            "live_segments/seq/next",
		CursorKey:         "relay/cursor",
		RelayURL:          srv.URL,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxEventsPerBlock: 2, // force a block flush after 2 events
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx) }()

	// Wait until at least 3 events have been processed.
	require.Eventually(t, func() bool {
		return c.LastUpstreamSeq() >= 3
	}, 3*time.Second, 10*time.Millisecond)

	// Cancel and let Run drain. ingest.Writer.Close on Consumer.Close
	// flushes any in-flight block and persists the seq counter.
	cancel()
	select {
	case err := <-runErr:
		require.True(t, err == nil || err == context.Canceled || err == context.DeadlineExceeded,
			"Run returned %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	require.NoError(t, c.Close())

	// At least one block must have flushed (2 events filled the block);
	// confirm relay/cursor was persisted at or below the last seq.
	persisted, err := LoadUpstreamCursor(st, "relay/cursor")
	require.NoError(t, err)
	require.GreaterOrEqual(t, persisted, int64(2),
		"cursor should advance after at least one block flush")
	require.LessOrEqual(t, persisted, int64(3))

	// And the on-disk segment must contain at least one block past
	// the 256-byte reserved header. Deeper validation lives in the
	// segment / ingest test suites.
	matches, err := filepath.Glob(filepath.Join(dir, "seg_*.jss"))
	require.NoError(t, err)
	require.Len(t, matches, 1)
	info, err := os.Stat(matches[0])
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(256), "segment file has at least one block past the reserved header")
}
```

You will need to add these imports if not already present: `"os"`, `"sync"` (for the cursor mutex). Drop `binary` if not used — the test snippet above included it as a sentinel. Goimports/lint will surface unused.

- [ ] **Step 3: Run the integration test**

Run: `just test ./internal/livestream -run TestConsumer_Run_HappyPath`
Expected: PASS in well under 1 second.

- [ ] **Step 4: Add a crash-recovery sub-test**

Append to `internal/livestream/consumer_test.go`:

```go
// TestConsumer_Run_ResumesFromPersistedCursor verifies the crash
// recovery story: kill the consumer mid-stream, reopen, and assert
// the second connection requests a cursor at or before the last
// durable seq.
func TestConsumer_Run_ResumesFromPersistedCursor(t *testing.T) {
	t.Parallel()

	f := &fakeFirehose{
		t: t,
		frames: [][]byte{
			encodeIdentityFrame(t, "did:plc:aaa", 10),
			encodeAccountFrame(t, "did:plc:aaa", 11),
			encodeIdentityFrame(t, "did:plc:bbb", 12),
			encodeIdentityFrame(t, "did:plc:ccc", 13),
		},
	}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")

	cfg := Config{
		SegmentsDir:       dir,
		Store:             st,
		SeqKey:            "live_segments/seq/next",
		CursorKey:         "relay/cursor",
		RelayURL:          srv.URL,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxEventsPerBlock: 2,
	}

	// First run — drain at least one block, then cancel.
	c1, err := Open(cfg)
	require.NoError(t, err)

	ctx1, cancel1 := context.WithCancel(t.Context())
	go func() { _ = c1.Run(ctx1) }()

	require.Eventually(t, func() bool { return c1.LastUpstreamSeq() >= 11 }, 3*time.Second, 10*time.Millisecond)
	cancel1()
	require.NoError(t, c1.Close())

	persistedAfterFirst, err := LoadUpstreamCursor(st, "relay/cursor")
	require.NoError(t, err)
	require.GreaterOrEqual(t, persistedAfterFirst, int64(11))

	// Second run — must request a cursor in its handshake.
	c2, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c2.Close() })

	ctx2, cancel2 := context.WithTimeout(t.Context(), 3*time.Second)
	t.Cleanup(cancel2)
	go func() { _ = c2.Run(ctx2) }()

	require.Eventually(t, func() bool {
		f.cursorsMu.Lock()
		defer f.cursorsMu.Unlock()
		return len(f.receivedCursors) >= 2
	}, 3*time.Second, 10*time.Millisecond)

	f.cursorsMu.Lock()
	defer f.cursorsMu.Unlock()
	require.NotEmpty(t, f.receivedCursors[1], "second connection must include a cursor")
	parsed, err := strconv.ParseInt(f.receivedCursors[1], 10, 64)
	require.NoError(t, err)
	require.GreaterOrEqual(t, parsed, int64(11), "second cursor advances from at least 11 (got %d)", parsed)
}
```

- [ ] **Step 5: Run all tests + race**

Run: `just test ./internal/livestream`
Run: `just test-race ./internal/livestream`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/livestream/consumer.go internal/livestream/consumer_test.go
git commit -m "$(cat <<'EOF'
feat(livestream): Consumer.Run with integration coverage

Run subscribes to the upstream firehose, drives ConvertEvent over
each batch, and pushes the resulting events into the underlying
writer. lastUpstream is updated only after every op of an event
has been buffered, so the OnAfterFlush hook always reads a value
that is less than or equal to the last durable seq in the just-
flushed block.

Coverage:
- Happy-path streams identity / account events through a fake
  firehose, asserts cursor advances on block flush.
- Crash-recovery test reopens the consumer and asserts the second
  websocket connection includes a cursor at or after the last
  persisted seq.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 12: Wire `cmd/jetstream serve` to start the consumer + phase gate

Final wiring task. `runServe` reads the phase, writes `bootstrap` on a fresh dir, refuses `steady_state`, and starts a third goroutine in the existing errgroup.

**Files:**
- Modify: `cmd/jetstream/main.go`
- Modify: `cmd/jetstream/serve_test.go`

- [ ] **Step 1: Write failing tests for the phase gate**

Append to `cmd/jetstream/serve_test.go`:

```go
// TestServe_RefusesSteadyStatePhase pins that the server crashes
// loudly when a data dir already shows phase=steady_state. The merge
// step is not yet implemented; silently doing nothing would be a
// silent-fallback failure mode.
func TestServe_RefusesSteadyStatePhase(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()

	// Pre-populate phase=steady_state.
	{
		s, err := store.Open(dataDir)
		require.NoError(t, err)
		require.NoError(t, lifecycle.WritePhase(s, lifecycle.PhaseSteadyState))
		require.NoError(t, s.Close())
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	err := newApp().Run(ctx, []string{
		"jetstream",
		"--log-format=text",
		"--log-level=warn",
		"serve",
		"--addr=127.0.0.1:0",
		"--debug-addr=127.0.0.1:0",
		"--data-dir=" + dataDir,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "steady-state phase not yet supported")
}

// TestServe_WritesBootstrapPhaseOnFreshDir pins the upgrade path:
// a data dir with no phase key is treated as bootstrap, with the
// phase key written before any goroutine starts.
func TestServe_WritesBootstrapPhaseOnFreshDir(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Empty list — backfill drains immediately.
		_, _ = w.Write([]byte(`{"repos":[]}`))
	}))
	t.Cleanup(relay.Close)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	done := make(chan error, 1)
	go func() {
		done <- newApp().Run(ctx, []string{
			"jetstream",
			"--log-format=text",
			"--log-level=warn",
			"serve",
			"--addr=127.0.0.1:0",
			"--debug-addr=127.0.0.1:0",
			"--shutdown-timeout=5s",
			"--relay-url=" + relay.URL,
			"--data-dir=" + dataDir,
		})
	}()

	// Wait for pebble to exist, then read the phase.
	require.Eventually(t, func() bool {
		s, err := store.Open(dataDir)
		if err != nil {
			return false
		}
		defer func() { _ = s.Close() }()
		p, err := lifecycle.ReadPhase(s)
		return err == nil && p == lifecycle.PhaseBootstrap
	}, 5*time.Second, 50*time.Millisecond, "phase=bootstrap was never written")

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not shut down")
	}
}
```

You will need to add imports: `"github.com/bluesky-social/jetstream/internal/lifecycle"`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `just test ./cmd/jetstream -run "TestServe_RefusesSteadyStatePhase|TestServe_WritesBootstrapPhaseOnFreshDir"`
Expected: FAIL — no phase logic in `runServe`.

- [ ] **Step 3: Wire `runServe`**

Modify `cmd/jetstream/main.go`. Add imports:

```go
"github.com/bluesky-social/jetstream/internal/lifecycle"
"github.com/bluesky-social/jetstream/internal/livestream"
```

In `runServe`, after `metaStore, err := store.Open(dataDir)` and the deferred close, but before the `ingestWriter` open:

```go
phase, err := lifecycle.ReadPhase(metaStore)
if err != nil {
    return fmt.Errorf("serve: read phase: %w", err)
}
if phase == "" {
    phase = lifecycle.PhaseBootstrap
    if err := lifecycle.WritePhase(metaStore, phase); err != nil {
        return fmt.Errorf("serve: write phase: %w", err)
    }
}
if phase == lifecycle.PhaseSteadyState {
    return errors.New("serve: steady-state phase not yet supported; the merge step has not been implemented")
}
```

After the existing `ingestWriter, err := ingest.Open(...)` block (and its defer), add the live consumer:

```go
liveConsumer, err := livestream.Open(livestream.Config{
    SegmentsDir: filepath.Join(dataDir, "backfill", "live_segments"),
    Store:       metaStore,
    SeqKey:      "live_segments/seq/next",
    CursorKey:   "relay/cursor",
    RelayURL:    cmd.String("relay-url"),
    Logger:      logger,
    Metrics:     livestream.NewMetrics(metrics.Registry),
})
if err != nil {
    return fmt.Errorf("livestream open: %w", err)
}
defer func() {
    if cerr := liveConsumer.Close(); cerr != nil {
        logger.Error("close live consumer", "err", cerr)
    }
}()
```

Add a third goroutine to the errgroup, after the existing two:

```go
g.Go(func() error {
    return liveConsumer.Run(gctx)
})
```

Update the comment block above the errgroup to mention three siblings instead of two.

- [ ] **Step 4: Run tests**

Run: `just test ./cmd/jetstream`
Expected: PASS — both new sub-tests + the existing `TestServe_BootstrapsAndShutsDownCleanly`.

- [ ] **Step 5: Run the full project test suite**

Run: `just test`
Expected: PASS across all packages.

- [ ] **Step 6: Run lint**

Run: `just lint`
Expected: PASS.

- [ ] **Step 7: Manual smoke build**

Run: `just build`
Expected: binary at `./bin/jetstream` builds cleanly.

- [ ] **Step 8: Commit**

```bash
git add cmd/jetstream/main.go cmd/jetstream/serve_test.go
git commit -m "$(cat <<'EOF'
feat(cmd): start live firehose consumer alongside backfill

runServe now consults the lifecycle.Phase pebble key. Fresh data
dirs are stamped phase=bootstrap before any goroutine starts.
phase=steady_state is rejected with an explicit "not yet supported"
error rather than silently doing nothing; this is the entry point
the merge-step PR will fill in. During bootstrap, a third sibling
goroutine drives the livestream.Consumer alongside the HTTP server
and the backfill engine, writing live events to
data/backfill/live_segments/.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 13: Final test sweep + race + long swarm

Belt-and-suspenders: rerun the full matrix to catch any cross-package regression introduced by the writer changes in Tasks 2–3.

- [ ] **Step 1: Full test suite under -race**

Run: `just test-race`
Expected: PASS. (~30s; swarm-dominated.)

- [ ] **Step 2: Long swarm**

Run: `just test-long`
Expected: PASS, including 1000-iter swarm.

- [ ] **Step 3: Final lint**

Run: `just lint`
Expected: PASS.

- [ ] **Step 4: No commit needed if everything passes.**

If anything broke that you didn't expect, treat it as a contributing-factor analysis: what assumption broke under -race or under the larger swarm? Don't paper over with `t.Skip` or extra retries — the project's PRACTICES.md is clear that tests should fail when invariants are violated.

---

## Summary of Commits

This plan produces 12 commits in sequence:

1. `feat(lifecycle): add Phase pebble key with round-trip`
2. `feat(ingest): make seq counter pebble key configurable`
3. `feat(ingest): add OnAfterFlush callback for downstream durability`
4. `feat(livestream): scaffold package with doc, errors, metrics`
5. `feat(livestream): persist upstream relay cursor in pebble`
6. `feat(livestream): URL helper that converts --relay-url to wss firehose`
7. `feat(livestream): pure ConvertEvent from streaming.Event to segment.Event`
8. `test(livestream): swarm test for ConvertEvent invariants`
9. `feat(livestream): add Config with required-field validation`
10. `feat(livestream): Consumer Open / Close with cursor advance hook`
11. `feat(livestream): Consumer.Run with integration coverage`
12. `feat(cmd): start live firehose consumer alongside backfill`

(No commit for Task 13 — it's a verification gate, not a code change.)

## Out of Scope (Re-confirmed)

- The merge step that compacts `live_segments/` into `segments/` (DESIGN.md §4.2). Future PR.
- Steady-state consumer pointed at `segments/`. Future PR; only the wiring changes.
- Auto-resync on `#sync` events (atmos full sync 1.1 not yet shipped).
- Lookaside file writes for `KindUpdate` / `KindDelete` (stored as ordinary events for now; future PR adds the lookaside).
- Block-time-based flush. Out of scope for this PR.
- Replicas (DESIGN.md §6).

