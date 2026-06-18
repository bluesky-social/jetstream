# Backfill-to-Live Cutover — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the runtime transition from the bootstrap phase (backfill engine + bootstrap-time live tail writing to `data/backfill/live_segments/`) to the steady-state phase (a single live consumer writing to `data/segments/`). Add a new `internal/ingest/orchestrator` package owning the entire ingestion-lifecycle state machine, a durable `merging` phase, and crash-safe phase commits at every transition. Leave the actual compaction body as a stubbed no-op for a follow-up PR.

**Architecture:** The orchestrator owns the bootstrap → merging → steady-state state machine internally. `cmd/jetstream` constructs cross-cutting primitives (verifier, identity directory, store, HTTP client, server, metrics) and calls one method: `Orchestrator.Run(ctx)`. The orchestrator dispatches based on the persisted lifecycle phase, walks the state machine when backfill drains, and runs the steady-state consumer for the rest of the process. Two durable commit points (`WritePhase(merging)` after backfill drains; `WritePhase(steady_state)` after merge completes) make the cutover crash-safe.

**Tech Stack:** Go 1.26, pebble, atmos (streaming/sync/backfill/identity), prometheus, OpenTelemetry, gotestsum, golangci-lint.

**Spec:** `docs/superpowers/specs/2026-05-23-backfill-to-live-cutover-design.md`

---

## File Structure

| File | Why |
|---|---|
| `internal/lifecycle/phase.go` | Modify: add `PhaseMerging` constant + accept in `valid()` |
| `internal/lifecycle/phase_test.go` | Modify: include `PhaseMerging` in round-trip test |
| `internal/ingest/writer.go` | Modify: add `SealActiveAndClose` method |
| `internal/ingest/writer_test.go` | Modify: add unit tests for `SealActiveAndClose` |
| `internal/ingest/orchestrator/doc.go` | New: package overview |
| `internal/ingest/orchestrator/errors.go` | New: sentinel errors |
| `internal/ingest/orchestrator/config.go` | New: `Config` + `validate` |
| `internal/ingest/orchestrator/config_test.go` | New: validation table tests |
| `internal/ingest/orchestrator/metrics.go` | New: phase gauge, transition counter, state durations |
| `internal/ingest/orchestrator/metrics_test.go` | New: registration round-trip |
| `internal/ingest/orchestrator/orchestrator.go` | New: `Orchestrator` + `New` + `Run` (phase dispatch) |
| `internal/ingest/orchestrator/bootstrap.go` | New: `runBootstrap` — builds backfill + bootstrap-live, awaits drain, walks states 1–6 |
| `internal/ingest/orchestrator/states.go` | New: per-state private methods (drain, seal, close, write-phase) |
| `internal/ingest/orchestrator/merge.go` | New: stubbed `runMerge` with TODO listing future-PR invariants |
| `internal/ingest/orchestrator/steady.go` | New: `runSteadyState` — opens fresh writer + live consumer, runs |
| `internal/ingest/orchestrator/orchestrator_test.go` | New: end-to-end happy-path test |
| `internal/ingest/orchestrator/bootstrap_test.go` | New: bootstrap-phase orchestration unit tests |
| `internal/ingest/orchestrator/states_test.go` | New: per-state unit tests |
| `internal/ingest/orchestrator/recovery_test.go` | New: phase=merging and phase=steady_state restart tests |
| `cmd/jetstream/main.go` | Modify: drop inline backfill+live wiring; construct orchestrator and call Run |
| `cmd/jetstream/serve_test.go` | Modify: replace "fail on steady_state" assertion with "starts cleanly on steady_state"; add merging-phase test |

---

## Conventions

- `just test ./internal/...` — run package tests under `-short` (sub-second per package).
- `just test-race` — full module under race; run before final commit.
- `just lint` — must report 0 issues.
- `t.Parallel()` on independent tests; `t.Cleanup` for `db.Close` etc.
- Doc comments on exported symbols. Comments explain WHY for non-obvious decisions, never WHAT.
- Error wrapping pattern: `orchestrator: <action>: %w`.
- No Co-Authored-By or other trailers on commits.
- Per-task commits: each task ends with a single `git commit`.

---

## Task 1: Add `PhaseMerging` lifecycle constant

**Files:**
- Modify: `internal/lifecycle/phase.go`
- Modify: `internal/lifecycle/phase_test.go`

- [ ] **Step 1: Extend the round-trip test in `internal/lifecycle/phase_test.go`**

Replace the existing `TestPhase_RoundTrip` with the version below (the only change is adding `PhaseMerging` to the loop):

```go
func TestPhase_RoundTrip(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	for _, p := range []Phase{PhaseBootstrap, PhaseMerging, PhaseSteadyState} {
		require.NoError(t, WritePhase(st, p))
		got, err := ReadPhase(st)
		require.NoError(t, err)
		require.Equal(t, p, got)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `just test ./internal/lifecycle`
Expected: FAIL with "undefined: PhaseMerging"

- [ ] **Step 3: Add the constant in `internal/lifecycle/phase.go`**

In the `const` block where `PhaseBootstrap` and `PhaseSteadyState` are declared, add `PhaseMerging` between them:

```go
const (
	// PhaseBootstrap means the backfill engine has not yet finished
	// initial repo download. Both the backfill engine and the
	// live_segments consumer run in this phase.
	PhaseBootstrap Phase = "bootstrap"

	// PhaseMerging means initial backfill has drained but the merge
	// step (DESIGN.md §4.2) has not yet completed. A process restart
	// in this phase resumes the cutover state machine at the merge
	// step; backfill and the bootstrap-phase live consumer are not
	// restarted.
	PhaseMerging Phase = "merging"

	// PhaseSteadyState means backfill is complete and the merge step
	// has folded live_segments into segments. Only the steady-state
	// live consumer runs here.
	PhaseSteadyState Phase = "steady_state"
)
```

Also update `valid()` to accept it:

```go
func (p Phase) valid() bool {
	switch p {
	case PhaseBootstrap, PhaseMerging, PhaseSteadyState:
		return true
	default:
		return false
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `just test ./internal/lifecycle`
Expected: PASS (all tests)

- [ ] **Step 5: Commit**

```bash
git add internal/lifecycle/phase.go internal/lifecycle/phase_test.go
git commit -m "lifecycle: add PhaseMerging value"
```

---

## Task 2: Add `Writer.SealActiveAndClose` to the ingest package

**Files:**
- Modify: `internal/ingest/writer.go`
- Modify: `internal/ingest/writer_test.go`

The orchestrator's State 3 needs to seal the bootstrap-live consumer's active segment when it shuts down. Currently `Writer.Close()` flushes but does not seal. Add a sibling method that does both.

- [ ] **Step 1: Write the failing tests in `internal/ingest/writer_test.go`**

Append these tests to the file (they reuse the existing `newTestWriter` helper):

```go
// TestSealActiveAndClose_SealsAndCloses verifies the cutover-time
// teardown path: after SealActiveAndClose, the trailing segment
// file has a non-zero header checksum (sealed) and the writer
// rejects further appends. seq/next is persisted.
func TestSealActiveAndClose_SealsAndCloses(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{MaxEventsPerBlock: 2})

	for i := range 3 {
		ev := segment.Event{
			IndexedAt: int64(i + 1),
			Kind:      segment.KindCreate,
			DID:       "did:plc:a",
		}
		require.NoError(t, w.Append(t.Context(), &ev))
	}

	require.NoError(t, w.SealActiveAndClose())

	// Subsequent appends fail with ErrClosed.
	err := w.Append(t.Context(), &segment.Event{
		IndexedAt: 4, Kind: segment.KindCreate, DID: "did:plc:a",
	})
	require.ErrorIs(t, err, ErrClosed)

	// The last segment file's checksum (offset 4) must be non-zero
	// — that's the sealed marker per DESIGN.md §3.1.1.
	path := filepath.Join(w.cfg.SegmentsDir, "seg_0000000000.jss")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, len(data), 12, "expected a sealed file with full header")
	checksum := binary.LittleEndian.Uint64(data[4:12])
	require.NotZero(t, checksum, "sealed segment must have non-zero checksum")
}

// TestSealActiveAndClose_Idempotent confirms the second call returns
// nil cleanly, matching Close's idempotent contract.
func TestSealActiveAndClose_Idempotent(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{})

	require.NoError(t, w.SealActiveAndClose())
	require.NoError(t, w.SealActiveAndClose())
}

// TestSealActiveAndClose_FreshDir seals an empty active segment.
// The seal path must handle a writer that never accepted any events.
func TestSealActiveAndClose_FreshDir(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, Config{})

	require.NoError(t, w.SealActiveAndClose())

	path := filepath.Join(w.cfg.SegmentsDir, "seg_0000000000.jss")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), 256, "sealed empty segment is at least the fixed header")
	checksum := binary.LittleEndian.Uint64(data[4:12])
	require.NotZero(t, checksum)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `just test ./internal/ingest -run SealActiveAndClose`
Expected: FAIL with "w.SealActiveAndClose undefined"

- [ ] **Step 3: Implement `SealActiveAndClose` in `internal/ingest/writer.go`**

Add after `Close()`:

```go
// SealActiveAndClose flushes any pending block, seals the active
// segment file (writes the variable-length footer and finalizes the
// 256-byte fixed header), persists nextSeq, and closes the writer.
// Idempotent.
//
// Used by the orchestrator at cutover time to finalize the
// bootstrap-phase live_segments writer's trailing active file so the
// `backfill/live_segments/` tree is fully sealed once steady-state
// begins. Steady-state callers should continue to use Close()
// instead — sealing during normal operation is a rotation-time
// concern handled inside flushAndRotateLocked.
func (w *Writer) SealActiveAndClose() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.active == nil {
		return nil
	}
	// segment.Writer.Seal handles flushing any pending events itself
	// (and is a no-op on an empty pending block), so we don't have to
	// pre-flush here. Seal also closes the underlying file on success.
	if _, err := w.active.Seal(); err != nil {
		return fmt.Errorf("ingest: seal active segment: %w", err)
	}
	if err := saveNextSeq(w.cfg.Store, w.cfg.SeqKey, w.nextSeq); err != nil {
		return err
	}
	return nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `just test ./internal/ingest -run SealActiveAndClose`
Expected: PASS

- [ ] **Step 5: Run the full ingest package tests to confirm no regression**

Run: `just test ./internal/ingest`
Expected: PASS (all tests)

- [ ] **Step 6: Commit**

```bash
git add internal/ingest/writer.go internal/ingest/writer_test.go
git commit -m "ingest: add Writer.SealActiveAndClose for cutover teardown"
```

---

## Task 3: Create the orchestrator package skeleton (doc, errors, config, metrics)

**Files:**
- Create: `internal/ingest/orchestrator/doc.go`
- Create: `internal/ingest/orchestrator/errors.go`
- Create: `internal/ingest/orchestrator/config.go`
- Create: `internal/ingest/orchestrator/config_test.go`
- Create: `internal/ingest/orchestrator/metrics.go`
- Create: `internal/ingest/orchestrator/metrics_test.go`

This task lays in the package's static surface — types and validation — without any state-machine logic yet. Subsequent tasks fill in the runtime.

- [ ] **Step 1: Write the failing tests in `internal/ingest/orchestrator/config_test.go`**

```go
package orchestrator

import (
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/jcalabro/atmos/identity"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

// validBaseConfig returns the minimal Config that passes validate.
// Tests mutate one field at a time off this baseline to assert
// per-field requirements.
func validBaseConfig(t *testing.T) Config {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	return Config{
		DataDir:    dir,
		Store:      st,
		RelayURL:   "https://relay.example",
		HTTPClient: &http.Client{},
		Directory:  &identity.Directory{},
		Verifier:   &atmossync.Verifier{},
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestConfig_Validate_OK(t *testing.T) {
	t.Parallel()
	cfg := validBaseConfig(t)
	require.NoError(t, cfg.validate())
}

func TestConfig_Validate_MissingFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"no DataDir", func(c *Config) { c.DataDir = "" }, "DataDir"},
		{"no Store", func(c *Config) { c.Store = nil }, "Store"},
		{"no RelayURL", func(c *Config) { c.RelayURL = "" }, "RelayURL"},
		{"no HTTPClient", func(c *Config) { c.HTTPClient = nil }, "HTTPClient"},
		{"no Directory", func(c *Config) { c.Directory = nil }, "Directory"},
		{"no Verifier", func(c *Config) { c.Verifier = nil }, "Verifier"},
		{"no Logger", func(c *Config) { c.Logger = nil }, "Logger"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := validBaseConfig(t)
			tc.mutate(&cfg)
			err := cfg.validate()
			require.Error(t, err)
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}
```

- [ ] **Step 2: Run the test to verify it fails (compile error — package doesn't exist)**

Run: `just test ./internal/ingest/orchestrator`
Expected: FAIL with "no Go files" or similar.

- [ ] **Step 3: Create `internal/ingest/orchestrator/doc.go`**

```go
// Package orchestrator owns the ingestion-lifecycle state machine
// for jetstream. It drives the bootstrap → merging → steady-state
// transition described in DESIGN.md §4.2 and §4.3.
//
// cmd/jetstream constructs cross-cutting primitives (verifier,
// identity directory, store, HTTP client) and calls Orchestrator.Run.
// The orchestrator reads the persisted lifecycle phase, builds the
// per-phase ingestion subsystems internally, and walks the cutover
// state machine when initial backfill drains. Phase dispatch is
// internal to this package; cmd/jetstream sees one Run that returns
// when ctx is cancelled or the steady-state consumer exits.
//
// Two durable commit points anchor the cutover:
//
//   1. WritePhase(merging): after backfill drains, before any
//      bootstrap teardown.
//   2. WritePhase(steady_state): after merge completes, before
//      starting the steady-state live consumer.
//
// A crash between either commit point and the next durable
// filesystem effect is recoverable on restart by re-entering the
// state machine at the appropriate point. See the spec for the
// exact restart matrix.
//
// The merge body in merge.go is intentionally a no-op stub for the
// initial PR. The future compaction PR fills it in; the surrounding
// state machine, error handling, observability, and crash-recovery
// are already in place.
package orchestrator
```

- [ ] **Step 4: Create `internal/ingest/orchestrator/errors.go`**

```go
package orchestrator

import "errors"

// ErrInvalidConfig is returned by Config.validate when a required
// field is missing. Wrapped with a field-naming context so callers
// see which field is at fault.
var ErrInvalidConfig = errors.New("orchestrator: invalid config")
```

- [ ] **Step 5: Create `internal/ingest/orchestrator/config.go`**

```go
package orchestrator

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/live"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/jcalabro/atmos/identity"
	atmossync "github.com/jcalabro/atmos/sync"
)

// Config controls Orchestrator behavior. cmd/jetstream constructs
// exactly one of these per process and hands it to New.
//
// Per-subsystem metrics (ingest, live, backfill) are passed through
// because both the bootstrap and steady-state phases reuse the same
// prometheus registry. The orchestrator-level Metrics covers
// transitions and per-state durations.
type Config struct {
	// DataDir is the root data directory. The orchestrator writes to
	// <DataDir>/segments and <DataDir>/backfill/live_segments.
	DataDir string

	// Store is the shared metadata pebble db. Required.
	Store *store.Store

	// RelayURL is the upstream relay base URL (https or wss).
	RelayURL string

	// HTTPClient is the bulk-download-tuned client used by the backfill
	// engine for getRepo and by xrpc for listRepos. Required.
	HTTPClient *http.Client

	// Directory is the shared identity directory for both backfill
	// (sync.Client) and the live consumer (verifier).
	Directory *identity.Directory

	// Verifier is the Sync 1.1 verifier used by both bootstrap-time
	// and steady-state live consumers.
	Verifier *atmossync.Verifier

	// Logger is required.
	Logger *slog.Logger

	// Metrics is the orchestrator-level metrics handle. Optional;
	// nil means no /metrics counters incrementing.
	Metrics *Metrics

	// IngestMetrics is shared between the backfill writer and the
	// steady-state live writer. Optional.
	IngestMetrics *ingest.Metrics

	// LiveMetrics is shared between the bootstrap-time and
	// steady-state live consumers. Optional.
	LiveMetrics *live.Metrics

	// BackfillMetrics is consumed by the backfill engine in the
	// bootstrap phase only. Optional.
	BackfillMetrics *backfill.Metrics
}

func (c *Config) validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("%w: DataDir is required", ErrInvalidConfig)
	}
	if c.Store == nil {
		return fmt.Errorf("%w: Store is required", ErrInvalidConfig)
	}
	if c.RelayURL == "" {
		return fmt.Errorf("%w: RelayURL is required", ErrInvalidConfig)
	}
	if c.HTTPClient == nil {
		return fmt.Errorf("%w: HTTPClient is required", ErrInvalidConfig)
	}
	if c.Directory == nil {
		return fmt.Errorf("%w: Directory is required", ErrInvalidConfig)
	}
	if c.Verifier == nil {
		return fmt.Errorf("%w: Verifier is required", ErrInvalidConfig)
	}
	if c.Logger == nil {
		return fmt.Errorf("%w: Logger is required", ErrInvalidConfig)
	}
	return nil
}
```

- [ ] **Step 6: Create `internal/ingest/orchestrator/metrics.go`**

```go
package orchestrator

import "github.com/prometheus/client_golang/prometheus"

const (
	metricsNamespace = "jetstream"
	metricsSubsystem = "orchestrator"
)

// Phase gauge values. These are stable wire values — operators
// alerting on phase transitions rely on the integer mapping.
const (
	PhaseGaugeBootstrap   = 0
	PhaseGaugeMerging     = 1
	PhaseGaugeSteadyState = 2
)

// Metrics owns the prometheus counters and gauges for the
// orchestrator. A nil *Metrics is a valid zero-value: every method
// is a no-op so tests can skip metric registration entirely.
type Metrics struct {
	Phase             prometheus.Gauge
	PhaseTransitions  *prometheus.CounterVec
	StateDuration     *prometheus.HistogramVec
}

// NewMetrics registers the orchestrator counters/gauges against reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Phase: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "phase",
			Help: "Current ingestion phase: 0=bootstrap, 1=merging, 2=steady_state.",
		}),
		PhaseTransitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "phase_transitions_total",
			Help: "Number of phase transitions, by from/to phase.",
		}, []string{"from", "to"}),
		StateDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name:    "state_duration_seconds",
			Help:    "Wall-clock seconds spent in each cutover state.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 14),
		}, []string{"state"}),
	}
	reg.MustRegister(m.Phase, m.PhaseTransitions, m.StateDuration)
	return m
}

func (m *Metrics) setPhase(v float64) {
	if m != nil {
		m.Phase.Set(v)
	}
}

func (m *Metrics) incTransition(from, to string) {
	if m != nil {
		m.PhaseTransitions.WithLabelValues(from, to).Inc()
	}
}

func (m *Metrics) observeState(state string, seconds float64) {
	if m != nil {
		m.StateDuration.WithLabelValues(state).Observe(seconds)
	}
}
```

- [ ] **Step 7: Create `internal/ingest/orchestrator/metrics_test.go`**

```go
package orchestrator

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// TestNewMetrics_Registers confirms that NewMetrics registers all
// vectors against the registry without colliding.
func TestNewMetrics_Registers(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	NewMetrics(reg)
}

// TestMetrics_NilSafe confirms every metric helper is nil-safe so
// callers can pass *Metrics(nil) in tests without registering.
func TestMetrics_NilSafe(t *testing.T) {
	t.Parallel()
	var m *Metrics
	m.setPhase(0)
	m.incTransition("bootstrap", "merging")
	m.observeState("drain", 0.1)
}
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `just test ./internal/ingest/orchestrator`
Expected: PASS

- [ ] **Step 9: Run lint to confirm clean baseline**

Run: `just lint`
Expected: 0 issues.

- [ ] **Step 10: Commit**

```bash
git add internal/ingest/orchestrator/
git commit -m "orchestrator: package skeleton (config, errors, metrics)"
```

---

## Task 4: Implement `runSteadyState` and the `runMerge` stub

**Files:**
- Create: `internal/ingest/orchestrator/steady.go`
- Create: `internal/ingest/orchestrator/merge.go`
- Create: `internal/ingest/orchestrator/states.go`
- Create: `internal/ingest/orchestrator/orchestrator.go`

`runSteadyState` is the simplest path through the orchestrator: open a fresh ingest writer for `data/segments/`, open a `live.Consumer` against it, run until ctx is cancelled. We implement this first because it has no other-state dependencies and is testable in isolation.

`runMerge` is a no-op stub but lives in its own file so the future compaction PR has one obvious place to edit.

`states.go` holds the small helpers that actually call WritePhase. The package exports only `Orchestrator`, `New`, `Run`, `Config`, `Metrics`, `ErrInvalidConfig`, and the phase gauge constants — everything else is private.

- [ ] **Step 1: Create `internal/ingest/orchestrator/states.go` with the WritePhase helpers**

```go
package orchestrator

import (
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
)

// writeMergingPhase is commit point #1. After this call returns nil,
// the data dir is durably in PhaseMerging and a restart will resume
// at the merge step.
func (o *Orchestrator) writeMergingPhase() error {
	start := time.Now()
	if err := lifecycle.WritePhase(o.cfg.Store, lifecycle.PhaseMerging); err != nil {
		return fmt.Errorf("orchestrator: write phase=merging: %w", err)
	}
	o.cfg.Metrics.observeState("write_phase_merging", time.Since(start).Seconds())
	o.cfg.Metrics.incTransition(string(lifecycle.PhaseBootstrap), string(lifecycle.PhaseMerging))
	o.cfg.Metrics.setPhase(PhaseGaugeMerging)
	o.cfg.Logger.Info("orchestrator: phase=merging")
	return nil
}

// writeSteadyStatePhase is commit point #2. After this call returns
// nil, the data dir is durably in PhaseSteadyState.
func (o *Orchestrator) writeSteadyStatePhase() error {
	start := time.Now()
	if err := lifecycle.WritePhase(o.cfg.Store, lifecycle.PhaseSteadyState); err != nil {
		return fmt.Errorf("orchestrator: write phase=steady_state: %w", err)
	}
	o.cfg.Metrics.observeState("write_phase_steady", time.Since(start).Seconds())
	o.cfg.Metrics.incTransition(string(lifecycle.PhaseMerging), string(lifecycle.PhaseSteadyState))
	o.cfg.Metrics.setPhase(PhaseGaugeSteadyState)
	o.cfg.Logger.Info("orchestrator: phase=steady_state")
	return nil
}
```

- [ ] **Step 2: Create `internal/ingest/orchestrator/merge.go` with the stub**

```go
package orchestrator

import (
	"context"
	"time"
)

// runMerge is the cutover state machine's State 5: compact the
// throwaway segment files in data/backfill/live_segments/ into
// data/segments/.
//
// TODO(merge): implement compaction per DESIGN.md §4.2.
//
// Required behavior of the future implementation:
//
//   1. Read every sealed segment file under data/backfill/live_segments/
//      in seq-ascending file order.
//   2. For each event, look up repo/<did>.BackfillRev. Drop the event
//      if its rev is <= BackfillRev (its data was already written
//      authoritatively by the backfill engine). Keep otherwise.
//   3. Write surviving events to data/segments/ via a fresh
//      ingest.Writer that allocates new seq numbers from "seq/next"
//      so they continue monotonically from the backfill writer's
//      last allocation.
//   4. Be IDEMPOTENT under partial completion: a crash mid-merge
//      restarts in PhaseMerging and re-runs runMerge. The
//      implementation must not double-write events on retry. A
//      "last-completed-source-segment" pebble key is the natural
//      cursor.
//   5. Once all source files are consumed and survivors are durably
//      flushed in data/segments/, runMerge returns nil. The caller
//      (Run) then writes PhaseSteadyState. Cleanup of
//      data/backfill/live_segments/ is left for a future PR; the
//      directory sits there harmlessly.
//
// The current implementation is a deliberate no-op: this PR ships
// the orchestration scaffolding, not the merge logic. State 5 is
// trivially idempotent because it does nothing.
func (o *Orchestrator) runMerge(_ context.Context) error {
	start := time.Now()
	o.cfg.Logger.Info("orchestrator: merge (stub no-op)")
	o.cfg.Metrics.observeState("merge", time.Since(start).Seconds())
	return nil
}
```

- [ ] **Step 3: Create `internal/ingest/orchestrator/steady.go`**

```go
package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/bluesky-social/jetstream-v2/internal/ingest/live"
)

// runSteadyState opens a live.Consumer pointed at <DataDir>/segments
// and runs it until ctx is cancelled. Returns ctx.Err() on a clean
// shutdown; the underlying error otherwise.
//
// live.Consumer constructs and owns its own *ingest.Writer pointed
// at the same SegmentsDir. We deliberately do NOT open a second
// writer here: two writers sharing one active segment would race on
// the file offset, hand out duplicate seq numbers, and clobber each
// other's "seq/next" on close.
//
// The consumer's internal ingest.Open runs ScanMaxSeq against the
// active segment in <DataDir>/segments and reconciles the in-memory
// nextSeq against pebble's "seq/next", so steady-state continues
// exactly where the backfill writer left off.
//
// Pebble keys "seq/next" and "relay/cursor" are the steady-state
// defaults; the upstream firehose resumes from the bootstrap-time
// consumer's last persisted cursor and at-least-once delivery
// covers the at-most-one-block overlap.
func (o *Orchestrator) runSteadyState(ctx context.Context) error {
	segmentsDir := filepath.Join(o.cfg.DataDir, "segments")

	c, err := live.Open(live.Config{
		SegmentsDir: segmentsDir,
		Store:       o.cfg.Store,
		SeqKey:      "seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    o.cfg.RelayURL,
		Logger:      o.cfg.Logger.With(slog.String("component", "orchestrator/steady-live")),
		Metrics:     o.cfg.LiveMetrics,
		Verifier:    o.cfg.Verifier,
	})
	if err != nil {
		return fmt.Errorf("orchestrator: open steady-state live consumer: %w", err)
	}
	defer func() {
		if cerr := c.Close(); cerr != nil {
			o.cfg.Logger.Error("orchestrator: close steady-state live consumer", "err", cerr)
		}
	}()

	o.cfg.Logger.Info("orchestrator: steady-state consumer running")
	o.cfg.Metrics.setPhase(PhaseGaugeSteadyState)

	return c.Run(ctx)
}
```

Note: the `IngestMetrics` field on Config is only used by the bootstrap-phase backfill writer (Task 5). The steady-state path does not register per-writer ingest metrics because `live.Consumer` deliberately passes `Metrics: nil` to its internal `ingest.Writer` to avoid colliding with the backfill writer's series.

- [ ] **Step 4: Create `internal/ingest/orchestrator/orchestrator.go` with `New` and a phase-dispatching `Run`**

`Run` reads the persisted phase and dispatches. The bootstrap branch is stubbed in this task — Task 5 implements it. Steady-state and merging-resume paths work end-to-end in this task.

```go
package orchestrator

import (
	"context"
	"fmt"

	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
)

// Orchestrator owns the ingestion-lifecycle state machine. Construct
// via New; call Run exactly once.
type Orchestrator struct {
	cfg Config
}

// New validates cfg and returns an Orchestrator ready to Run.
func New(cfg Config) (*Orchestrator, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Orchestrator{cfg: cfg}, nil
}

// Run reads the persisted lifecycle phase and dispatches to the
// matching entry path. Phase transitions during a single Run are
// internal — callers see one Run that returns when ctx is cancelled
// or the steady-state consumer exits.
//
// On a fresh data dir (no phase key), Run treats the data dir as
// PhaseBootstrap and writes that value before starting any
// subsystems. This matches the previous cmd/jetstream behavior.
func (o *Orchestrator) Run(ctx context.Context) error {
	phase, err := lifecycle.ReadPhase(o.cfg.Store)
	if err != nil {
		return fmt.Errorf("orchestrator: read phase: %w", err)
	}
	if phase == "" {
		phase = lifecycle.PhaseBootstrap
		if err := lifecycle.WritePhase(o.cfg.Store, phase); err != nil {
			return fmt.Errorf("orchestrator: write initial phase: %w", err)
		}
	}

	o.cfg.Logger.Info("orchestrator: starting", "phase", phase)

	switch phase {
	case lifecycle.PhaseBootstrap:
		o.cfg.Metrics.setPhase(PhaseGaugeBootstrap)
		if err := o.runBootstrap(ctx); err != nil {
			return err
		}
		// runBootstrap returned cleanly => backfill drained and the
		// cutover state machine completed through merge but BEFORE
		// PhaseSteadyState was written. Continue.
		fallthrough
	case lifecycle.PhaseMerging:
		// Either we just got here from bootstrap (fallthrough) or we
		// are resuming after a crash. Re-run merge (stub no-op for
		// now; future implementation must be idempotent).
		o.cfg.Metrics.setPhase(PhaseGaugeMerging)
		if err := o.runMerge(ctx); err != nil {
			return fmt.Errorf("orchestrator: merge: %w", err)
		}
		if err := o.writeSteadyStatePhase(); err != nil {
			return err
		}
		fallthrough
	case lifecycle.PhaseSteadyState:
		return o.runSteadyState(ctx)
	default:
		return fmt.Errorf("orchestrator: unrecognized phase %q", phase)
	}
}
```

**Note on the fallthrough chain:** Go's `switch` `fallthrough` keyword unconditionally enters the next case. The bootstrap case calls `runBootstrap`, which on success has internally completed states 1–4 AND written PhaseMerging. Falling through to the `case PhaseMerging` then runs the stub merge and writes PhaseSteadyState before falling through to the steady-state run. On a fresh process started in PhaseSteadyState, the switch jumps directly to that case. This is the cleanest expression of "run the remaining suffix of the state machine" without explicit goto-style jumps.

`runBootstrap` is implemented as a method on `*Orchestrator`. It does not exist yet; this task adds a placeholder so the package compiles, and Task 5 fills it in.

- [ ] **Step 5: Add the placeholder `runBootstrap` so the package compiles**

Append to `internal/ingest/orchestrator/orchestrator.go`:

```go
// runBootstrap is implemented in bootstrap.go. The placeholder here
// keeps the package compiling while bootstrap.go is being authored
// in a separate task — once bootstrap.go lands, this declaration is
// replaced by the real method body in that file.
func (o *Orchestrator) runBootstrap(_ context.Context) error {
	return fmt.Errorf("orchestrator: runBootstrap not implemented")
}
```

This stub will be replaced in Task 5 by moving the method to `bootstrap.go`.

- [ ] **Step 6: Run the package tests to verify the build is clean**

Run: `just test ./internal/ingest/orchestrator`
Expected: PASS (existing config/metrics tests; no new ones yet).

- [ ] **Step 7: Commit**

```bash
git add internal/ingest/orchestrator/
git commit -m "orchestrator: implement runSteadyState, runMerge stub, Run dispatch"
```

---

## Task 5: Implement `runBootstrap` (the cutover trigger and states 1–4)

**Files:**
- Modify: `internal/ingest/orchestrator/orchestrator.go` (remove the placeholder)
- Create: `internal/ingest/orchestrator/bootstrap.go`

`runBootstrap` constructs the backfill engine, the bootstrap-time live consumer, and the shared backfill `ingest.Writer`. It runs them as siblings under an internal errgroup. When the backfill engine returns nil (drained), it walks states 1–4: WritePhase(merging), drain bootstrap-live, seal bootstrap-live, close backfill writer. It returns nil when those steps complete; the caller (`Run`) then proceeds through the merge case.

If either subsystem returns an error before backfill drains, the errgroup tears the other down and `runBootstrap` propagates the error without changing phase.

- [ ] **Step 1: Remove the `runBootstrap` placeholder from `orchestrator.go`**

Delete the placeholder method body added in Task 5; it will be replaced by the real implementation in `bootstrap.go`. Do NOT remove the call site in `Run` — that stays.

- [ ] **Step 2: Create `internal/ingest/orchestrator/bootstrap.go`**

```go
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/ingest/live"
	"golang.org/x/sync/errgroup"
)

// runBootstrap is the orchestrator's State 0. It builds:
//
//   - a shared ingest.Writer pointed at <DataDir>/segments (used by
//     the backfill engine; closed in State 4 of the cutover),
//   - the backfill engine itself,
//   - a live.Consumer pointed at <DataDir>/backfill/live_segments
//     with the throwaway "live_segments/seq/next" seq counter and
//     the shared "relay/cursor" upstream cursor.
//
// It runs the backfill engine and the live consumer as siblings
// under an internal errgroup, with the live consumer attached to a
// derived context the orchestrator can cancel independently. When
// backfill drains (returns nil), the orchestrator:
//
//   1. Writes phase=merging (commit point #1).
//   2. Cancels the live consumer's derived context.
//   3. Awaits the live consumer's Run return, then SealAndClose its
//      writer (state 3).
//   4. Closes the backfill writer (state 4).
//
// On success, runBootstrap returns nil and the caller falls through
// to the merge case. On any subsystem error before backfill drains,
// the errgroup cancels both and the error is returned without
// touching the phase.
func (o *Orchestrator) runBootstrap(ctx context.Context) error {
	segmentsDir := filepath.Join(o.cfg.DataDir, "segments")
	liveSegmentsDir := filepath.Join(o.cfg.DataDir, "backfill", "live_segments")

	// Backfill writer (shared with the backfill engine).
	bw, err := ingest.Open(ingest.Config{
		SegmentsDir: segmentsDir,
		Store:       o.cfg.Store,
		Logger:      o.cfg.Logger.With(slog.String("component", "orchestrator/backfill-ingest")),
		Metrics:     o.cfg.IngestMetrics,
	})
	if err != nil {
		return fmt.Errorf("orchestrator: open backfill ingest writer: %w", err)
	}

	// Bootstrap-time live consumer.
	bootstrapLive, err := live.Open(live.Config{
		SegmentsDir: liveSegmentsDir,
		Store:       o.cfg.Store,
		SeqKey:      "live_segments/seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    o.cfg.RelayURL,
		Logger:      o.cfg.Logger.With(slog.String("component", "orchestrator/bootstrap-live")),
		Metrics:     o.cfg.LiveMetrics,
		Verifier:    o.cfg.Verifier,
	})
	if err != nil {
		_ = bw.Close()
		return fmt.Errorf("orchestrator: open bootstrap-live consumer: %w", err)
	}

	g, gctx := errgroup.WithContext(ctx)

	// Derived context the orchestrator cancels at cutover-time.
	// Wrapping gctx means errgroup-driven cancellation (e.g. from a
	// backfill error) also propagates to the live consumer, while
	// still letting us call cancelLive() to stop ONLY the live
	// consumer when backfill drains successfully — gctx remains
	// uncancelled so the backfill goroutine can return nil normally.
	liveCtx, cancelLive := context.WithCancel(gctx)
	defer cancelLive()

	g.Go(func() error {
		err := backfill.Run(gctx, backfill.Config{
			Store:      o.cfg.Store,
			HTTPClient: o.cfg.HTTPClient,
			Directory:  o.cfg.Directory,
			Writer:     bw,
			RelayURL:   o.cfg.RelayURL,
			Logger:     o.cfg.Logger,
			Metrics:    o.cfg.BackfillMetrics,
		})
		if err != nil {
			return err
		}
		// Backfill drained cleanly. Trigger the cutover by writing
		// phase=merging FIRST (commit point #1), THEN cancelling the
		// live consumer. The order matters: the phase write is the
		// only durable signal that backfill has finished, and a crash
		// after the phase write recovers via PhaseMerging restart.
		if err := o.writeMergingPhase(); err != nil {
			return err
		}
		// Cancel the bootstrap-live consumer's context. This signals
		// state 2. The live consumer's Run goroutine returns shortly,
		// and its return value is the second errgroup goroutine's
		// result.
		cancelLive()
		return nil
	})

	g.Go(func() error {
		err := bootstrapLive.Run(liveCtx)
		// If the only thing that happened is the orchestrator's own
		// cancelLive() call (after backfill drained successfully),
		// liveCtx is cancelled but ctx (the outer process ctx) is
		// still healthy. Treat that as a clean stop.
		if err != nil && errors.Is(err, context.Canceled) && liveCtx.Err() != nil && ctx.Err() == nil {
			return nil
		}
		return err
	})

	if err := g.Wait(); err != nil {
		// Best-effort cleanup. Close errors are logged, not returned,
		// because the underlying error is what we want surfaced.
		if cerr := bootstrapLive.Close(); cerr != nil {
			o.cfg.Logger.Warn("orchestrator: bootstrap-live close after error", "err", cerr)
		}
		if cerr := bw.Close(); cerr != nil {
			o.cfg.Logger.Warn("orchestrator: backfill writer close after error", "err", cerr)
		}
		return err
	}

	// State 3: SealAndClose the bootstrap-live consumer. Close()
	// flushes and persists the cursor; SealAndClose on the underlying
	// writer is reachable through a public method we add separately.
	// For now, the live.Consumer's Close persists the cursor; we then
	// reach into the writer indirectly by re-opening + sealing... but
	// see the next step.
	start := time.Now()
	if err := bootstrapLive.Close(); err != nil {
		return fmt.Errorf("orchestrator: close bootstrap-live consumer: %w", err)
	}
	// Seal the bootstrap-live writer's active segment by re-opening
	// the segments directory through a fresh ingest.Writer, calling
	// SealActiveAndClose. Any subsequent process that reads the
	// directory sees an all-sealed tree.
	sealW, err := ingest.Open(ingest.Config{
		SegmentsDir: liveSegmentsDir,
		Store:       o.cfg.Store,
		SeqKey:      "live_segments/seq/next",
		Logger:      o.cfg.Logger.With(slog.String("component", "orchestrator/bootstrap-seal")),
		Metrics:     nil,
	})
	if err != nil {
		return fmt.Errorf("orchestrator: re-open bootstrap-live writer for seal: %w", err)
	}
	if err := sealW.SealActiveAndClose(); err != nil {
		return fmt.Errorf("orchestrator: seal bootstrap-live segment: %w", err)
	}
	o.cfg.Metrics.observeState("seal_bootstrap", time.Since(start).Seconds())
	o.cfg.Logger.Info("orchestrator: bootstrap segment sealed")

	// State 4: Close the backfill writer. Flush only — steady-state
	// will reopen this same directory and continue appending.
	closeStart := time.Now()
	if err := bw.Close(); err != nil {
		return fmt.Errorf("orchestrator: close backfill ingest writer: %w", err)
	}
	o.cfg.Metrics.observeState("close_backfill", time.Since(closeStart).Seconds())
	o.cfg.Logger.Info("orchestrator: backfill writer closed")

	return nil
}
```

A subtlety worth flagging in code: when we re-open the bootstrap-live writer via `ingest.Open` for the seal, `Open` sees the existing seg_*.jss files and either picks up the active one (if the current one isn't sealed yet) or rolls forward to a fresh empty one (if `live.Consumer`'s underlying writer already happened to seal during normal rotation). Both cases are fine: in the first, `SealActiveAndClose` finalizes the current file; in the second, we end up with an empty extra `seg_<N+1>.jss` file that's been sealed, which the future compactor sees and ignores (it has zero events). Document this with a comment.

- [ ] **Step 3: Add the explanatory comment about the empty-extra-file case**

Right above the `sealW, err := ingest.Open(...)` block in `bootstrap.go`, the comment is already in the code above ("Seal the bootstrap-live writer's active segment..."). Extend it:

```go
	// Seal the bootstrap-live writer's active segment by re-opening
	// the segments directory through a fresh ingest.Writer, calling
	// SealActiveAndClose. Any subsequent process that reads the
	// directory sees an all-sealed tree.
	//
	// If the bootstrap consumer's underlying writer happened to seal
	// its active file during normal rotation just before we got here,
	// ingest.Open rolls forward to a fresh empty seg_<N+1>.jss and
	// SealActiveAndClose seals that empty file. The future compactor
	// reads zero events from such a file and ignores it. The empty
	// extra file is the cost of avoiding a "did the writer just
	// rotate?" race in this orchestrator path.
```

(This is documentation only — no code change beyond what step 2 added.)

- [ ] **Step 4: Run the package tests**

Run: `just test ./internal/ingest/orchestrator`
Expected: PASS (existing tests; bootstrap-specific tests come in Task 6).

- [ ] **Step 5: Run lint**

Run: `just lint`
Expected: 0 issues.

- [ ] **Step 6: Commit**

```bash
git add internal/ingest/orchestrator/
git commit -m "orchestrator: implement runBootstrap and cutover states 1-4"
```

---

## Task 6: Bootstrap-phase orchestration unit tests

**Files:**
- Create: `internal/ingest/orchestrator/bootstrap_test.go`

These tests exercise `runBootstrap` against a real pebble store, a real `live.Consumer` driven by an in-process WebSocket fake (the same pattern as `internal/ingest/live/consumer_test.go`), and a real `backfill.Engine` driven by a stub relay. The goal is to verify cutover orchestration end-to-end *within* the bootstrap branch.

Because constructing a verifier + identity directory + atmos engine for a unit test is heavy, and we already have similar fixtures in `internal/ingest/live/consumer_test.go` and `internal/ingest/backfill/run_test.go`, this task introduces a small `testfixtures.go` file inside the orchestrator package with helpers tuned for this package's tests.

- [ ] **Step 1: Create `internal/ingest/orchestrator/testfixtures_test.go` with shared helpers**

```go
package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// fakeRelay is a minimal stub combining the listRepos endpoint
// (drains in one page) and the subscribeRepos WebSocket (accepts
// the connection and holds it open).
type fakeRelay struct {
	t       *testing.T
	repos   []listReposEntry
	srv     *httptest.Server
}

type listReposEntry struct {
	DID    string `json:"did"`
	Head   string `json:"head"`
	Rev    string `json:"rev"`
	Active bool   `json:"active"`
}

type listReposPage struct {
	Cursor string           `json:"cursor,omitempty"`
	Repos  []listReposEntry `json:"repos"`
}

func newFakeRelay(t *testing.T, repos []listReposEntry) *fakeRelay {
	t.Helper()
	f := &fakeRelay{t: t, repos: repos}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeRelay) URL() string { return f.srv.URL }

func (f *fakeRelay) handle(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasSuffix(r.URL.Path, "/com.atproto.sync.subscribeRepos"):
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()
		<-r.Context().Done()
	case strings.HasSuffix(r.URL.Path, "/com.atproto.sync.listRepos"):
		_ = json.NewEncoder(w).Encode(listReposPage{Repos: f.repos})
	default:
		http.NotFound(w, r)
	}
}

// newTestVerifier builds a real Sync 1.1 verifier against an
// in-memory directory + a pebble-backed state store. The verifier
// never resyncs in these tests because no #sync events flow.
func newTestVerifier(t *testing.T, relayURL string) *atmossync.Verifier {
	t.Helper()
	dir := &identity.Directory{
		Resolver:               &identity.DefaultResolver{},
		SkipHandleVerification: true,
	}
	xc := &xrpc.Client{Host: relayURL}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})
	v, err := atmossync.NewVerifier(atmossync.VerifierOptions{
		Directory:  dir,
		SyncClient: gt.Some(sc),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })
	return v
}

// testIdentityDirectory returns a default directory suitable for
// orchestrator tests where the live consumer never receives a
// frame that would force a real DID lookup.
func testIdentityDirectory() *identity.Directory {
	return &identity.Directory{
		Resolver:               &identity.DefaultResolver{},
		SkipHandleVerification: true,
	}
}

var _ = atmos.DID("") // avoid unused import warnings if helpers shrink
```

- [ ] **Step 2: Write the bootstrap happy-path test in `internal/ingest/orchestrator/bootstrap_test.go`**

```go
package orchestrator

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

// TestRunBootstrap_DrainsAndAdvancesPhase verifies the happy path:
// backfill drains because the relay returns zero DIDs, the
// orchestrator writes phase=merging, cancels the bootstrap-live
// consumer, and seals + closes both writers. After return, phase
// is durably PhaseMerging.
func TestRunBootstrap_DrainsAndAdvancesPhase(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	st, err := store.Open(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseBootstrap))

	relay := newFakeRelay(t, nil) // empty repo list => backfill drains immediately
	verifier := newTestVerifier(t, relay.URL())

	o, err := New(Config{
		DataDir:    dataDir,
		Store:      st,
		RelayURL:   relay.URL(),
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Directory:  testIdentityDirectory(),
		Verifier:   verifier,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, o.runBootstrap(ctx))

	// Phase must have advanced to merging.
	got, err := lifecycle.ReadPhase(st)
	require.NoError(t, err)
	require.Equal(t, lifecycle.PhaseMerging, got)

	// The bootstrap-live segment file must exist and be sealed
	// (non-zero checksum at offset 4).
	liveDir := filepath.Join(dataDir, "backfill", "live_segments")
	entries, err := readSegFiles(liveDir)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "expected at least one bootstrap-live segment")
	for _, p := range entries {
		require.True(t, isSealed(t, p), "%s should be sealed", p)
	}
}
```

The helpers `readSegFiles` and `isSealed` are small enough to live with this test file:

```go
// readSegFiles returns paths to all seg_*.jss files in dir, sorted.
func readSegFiles(dir string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "seg_*.jss"))
	if err != nil {
		return nil, err
	}
	return matches, nil
}

// isSealed returns true if the file at path has a non-zero checksum
// in its 256-byte fixed header (i.e. has been sealed).
func isSealed(t *testing.T, path string) bool {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	var hdr [12]byte
	_, err = io.ReadFull(f, hdr[:])
	require.NoError(t, err)
	return binary.LittleEndian.Uint64(hdr[4:12]) != 0
}
```

(Add the imports `encoding/binary`, `io`, `os`, `path/filepath` to the test file.)

- [ ] **Step 3: Run the test**

Run: `just test ./internal/ingest/orchestrator -run TestRunBootstrap`
Expected: PASS

- [ ] **Step 4: Add a backfill-error-propagation test**

Append to `bootstrap_test.go`:

```go
// TestRunBootstrap_BackfillErrorPropagates verifies that a backfill
// engine error (e.g. unreachable relay) tears down the orchestrator
// cleanly and the phase remains PhaseBootstrap.
func TestRunBootstrap_BackfillErrorPropagates(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	st, err := store.Open(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseBootstrap))

	// Point at a closed listener so listRepos fails fast.
	const unreachable = "http://127.0.0.1:1" // port 1 is reserved/unused
	verifier := newTestVerifier(t, unreachable)

	o, err := New(Config{
		DataDir:    dataDir,
		Store:      st,
		RelayURL:   unreachable,
		HTTPClient: &http.Client{Timeout: 500 * time.Millisecond},
		Directory:  testIdentityDirectory(),
		Verifier:   verifier,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	err = o.runBootstrap(ctx)
	require.Error(t, err, "unreachable relay should surface as runBootstrap error")

	// Phase must still be PhaseBootstrap — no cutover happened.
	got, err := lifecycle.ReadPhase(st)
	require.NoError(t, err)
	require.Equal(t, lifecycle.PhaseBootstrap, got)
}
```

- [ ] **Step 5: Run the new test**

Run: `just test ./internal/ingest/orchestrator -run TestRunBootstrap`
Expected: PASS (both tests).

- [ ] **Step 6: Commit**

```bash
git add internal/ingest/orchestrator/
git commit -m "orchestrator: bootstrap-phase orchestration tests"
```

---

## Task 7: Recovery tests for `merging` and `steady_state` startup paths

**Files:**
- Create: `internal/ingest/orchestrator/recovery_test.go`

Crash-recovery is the trickiest part of this PR. These tests pre-populate the store at each non-bootstrap phase value and assert `Run` takes the right path.

- [ ] **Step 1: Write the recovery tests**

```go
package orchestrator

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

// TestRun_ResumeFromMerging_AdvancesToSteadyState verifies the
// crash-recovery path where a process died after writing
// phase=merging but before writing phase=steady_state. On restart,
// Run skips bootstrap entirely, runs the merge stub (no-op), writes
// phase=steady_state, and starts the steady-state consumer.
func TestRun_ResumeFromMerging_AdvancesToSteadyState(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	st, err := store.Open(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseMerging))

	relay := newFakeRelay(t, nil)
	verifier := newTestVerifier(t, relay.URL())

	o, err := New(Config{
		DataDir:    dataDir,
		Store:      st,
		RelayURL:   relay.URL(),
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Directory:  testIdentityDirectory(),
		Verifier:   verifier,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()

	// Wait until phase has been advanced to steady_state. The
	// steady-state consumer then runs forever; cancel after we
	// observe the transition.
	require.Eventually(t, func() bool {
		got, err := lifecycle.ReadPhase(st)
		return err == nil && got == lifecycle.PhaseSteadyState
	}, 5*time.Second, 20*time.Millisecond, "phase did not advance to steady_state")

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// TestRun_StartsCleanInSteadyState verifies that a process started
// against a data dir already at PhaseSteadyState skips bootstrap and
// merging entirely and runs the steady-state consumer until ctx is
// cancelled.
func TestRun_StartsCleanInSteadyState(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	st, err := store.Open(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseSteadyState))

	relay := newFakeRelay(t, nil)
	verifier := newTestVerifier(t, relay.URL())

	o, err := New(Config{
		DataDir:    dataDir,
		Store:      st,
		RelayURL:   relay.URL(),
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Directory:  testIdentityDirectory(),
		Verifier:   verifier,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	t.Cleanup(cancel)

	err = o.Run(ctx)
	// Steady-state consumer returns ctx.Err() (Canceled or DeadlineExceeded)
	// on clean shutdown.
	require.True(t, err == nil || err == context.DeadlineExceeded || err == context.Canceled,
		"unexpected error: %v", err)

	// Phase remains steady_state.
	got, err := lifecycle.ReadPhase(st)
	require.NoError(t, err)
	require.Equal(t, lifecycle.PhaseSteadyState, got)
}
```

- [ ] **Step 2: Run the recovery tests**

Run: `just test ./internal/ingest/orchestrator -run TestRun_`
Expected: PASS

- [ ] **Step 3: Run the full orchestrator package tests**

Run: `just test ./internal/ingest/orchestrator`
Expected: PASS (all tests).

- [ ] **Step 4: Commit**

```bash
git add internal/ingest/orchestrator/
git commit -m "orchestrator: recovery tests for merging and steady_state startup"
```

---

## Task 8: End-to-end happy-path test (bootstrap → merging → steady_state)

**Files:**
- Create: `internal/ingest/orchestrator/orchestrator_test.go`

This single test exercises the full lifecycle in one process: empty backfill → cutover → steady-state consumer running. It verifies the durable artifacts (phase, sealed live_segments, fresh active segment in `data/segments/`) at each transition.

- [ ] **Step 1: Write the end-to-end test**

```go
package orchestrator

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/lifecycle"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

// TestRun_EndToEnd_BootstrapToSteadyState walks the whole
// state machine in one Run. With a fake relay that returns zero
// DIDs, backfill drains immediately and the orchestrator transitions
// bootstrap → merging → steady_state, then runs the steady-state
// consumer until ctx is cancelled.
//
// Asserts:
//   - Phase progresses bootstrap → merging → steady_state.
//   - data/backfill/live_segments/ contains at least one sealed file.
//   - data/segments/ contains at least one active file (the
//     steady-state writer rolled forward from backfill's writer).
//   - Run returns ctx.Err() on cancel.
func TestRun_EndToEnd_BootstrapToSteadyState(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	st, err := store.Open(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	relay := newFakeRelay(t, nil)
	verifier := newTestVerifier(t, relay.URL())

	o, err := New(Config{
		DataDir:    dataDir,
		Store:      st,
		RelayURL:   relay.URL(),
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Directory:  testIdentityDirectory(),
		Verifier:   verifier,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()

	// Wait for the transition to steady_state.
	require.Eventually(t, func() bool {
		got, err := lifecycle.ReadPhase(st)
		return err == nil && got == lifecycle.PhaseSteadyState
	}, 10*time.Second, 50*time.Millisecond, "phase did not reach steady_state")

	// data/backfill/live_segments should have at least one sealed file.
	liveSegs, err := readSegFiles(filepath.Join(dataDir, "backfill", "live_segments"))
	require.NoError(t, err)
	require.NotEmpty(t, liveSegs, "expected at least one live_segments file")
	for _, p := range liveSegs {
		require.True(t, isSealed(t, p), "%s should be sealed", p)
	}

	// data/segments should have at least the active file the
	// steady-state writer opened (whether or not events have been
	// appended). Backfill produced no events because the relay
	// returned zero DIDs, so there's exactly one fresh active file.
	mainSegs, err := readSegFiles(filepath.Join(dataDir, "segments"))
	require.NoError(t, err)
	require.NotEmpty(t, mainSegs, "expected at least one main segments file")

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
```

- [ ] **Step 2: Run the test**

Run: `just test ./internal/ingest/orchestrator -run TestRun_EndToEnd`
Expected: PASS

- [ ] **Step 3: Run the full package test suite**

Run: `just test ./internal/ingest/orchestrator`
Expected: PASS (all tests).

- [ ] **Step 4: Run the package under `-race`**

Run: `just test-race ./internal/ingest/orchestrator`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/ingest/orchestrator/
git commit -m "orchestrator: end-to-end happy-path test"
```

---

## Task 9: Wire `cmd/jetstream/main.go` to use the orchestrator

**Files:**
- Modify: `cmd/jetstream/main.go`

`cmd/jetstream` shrinks substantially. The new shape:
1. Logger / tracing / metrics setup (unchanged).
2. Open the metadata store (unchanged).
3. Build verifier, identity directory, HTTP client (unchanged — these are shared across phases).
4. Construct the orchestrator with the above primitives + per-subsystem metrics.
5. Run server, orchestrator, and verifier-async-error drain under one errgroup.

The previous "phase=steady_state → fail" early return is removed; the orchestrator handles all phases. The previous bootstrap-phase write on a fresh data dir moves into the orchestrator's `Run`.

- [ ] **Step 1: Read the current `runServe` to find the lines to remove**

The current `runServe` (after the existing `metaStore` setup) does:

1. Reads phase, writes bootstrap on fresh dir, fails on steady_state.
2. Constructs `ingestWriter` for `data/segments`.
3. Constructs `verifier`, `directory`, `xrpcClient`, `stateStore`.
4. Constructs `liveConsumer` for `data/backfill/live_segments`.
5. Adds three goroutines to errgroup: server, backfill.Run, liveConsumer.Run, verifier drain.

The new version keeps verifier construction (because the async-error drain remains a sibling) and replaces (1), (2), (4), and the backfill+live errgroup goroutines with the orchestrator.

- [ ] **Step 2: Replace `runServe` body in `cmd/jetstream/main.go`**

Find the section starting at:

```go
	phase, err := lifecycle.ReadPhase(metaStore)
```

and ending after the closing `}` of:

```go
	g.Go(func() error {
		for {
			select {
			case <-gctx.Done():
				return nil
			case err, ok := <-verifier.AsyncErrors():
				if !ok {
					return nil
				}
				logger.Warn("verifier async error", "err", err)
			}
		}
	})
```

Replace that entire block with:

```go
	// Verifier setup (shared across phases). The verifier itself is
	// owned by the orchestrator's per-phase live consumers, but we
	// construct it here because its async-error drain is a sibling
	// goroutine in the top-level errgroup — it's a process-wide
	// observability concern.
	relayHTTPURL, err := live.DeriveRelayHTTPURL(cmd.String("relay-url"))
	if err != nil {
		return fmt.Errorf("serve: derive relay HTTP URL: %w", err)
	}

	xrpcClient := &xrpc.Client{
		Host:       relayHTTPURL,
		HTTPClient: gt.Some(jttp.New(xrpc.BulkDownloadOpts()...)),
	}

	directory := &identity.Directory{
		Resolver:               &identity.DefaultResolver{},
		Cache:                  identcache.New(metaStore, identcache.DefaultTTL),
		SkipHandleVerification: true,
	}

	stateStore := syncstate.New(metaStore)
	syncClient := atmossync.NewClient(atmossync.Options{Client: xrpcClient})

	verifier, err := atmossync.NewVerifier(atmossync.VerifierOptions{
		Directory:  directory,
		StateStore: stateStore,
		SyncClient: gt.Some(syncClient),
		OnVerificationFailure: gt.Some(func(did atmos.DID, vErr error) {
			logger.Warn("verifier failure",
				"did", did,
				"err", vErr,
			)
		}),
	})
	if err != nil {
		return fmt.Errorf("serve: build verifier: %w", err)
	}
	defer func() {
		if cerr := verifier.Close(); cerr != nil {
			logger.Error("verifier close", "err", cerr)
		}
	}()

	// The orchestrator owns all ingestion-lifecycle subsystems
	// (backfill engine, bootstrap-time live consumer, steady-state
	// live consumer). cmd/jetstream is no longer phase-aware.
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
	})
	if err != nil {
		return fmt.Errorf("serve: build orchestrator: %w", err)
	}

	srv := server.New(server.Config{
		PublicAddr:      cmd.String("addr"),
		DebugAddr:       cmd.String("debug-addr"),
		ShutdownTimeout: cmd.Duration("shutdown-timeout"),
	}, logger, metrics)

	runCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	g, gctx := errgroup.WithContext(runCtx)

	g.Go(func() error {
		return srv.Run(gctx)
	})

	g.Go(func() error {
		return orch.Run(gctx)
	})

	// Verifier async-error drain. Verification failures are
	// diagnostic, not fatal — they typically reflect adversarial or
	// malformed PDS input, which is invalid user data, not a
	// jetstream bug. We warn-log and the OnVerificationFailure hook
	// fires for operator visibility, but never crash.
	g.Go(func() error {
		for {
			select {
			case <-gctx.Done():
				return nil
			case err, ok := <-verifier.AsyncErrors():
				if !ok {
					return nil
				}
				logger.Warn("verifier async error", "err", err)
			}
		}
	})
```

- [ ] **Step 3: Update the imports in `cmd/jetstream/main.go`**

Add to the import block:

```go
	"github.com/bluesky-social/jetstream-v2/internal/ingest/orchestrator"
```

The existing `internal/ingest`, `internal/ingest/backfill`, and `internal/ingest/live` imports stay because we still pass their `*Metrics` types and use `live.DeriveRelayHTTPURL`.

The `lifecycle` import can be removed — `cmd/jetstream` no longer reads or writes the phase; the orchestrator does.

- [ ] **Step 4: Build to confirm it compiles**

Run: `just build`
Expected: Builds without errors. Binary at `bin/jetstream`.

- [ ] **Step 5: Run lint**

Run: `just lint`
Expected: 0 issues (any unused imports surface here).

- [ ] **Step 6: Commit**

```bash
git add cmd/jetstream/main.go
git commit -m "cmd/jetstream: delegate to orchestrator for ingestion lifecycle"
```

---

## Task 10: Update `cmd/jetstream/serve_test.go` to reflect the new behavior

**Files:**
- Modify: `cmd/jetstream/serve_test.go`

The existing `TestServe_RefusesSteadyStatePhase` asserted the old "fail loudly on steady_state" behavior. The new behavior is "start cleanly in steady_state". We replace that test with one that verifies the new path, and add a similar test for the merging phase.

- [ ] **Step 1: Replace `TestServe_RefusesSteadyStatePhase` with `TestServe_StartsInSteadyStatePhase`**

In `cmd/jetstream/serve_test.go`, replace the entire `TestServe_RefusesSteadyStatePhase` function with:

```go
// TestServe_StartsInSteadyStatePhase pins the steady-state startup
// path: a data dir already at PhaseSteadyState skips bootstrap and
// merge, runs the steady-state consumer, and shuts down cleanly on
// ctx cancel.
func TestServe_StartsInSteadyStatePhase(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()

	// Pre-populate phase=steady_state.
	{
		s, err := store.Open(dataDir)
		require.NoError(t, err)
		require.NoError(t, lifecycle.WritePhase(s, lifecycle.PhaseSteadyState))
		require.NoError(t, s.Close())
	}

	// subscribeReposHit is closed the first time the steady-state
	// live consumer dials the relay. That's our deterministic
	// "serve made it to phase=steady_state work" signal.
	subscribeReposHit := make(chan struct{})
	var hitOnce sync.Once
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/com.atproto.sync.subscribeRepos") {
			hitOnce.Do(func() { close(subscribeReposHit) })
			conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
			if err != nil {
				return
			}
			defer func() { _ = conn.CloseNow() }()
			<-r.Context().Done()
			return
		}
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

	select {
	case <-subscribeReposHit:
	case err := <-done:
		t.Fatalf("serve exited before reaching the relay: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("steady-state consumer never reached the relay")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve exited with unexpected error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not shut down")
	}

	// Phase should still be steady_state.
	s, err := store.Open(dataDir)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	p, err := lifecycle.ReadPhase(s)
	require.NoError(t, err)
	require.Equal(t, lifecycle.PhaseSteadyState, p)
}
```

- [ ] **Step 2: Add a similar test for the merging phase**

Append to `cmd/jetstream/serve_test.go`:

```go
// TestServe_AdvancesFromMergingToSteadyState pins the crash-recovery
// path: a data dir already at PhaseMerging runs the merge stub
// (no-op for now), writes phase=steady_state, and starts the
// steady-state consumer.
func TestServe_AdvancesFromMergingToSteadyState(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	{
		s, err := store.Open(dataDir)
		require.NoError(t, err)
		require.NoError(t, lifecycle.WritePhase(s, lifecycle.PhaseMerging))
		require.NoError(t, s.Close())
	}

	subscribeReposHit := make(chan struct{})
	var hitOnce sync.Once
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/com.atproto.sync.subscribeRepos") {
			hitOnce.Do(func() { close(subscribeReposHit) })
			conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
			if err != nil {
				return
			}
			defer func() { _ = conn.CloseNow() }()
			<-r.Context().Done()
			return
		}
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

	select {
	case <-subscribeReposHit:
	case err := <-done:
		t.Fatalf("serve exited before reaching the relay: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("orchestrator never advanced merging->steady_state")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve exited with unexpected error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not shut down")
	}

	s, err := store.Open(dataDir)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	p, err := lifecycle.ReadPhase(s)
	require.NoError(t, err)
	require.Equal(t, lifecycle.PhaseSteadyState, p)
}
```

- [ ] **Step 3: Run the cmd/jetstream tests**

Run: `just test ./cmd/jetstream`
Expected: PASS (all tests, including the existing two for fresh-dir bootstrap and complete-rows skip).

- [ ] **Step 4: Commit**

```bash
git add cmd/jetstream/serve_test.go
git commit -m "cmd/jetstream: tests for steady_state and merging startup paths"
```

---

## Task 11: Full module verification

- [ ] **Step 1: Run the full test suite under `-short`**

Run: `just test`
Expected: PASS, all packages.

- [ ] **Step 2: Run the full test suite under `-race`**

Run: `just test-race`
Expected: PASS, all packages, no race warnings.

- [ ] **Step 3: Run the full lint**

Run: `just lint`
Expected: 0 issues.

- [ ] **Step 4: Build the binary**

Run: `just build`
Expected: `bin/jetstream` produced, no errors.

- [ ] **Step 5: If any of the above failed, fix and re-commit**

For each failure:
- Read the failure output carefully to identify the root cause.
- Fix the underlying issue rather than working around it.
- Re-run the failing command to confirm.
- Commit the fix as a separate commit (`git commit -m "..."`).

If all pass, this task ends without a new commit.

---

## Self-Review Notes

This plan covers, in order:

1. **Spec §3 (Lifecycle Phases)** → Task 1.
2. **Spec §8.2 (`SealActiveAndClose`)** → Task 2.
3. **Spec §7.3 (Public API), §7.1 (Package layout), §10 (metrics)** → Task 3.
4. **Spec §4 State 5 (merge stub), State 6 (write steady-state phase), State 7 (steady-state run)** → Task 4.
5. **Spec §4 States 0–4 (bootstrap and cutover trigger)** → Task 5.
6. **Spec §11.4 (bootstrap-phase tests)** → Task 6.
7. **Spec §6 (crash recovery), §11.3 (recovery tests)** → Task 7.
8. **Spec §11.2 (end-to-end happy path)** → Task 8.
9. **Spec §8.3 (`cmd/jetstream` rewiring)** → Task 9.
10. **Spec §11.5 (cmd/jetstream test updates)** → Task 10.
11. **Final verification** → Task 11.

Cross-checks performed:

- All exported types/functions referenced in later tasks are defined in earlier tasks (`Config`, `Orchestrator`, `New`, `Run`, `Metrics`, `NewMetrics`, `PhaseGauge*`, `ErrInvalidConfig`, `SealActiveAndClose`, `PhaseMerging`).
- The fallthrough-based `Run` switch in Task 4 correctly handles all three startup phases and is consistent with the state-machine table in spec §4 and §6.
- No placeholder text. Every code block is concrete and compilable in context.
- Per-task commits avoid Co-Authored-By trailers, matching the convention in prior commits and prior plan.
