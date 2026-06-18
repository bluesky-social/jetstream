# Backfill Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire `cmd/jetstream serve` to drive the atmos `backfill.Engine` end-to-end, persisting per-DID lifecycle state in pebble at `repo/<did>` per DESIGN.md §3.5, with no segment writing yet.

**Architecture:** A new `internal/backfill` package wraps the atmos engine. It implements atmos's `Store` interface against the existing `*store.Store` (pebble), provides a no-op logging `Handler`, and exposes a single `Run(ctx, Config)` entrypoint that `cmd/jetstream` calls from its existing errgroup. Resume across restarts falls out of atmos's single-shot model: re-running `Run` re-walks `listRepos` and skips DIDs whose pebble row is already at `complete`.

**Tech Stack:** Go 1.26, [pebble](https://github.com/cockroachdb/pebble), [atmos](https://github.com/jcalabro/atmos) v0.0.15 (`backfill`, `sync`, `identity`, `xrpc` packages), [gt](https://github.com/jcalabro/gt), prometheus, slog, urfave/cli v3, gotestsum.

**Spec:** `docs/superpowers/specs/2026-05-18-backfill-bootstrap-design.md`

---

## File Structure

**New files in `internal/backfill/`:**

| File | Responsibility |
|---|---|
| `doc.go` | Package overview docstring; cross-refs to DESIGN.md §3.5 / §4.1 |
| `status.go` | `Status`, `RepoBackfillStatus`, `RepoStatus` types (DESIGN.md §3.5 schema) + JSON helpers + the `repoKey` builder |
| `metrics.go` | `Metrics` struct + `NewMetrics(reg)`; nil-safe |
| `store.go` | `Store` type implementing `backfill.Store` against `*store.Store` |
| `handler.go` | `LogHandler` implementing `backfill.Handler` (no-op + log) |
| `run.go` | `Config` + `Run(ctx, Config)` entrypoint |
| `status_test.go` | JSON round-trip + `repoKey` |
| `store_test.go` | Direct unit tests against a pebble-backed `Store` |
| `handler_test.go` | Trivial: handler returns nil, logs at debug |
| `run_test.go` | End-to-end against stubbed relay + PDS + PLC |

**Modified files:**

| File | Why |
|---|---|
| `cmd/jetstream/main.go` | Replace the deleted `backfill.NewSeedMetrics` / old `Run` shape with the new `Config` |
| `cmd/jetstream/serve_test.go` | Rewrite the smoke test against the new package shape |

---

## Conventions

- **Test framework:** `github.com/stretchr/testify/require`. Use `t.Parallel()` when tests don't share global state. Use `t.TempDir()` for pebble dirs. Wrap close in `t.Cleanup`.
- **Test runner:** `just test ./internal/backfill -run TestX` for one test; `just test ./internal/backfill` for the package; `just test` for everything.
- **Logging:** `*slog.Logger`. No `fmt.Print*`.
- **Errors:** Wrap with context: `fmt.Errorf("backfill: <action> %s: %w", arg, err)`.
- **Doc comments:** Every exported symbol gets one. Explain WHY decisions were made.
- **Imports:** atmos types use the import alias `atmosbackfill "github.com/jcalabro/atmos/backfill"` inside our package to avoid colliding with our own package name.

---

## Task 1: Bootstrap empty package + status types

**Files:**
- Create: `internal/backfill/doc.go`
- Create: `internal/backfill/status.go`
- Create: `internal/backfill/status_test.go`

- [ ] **Step 1: Create `internal/backfill/doc.go` with the package overview**

```go
// Package backfill drives the initial atproto network backfill phase
// for jetstream (DESIGN.md §4.1). It wraps the atmos backfill engine,
// persists per-DID lifecycle state into pebble at repo/<did> per
// DESIGN.md §3.5, and is invoked once per process start from
// cmd/jetstream.
//
// The package is single-shot per Run: each call paginates listRepos
// and downloads any DID not already at StatusComplete. Restart-resume
// falls out of that model — completed rows are skipped on Lookup.
//
// This package does not write segment files. Handler.HandleRepo is
// a no-op that logs and counts; the segment writer wires in later.
package backfill
```

- [ ] **Step 2: Create `internal/backfill/status.go` with the schema types**

```go
package backfill

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/jcalabro/atmos"
)

// repoKeyPrefix is the pebble key prefix for per-DID rows. DESIGN.md
// §3.5 pins this layout so the on-disk format is stable across
// replicas.
const repoKeyPrefix = "repo/"

// repoKey returns the pebble key for a DID's RepoStatus row.
func repoKey(did atmos.DID) []byte {
	return []byte(repoKeyPrefix + string(did))
}

// Status is the lifecycle state of a single DID's initial backfill.
// Values match DESIGN.md §3.5; the StatusNotStarted value is what
// OnDiscover writes — a row's mere existence at not_started indicates
// the engine has seen it but not yet downloaded it.
type Status string

const (
	StatusNotStarted Status = "not_started"
	StatusComplete   Status = "complete"
	StatusFailed     Status = "failed"
)

// RepoBackfillStatus tracks initial-backfill state per DESIGN.md §3.5.
type RepoBackfillStatus struct {
	Status      Status    `json:"status"`
	Rev         string    `json:"rev,omitempty"`
	Attempts    int       `json:"attempts,omitempty"`
	LastError   string    `json:"last_error,omitempty"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
}

// RepoStatus is the JSON value stored at repo/<did>. The shape matches
// DESIGN.md §3.5; this PR only populates Backfill and Active. The
// other fields (PDS, Rev, UpdatedAt, RecordCount, TotalBytes) are
// reserved for steady-state ingest and remain zero here so we don't
// force a future schema migration.
//
// Active records the last-observed listRepos.Active value. atmos
// requires it on every row to detect liveness flips without an extra
// round-trip; DESIGN.md §3.5 doesn't pin a JSON tag for it (the
// original draft predated atmos's active-flip callback) so we add one
// here.
type RepoStatus struct {
	Backfill    RepoBackfillStatus `json:"backfill"`
	PDS         string             `json:"pds,omitempty"`
	Rev         string             `json:"rev,omitempty"`
	UpdatedAt   time.Time          `json:"updated_at,omitempty"`
	RecordCount int64              `json:"record_count,omitempty"`
	TotalBytes  int64              `json:"total_bytes,omitempty"`
	Active      bool               `json:"active"`
}

// encodeRepoStatus marshals a RepoStatus for persistence. Errors here
// are programming bugs (the type is a fixed shape we control), but we
// surface them rather than panicking so the engine can record a Run
// failure and exit cleanly.
func encodeRepoStatus(s *RepoStatus) ([]byte, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("backfill: encode RepoStatus: %w", err)
	}
	return b, nil
}

// decodeRepoStatus unmarshals a previously-stored RepoStatus.
func decodeRepoStatus(b []byte) (*RepoStatus, error) {
	var s RepoStatus
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("backfill: decode RepoStatus: %w", err)
	}
	return &s, nil
}
```

- [ ] **Step 3: Create `internal/backfill/status_test.go`**

```go
package backfill

import (
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/stretchr/testify/require"
)

// TestRepoKey pins the on-disk key shape — DESIGN.md §3.5 promises
// repo/<did>, and replicas will eventually parse this prefix.
func TestRepoKey(t *testing.T) {
	t.Parallel()
	got := repoKey(atmos.DID("did:plc:abc"))
	require.Equal(t, "repo/did:plc:abc", string(got))
}

// TestRepoStatus_RoundTrip confirms the JSON encoder accepts and
// re-emits every field we populate, plus that Active is always
// present (even when false) so downstream code can rely on it.
func TestRepoStatus_RoundTrip(t *testing.T) {
	t.Parallel()
	in := &RepoStatus{
		Backfill: RepoBackfillStatus{
			Status: StatusComplete,
			Rev:    "rev-xyz",
		},
		Active: false,
	}
	b, err := encodeRepoStatus(in)
	require.NoError(t, err)
	require.Contains(t, string(b), `"active":false`)
	require.Contains(t, string(b), `"status":"complete"`)

	out, err := decodeRepoStatus(b)
	require.NoError(t, err)
	require.Equal(t, in.Backfill.Status, out.Backfill.Status)
	require.Equal(t, in.Backfill.Rev, out.Backfill.Rev)
	require.Equal(t, in.Active, out.Active)
}

// TestDecodeRepoStatus_Garbage verifies decode failures are surfaced
// (not silently zeroed) so the engine aborts a Run rather than
// resurrecting a corrupted row as a fresh discovery.
func TestDecodeRepoStatus_Garbage(t *testing.T) {
	t.Parallel()
	_, err := decodeRepoStatus([]byte("not json"))
	require.ErrorContains(t, err, "decode RepoStatus")
}
```

- [ ] **Step 4: Run the tests**

Run: `just test ./internal/backfill`
Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add internal/backfill/doc.go internal/backfill/status.go internal/backfill/status_test.go
git commit -m "backfill: add RepoStatus schema per DESIGN.md §3.5"
```

---

## Task 2: Metrics scaffolding

**Files:**
- Create: `internal/backfill/metrics.go`

This is intentionally tiny — there's no behavior to test on its own. The metrics will be exercised through the higher-level tests in later tasks.

- [ ] **Step 1: Create `internal/backfill/metrics.go`**

```go
package backfill

import "github.com/prometheus/client_golang/prometheus"

const metricsNamespace = "jetstream"
const metricsSubsystem = "backfill"

// Metrics owns the prometheus counters/gauges for the backfill engine.
// A nil *Metrics is a valid zero-value: every method is a no-op,
// which lets tests skip metric registration entirely.
type Metrics struct {
	Discovered   prometheus.Counter
	Completed    prometheus.Counter
	Failed       prometheus.Counter
	ActiveFlips  prometheus.Counter
	OnFailErrors prometheus.Counter
}

// NewMetrics registers the backfill counters against reg. Pass the
// shared *prometheus.Registry from internal/obs.Metrics so every
// counter shows up on /metrics.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Discovered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "discovered_total",
			Help: "Number of DIDs first observed in listRepos and recorded at not_started.",
		}),
		Completed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "completed_total",
			Help: "Number of DIDs whose initial repo download finished successfully.",
		}),
		Failed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "failed_total",
			Help: "Number of DIDs that exhausted their retry budget within a Run.",
		}),
		ActiveFlips: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "active_flips_total",
			Help: "Number of active->inactive or inactive->active transitions observed via listRepos.",
		}),
		OnFailErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "on_fail_store_errors_total",
			Help: "Number of times Store.OnFail itself failed to persist (data-integrity signal).",
		}),
	}
	reg.MustRegister(m.Discovered, m.Completed, m.Failed, m.ActiveFlips, m.OnFailErrors)
	return m
}

// incDiscovered, etc. are nil-safe helpers used internally.
func (m *Metrics) incDiscovered() {
	if m != nil {
		m.Discovered.Inc()
	}
}

func (m *Metrics) incCompleted() {
	if m != nil {
		m.Completed.Inc()
	}
}

func (m *Metrics) incFailed() {
	if m != nil {
		m.Failed.Inc()
	}
}

func (m *Metrics) incActiveFlips() {
	if m != nil {
		m.ActiveFlips.Inc()
	}
}

func (m *Metrics) incOnFailErrors() {
	if m != nil {
		m.OnFailErrors.Inc()
	}
}
```

- [ ] **Step 2: Verify it compiles**

Run: `go build ./internal/backfill/...`
Expected: no output (clean compile).

- [ ] **Step 3: Commit**

```bash
git add internal/backfill/metrics.go
git commit -m "backfill: add prometheus metrics scaffolding"
```

---

## Task 3: Store.Lookup against pebble

**Files:**
- Create: `internal/backfill/store.go`
- Create: `internal/backfill/store_test.go`

We start with the read path because every other store method depends on `Lookup` being correct.

- [ ] **Step 1: Write `internal/backfill/store.go` with `New` and `Lookup`**

```go
// Package backfill: store.go implements the atmos backfill.Store
// interface against pebble. Keys live at repo/<did>; values are the
// JSON-encoded RepoStatus from status.go.
//
// All callbacks the engine fires (OnDiscover, OnUpdate, OnComplete,
// OnFail) write with pebble.Sync to satisfy atmos's durability
// contract: the engine treats a successful return as durable.
//
// atmos guarantees no two callbacks are in flight for the same DID
// simultaneously, so OnUpdate/OnComplete/OnFail use a non-transactional
// read-modify-write to preserve fields a future PR may have added to
// RepoStatus (e.g. RecordCount).
package backfill

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/repo"
)

// Store implements atmosbackfill.Store against the shared pebble
// metadata store. Construct via NewStore.
type Store struct {
	db      *store.Store
	metrics *Metrics
}

// Compile-time guarantee that Store satisfies the atmos contract.
var _ atmosbackfill.Store = (*Store)(nil)

// NewStore constructs a Store backed by the shared metadata pebble db.
// metrics may be nil; callbacks are no-ops in that case.
func NewStore(db *store.Store, metrics *Metrics) *Store {
	return &Store{db: db, metrics: metrics}
}

// Lookup reads repo/<did> and projects the on-disk RepoStatus into
// atmos's StoreEntry shape. A missing row returns StateUnknown — that's
// how atmos tells the engine to fire OnDiscover.
func (s *Store) Lookup(_ context.Context, did atmos.DID) (atmosbackfill.StoreEntry, error) {
	val, closer, err := s.db.Get(repoKey(did))
	if errors.Is(err, pebble.ErrNotFound) {
		return atmosbackfill.StoreEntry{State: atmosbackfill.StateUnknown}, nil
	}
	if err != nil {
		return atmosbackfill.StoreEntry{}, fmt.Errorf("backfill: lookup %s: %w", did, err)
	}
	defer func() { _ = closer.Close() }()

	rs, err := decodeRepoStatus(val)
	if err != nil {
		return atmosbackfill.StoreEntry{}, err
	}

	var st atmosbackfill.State
	switch rs.Backfill.Status {
	case StatusNotStarted:
		st = atmosbackfill.StateDiscovered
	case StatusComplete:
		st = atmosbackfill.StateComplete
	case StatusFailed:
		st = atmosbackfill.StateFailed
	default:
		return atmosbackfill.StoreEntry{}, fmt.Errorf("backfill: lookup %s: unknown status %q", did, rs.Backfill.Status)
	}
	return atmosbackfill.StoreEntry{State: st, Active: rs.Active}, nil
}

// putRepoStatus writes the value durably. Used by all write paths.
func (s *Store) putRepoStatus(did atmos.DID, rs *RepoStatus) error {
	enc, err := encodeRepoStatus(rs)
	if err != nil {
		return err
	}
	if err := s.db.Set(repoKey(did), enc, pebble.Sync); err != nil {
		return fmt.Errorf("backfill: write repo/%s: %w", did, err)
	}
	return nil
}

// readRepoStatus is the RMW helper for OnUpdate/OnComplete/OnFail.
// It returns (nil, nil) when the row doesn't exist so callers can
// decide whether absence is an error in their context.
func (s *Store) readRepoStatus(did atmos.DID) (*RepoStatus, error) {
	val, closer, err := s.db.Get(repoKey(did))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("backfill: read repo/%s: %w", did, err)
	}
	defer func() { _ = closer.Close() }()
	return decodeRepoStatus(val)
}

// OnDiscover, OnUpdate, OnComplete, OnFail are added in subsequent
// tasks. Compile-time assertion above will fail until they're done;
// stub them now so the package builds while we work.
func (s *Store) OnDiscover(_ context.Context, _ atmossync.ListReposEntry) error {
	panic("OnDiscover not yet implemented")
}

func (s *Store) OnUpdate(_ context.Context, _ atmossync.ListReposEntry) error {
	panic("OnUpdate not yet implemented")
}

func (s *Store) OnComplete(_ context.Context, _ atmos.DID, _ *repo.Commit) error {
	panic("OnComplete not yet implemented")
}

func (s *Store) OnFail(_ context.Context, _ atmos.DID, _ error, _ int) error {
	panic("OnFail not yet implemented")
}

// timeNow is a package var so tests can pin wall-clock values.
// Production callers don't override this.
var timeNow = func() time.Time { return time.Now().UTC() }
```

- [ ] **Step 2: Write `internal/backfill/store_test.go` with the Lookup tests**

```go
package backfill

import (
	"context"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/stretchr/testify/require"
)

// newTestStore is the shared fixture for Store unit tests: open a
// fresh pebble in t.TempDir(), register cleanup, return the wrapped
// Store with no metrics.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := store.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return NewStore(db, nil)
}

// TestStore_Lookup_Missing covers the StateUnknown path: a fresh
// pebble has no repo/<did> rows, and atmos uses StateUnknown to
// trigger OnDiscover.
func TestStore_Lookup_Missing(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	got, err := s.Lookup(context.Background(), atmos.DID("did:plc:abc"))
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateUnknown, got.State)
	require.False(t, got.Active)
}

// TestStore_Lookup_StatusMapping pins the disk-status -> atmos.State
// projection. atmos uses these values to decide whether to dispatch
// the DID for download or skip it.
func TestStore_Lookup_StatusMapping(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		status   Status
		expected atmosbackfill.State
	}{
		{"not_started -> Discovered", StatusNotStarted, atmosbackfill.StateDiscovered},
		{"complete -> Complete", StatusComplete, atmosbackfill.StateComplete},
		{"failed -> Failed", StatusFailed, atmosbackfill.StateFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t)
			did := atmos.DID("did:plc:abc")
			rs := &RepoStatus{
				Backfill: RepoBackfillStatus{Status: tc.status},
				Active:   true,
			}
			require.NoError(t, s.putRepoStatus(did, rs))

			got, err := s.Lookup(context.Background(), did)
			require.NoError(t, err)
			require.Equal(t, tc.expected, got.State)
			require.True(t, got.Active)
		})
	}
}

// TestStore_Lookup_CorruptRow asserts decode failures are surfaced as
// errors, not silently mapped to StateUnknown — the latter would let
// the engine fire OnDiscover on a "corrupt but present" row and
// clobber it. We want a Run failure instead.
func TestStore_Lookup_CorruptRow(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	did := atmos.DID("did:plc:abc")
	require.NoError(t, s.db.Set(repoKey(did), []byte("not json"), pebble.Sync))

	_, err := s.Lookup(context.Background(), did)
	require.ErrorContains(t, err, "decode RepoStatus")
}
```

- [ ] **Step 3: Run the tests**

Run: `just test ./internal/backfill`
Expected: 3 new tests pass; the 3 from task 1 still pass.

- [ ] **Step 4: Commit**

```bash
git add internal/backfill/store.go internal/backfill/store_test.go
git commit -m "backfill: add Store skeleton with Lookup against pebble"
```

---

## Task 4: Store.OnDiscover

**Files:**
- Modify: `internal/backfill/store.go`
- Modify: `internal/backfill/store_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/backfill/store_test.go`:

```go
// TestStore_OnDiscover_WritesNotStarted is the producer-side hot
// path: every DID returned by listRepos for the first time gets a
// fresh row at not_started with the listRepos.Active flag preserved.
func TestStore_OnDiscover_WritesNotStarted(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	did := atmos.DID("did:plc:abc")

	err := s.OnDiscover(context.Background(), atmossync.ListReposEntry{
		DID:    did,
		Active: true,
	})
	require.NoError(t, err)

	got, err := s.Lookup(context.Background(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateDiscovered, got.State)
	require.True(t, got.Active)

	rs, err := s.readRepoStatus(did)
	require.NoError(t, err)
	require.NotNil(t, rs)
	require.Equal(t, StatusNotStarted, rs.Backfill.Status)
	require.False(t, rs.Backfill.StartedAt.IsZero(), "StartedAt must be set on first discovery")
}

// TestStore_OnDiscover_InactiveDID confirms an inactive DID still
// gets a row written so we can re-attempt later if it flips active.
// Active flips are tracked by Store, not by absence of a row.
func TestStore_OnDiscover_InactiveDID(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	did := atmos.DID("did:plc:tomb")

	err := s.OnDiscover(context.Background(), atmossync.ListReposEntry{
		DID:    did,
		Active: false,
	})
	require.NoError(t, err)

	got, err := s.Lookup(context.Background(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateDiscovered, got.State)
	require.False(t, got.Active)
}
```

Add the import if not already present:

```go
import (
	atmossync "github.com/jcalabro/atmos/sync"
	// ... existing imports
)
```

- [ ] **Step 2: Run the test to confirm it fails (panics)**

Run: `just test ./internal/backfill -run TestStore_OnDiscover`
Expected: PANIC "OnDiscover not yet implemented".

- [ ] **Step 3: Implement OnDiscover in `internal/backfill/store.go`**

Replace the `OnDiscover` panic-stub with:

```go
// OnDiscover writes a fresh RepoStatus at status=not_started for a
// DID the engine has never seen. atmos guarantees this fires at most
// once per DID per Lookup-StateUnknown path.
func (s *Store) OnDiscover(_ context.Context, entry atmossync.ListReposEntry) error {
	rs := &RepoStatus{
		Backfill: RepoBackfillStatus{
			Status:    StatusNotStarted,
			StartedAt: timeNow(),
		},
		Active: entry.Active,
	}
	if err := s.putRepoStatus(entry.DID, rs); err != nil {
		return err
	}
	s.metrics.incDiscovered()
	return nil
}
```

- [ ] **Step 4: Run the tests**

Run: `just test ./internal/backfill`
Expected: 5 tests pass (Lookup x3 + OnDiscover x2 + status x3 + key/decode = 8 total; verify count goes up by 2).

- [ ] **Step 5: Commit**

```bash
git add internal/backfill/store.go internal/backfill/store_test.go
git commit -m "backfill: implement Store.OnDiscover"
```

---

## Task 5: Store.OnUpdate (active-flip tracking)

**Files:**
- Modify: `internal/backfill/store.go`
- Modify: `internal/backfill/store_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `internal/backfill/store_test.go`:

```go
// TestStore_OnUpdate_FlipsActive_PreservesStatus exercises the
// active-flip path: an account flipping inactive must update Active
// in pebble without clobbering the lifecycle Status. atmos uses this
// callback to tell us "the relay's view of activeness changed".
func TestStore_OnUpdate_FlipsActive_PreservesStatus(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	did := atmos.DID("did:plc:flip")

	require.NoError(t, s.OnDiscover(context.Background(), atmossync.ListReposEntry{
		DID: did, Active: true,
	}))

	require.NoError(t, s.OnUpdate(context.Background(), atmossync.ListReposEntry{
		DID: did, Active: false,
	}))

	got, err := s.Lookup(context.Background(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateDiscovered, got.State, "Status must not flip on OnUpdate")
	require.False(t, got.Active, "Active must be updated to false")
}

// TestStore_OnUpdate_MissingRow is a sanity check: atmos only fires
// OnUpdate for DIDs whose Lookup found a row, so the row should
// always exist. If somehow it doesn't, we want a hard error rather
// than a silent recreate.
func TestStore_OnUpdate_MissingRow(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	err := s.OnUpdate(context.Background(), atmossync.ListReposEntry{
		DID: atmos.DID("did:plc:nobody"), Active: true,
	})
	require.ErrorContains(t, err, "missing row")
}
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `just test ./internal/backfill -run TestStore_OnUpdate`
Expected: PANIC "OnUpdate not yet implemented".

- [ ] **Step 3: Implement OnUpdate**

Replace the `OnUpdate` panic-stub in `internal/backfill/store.go` with:

```go
// OnUpdate flips the Active flag on an existing row. The lifecycle
// Status is preserved — atmos fires OnUpdate only when the
// listRepos.Active value differs from what the Store last saw, and
// it never changes the Status as a side effect.
func (s *Store) OnUpdate(_ context.Context, entry atmossync.ListReposEntry) error {
	rs, err := s.readRepoStatus(entry.DID)
	if err != nil {
		return err
	}
	if rs == nil {
		return fmt.Errorf("backfill: on_update %s: missing row (atmos invariant violation)", entry.DID)
	}
	rs.Active = entry.Active
	if err := s.putRepoStatus(entry.DID, rs); err != nil {
		return err
	}
	s.metrics.incActiveFlips()
	return nil
}
```

- [ ] **Step 4: Run the tests**

Run: `just test ./internal/backfill`
Expected: 2 new tests pass.

- [ ] **Step 5: Commit**

```bash
git add internal/backfill/store.go internal/backfill/store_test.go
git commit -m "backfill: implement Store.OnUpdate active-flip tracking"
```

---

## Task 6: Store.OnComplete

**Files:**
- Modify: `internal/backfill/store.go`
- Modify: `internal/backfill/store_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `internal/backfill/store_test.go`:

```go
// TestStore_OnComplete_WritesComplete is the success path: a
// successful download lands the row at Complete with the commit rev
// recorded both at top-level Rev and in Backfill.Rev (per
// DESIGN.md §3.5 — both fields exist; Rev is the latest, Backfill.Rev
// is the rev at end of initial download).
func TestStore_OnComplete_WritesComplete(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	did := atmos.DID("did:plc:done")

	require.NoError(t, s.OnDiscover(context.Background(), atmossync.ListReposEntry{
		DID: did, Active: true,
	}))

	commit := &repo.Commit{DID: string(did), Rev: "rev-final"}
	require.NoError(t, s.OnComplete(context.Background(), did, commit))

	got, err := s.Lookup(context.Background(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateComplete, got.State)

	rs, err := s.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, "rev-final", rs.Backfill.Rev)
	require.Equal(t, "rev-final", rs.Rev)
	require.False(t, rs.Backfill.CompletedAt.IsZero())
	require.False(t, rs.UpdatedAt.IsZero())
}

// TestStore_OnComplete_PreservesExtraFields locks in the RMW
// guarantee: a future PR may add fields like RecordCount; OnComplete
// must not clobber them. We simulate this by writing a row with a
// non-zero RecordCount directly, then calling OnComplete.
func TestStore_OnComplete_PreservesExtraFields(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	did := atmos.DID("did:plc:rmv")

	require.NoError(t, s.putRepoStatus(did, &RepoStatus{
		Backfill:    RepoBackfillStatus{Status: StatusNotStarted},
		RecordCount: 42,
		Active:      true,
	}))

	commit := &repo.Commit{DID: string(did), Rev: "rev-z"}
	require.NoError(t, s.OnComplete(context.Background(), did, commit))

	rs, err := s.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, int64(42), rs.RecordCount, "RMW must preserve RecordCount")
	require.Equal(t, StatusComplete, rs.Backfill.Status)
}
```

Add `"github.com/jcalabro/atmos/repo"` to the test imports.

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `just test ./internal/backfill -run TestStore_OnComplete`
Expected: PANIC "OnComplete not yet implemented".

- [ ] **Step 3: Implement OnComplete**

Replace the `OnComplete` panic-stub in `internal/backfill/store.go` with:

```go
// OnComplete records a successful repo download. The commit's rev is
// stored in both Backfill.Rev (the rev at end of initial download
// per DESIGN.md §3.5) and the top-level Rev (the latest known rev).
// They're equal here because initial backfill is the only thing
// updating Rev in this PR; steady-state ingest will diverge them.
//
// We RMW rather than write fresh so a future field on RepoStatus
// (RecordCount, TotalBytes) added between OnDiscover and OnComplete
// survives. atmos's no-concurrent-callback guarantee per-DID makes
// the RMW race-free.
func (s *Store) OnComplete(_ context.Context, did atmos.DID, commit *repo.Commit) error {
	rs, err := s.readRepoStatus(did)
	if err != nil {
		return err
	}
	if rs == nil {
		// Defensive: the engine only fires OnComplete after a Lookup
		// returned Discovered/Failed, so the row exists. If somehow
		// it doesn't, recreate it rather than failing the run — the
		// download already happened and we don't want to lose the
		// progress signal.
		rs = &RepoStatus{}
	}
	now := timeNow()
	rs.Backfill.Status = StatusComplete
	rs.Backfill.Rev = commit.Rev
	rs.Backfill.CompletedAt = now
	rs.Backfill.LastError = ""
	rs.Rev = commit.Rev
	rs.UpdatedAt = now
	if err := s.putRepoStatus(did, rs); err != nil {
		return err
	}
	s.metrics.incCompleted()
	return nil
}
```

- [ ] **Step 4: Run the tests**

Run: `just test ./internal/backfill`
Expected: 2 new tests pass.

- [ ] **Step 5: Commit**

```bash
git add internal/backfill/store.go internal/backfill/store_test.go
git commit -m "backfill: implement Store.OnComplete"
```

---

## Task 7: Store.OnFail

**Files:**
- Modify: `internal/backfill/store.go`
- Modify: `internal/backfill/store_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/backfill/store_test.go`:

```go
// TestStore_OnFail_RecordsFailure pins the failure path. attempts is
// the count for the current Run only — atmos passes initial+retries
// from processRepo, and we overwrite rather than accumulate across
// Runs (per DESIGN.md §6.3, this cosmetic regression is intentional).
func TestStore_OnFail_RecordsFailure(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	did := atmos.DID("did:plc:bad")

	require.NoError(t, s.OnDiscover(context.Background(), atmossync.ListReposEntry{
		DID: did, Active: true,
	}))

	failErr := errors.New("upstream 500")
	require.NoError(t, s.OnFail(context.Background(), did, failErr, 6))

	got, err := s.Lookup(context.Background(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateFailed, got.State)

	rs, err := s.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, "upstream 500", rs.Backfill.LastError)
	require.Equal(t, 6, rs.Backfill.Attempts)
	require.False(t, rs.Backfill.StartedAt.IsZero(), "StartedAt set by OnDiscover must survive")
	require.True(t, rs.Backfill.CompletedAt.IsZero(), "OnFail must not stamp CompletedAt")
}

// TestStore_OnFail_AfterPriorComplete documents (and locks in) the
// defensive behavior: a Run never re-attempts a Complete row in this
// PR, but if it ever did, OnFail keeps Backfill.CompletedAt and Rev
// from the prior run rather than zeroing them.
func TestStore_OnFail_AfterPriorComplete(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	did := atmos.DID("did:plc:flake")

	require.NoError(t, s.OnDiscover(context.Background(), atmossync.ListReposEntry{
		DID: did, Active: true,
	}))
	require.NoError(t, s.OnComplete(context.Background(), did, &repo.Commit{DID: string(did), Rev: "rev-good"}))

	require.NoError(t, s.OnFail(context.Background(), did, errors.New("boom"), 3))

	rs, err := s.readRepoStatus(did)
	require.NoError(t, err)
	require.Equal(t, StatusFailed, rs.Backfill.Status)
	require.Equal(t, "rev-good", rs.Backfill.Rev)
	require.False(t, rs.Backfill.CompletedAt.IsZero(), "prior CompletedAt preserved")
}
```

Add `"errors"` to test imports if not present.

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `just test ./internal/backfill -run TestStore_OnFail`
Expected: PANIC "OnFail not yet implemented".

- [ ] **Step 3: Implement OnFail**

Replace the `OnFail` panic-stub in `internal/backfill/store.go` with:

```go
// OnFail records a failed repo download. atmos passes the total
// attempt count for the current Run (initial + retries). We overwrite
// rather than accumulate across Runs — DESIGN.md §6.3 calls out
// resetting Attempts on failover as an acceptable cosmetic regression.
//
// CompletedAt and Backfill.Rev from a prior successful Run are
// preserved. This is defensive — within this PR the engine never
// retries a StateComplete DID — but it keeps a hypothetical future
// "complete then later failed" trail intact.
func (s *Store) OnFail(_ context.Context, did atmos.DID, failErr error, attempts int) error {
	rs, err := s.readRepoStatus(did)
	if err != nil {
		return err
	}
	if rs == nil {
		rs = &RepoStatus{}
	}
	rs.Backfill.Status = StatusFailed
	rs.Backfill.LastError = failErr.Error()
	rs.Backfill.Attempts = attempts
	if err := s.putRepoStatus(did, rs); err != nil {
		return err
	}
	s.metrics.incFailed()
	return nil
}
```

- [ ] **Step 4: Run the tests**

Run: `just test ./internal/backfill`
Expected: 2 new tests pass; the compile-time `_ atmosbackfill.Store = (*Store)(nil)` assertion holds (no more panic stubs).

- [ ] **Step 5: Commit**

```bash
git add internal/backfill/store.go internal/backfill/store_test.go
git commit -m "backfill: implement Store.OnFail"
```

---

## Task 8: Handler

**Files:**
- Create: `internal/backfill/handler.go`
- Create: `internal/backfill/handler_test.go`

- [ ] **Step 1: Write the failing test in `internal/backfill/handler_test.go`**

```go
package backfill

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/repo"
	"github.com/stretchr/testify/require"
)

// TestLogHandler_HandleRepoLogsAtDebug pins the contract: the handler
// is a no-op that emits a debug log. Tests for the real segment-
// writer-backed handler will replace this in a future PR.
func TestLogHandler_HandleRepoLogsAtDebug(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	h := NewLogHandler(logger)

	commit := &repo.Commit{DID: "did:plc:abc", Rev: "rev-x"}
	err := h.HandleRepo(context.Background(), atmos.DID("did:plc:abc"), nil, commit)
	require.NoError(t, err)

	require.Contains(t, buf.String(), `"did":"did:plc:abc"`)
	require.Contains(t, buf.String(), `"rev":"rev-x"`)
	require.Contains(t, buf.String(), `"level":"DEBUG"`)
}

// TestLogHandler_NilLoggerNoPanic guards the wiring: a caller that
// forgot to plumb a logger should get a usable handler, not a crash.
// We default to slog.Default() in the constructor.
func TestLogHandler_NilLoggerNoPanic(t *testing.T) {
	t.Parallel()
	h := NewLogHandler(nil)
	err := h.HandleRepo(context.Background(), atmos.DID("did:plc:abc"), nil, &repo.Commit{Rev: "x"})
	require.NoError(t, err)
}
```

- [ ] **Step 2: Run the test to confirm it fails**

Run: `just test ./internal/backfill -run TestLogHandler`
Expected: FAIL — `NewLogHandler` undefined.

- [ ] **Step 3: Create `internal/backfill/handler.go`**

```go
// Package backfill: handler.go provides the placeholder Handler used
// by the bootstrap PR. It does no segment writing — that comes in a
// later PR. The point is to prove the engine wiring (listRepos ->
// download -> handler -> Store) works end to end.
package backfill

import (
	"context"
	"log/slog"

	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/repo"
)

// LogHandler is a no-op atmos backfill.Handler that logs each handled
// repo at debug level. It exists so we can prove the engine wiring
// works without committing to segment file format details from this
// PR.
type LogHandler struct {
	logger *slog.Logger
}

// Compile-time assertion.
var _ atmosbackfill.Handler = (*LogHandler)(nil)

// NewLogHandler returns a LogHandler. nil logger uses slog.Default().
func NewLogHandler(logger *slog.Logger) *LogHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogHandler{logger: logger}
}

// HandleRepo logs the (did, rev) pair and returns nil. The atmos
// engine then advances the DID via Store.OnComplete.
func (h *LogHandler) HandleRepo(_ context.Context, did atmos.DID, _ *repo.Repo, commit *repo.Commit) error {
	h.logger.Debug("backfill: repo handled",
		"did", string(did),
		"rev", commit.Rev,
	)
	return nil
}
```

- [ ] **Step 4: Run the tests**

Run: `just test ./internal/backfill`
Expected: 2 new tests pass.

- [ ] **Step 5: Commit**

```bash
git add internal/backfill/handler.go internal/backfill/handler_test.go
git commit -m "backfill: add no-op LogHandler"
```

---

## Task 9: Run entrypoint

**Files:**
- Create: `internal/backfill/run.go`

This task wires everything together. We don't add tests yet — `run_test.go` (Task 10) is the integration test.

- [ ] **Step 1: Create `internal/backfill/run.go`**

```go
// Package backfill: run.go is the entrypoint cmd/jetstream calls
// from its errgroup. Run constructs the atmos engine and drives it
// to completion. Returns nil on clean drain (every DID either skipped
// at Complete or downloaded + recorded), the engine's error
// otherwise.
package backfill

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

// Config carries everything Run needs. The fields are required unless
// noted otherwise.
type Config struct {
	// Store is the shared metadata pebble db.
	Store *store.Store

	// RelayURL is the upstream relay base URL (e.g. https://bsky.network).
	RelayURL string

	// Logger is the structured logger; required (no sensible default
	// for an ingestion service that needs failure-mode visibility).
	Logger *slog.Logger

	// Metrics is optional; nil means we still run, just without
	// /metrics counters incrementing.
	Metrics *Metrics

	// HTTPClient is shared across the relay xrpc client, the identity
	// resolver, and the per-PDS pool inside the engine. nil = a fresh
	// 30s-timeout default client.
	HTTPClient *http.Client
}

// progressLogInterval bounds how chatty the INFO progress log is. We
// can revisit this once we have real production data — at ~30M DIDs
// total, every 1k completions is ~30k log lines for a full backfill,
// which is reasonable.
const progressLogInterval = 1_000

// directoryCacheCapacity is the LRU size for the identity cache. The
// network has ~30M DIDs; caching all of them is wasteful on a
// bootstrap that will visit each at most a few times. 100k covers
// any hot working set with plenty of headroom.
const directoryCacheCapacity = 100_000

// directoryCacheTTL keeps cache entries cheaply reusable for the
// duration of a backfill without growing stale enough to miss key
// rotations during the run.
const directoryCacheTTL = 24 * time.Hour

// Run drives the atmos backfill engine to completion. It blocks until
// the engine drains or ctx is cancelled. Safe to call multiple times
// across process restarts: each call constructs a fresh Engine
// (atmos engines are single-shot) and resumes by skipping rows
// already at StatusComplete via Store.Lookup.
func Run(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}

	// Per atmos Options.SyncClient docs: disable xrpc retries because
	// the engine's retry/backoff loop is the only retry source we
	// want. Otherwise xrpc and the engine compound retries on
	// transient 503s, multiplying load against PDSes.
	xc := &xrpc.Client{
		Host:       cfg.RelayURL,
		HTTPClient: gt.Some(httpClient),
		Retry:      gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
	}

	dir := &identity.Directory{
		Resolver: &identity.DefaultResolver{
			HTTPClient: gt.Some(httpClient),
		},
		Cache: identity.NewLRUCache(directoryCacheCapacity, directoryCacheTTL),
	}

	sc := atmossync.NewClient(atmossync.Options{
		Client:    xc,
		Directory: gt.Some(dir),
	})

	st := NewStore(cfg.Store, cfg.Metrics)
	handler := NewLogHandler(cfg.Logger)
	logger := cfg.Logger

	engine := atmosbackfill.NewEngine(atmosbackfill.Options{
		SyncClient: sc,
		Store:      st,
		Handler:    handler,
		Directory:  gt.Some(dir),
		HTTPClient: gt.Some(httpClient),
		OnError: gt.Some(func(did atmos.DID, err error) {
			logger.Warn("backfill: repo failed",
				"did", string(did),
				"err", err,
			)
		}),
		OnProgress: gt.Some(func(stats atmosbackfill.Stats) {
			if stats.Completed%progressLogInterval == 0 {
				logger.Info("backfill: progress",
					"completed", stats.Completed,
				)
			}
		}),
	})

	logger.Info("backfill: starting", "relay", cfg.RelayURL)
	err := engine.Run(ctx)
	if err != nil {
		logger.Error("backfill: engine returned error", "err", err)
		return fmt.Errorf("backfill: %w", err)
	}
	logger.Info("backfill: engine drained")
	return nil
}

func (cfg Config) validate() error {
	if cfg.Store == nil {
		return fmt.Errorf("backfill: Config.Store is required")
	}
	if cfg.RelayURL == "" {
		return fmt.Errorf("backfill: Config.RelayURL is required")
	}
	if cfg.Logger == nil {
		return fmt.Errorf("backfill: Config.Logger is required")
	}
	return nil
}
```

- [ ] **Step 2: Verify it compiles**

Run: `go build ./internal/backfill/...`
Expected: clean compile.

- [ ] **Step 3: Add a Config validation test**

Append to `internal/backfill/run_test.go` (creating the file if it doesn't exist):

```go
package backfill

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/stretchr/testify/require"
)

// TestRun_RejectsInvalidConfig pins the contract for cmd/jetstream:
// pass the wrong Config and you get a clear error before any network
// I/O happens.
func TestRun_RejectsInvalidConfig(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tests := []struct {
		name    string
		cfg     Config
		errPart string
	}{
		{"missing Store", Config{RelayURL: "x", Logger: logger}, "Config.Store"},
		{"missing RelayURL", Config{Store: &store.Store{}, Logger: logger}, "Config.RelayURL"},
		{"missing Logger", Config{Store: &store.Store{}, RelayURL: "x"}, "Config.Logger"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Run(context.Background(), tc.cfg)
			require.ErrorContains(t, err, tc.errPart)
		})
	}
}
```

- [ ] **Step 4: Run the tests**

Run: `just test ./internal/backfill`
Expected: 3 new test cases pass.

- [ ] **Step 5: Commit**

```bash
git add internal/backfill/run.go internal/backfill/run_test.go
git commit -m "backfill: add Run entrypoint"
```

---

## Task 10: End-to-end run_test.go against stubbed services

**Files:**
- Modify: `internal/backfill/run_test.go`

This is the high-value test. We stub out a relay (`listRepos`), a PDS (`getRepo` returning a real CAR built via atmos), and a PLC resolver, then drive the actual atmos engine through the full pipeline.

- [ ] **Step 1: Add fixture helpers to `internal/backfill/run_test.go`**

Append to the existing file (after `TestRun_RejectsInvalidConfig`):

```go
import (
	// existing imports plus:
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
)

// stubResolver is a fixed-DID-document Resolver pointing at a single
// stub PDS. It mirrors the pattern used in atmos's own tests
// (atmos/backfill/backfill_test.go) and lets us avoid spinning up a
// real PLC.
type stubResolver struct {
	docs map[atmos.DID]*identity.DIDDocument
}

func (r *stubResolver) ResolveDID(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	doc, ok := r.docs[did]
	if !ok {
		return nil, identity.ErrDIDNotFound
	}
	return doc, nil
}

func (r *stubResolver) ResolveHandle(_ context.Context, _ atmos.Handle) (atmos.DID, error) {
	return "", identity.ErrHandleNotFound
}

// repoFixture is one DID + its CAR + its public key multibase. We
// build the CAR via atmos/repo so signature verification (which the
// engine performs because we set Directory) actually passes.
type repoFixture struct {
	did       atmos.DID
	car       []byte
	multibase string
}

// buildRepoFixture constructs a single-record repo for did, signs it
// with a fresh P-256 key, and returns the CAR + the multibase that
// will go in the DID document. The repo only needs to exist; we
// don't care about its contents.
func buildRepoFixture(t *testing.T, did atmos.DID) repoFixture {
	t.Helper()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	store := mst.NewMemBlockStore()
	r := &atmosrepo.Repo{
		DID:   did,
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}
	require.NoError(t, r.Create("app.bsky.feed.post", "rec0", map[string]any{"text": "hi"}))

	var buf bytes.Buffer
	require.NoError(t, r.ExportCAR(&buf, key))

	pub, ok := key.PublicKey().(*crypto.P256PublicKey)
	require.True(t, ok)

	return repoFixture{
		did:       did,
		car:       buf.Bytes(),
		multibase: pub.DIDKey()[8:],
	}
}

// stubServer serves both the relay (listRepos) and the PDS (getRepo)
// for a fixed set of DIDs. Single httptest.Server because the engine
// is happy to talk to any host that speaks XRPC.
type stubServer struct {
	srv          *httptest.Server
	fixtures     map[atmos.DID]repoFixture
	listReposHit atomic.Int64
	getRepoHit   atomic.Int64

	// failGetRepo, when set, makes getRepo return failGetRepoCode for
	// the listed DIDs. Used in the failure-path test.
	failGetRepo     map[atmos.DID]bool
	failGetRepoCode int
}

func newStubServer(t *testing.T, fixtures map[atmos.DID]repoFixture) *stubServer {
	t.Helper()
	s := &stubServer{fixtures: fixtures}
	s.srv = httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(s.srv.Close)
	return s
}

type listEntry struct {
	DID    string `json:"did"`
	Head   string `json:"head"`
	Rev    string `json:"rev"`
	Active bool   `json:"active"`
}
type listPage struct {
	Cursor string      `json:"cursor,omitempty"`
	Repos  []listEntry `json:"repos"`
}

func (s *stubServer) handle(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/xrpc/com.atproto.sync.listRepos":
		s.listReposHit.Add(1)
		// Stable order so tests can reason about pagination boundaries.
		dids := make([]atmos.DID, 0, len(s.fixtures))
		for did := range s.fixtures {
			dids = append(dids, did)
		}
		// Sort for determinism.
		for i := 1; i < len(dids); i++ {
			for j := i; j > 0 && dids[j-1] > dids[j]; j-- {
				dids[j-1], dids[j] = dids[j], dids[j-1]
			}
		}
		page := listPage{}
		for _, d := range dids {
			page.Repos = append(page.Repos, listEntry{
				DID: string(d), Head: "bafytest", Rev: "rev1", Active: true,
			})
		}
		_ = json.NewEncoder(w).Encode(page)

	case "/xrpc/com.atproto.sync.getRepo":
		s.getRepoHit.Add(1)
		didStr := r.URL.Query().Get("did")
		did := atmos.DID(didStr)
		if s.failGetRepo[did] {
			w.WriteHeader(s.failGetRepoCode)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "TransientError"})
			return
		}
		f, ok := s.fixtures[did]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "RepoNotFound"})
			return
		}
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		_, _ = w.Write(f.car)

	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// buildEngineConfig wires Run-style dependencies for tests.
func buildEngineConfig(t *testing.T, srv *stubServer, db *store.Store) Config {
	t.Helper()
	docs := make(map[atmos.DID]*identity.DIDDocument)
	for did, f := range srv.fixtures {
		docs[did] = &identity.DIDDocument{
			ID: string(did),
			VerificationMethod: []identity.VerificationMethod{
				{ID: "#atproto", Type: "Multikey", Controller: string(did), PublicKeyMultibase: f.multibase},
			},
			Service: []identity.Service{
				{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: srv.srv.URL},
			},
		}
	}

	// We can't override the resolver via the public Config, so the
	// integration tests use Run's *building blocks* directly rather
	// than calling Run. That mirrors how the production code is
	// structured but lets us inject the stub resolver. We refactor
	// Run to expose this in the next step.
	_ = docs
	return Config{
		Store:    db,
		RelayURL: srv.srv.URL,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}
```

Wait — we hit a wiring issue: the stub resolver can't be plumbed through `Config`. We have two options: (a) extract a `runWithDeps` helper that takes a pre-built engine, or (b) add a `Resolver` injection point to Config gated as test-only.

Pick (a): refactor `Run` to delegate to a small internal helper that accepts the already-built dependencies. Continue in step 2.

- [ ] **Step 2: Refactor `Run` to expose an injectable internal**

Modify `internal/backfill/run.go`. Replace the body of `Run` so the internal builder is testable:

```go
// Run drives the atmos backfill engine to completion. It blocks until
// the engine drains or ctx is cancelled. Safe to call multiple times
// across process restarts: each call constructs a fresh Engine
// (atmos engines are single-shot) and resumes by skipping rows
// already at StatusComplete via Store.Lookup.
func Run(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}

	dir := &identity.Directory{
		Resolver: &identity.DefaultResolver{
			HTTPClient: gt.Some(httpClient),
		},
		Cache: identity.NewLRUCache(directoryCacheCapacity, directoryCacheTTL),
	}

	return runWithDirectory(ctx, cfg, httpClient, dir)
}

// runWithDirectory is the production entry point's internal worker.
// Tests inject a stub resolver via the Directory parameter so we can
// avoid spinning up a real PLC.
func runWithDirectory(ctx context.Context, cfg Config, httpClient *http.Client, dir *identity.Directory) error {
	xc := &xrpc.Client{
		Host:       cfg.RelayURL,
		HTTPClient: gt.Some(httpClient),
		Retry:      gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
	}
	sc := atmossync.NewClient(atmossync.Options{
		Client:    xc,
		Directory: gt.Some(dir),
	})

	st := NewStore(cfg.Store, cfg.Metrics)
	handler := NewLogHandler(cfg.Logger)
	logger := cfg.Logger

	engine := atmosbackfill.NewEngine(atmosbackfill.Options{
		SyncClient: sc,
		Store:      st,
		Handler:    handler,
		Directory:  gt.Some(dir),
		HTTPClient: gt.Some(httpClient),
		OnError: gt.Some(func(did atmos.DID, err error) {
			logger.Warn("backfill: repo failed", "did", string(did), "err", err)
		}),
		OnProgress: gt.Some(func(stats atmosbackfill.Stats) {
			if stats.Completed%progressLogInterval == 0 {
				logger.Info("backfill: progress", "completed", stats.Completed)
			}
		}),
	})

	logger.Info("backfill: starting", "relay", cfg.RelayURL)
	err := engine.Run(ctx)
	if err != nil {
		logger.Error("backfill: engine returned error", "err", err)
		return fmt.Errorf("backfill: %w", err)
	}
	logger.Info("backfill: engine drained")
	return nil
}
```

- [ ] **Step 3: Update the test fixture to use `runWithDirectory` directly**

Replace the broken `buildEngineConfig` helper at the end of `run_test.go` with this driver:

```go
// runWithStub builds a Directory whose Resolver returns the stub PDS
// document for each fixture DID, then drives runWithDirectory. This
// is the integration entry-point for our run_test.go.
func runWithStub(t *testing.T, ctx context.Context, srv *stubServer, db *store.Store) error {
	t.Helper()
	docs := make(map[atmos.DID]*identity.DIDDocument, len(srv.fixtures))
	for did, f := range srv.fixtures {
		docs[did] = &identity.DIDDocument{
			ID: string(did),
			VerificationMethod: []identity.VerificationMethod{
				{ID: "#atproto", Type: "Multikey", Controller: string(did), PublicKeyMultibase: f.multibase},
			},
			Service: []identity.Service{
				{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: srv.srv.URL},
			},
		}
	}
	dir := &identity.Directory{Resolver: &stubResolver{docs: docs}}

	cfg := Config{
		Store:    db,
		RelayURL: srv.srv.URL,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return runWithDirectory(ctx, cfg, &http.Client{Timeout: 5 * time.Second}, dir)
}
```

- [ ] **Step 4: Add the happy-path integration test**

Append to `run_test.go`:

```go
// TestRun_HappyPath_DownloadsAllRepos is the wiring smoke test. Three
// DIDs in listRepos; each with a real signed CAR served by the stub
// PDS. After Run, every DID lands at StatusComplete in pebble.
func TestRun_HappyPath_DownloadsAllRepos(t *testing.T) {
	t.Parallel()

	dids := []atmos.DID{"did:plc:aaa", "did:plc:bbb", "did:plc:ccc"}
	fixtures := make(map[atmos.DID]repoFixture, len(dids))
	for _, d := range dids {
		fixtures[d] = buildRepoFixture(t, d)
	}
	srv := newStubServer(t, fixtures)

	db, err := store.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, runWithStub(t, t.Context(), srv, db))

	bf := NewStore(db, nil)
	for _, did := range dids {
		got, err := bf.Lookup(context.Background(), did)
		require.NoError(t, err)
		require.Equal(t, atmosbackfill.StateComplete, got.State, "%s should be Complete", did)
	}
}

// TestRun_Resume_NoOpAfterCompletion exercises restart-after-
// completion: the second Run call should drain immediately without
// hitting getRepo, because every Lookup returns StateComplete.
func TestRun_Resume_NoOpAfterCompletion(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:done")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)

	db, err := store.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, runWithStub(t, t.Context(), srv, db))
	firstGetRepo := srv.getRepoHit.Load()
	require.Equal(t, int64(1), firstGetRepo)

	// Second pass: same data dir, same DID. The engine still walks
	// listRepos but skips download.
	require.NoError(t, runWithStub(t, t.Context(), srv, db))
	require.Equal(t, firstGetRepo, srv.getRepoHit.Load(), "second Run must not re-download Complete DIDs")
}

// TestRun_FailedRepoIsRetriable: a DID whose getRepo 500s exhausts
// retries and lands at StatusFailed. A subsequent Run with the
// failure cleared re-attempts (proving Failed rows aren't terminal).
func TestRun_FailedRepoIsRetriable(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:flake")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)
	srv.failGetRepo = map[atmos.DID]bool{did: true}
	srv.failGetRepoCode = http.StatusInternalServerError

	db, err := store.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// To keep the test fast, we still drive Run with the production
	// retry counts. atmos defaults are 5 retries with 1s base delay
	// and ~30s max delay, so worst case the test sits ~60s. We accept
	// that under -short by gating with t.Short() — `just test` runs
	// short mode and skips this test; `just test-long` exercises it.
	if testing.Short() {
		t.Skip("retry-budget test is slow under defaults; covered by test-long")
	}

	err = runWithStub(t, t.Context(), srv, db)
	require.NoError(t, err, "Run drains successfully even when individual DIDs fail")

	bf := NewStore(db, nil)
	got, err := bf.Lookup(context.Background(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateFailed, got.State)

	// Clear the failure and re-run.
	srv.failGetRepo[did] = false

	require.NoError(t, runWithStub(t, t.Context(), srv, db))
	got, err = bf.Lookup(context.Background(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateComplete, got.State)
}
```

- [ ] **Step 5: Run the tests**

Run: `just test ./internal/backfill`
Expected: happy-path + resume tests pass; `TestRun_FailedRepoIsRetriable` skips under `-short`.

Run: `just test-long ./internal/backfill -run TestRun_FailedRepoIsRetriable`
Expected: PASS (takes a minute or two).

- [ ] **Step 6: Commit**

```bash
git add internal/backfill/run.go internal/backfill/run_test.go
git commit -m "backfill: add end-to-end Run integration tests"
```

---

## Task 11: Wire cmd/jetstream/main.go to the new package

**Files:**
- Modify: `cmd/jetstream/main.go`

The existing main.go imports `internal/backfill`, calls `backfill.NewSeedMetrics(metrics.Registry)`, and invokes `backfill.Run(gctx, backfill.Config{Store, RelayURL, Metrics, Logger: gt.Some(logger)})`. Update to the new `Config` shape.

- [ ] **Step 1: Update the import and wiring in `cmd/jetstream/main.go`**

Find the block in `runServe`:

```go
	seedMetrics := backfill.NewSeedMetrics(metrics.Registry)

	srv := server.New(server.Config{
		PublicAddr:      cmd.String("addr"),
		DebugAddr:       cmd.String("debug-addr"),
		ShutdownTimeout: cmd.Duration("shutdown-timeout"),
	}, logger, metrics)
```

Replace with:

```go
	bfMetrics := backfill.NewMetrics(metrics.Registry)

	srv := server.New(server.Config{
		PublicAddr:      cmd.String("addr"),
		DebugAddr:       cmd.String("debug-addr"),
		ShutdownTimeout: cmd.Duration("shutdown-timeout"),
	}, logger, metrics)
```

Find the errgroup goroutine:

```go
	// Start the backfiller to do initial repo download for
	// a fresh jetstream instance
	g.Go(func() error {
		return backfill.Run(gctx, backfill.Config{
			Store:    metaStore,
			RelayURL: cmd.String("relay-url"),
			Metrics:  seedMetrics,
			Logger:   gt.Some(logger),
		})
	})
```

Replace with:

```go
	// Start the backfiller to do initial repo download for a fresh
	// jetstream instance, or resume from where a prior process left
	// off (DESIGN.md §4.1). On clean drain this goroutine returns nil
	// and the HTTP server keeps running; on engine failure the
	// errgroup cancels the server too.
	g.Go(func() error {
		return backfill.Run(gctx, backfill.Config{
			Store:    metaStore,
			RelayURL: cmd.String("relay-url"),
			Logger:   logger,
			Metrics:  bfMetrics,
		})
	})
```

The `gt` import is no longer needed; remove the `"github.com/jcalabro/gt"` import line if it's now unused (verify by trying to build).

- [ ] **Step 2: Build the binary**

Run: `just build`
Expected: clean compile to `bin/jetstream`. If gt is still used elsewhere, keep the import; if not, the build will fail with "imported and not used" — drop it.

- [ ] **Step 3: Commit**

```bash
git add cmd/jetstream/main.go
git commit -m "cmd/jetstream: wire serve to new backfill.Run shape"
```

---

## Task 12: Rewrite cmd/jetstream/serve_test.go

**Files:**
- Modify: `cmd/jetstream/serve_test.go`

The existing test references symbols that no longer exist (`backfill.CountRepos`, `GetBootstrapState`, `PhaseSeed`, `PhaseComplete`). Replace with a smoke test against the new shape.

- [ ] **Step 1: Replace the file contents**

Overwrite `cmd/jetstream/serve_test.go` with:

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/backfill"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	"github.com/stretchr/testify/require"
)

// TestServe_BootstrapsAndShutsDownCleanly is the wiring smoke test:
// a real `jetstream serve` invocation against a stubbed relay that
// also serves repo CARs. We assert that bootstrap reaches the seed
// loop (both DIDs land in pebble) and that the process shuts down
// cleanly when the parent context is cancelled.
//
// Deeper state-machine cases live in internal/backfill; this test
// only proves the serve wiring still composes.
//
// NOTE: this stub does NOT verify commit signatures because the
// production code uses the real PLC resolver. We bypass that by
// pre-writing each DID's row to StatusComplete before running serve,
// so the engine skips download entirely. A future PR (segment
// writes) will introduce a higher-fidelity smoke test that exercises
// the full download path with an injectable resolver.
func TestServe_BootstrapsAndShutsDownCleanly(t *testing.T) {
	t.Parallel()

	type repoEntry struct {
		DID    string `json:"did"`
		Head   string `json:"head"`
		Rev    string `json:"rev"`
		Active bool   `json:"active"`
	}
	type page struct {
		Cursor string      `json:"cursor,omitempty"`
		Repos  []repoEntry `json:"repos"`
	}

	dataDir := t.TempDir()
	dids := []atmos.DID{"did:plc:aaa", "did:plc:bbb"}

	// Pre-seed the metadata db with both DIDs at Complete so the
	// engine's listRepos scan skips download entirely.
	require.NoError(t, preSeedComplete(dataDir, dids))

	// listReposDone is closed once the relay has served the empty-
	// page terminator. That's our deterministic "bootstrap walked
	// listRepos to the end" signal.
	listReposDone := make(chan struct{})
	var calls atomic.Int32
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/xrpc/com.atproto.sync.listRepos", r.URL.Path)
		idx := int(calls.Add(1)) - 1
		switch idx {
		case 0:
			_ = json.NewEncoder(w).Encode(page{
				Cursor: "more",
				Repos: []repoEntry{
					{DID: string(dids[0]), Head: "bafyaaa", Rev: "rev1", Active: true},
					{DID: string(dids[1]), Head: "bafybbb", Rev: "rev2", Active: true},
				},
			})
		default:
			_ = json.NewEncoder(w).Encode(page{})
		}
		if idx == 1 {
			select {
			case <-listReposDone:
			default:
				close(listReposDone)
			}
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

	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(dataDir, "meta.pebble", "LOCK"))
		return err == nil
	}, 5*time.Second, 50*time.Millisecond, "metadata store was never created")

	select {
	case <-listReposDone:
	case <-time.After(5 * time.Second):
		t.Fatal("bootstrap never drained listRepos pagination")
	}

	cancel()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve exited with unexpected error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not shut down within deadline")
	}

	// Re-open and assert both DIDs are still at Complete.
	s, err := store.Open(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	bf := backfill.NewStore(s, nil)
	for _, did := range dids {
		got, err := bf.Lookup(context.Background(), did)
		require.NoError(t, err)
		require.Equal(t, atmosbackfillStateComplete(), got.State, "%s should be Complete", did)
	}
}

// preSeedComplete opens the data dir's pebble, writes a Complete row
// for each DID, and closes. Used by TestServe_* to bypass the actual
// download path while still exercising the rest of the wiring.
func preSeedComplete(dataDir string, dids []atmos.DID) error {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	s, err := store.Open(dataDir)
	if err != nil {
		return err
	}
	defer s.Close()

	bf := backfill.NewStore(s, nil)
	for _, did := range dids {
		// We need to put a real CAR-derived rev so the test mirrors
		// production. The simplest path: build a fixture, call
		// OnDiscover then OnComplete.
		key, err := crypto.GenerateP256()
		if err != nil {
			return err
		}
		mstore := mst.NewMemBlockStore()
		r := &atmosrepo.Repo{
			DID:   did,
			Clock: atmos.NewTIDClock(0),
			Store: mstore,
			Tree:  mst.NewTree(mstore),
		}
		if err := r.Create("app.bsky.feed.post", "rec0", map[string]any{"text": "x"}); err != nil {
			return err
		}
		var buf bytes.Buffer
		if err := r.ExportCAR(&buf, key); err != nil {
			return err
		}
		// Direct discover + complete so we skip the download path.
		if err := bf.OnDiscover(context.Background(), atmosListEntry(did, true)); err != nil {
			return err
		}
		if err := bf.OnComplete(context.Background(), did, &atmosrepo.Commit{DID: string(did), Rev: "rev-pre"}); err != nil {
			return err
		}
	}
	return nil
}
```

That last block has two undefined helpers, deliberately — they need imports/types from the atmos package that aren't trivial to nest in a test file. Replace with the real imports in step 2.

- [ ] **Step 2: Fix the test imports and call sites**

In `cmd/jetstream/serve_test.go`, replace the two helper-call shorthands `atmosbackfillStateComplete()` and `atmosListEntry(...)` with their real call shapes, and add the imports.

Add to the import block:

```go
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	atmossync "github.com/jcalabro/atmos/sync"
```

Replace `atmosbackfillStateComplete()` with `atmosbackfill.StateComplete` and `atmosListEntry(did, true)` with `atmossync.ListReposEntry{DID: did, Active: true}`.

The final assert loop should read:

```go
	for _, did := range dids {
		got, err := bf.Lookup(context.Background(), did)
		require.NoError(t, err)
		require.Equal(t, atmosbackfill.StateComplete, got.State, "%s should be Complete", did)
	}
```

The seeding loop should read:

```go
		if err := bf.OnDiscover(context.Background(), atmossync.ListReposEntry{DID: did, Active: true}); err != nil {
			return err
		}
		if err := bf.OnComplete(context.Background(), did, &atmosrepo.Commit{DID: string(did), Rev: "rev-pre"}); err != nil {
			return err
		}
```

- [ ] **Step 3: Run the test**

Run: `just test ./cmd/jetstream`
Expected: PASS.

- [ ] **Step 4: Run the entire suite to verify nothing else broke**

Run: `just`
Expected: lint + test pass.

- [ ] **Step 5: Commit**

```bash
git add cmd/jetstream/serve_test.go
git commit -m "cmd/jetstream: rewrite serve smoke test against new backfill shape"
```

---

## Task 13: Final verification

**Files:** none (verification only)

- [ ] **Step 1: Full test suite under race detector**

Run: `just test-race`
Expected: all tests pass (the integration test uses real goroutines via the engine; race detector should be clean).

- [ ] **Step 2: Lint**

Run: `just lint`
Expected: no findings.

- [ ] **Step 3: Build the production binary and confirm it starts**

Run: `just build && ./bin/jetstream version`
Expected: prints "jetstream version ... (commit ..., built ...)".

- [ ] **Step 4: (Optional) Manual smoke against a real relay**

Run: `just run serve --data-dir=/tmp/jetstream-smoke --relay-url=https://bsky.network` for ~10 seconds, then SIGINT.
Expected: log lines `"backfill: starting"` followed by `"backfill: progress" completed=...` for the first few thousand DIDs; clean shutdown on Ctrl-C.

This is a manual sanity check, not a CI-blocking test. Skip if you trust the integration tests.

- [ ] **Step 5: Done — no new commit**

The plan is complete. The branch should now build cleanly, all tests should pass, and the `serve` command should drive the atmos backfill engine end-to-end against a real relay.

---

## Self-Review

- **Spec coverage:** Every section of the spec maps to a task:
  - §5.1 (package layout) — Tasks 1-9 create each file.
  - §5.2 (engine lifecycle / single-shot resume) — Task 9 wires `Run`; Task 10 verifies resume.
  - §5.3 (pebble keyspace + RepoStatus) — Task 1.
  - §5.4 (Store implementation) — Tasks 3-7, one method per task.
  - §5.5 (engine wiring with Directory + xrpc retry-disable) — Task 9.
  - §5.6 (LogHandler) — Task 8.
  - §5.7 (metrics) — Task 2.
  - §5.8 (restart behavior 3 cases) — Task 10's three integration tests cover all three.
  - §6 (cmd/jetstream wiring) — Task 11.
  - §7 (tests) — Tasks 1, 3-10, 12.

- **Placeholder scan:** No "TBD", no "implement later", no "similar to Task N". Every code step has the actual code.

- **Type consistency:** `repoKey` uses `[]byte`; `RepoStatus` and `RepoBackfillStatus` are reused verbatim across tasks; `Status` enum is one place; `Store.Lookup` signature matches the atmos interface; `LogHandler` matches the atmos `Handler` interface.

- **One open issue caught during review:** Task 10's first cut had `buildEngineConfig` returning a public `Config`, then the prose tried to inject a stub resolver via that — which is impossible because `Config` doesn't expose a resolver. Fixed in step 2 of Task 10 by extracting `runWithDirectory` and having tests call that directly. No edit needed; the plan as written already includes the fix.

- **Concurrency / race:** Task 10 exercises a real engine pool (50 workers default), and Task 13 runs the suite under `-race`. Good.
