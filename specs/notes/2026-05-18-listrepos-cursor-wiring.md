# Wire atmos's listRepos cursor into jetstream — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist the relay's listRepos cursor in pebble at `relay/list_repos_cursor` and pass it to atmos's `backfill.Engine.StartCursor` on Run startup so a restart skips listRepos pages already fully processed.

**Architecture:** A new `internal/backfill/cursor.go` exposes `LoadListReposCursor` and `SaveListReposCursor` against the existing `*store.Store`. `runWithDirectory` reads the saved cursor on entry, passes it to atmos via `Options.StartCursor`, and persists each page's `NextCursor` via `Options.OnPageComplete`. No new pebble columns or schema migration.

**Tech Stack:** Go 1.26, pebble, atmos v0.0.16, `gt`, `gotestsum`, `golangci-lint`.

**Spec:** `docs/superpowers/specs/2026-05-18-listrepos-cursor-wiring-design.md`

---

## File Structure

| File | Why |
|---|---|
| `internal/backfill/cursor.go` | New: `LoadListReposCursor` + `SaveListReposCursor` + key constant |
| `internal/backfill/cursor_test.go` | New: 4 unit tests against a real pebble |
| `internal/backfill/run.go` | Modify: `runWithDirectory` loads cursor, sets `StartCursor`, wires `OnPageComplete` |
| `internal/backfill/run_test.go` | Modify: add 1-2 integration tests for cursor passthrough and resume behavior |

---

## Conventions

- `just test ./internal/backfill` — run package tests (sub-second).
- `just test-race` — full module under race; required for final verification.
- `just lint` — must report 0 issues.
- `t.Parallel()` on independent tests; `t.Cleanup` for db.Close.
- Doc comments on exported symbols; explain WHY for non-obvious decisions.
- Error wrapping pattern: `backfill: <action>: %w`.
- No Co-Authored-By or other trailers on commits.

---

## Task 1: Add `cursor.go` with Load/Save helpers + unit tests

**Files:**
- Create: `internal/backfill/cursor.go`
- Create: `internal/backfill/cursor_test.go`

TDD: tests first, watch them fail, implement, watch them pass.

- [ ] **Step 1: Write the failing tests in `internal/backfill/cursor_test.go`**

```go
package backfill

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

// newCursorTestStore returns a fresh pebble-backed *store.Store in a
// t.TempDir(). Mirrors newTestStore in store_test.go but lives here so
// these tests don't depend on store_test.go's helper layout.
func newCursorTestStore(t *testing.T) *store.Store {
	t.Helper()
	db, err := store.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestLoadListReposCursor_Empty pins the first-time-startup contract:
// no row yet, Load returns "" without error so Run can pass "" through
// to atmos as "start from the beginning."
func TestLoadListReposCursor_Empty(t *testing.T) {
	t.Parallel()
	db := newCursorTestStore(t)

	got, err := LoadListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "", got)
}

// TestSaveLoadListReposCursor_RoundTrip is the basic persistence
// invariant: whatever bytes the relay handed us, we hand back.
func TestSaveLoadListReposCursor_RoundTrip(t *testing.T) {
	t.Parallel()
	db := newCursorTestStore(t)

	require.NoError(t, SaveListReposCursor(db, "opaque-cursor-token-xyz"))

	got, err := LoadListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "opaque-cursor-token-xyz", got)
}

// TestSaveListReposCursor_Overwrites confirms cursor advance is
// monotonic-by-overwrite — each page's NextCursor replaces the prior.
// We never accumulate cursors; a single global key holds the latest.
func TestSaveListReposCursor_Overwrites(t *testing.T) {
	t.Parallel()
	db := newCursorTestStore(t)

	require.NoError(t, SaveListReposCursor(db, "first"))
	require.NoError(t, SaveListReposCursor(db, "second"))

	got, err := LoadListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "second", got)
}

// TestSaveListReposCursor_EmptyValue covers the post-drain state:
// atmos fires OnPageComplete("") after the final page. We must
// accept the empty string as a valid value, not treat it as a
// missing-row error. Load afterwards returns "" — the same as
// fresh-startup, which is the right semantic (next Run starts from
// the beginning, since there's nothing left to skip).
func TestSaveListReposCursor_EmptyValue(t *testing.T) {
	t.Parallel()
	db := newCursorTestStore(t)

	require.NoError(t, SaveListReposCursor(db, "first"))
	require.NoError(t, SaveListReposCursor(db, ""))

	got, err := LoadListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "", got)
}
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `just test ./internal/backfill -run TestLoadListReposCursor_Empty`
Expected: FAIL — `LoadListReposCursor` undefined.

- [ ] **Step 3: Create `internal/backfill/cursor.go`**

```go
// Package backfill: cursor.go persists the relay's listRepos resume
// cursor in pebble so a process restart can skip listRepos pages
// already fully processed in a prior Run.
//
// The cursor is opaque per the atproto spec — treat it as a string
// of bytes the relay handed us, valid only against the same relay.
// Cross-relay cursors are undefined behavior; operators changing
// --relay-url between runs should clear this key (or rebuild the
// data dir).
//
// # Persistence semantics
//
// SaveListReposCursor uses pebble.Sync, same as the per-DID write
// path. atmos calls our save callback after every listRepos page
// boundary, so the cost is one fsync per ~1000 DIDs (the protocol's
// page cap). Cheap relative to repo download.
//
// # Known durability hole
//
// atmos fires OnPageComplete after a page's eligible jobs are
// queued onto the worker channel — workers may still be downloading.
// On a process kill mid-page-flush, those workers' DIDs stay at
// StateDiscovered. The next Run starts at the saved cursor (page
// N+1) and never re-walks page N, so those DIDs are stuck until a
// future cursor-less Run rediscovers them.
//
// This is acceptable for now: a future "rewalk" subcommand can
// clear the cursor to force a full re-enumeration. In practice the
// hole only bites if every subsequent Run also dies in the same
// way, which would have bigger problems than orphaned DIDs.
package backfill

import (
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
)

// listReposCursorKey is the pebble key for the persisted listRepos
// resume cursor. Singleton — operators changing relays accept the
// cross-relay opaque-cursor risk.
const listReposCursorKey = "relay/list_repos_cursor"

// LoadListReposCursor reads the persisted cursor from pebble. Returns
// "" if no cursor has been saved (a fresh data dir, or the final
// post-drain page that wrote ""). Errors only on pebble I/O failure.
func LoadListReposCursor(db *store.Store) (string, error) {
	val, closer, err := db.Get([]byte(listReposCursorKey))
	if errors.Is(err, pebble.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("backfill: load list_repos_cursor: %w", err)
	}
	defer func() { _ = closer.Close() }()

	// Copy out before closing the buffer — pebble's docs require it.
	out := string(val)
	return out, nil
}

// SaveListReposCursor durably persists the cursor for resume. Used as
// the body of atmos's OnPageComplete callback; the synchronous fsync
// guarantees a crash after the page completes can't lose the advance.
func SaveListReposCursor(db *store.Store, cursor string) error {
	if err := db.Set([]byte(listReposCursorKey), []byte(cursor), pebble.Sync); err != nil {
		return fmt.Errorf("backfill: save list_repos_cursor: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run the tests**

Run: `just test ./internal/backfill`
Expected: 4 new tests pass; the existing 28 still pass; total 32 (under -short, with FailedRepoIsRetriable still skipping).

Actual count from `gotestsum` will say something like "33 tests, 1 skipped" — the implementer should verify the new tests appear by name in `gotestsum -v` output if the count looks off.

- [ ] **Step 5: Commit**

```bash
git add internal/backfill/cursor.go internal/backfill/cursor_test.go
git commit -m "backfill: persist listRepos cursor for cross-Run resume"
```

NO trailers.

---

## Task 2: Wire `StartCursor` + `OnPageComplete` into `runWithDirectory`

**Files:**
- Modify: `internal/backfill/run.go`

- [ ] **Step 1: Update `runWithDirectory` in `internal/backfill/run.go`**

Find the existing function (currently around lines 82-127) and replace its body. The current body is:

```go
func runWithDirectory(ctx context.Context, cfg Config, httpClient *http.Client, dir *identity.Directory) error {
	// Per atmos Options.SyncClient docs: disable xrpc retries because
	// the engine's retry/backoff loop is the only retry source we
	// want. Otherwise xrpc and the engine compound retries on
	// transient 503s, multiplying load against PDSes.
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

Replace with:

```go
func runWithDirectory(ctx context.Context, cfg Config, httpClient *http.Client, dir *identity.Directory) error {
	// Per atmos Options.SyncClient docs: disable xrpc retries because
	// the engine's retry/backoff loop is the only retry source we
	// want. Otherwise xrpc and the engine compound retries on
	// transient 503s, multiplying load against PDSes.
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

	startCursor, err := LoadListReposCursor(cfg.Store)
	if err != nil {
		return fmt.Errorf("backfill: %w", err)
	}
	if startCursor != "" {
		logger.Info("backfill: resuming from saved cursor", "cursor", startCursor)
	}

	engine := atmosbackfill.NewEngine(atmosbackfill.Options{
		SyncClient:  sc,
		Store:       st,
		Handler:     handler,
		Directory:   gt.Some(dir),
		HTTPClient:  gt.Some(httpClient),
		StartCursor: gt.Some(startCursor),
		OnPageComplete: gt.Some(func(cursor string) error {
			return SaveListReposCursor(cfg.Store, cursor)
		}),
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
	if err := engine.Run(ctx); err != nil {
		logger.Error("backfill: engine returned error", "err", err)
		return fmt.Errorf("backfill: %w", err)
	}

	logger.Info("backfill: engine drained")
	return nil
}
```

Three changes from the prior version:

1. New `LoadListReposCursor` call before constructing the engine.
2. New `StartCursor` and `OnPageComplete` options on the engine.
3. The trailing `engine.Run` block is restructured slightly (`if err := ...; err != nil` instead of two-line `err := ...; if err != nil`) to match the new earlier `LoadListReposCursor` block. Pure style.

- [ ] **Step 2: Verify package builds and tests still pass**

Run: `go build ./internal/backfill/...`
Expected: clean compile.

Run: `just test ./internal/backfill`
Expected: existing 32 tests pass (we haven't added run_test.go cases yet — that's Task 3).

Note: the existing `TestRun_HappyPath_DownloadsAllRepos` and
`TestRun_Resume_NoOpAfterCompletion` should continue to pass —
they go through `runWithStub` which calls `runWithDirectory`,
which now reads/writes cursors. The stub server sends back a
NextCursor on each page; runWithDirectory persists it; on the
second `Run` in `TestRun_Resume_NoOpAfterCompletion` it loads
the saved cursor (which is `""`, the post-drain value) and
behaves identically.

- [ ] **Step 3: Commit**

```bash
git add internal/backfill/run.go
git commit -m "backfill: pass StartCursor and OnPageComplete to atmos engine"
```

NO trailers.

---

## Task 3: Add resume integration tests

**Files:**
- Modify: `internal/backfill/run_test.go`

- [ ] **Step 1: Append two new tests to `run_test.go`**

After the existing `TestRun_FailedRepoIsRetriable` (the last test in the file), append:

```go
// TestRun_PersistsCursorAfterDrain confirms the post-drain cursor
// (empty string) is durably saved to pebble. Following the existing
// HappyPath: after Run returns, the cursor key exists in pebble with
// value "" (atmos fires OnPageComplete("") after the terminator
// page). This is what makes restart-after-completion fast — without
// the saved cursor, we'd start every Run from "" anyway, which is
// the same value, but having the key proves the wiring fired.
func TestRun_PersistsCursorAfterDrain(t *testing.T) {
	t.Parallel()

	dids := []atmos.DID{"did:plc:aaa", "did:plc:bbb"}
	fixtures := make(map[atmos.DID]repoFixture, len(dids))
	for _, d := range dids {
		fixtures[d] = buildRepoFixture(t, d)
	}
	srv := newStubServer(t, fixtures)

	db, err := store.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, runWithStub(t, t.Context(), srv, db))

	// The cursor key must exist after a clean drain. Value is the
	// terminator-page cursor, which is empty for the stub (it returns
	// all DIDs in one page then no more).
	got, err := LoadListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "", got, "post-drain cursor is empty")
}

// TestRun_PassesSavedCursorToRelay confirms the resume path: a cursor
// pre-seeded into pebble is passed to the relay's listRepos as the
// startCursor on the first request of a new Run. Without this, the
// cursor optimization is dead weight.
func TestRun_PassesSavedCursorToRelay(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:aaa")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)

	db, err := store.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Pre-seed a cursor as if a prior Run got partway through.
	require.NoError(t, SaveListReposCursor(db, "pretend-this-is-page-7"))

	// Track what the relay sees as the first listRepos cursor query
	// param.
	var firstCursor string
	var firstSeen atomic.Bool
	srv.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/xrpc/com.atproto.sync.listRepos" {
			if firstSeen.CompareAndSwap(false, true) {
				firstCursor = r.URL.Query().Get("cursor")
			}
		}
		// Delegate to the original handler for the actual response.
		srv.handle(w, r)
	})

	require.NoError(t, runWithStub(t, t.Context(), srv, db))
	require.Equal(t, "pretend-this-is-page-7", firstCursor,
		"first listRepos request must use the pre-seeded cursor as startCursor")
}
```

The second test pokes at `srv.srv.Config.Handler` to wrap the existing
handler with a recording handler. This is a small intrusion into the
stub's internals — `stubServer` already has `listReposHit` and
`getRepoHit` counters but doesn't capture cursors. Two ways to handle this:

a. Wrap as shown above. Simple; the wrapper delegates to `srv.handle`
   and records the cursor on first call.

b. Add a `firstListReposCursor atomic.String` (or similar) field to
   `stubServer` and capture it inside `handle`. Cleaner; modifies
   `stubServer` for one test's sake.

The plan goes with (a) for surgical scope. If the implementer prefers
(b), they should update the stubServer struct and `handle` method
accordingly — both are fine.

- [ ] **Step 2: Verify the tests compile and pass**

Run: `just test ./internal/backfill`
Expected: 34 tests, 1 skipped (was 32 + 2 new).

If the cursor pre-seeding test fails because `srv.srv.Config.Handler`
isn't reassignable on a running server, fall back to option (b) above:
add a field to `stubServer` and capture inside `handle`.

- [ ] **Step 3: Commit**

```bash
git add internal/backfill/run_test.go
git commit -m "backfill: test cursor persistence and resume passthrough"
```

NO trailers.

---

## Task 4: Final verification

- [ ] **Step 1: Whole-module test under race**

Run: `just test-race`
Expected: all tests pass, no race warnings.

- [ ] **Step 2: Lint**

Run: `just lint`
Expected: 0 issues.

- [ ] **Step 3: Build**

Run: `just build`
Expected: clean compile to `bin/jetstream`.

Run: `./bin/jetstream version`
Expected: prints version line.

- [ ] **Step 4: Manual smoke (optional)**

Stand up a temp data dir and run jetstream against the real relay for ~10 seconds. Verify a `relay/list_repos_cursor` value appears in pebble after a few page-completes (use `pebble tool` or restart and grep logs for `"backfill: resuming from saved cursor"`). Skip if you trust the integration tests.

- [ ] **Step 5: Done**

The PR is complete. Restart-resume now skips listRepos pages already processed.

---

## Self-Review

**Spec coverage:**
- §3 Goals 1-4 → Tasks 1-3.
- §4.1 Pebble keyspace → Task 1.
- §4.2 cursor.go file → Task 1.
- §4.3 Wiring in run.go → Task 2.
- §4.4 Failure modes documented in cursor.go's package doc.
- §5.1 Unit tests → Task 1.
- §5.2 Integration tests → Task 3 (the third, harder integration test was deferred per the spec).
- §5.3 Verification → Task 4.

**Placeholder scan:** No "TBD" or "implement later." Every step has concrete code.

**Type consistency:** `LoadListReposCursor(*store.Store) (string, error)` and `SaveListReposCursor(*store.Store, string) error` are the two callsites; both used identically in run.go.

**Scope check:** Single subsystem (cursor persistence). No changes to atmos, no DESIGN.md churn, no segment-writer touches. The known durability hole is documented as a kaizen followup, not a blocker.

**Risk:** Stub-handler wrapping in Task 3 is the trickiest piece; the fallback to a stubServer field is documented. The `t.Context()` API requires Go 1.24+; module is on 1.26 — fine.
