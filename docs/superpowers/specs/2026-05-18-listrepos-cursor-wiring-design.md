# Wire atmos's `StartCursor` + `OnPageComplete` into jetstream's backfill

## 1. Summary

Persist the relay's `listRepos` cursor in pebble after every page boundary,
and load it on `Run` startup so a restart skips listRepos pages that were
fully processed in a prior Run. Uses atmos v0.0.16's new
`backfill.Options.StartCursor` and `OnPageComplete` surface (added in
`/home/jcalabro/go/src/github.com/jcalabro/atmos/docs/superpowers/specs/2026-05-18-listrepos-cursor-design.md`).

Today, restart means re-walking ~30M `listRepos` entries from cursor `""`,
each one paying a pebble `Get` to discover it's already at
`StatusComplete`. With cursor persistence, a clean restart pays nothing
beyond a single `Get` for the saved cursor.

## 2. Goals

1. On `Run` startup, load the most recent persisted listRepos cursor (if
   any) from pebble and pass it to atmos as `Options.StartCursor`.
2. Wire `Options.OnPageComplete` to a callback that durably writes the
   cursor argument to pebble before returning.
3. Add a single new pebble key, `relay/list_repos_cursor`, to hold the
   saved cursor.
4. Tests cover: first-time load (no row → empty cursor), mid-Run save
   (each page advances the persisted value), restart-resume (a fresh
   `Run` reads the saved cursor and passes it to atmos).

## 3. Non-Goals

- **Per-relay keying.** The persisted cursor is a single global value
  at `relay/list_repos_cursor`. Operators changing `--relay-url` between
  runs accept the consequence: the new relay sees an opaque-to-it
  cursor and either ignores it (starting from the beginning) or 400s.
  Per atproto spec, cursors are opaque-but-stable per relay; cross-relay
  cursors are undefined behavior. Documenting this is enough.
- **DESIGN.md update.** §3.5 describes per-DID resume; the listRepos
  cursor is an implementation optimization on top of the same
  resume model. No protocol-level change.
- **Readiness gate.** The HTTP server keeps serving its current
  routes; we don't gate readiness on backfill completion in this PR.
- **Cursor invalidation on schema bump.** A future schema change can
  add a version byte to the persisted value; for now the cursor is
  stored as a bare string.

## 4. Architecture

### 4.1 Pebble keyspace

Add one singleton key:

```
relay/list_repos_cursor    -> string (the cursor to use as startCursor on next Run)
```

Empty value means "no cursor saved" (semantically equivalent to absent
row). Both atmos and the relay treat `startCursor=""` as "start from
the beginning," so the load path normalizes "no row" and "row with
empty value" to the same outcome.

### 4.2 New file: `internal/backfill/cursor.go`

A small file with two functions and one constant. Lives next to
`store.go` because both deal with the same pebble db.

```go
// listReposCursorKey is the pebble key for the persisted listRepos
// resume cursor. Singleton (no per-relay namespacing — operators
// changing relay accept the cross-relay opaque-cursor risk and should
// clear this key manually if needed).
const listReposCursorKey = "relay/list_repos_cursor"

// LoadListReposCursor reads the persisted cursor from pebble. Returns
// "" if no cursor has been saved (a fresh data dir or a never-completed
// page). Errors only on pebble I/O failure.
func LoadListReposCursor(db *store.Store) (string, error)

// SaveListReposCursor durably persists the cursor for resume. Used as
// the body of atmos's OnPageComplete callback. Synchronous fsync so a
// crash after the page completes can't lose the advance.
func SaveListReposCursor(db *store.Store, cursor string) error
```

These are exported because they're consumed from `run.go` (different
file, same package, so unexported would also work — but exported keeps
the symmetry with `NewStore`/`NewLogHandler`/`NewMetrics` and lets
ops scripts or future tests poke at them directly).

Implementation detail: writes use `pebble.Sync`, same as the per-DID
write path. The cursor save is a single small key-value put per page
(~1 every 1000 DIDs at the protocol's listRepos cap), so the fsync
cost is negligible relative to repo downloads.

### 4.3 Wiring in `run.go`

`Run` and `runWithDirectory` thread the cursor through:

```go
func runWithDirectory(ctx context.Context, cfg Config, httpClient *http.Client, dir *identity.Directory) error {
    // ... existing xrpc/sync client setup ...

    startCursor, err := LoadListReposCursor(cfg.Store)
    if err != nil {
        return fmt.Errorf("backfill: load cursor: %w", err)
    }

    if startCursor != "" {
        cfg.Logger.Info("backfill: resuming from saved cursor", "cursor", startCursor)
    }

    engine := atmosbackfill.NewEngine(atmosbackfill.Options{
        SyncClient:  sc,
        Store:       st,
        Handler:     handler,
        Directory:   gt.Some(dir),
        HTTPClient:  gt.Some(httpClient),
        StartCursor: gt.Some(startCursor),
        OnPageComplete: gt.Some(func(cursor string) error {
            if err := SaveListReposCursor(cfg.Store, cursor); err != nil {
                return fmt.Errorf("save cursor: %w", err)
            }
            return nil
        }),
        OnError:    /* unchanged */,
        OnProgress: /* unchanged */,
    })

    // ... existing engine.Run + logging ...
}
```

Note: even when the saved cursor is empty (`""`), we still pass it as
`StartCursor`. atmos's `ValOr("")` already does this internally when
the option is None, but passing it explicitly is harmless and keeps
the flow uniform. We also still install `OnPageComplete` on first-time
runs so the cursor advances as we go.

### 4.4 Failure modes

- **Pebble read fails on Run startup.** `Run` returns the wrapped
  error and the errgroup cancels the server. Acceptable: pebble I/O
  errors at startup are fatal anyway (we couldn't have looked up
  per-DID rows either).
- **Pebble write fails inside OnPageComplete.** atmos's
  `producerLoop` wraps the callback's error and returns it from
  `engine.Run`, which our code wraps as `backfill: %w` and returns.
  Same as any other Run-aborting failure. The persisted cursor stays
  at the previous page; on restart we re-process from there. No data
  loss — at-least-once delivery + per-DID Lookup-skip handles it.
- **Cursor saved but worker pool didn't drain.** Per atmos's
  contract, OnPageComplete fires AFTER all of page N's eligible jobs
  are on the worker channel. Workers may still be downloading. On
  process kill, those workers' DIDs stay at `StateDiscovered`. On
  restart with the saved cursor, atmos resumes at page N+1 and never
  re-walks page N — but those DIDs are at `StateDiscovered`, not
  `StateComplete`, so they're stuck in limbo until a future
  cursor-less Run rediscovers them.

  This is a real durability hole. Two ways to close it:

  a. **Periodic cursor reset.** A future Run with `--rewalk` (or
     similar) clears the saved cursor before starting. Lets operators
     manually trigger a full re-walk after suspecting orphans.

  b. **Hold-off on cursor advance until workers drain page N.**
     Strictly correct but breaks atmos's design (producer/worker
     concurrency).

  Option (a) is the right call for this PR — document it as a
  followup, not a blocker. The hole only matters if the process is
  killed mid-page-flush and the orphaned DIDs are never seen again,
  which in practice means after the entire network has been
  re-enumerated successfully — a rare condition.

  Note this in the cursor.go doc.

### 4.5 Operator notes

A few one-liners worth surfacing in the cursor.go package doc:

- The cursor is opaque per atproto spec. Treat it as a black box.
- Cursors are not portable across relays. If you change `--relay-url`,
  delete this key (or rebuild the data dir).
- Persistence happens at every listRepos page boundary, on the
  producer goroutine, with `pebble.Sync`. Cost: 1 small put per
  ~1000 DIDs.

## 5. Test Plan

Tests live in `internal/backfill/cursor_test.go` and `internal/backfill/run_test.go`.

### 5.1 Unit tests (`cursor_test.go`)

- `TestLoadListReposCursor_Empty` — fresh pebble, returns `""`, no error.
- `TestSaveLoadListReposCursor_RoundTrip` — Save then Load returns the
  exact value.
- `TestSaveListReposCursor_Overwrites` — second Save replaces the first
  (cursor advances forward, never appended).
- `TestSaveListReposCursor_EmptyValue` — Saving `""` is allowed; Load
  afterwards returns `""`. Treats "explicitly cleared" the same as
  "never set." This is the post-drain state too: atmos fires
  `OnPageComplete("")` for the final page.

### 5.2 Integration tests (`run_test.go`)

Add to the existing run_test.go (which already has `runWithStub`):

- `TestRun_Resume_UsesPersistedCursor` — first Run completes (3 DIDs);
  pebble has `relay/list_repos_cursor` set to `""` (the final-page
  cursor). Second Run starts, reads `""`, passes it to atmos, walks
  listRepos from the beginning AGAIN — but every DID is already
  Complete, so no `getRepo` fires. Same shape as the existing
  `TestRun_Resume_NoOpAfterCompletion` plus a pebble assertion that
  the cursor key is present.

- `TestRun_Resume_PicksUpAtSavedCursor` — three DIDs across two
  listRepos pages (the existing `stubServer` paginates 3-per-page,
  so we'd need 4+ DIDs to get two pages). First Run is killed
  mid-pages-2-onward by passing a cancelled ctx after page 1. Pebble
  has `relay/list_repos_cursor` set to page-1's NextCursor (the
  third DID's identifier per `stubServer`'s convention). Second Run
  reads that cursor, passes it as startCursor, and the stub server
  receives it on its first listRepos call. Assert via the stub's
  request log.

- `TestRun_OnPageCompleteWriteFails_AbortsRun` — inject a fault into
  the pebble write path (close the db before the second page would
  fire OnPageComplete; or simpler, close the Store after the first
  Run and assert the second Run with the broken Store fails). This
  one's tricky to set up cleanly; defer if the harness gets too
  complicated.

The third test is "nice to have." If wiring it gets gnarly, replace
with a unit-level test on `SaveListReposCursor` against a closed
pebble db (which returns an error from `Set`).

### 5.3 Verification

`just test`, `just lint`, `just test-race` all green.

## 6. Migration Notes

None. The first `Run` after this PR ships finds no `relay/list_repos_cursor`
row, treats it as "" (start from beginning), and behaves identically to
the previous shape — except it now writes the cursor as it goes.

Operators with deployed jetstream instances upgrade in place; the next
restart benefits from the persisted cursor.

## 7. Open Questions

None — all resolved during brainstorming:

- Cursor key: single global `relay/list_repos_cursor`.
- DESIGN.md update: deferred (implementation optimization, not protocol).
- Readiness gate: deferred to segment-writing PR.
- Per-page durability hole (orphaned `StateDiscovered` rows after a
  kill mid-flush): documented; mitigation via manual `--rewalk` is a
  followup.

## 8. References

- Atmos cursor design: `/home/jcalabro/go/src/github.com/jcalabro/atmos/docs/superpowers/specs/2026-05-18-listrepos-cursor-design.md`
- Atmos cursor plan: `/home/jcalabro/go/src/github.com/jcalabro/atmos/docs/superpowers/plans/2026-05-18-listrepos-cursor.md`
- DESIGN.md §3.5 (Metadata Store)
- DESIGN.md §4.1 (Bootstrap Phase)
- atproto spec for `com.atproto.sync.listRepos`: cursor is opaque,
  stable, valid for resumption.
