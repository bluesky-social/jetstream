# `requireHello` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `?requireHello=true` query-param support to `/subscribe`, exactly matching jetstream v1's contract: pause event delivery until the client sends a valid `options_update`, disconnect on any invalid update during the wait.

**Architecture:** Single-file change in `internal/subscribe/handler.go`. Parse `requireHello` alongside the existing query parse. Allocate a `chan struct{}` + `sync.Once` per connection. Reader goroutine signals the channel when the first valid `options_update` arrives. The main `serve` body waits on that signal (or `ctx.Done()`) BEFORE calling `broadcaster.Subscribe()`, so the broadcaster never queues a single event for a pre-hello client.

**Tech Stack:** Go 1.24, `coder/websocket`, `sync.Once`, `httptest.NewServer`, `testify/require`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-05-27-subscribe-require-hello-design.md`

---

## File Map

| File | Change |
|------|--------|
| `internal/subscribe/handler.go` | Parse `requireHello`. Add `helloCh`/`sync.Once`. Move `Subscribe()` after the hello wait. New helper `parseRequireHello`. |
| `internal/subscribe/handler_test.go` | Add 5 integration tests + 1 table-driven unit test. Reuse existing `readOneFrame`, `publishCommit`, `publishIdentity`, `jsonMust` helpers. |
| `internal/subscribe/doc.go` | Move `requireHello` from the "Out of scope" list to the V1 PARITY list. |

No new files. No new types. No metric changes.

---

## Reusable context every implementer needs

**Existing test helpers in `internal/subscribe/handler_test.go`:**
- `newSteadyStateStore(t *testing.T) *store.Store` — opens a store and writes `PhaseSteadyState` so the readiness gate passes.
- `readOneFrame(t, ctx, conn) []byte` — reads one text frame with a 1s deadline; t.Fatals on error.
- `publishIdentity(t, b, did, indexedAt)` — publishes a minimal identity event.
- `publishCommit(t, b, did, collection, indexedAt)` — publishes a minimal create commit.
- `jsonMust[T any](t, v) []byte` — JSON-marshals or t.Fatals.

**Existing types/symbols you'll use:**
- `SubscriberSourcedMessage{Type string, Payload json.RawMessage}`
- `UpdatePayload{WantedCollections []string, WantedDIDs []string, MaxMessageSizeBytes int}`
- `SubMessageTypeOptionsUpdate = "options_update"`
- `websocket.StatusInvalidFramePayloadData`, `websocket.StatusPolicyViolation`, `websocket.CloseStatus(err)`

**Test execution from repo root:** `just test` runs the full suite. To run subscribe-package tests fast: `go test ./internal/subscribe/... -run <Pattern> -race -count=1`.

**Branch:** Work happens on a new branch `require-hello` cut from `main`. Each task ends in a commit. No squashing between tasks.

---

## Task list (overview)

1. Cut the branch and add the `parseRequireHello` helper + table-driven test.
2. Plumb `requireHello` through `serve` (no behavior change yet — just parse and ignore).
3. Add `helloCh` + `sync.Once`, signal from the reader goroutine.
4. Move `broadcaster.Subscribe()` after the hello wait. This is the behavior change.
5. Add `TestHandler_RequireHello_BlocksUntilOptionsUpdate` — happy path lockdown.
6. Add `TestHandler_RequireHello_InvalidUpdateDisconnects` — invalid-update disconnect lockdown.
7. Add `TestHandler_RequireHello_FalseHasNoEffect` — `requireHello!=true` is a no-op.
8. Add `TestHandler_RequireHello_NoLeakOnClientDisconnect` — robustness against client-side disconnect during wait.
9. Add `TestHandler_RequireHello_MultipleUpdatesDoNotPanic` — robustness against repeated updates.
10. Update `doc.go` (V1 PARITY list).
11. Final verification: full `just test`, lint, race.

---

### Task 1: Branch + `parseRequireHello` helper

**Files:**
- Modify: `internal/subscribe/handler.go` (add helper near `truncateCloseReason`)
- Modify: `internal/subscribe/handler_test.go` (add table-driven test)

The helper is intentionally tiny and isolated so the parse rule (`requireHello == "true"`, case-sensitive) is locked down by a unit test independent of the websocket plumbing. Locking this down first prevents drift from "be liberal in what you accept" instincts later.

- [ ] **Step 1.1: Cut the branch**

```bash
git checkout main
git pull --ff-only
git checkout -b require-hello
```

Expected: `git status` shows clean working tree on `require-hello`.

- [ ] **Step 1.2: Write the failing test**

Append to `internal/subscribe/handler_test.go`:

```go
func TestParseRequireHello(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want bool
	}{
		{"true", true},
		{"True", false},
		{"TRUE", false},
		{"false", false},
		{"1", false},
		{"", false},
		{"yes", false},
		{"true ", false},
		{" true", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			values := url.Values{}
			if tc.in != "" {
				values.Set("requireHello", tc.in)
			}
			require.Equal(t, tc.want, parseRequireHello(values))
		})
	}
}
```

Note: `net/url` import will need to be added to the test file's imports if not already present. (It is — handler_test.go already uses url.Values via earlier filter tests indirectly, but verify with `goimports -w` or add manually.)

- [ ] **Step 1.3: Run the test, verify it fails**

```
go test ./internal/subscribe/... -run TestParseRequireHello -count=1
```

Expected: FAIL with `undefined: parseRequireHello` and (if `url` import is missing) an additional unused/unimported error.

- [ ] **Step 1.4: Add the helper**

In `internal/subscribe/handler.go`, just below `truncateCloseReason`, add:

```go
// parseRequireHello returns true iff the requireHello query parameter is
// exactly the literal string "true". V1 PARITY: jetstream v1 uses
//
//	c.Request().URL.Query().Get("requireHello") == "true"
//
// so any other value — including "True", "1", or "yes" — is treated as
// false. Existing v1 clients send "true" or omit the param; we must not
// "be liberal in what we accept" or we change wire semantics for them.
// TestParseRequireHello locks this down — touch with care.
func parseRequireHello(values url.Values) bool {
	return values.Get("requireHello") == "true"
}
```

The `url` package is already imported by handler.go (line 10). No import changes needed.

- [ ] **Step 1.5: Run the test, verify it passes**

```
go test ./internal/subscribe/... -run TestParseRequireHello -count=1 -race
```

Expected: PASS.

- [ ] **Step 1.6: Run the full subscribe package tests to confirm no regressions**

```
go test ./internal/subscribe/... -count=1 -race
```

Expected: PASS (all existing tests + the new one).

- [ ] **Step 1.7: Commit**

```bash
git add internal/subscribe/handler.go internal/subscribe/handler_test.go
git commit -m "subscribe: add parseRequireHello helper

Extracts the v1-parity 'exactly the literal string \"true\"' rule into
a tiny helper so the parse semantics are locked down independent of
the websocket plumbing."
```

---

### Task 2: Plumb `requireHello` through `serve` (no behavior yet)

**Files:**
- Modify: `internal/subscribe/handler.go` (call `parseRequireHello` in `serve`, store the result, but do nothing with it yet)

This task is intentionally a no-op behavior change. We extract the bool, hold it in scope, and confirm the existing test suite still passes. The next two tasks add the actual hello-gating. Splitting it like this keeps each commit small and bisectable.

- [ ] **Step 2.1: Modify `serve` to extract the bool**

In `internal/subscribe/handler.go`, immediately after the `initialFilter, perr := ParseQuery(values)` block (around line 68-72), add:

```go
	requireHello := parseRequireHello(values)
	_ = requireHello // wired up in Task 4
```

Specifically, change this region:

```go
	initialFilter, perr := ParseQuery(values)
	if perr != nil {
		http.Error(w, perr.Error(), http.StatusBadRequest)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
```

to:

```go
	initialFilter, perr := ParseQuery(values)
	if perr != nil {
		http.Error(w, perr.Error(), http.StatusBadRequest)
		return
	}

	requireHello := parseRequireHello(values)
	_ = requireHello // wired up in Task 4

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
```

- [ ] **Step 2.2: Run the full subscribe tests**

```
go test ./internal/subscribe/... -count=1 -race
```

Expected: PASS — no behavior change, same tests pass.

- [ ] **Step 2.3: Run vet to catch the unused-var case**

```
go vet ./internal/subscribe/...
```

Expected: clean (the `_ =` discard is sufficient).

- [ ] **Step 2.4: Commit**

```bash
git add internal/subscribe/handler.go
git commit -m "subscribe: parse requireHello in serve

Extracts the bool but does not yet act on it. Behavior unchanged.
Wiring lands in the next two commits."
```

---

### Task 3: Add `helloCh` and signal it from the reader goroutine

**Files:**
- Modify: `internal/subscribe/handler.go` (allocate channel + Once, signal on valid options_update)

Still no externally observable behavior change. We allocate the signaling primitive and wire the reader to fire it; nothing waits on it yet. Doing this as its own task keeps the diff small.

- [ ] **Step 3.1: Add the `sync` import**

In `internal/subscribe/handler.go`, find the import block (lines 3-18) and add `"sync"` alphabetically. The new import block should include:

```go
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/coder/websocket"
)
```

- [ ] **Step 3.2: Allocate `helloCh` and the Once before the reader goroutine**

In `serve`, immediately AFTER the `filterPtr.Store(initialFilter)` line and BEFORE `subCh, doneCh, unsubscribe := broadcaster.Subscribe()`, insert:

```go
	// helloCh is closed by the reader goroutine on the first valid
	// options_update IFF requireHello is set. The signal is idempotent
	// via sync.Once so a chatty client sending multiple updates doesn't
	// panic on a closed channel. The pre-Subscribe wait below selects
	// on this channel and ctx.Done().
	helloCh := make(chan struct{})
	var helloOnce sync.Once
	signalHello := func() {
		helloOnce.Do(func() { close(helloCh) })
	}
```

The `signalHello` closure captures `helloCh` and `helloOnce`. Reader goroutine will call it; the writer side will read from `helloCh`.

- [ ] **Step 3.3: Call `signalHello` from the reader on valid options_update**

In the reader goroutine inside `serve`, find the happy-path `options_update` branch:

```go
				newFilter, err := ParseUpdatePayload(update)
				if err != nil {
					m.incOptionsUpdateError(optionsUpdateErrorReasonInvalidOptions)
					reason := truncateCloseReason(err.Error())
					_ = conn.Close(websocket.StatusPolicyViolation, reason)
					return
				}
				filterPtr.Store(newFilter)
				m.incOptionsUpdates()
```

Change the last two lines to:

```go
				filterPtr.Store(newFilter)
				m.incOptionsUpdates()
				signalHello() // no-op if not waiting (Task 4 makes it gate Subscribe)
```

- [ ] **Step 3.4: Suppress the unused-variable warning**

Update the temporary discard in `serve` from:

```go
	requireHello := parseRequireHello(values)
	_ = requireHello // wired up in Task 4
```

to:

```go
	requireHello := parseRequireHello(values)
	_ = requireHello   // wait/select wired up in Task 4
	_ = helloCh        // reader signals; writer-side wait wired in Task 4
```

(Both discards drop in Task 4.)

- [ ] **Step 3.5: Run the full subscribe tests**

```
go test ./internal/subscribe/... -count=1 -race
```

Expected: PASS — `signalHello` runs but no goroutine waits on `helloCh`, so behavior is unchanged.

- [ ] **Step 3.6: Commit**

```bash
git add internal/subscribe/handler.go
git commit -m "subscribe: signal hello channel on valid options_update

Allocates a per-connection helloCh (sync.Once-guarded for idempotence)
and signals it from the reader goroutine when a valid options_update
arrives. Nothing waits on the channel yet; that gate lands next."
```

---

### Task 4: Gate `broadcaster.Subscribe()` on hello

**Files:**
- Modify: `internal/subscribe/handler.go` (move Subscribe after the wait, drop the temporary `_ =` discards)

This is the actual behavior change. Subscribe — and therefore the per-connection bounded channel that receives broadcaster events — does not exist until either the client sends a valid options_update OR the connection is cancelled.

**Important ordering invariants:**
- The reader goroutine must already be running when we wait on `helloCh`, because it's the goroutine that signals it.
- `ctx, cancel := context.WithCancel(r.Context())` must already exist, because the reader goroutine `defer cancel()`s on read errors and the wait selects on `<-ctx.Done()`.
- The reader goroutine references `subCh` / `doneCh` / `unsubscribe` exactly zero times — those are writer-loop-only — so moving `Subscribe()` after the reader goroutine starts is safe.

- [ ] **Step 4.1: Reorder the body of `serve`**

The current shape (after Task 3) is roughly:

```go
filterPtr.Store(initialFilter)

helloCh := make(chan struct{})
var helloOnce sync.Once
signalHello := func() { helloOnce.Do(func() { close(helloCh) }) }

subCh, doneCh, unsubscribe := broadcaster.Subscribe()
defer unsubscribe()

ctx, cancel := context.WithCancel(r.Context())
defer cancel()

go func() { /* reader, calls signalHello */ }()

// ping ticker + writer loop
```

Restructure to:

```go
filterPtr.Store(initialFilter)

helloCh := make(chan struct{})
var helloOnce sync.Once
signalHello := func() { helloOnce.Do(func() { close(helloCh) }) }

ctx, cancel := context.WithCancel(r.Context())
defer cancel()

// Reader goroutine starts BEFORE Subscribe so it can signal hello
// without any events queued behind it.
go func() { /* reader, calls signalHello */ }()

if requireHello {
	// V1 PARITY: pause replay/live-tail until the first valid
	// options_update. Matches v1 README §"Options Updates":
	// "a client can connect with ?requireHello=true ... to pause
	// replay/live-tail until the first Options Update message is
	// sent by the client over the socket."
	//
	// Invalid updates during the wait disconnect the client via the
	// reader goroutine's existing close paths, which cancel ctx.
	select {
	case <-helloCh:
	case <-ctx.Done():
		return
	}
}

subCh, doneCh, unsubscribe := broadcaster.Subscribe()
defer unsubscribe()

// ping ticker + writer loop (unchanged below this point)
```

Drop the two `_ =` discards that were temporary in Tasks 2 and 3.

- [ ] **Step 4.2: Verify the `serve` body in full**

After the edit, the `serve` body from line ~83 onward should read approximately like the snippet above. Read the file and confirm.

- [ ] **Step 4.3: Run the full subscribe tests**

```
go test ./internal/subscribe/... -count=1 -race
```

Expected: PASS — none of the existing tests use `requireHello=true`, so behavior is unchanged for them. The new guard only fires when `requireHello` is the literal string `"true"`.

- [ ] **Step 4.4: Run the whole-repo lint**

```
just lint
```

(or `golangci-lint run ./...` if `just` isn't on path).

Expected: clean.

- [ ] **Step 4.5: Commit**

```bash
git add internal/subscribe/handler.go
git commit -m "subscribe: gate broadcaster.Subscribe on requireHello

V1 PARITY: ?requireHello=true blocks event delivery until the first
valid options_update lands. Implemented by delaying broadcaster.Subscribe
until after the hello signal (or ctx cancel). The broadcaster never
queues an event for a pre-hello client, so no drain logic is needed."
```

---

### Task 5: Test — `TestHandler_RequireHello_BlocksUntilOptionsUpdate`

**Files:**
- Modify: `internal/subscribe/handler_test.go` (append the test)

This test is the primary lockdown for the v1 contract: no events before hello, and events flow normally after hello.

- [ ] **Step 5.1: Add a small helper for short-deadline reads**

If not already present (it isn't — `readOneFrame` uses a fixed 1s deadline), add this helper near `readOneFrame` in `handler_test.go`:

```go
// readOneFrameWithin reads one text frame with the given timeout.
// Returns the frame bytes on success, or nil + error if the deadline
// hit (typical use: assert "no frame within X").
func readOneFrameWithin(ctx context.Context, conn *websocket.Conn, d time.Duration) ([]byte, error) {
	rctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	mt, frame, err := conn.Read(rctx)
	if err != nil {
		return nil, err
	}
	if mt != websocket.MessageText {
		return nil, fmt.Errorf("unexpected message type %v", mt)
	}
	return frame, nil
}
```

- [ ] **Step 5.2: Add the test**

Append to `handler_test.go`:

```go
func TestHandler_RequireHello_BlocksUntilOptionsUpdate(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)

	m := NewMetrics()
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), m)
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?requireHello=true"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	// Give the handler time to start the reader goroutine but NOT
	// time to subscribe (it shouldn't subscribe until hello).
	time.Sleep(50 * time.Millisecond)

	// Publish a matching event. Because Subscribe() hasn't been called
	// yet, the broadcaster has no per-connection channel to queue this
	// into — the event must be dropped silently.
	publishIdentity(t, b, "did:plc:pre-hello", 1)

	// Confirm the client receives nothing within 100ms.
	_, rerr := readOneFrameWithin(ctx, conn, 100*time.Millisecond)
	require.Error(t, rerr, "must not receive a frame before hello")
	require.True(t, errors.Is(rerr, context.DeadlineExceeded),
		"expected DeadlineExceeded, got %v", rerr)

	// Send the hello.
	hello := SubscriberSourcedMessage{
		Type:    SubMessageTypeOptionsUpdate,
		Payload: jsonMust(t, UpdatePayload{}),
	}
	require.NoError(t, conn.Write(ctx, websocket.MessageText, jsonMust(t, hello)))

	// Give the handler time to subscribe.
	time.Sleep(50 * time.Millisecond)

	// Publish a fresh event AFTER hello. Only this one should arrive.
	publishIdentity(t, b, "did:plc:post-hello", 2)

	frame := readOneFrame(t, ctx, conn)
	require.Contains(t, string(frame), "did:plc:post-hello")
	require.NotContains(t, string(frame), "did:plc:pre-hello",
		"pre-hello publish must be dropped, not queued")
}
```

You'll need `"errors"` in the test file's import block — verify it's there. (It's used by other tests; should already be present.)

- [ ] **Step 5.3: Run the test, verify it passes**

```
go test ./internal/subscribe/... -run TestHandler_RequireHello_BlocksUntilOptionsUpdate -count=1 -race
```

Expected: PASS.

- [ ] **Step 5.4: Run the full subscribe suite for regressions**

```
go test ./internal/subscribe/... -count=1 -race
```

Expected: PASS.

- [ ] **Step 5.5: Commit**

```bash
git add internal/subscribe/handler_test.go
git commit -m "subscribe: lock down requireHello blocks-until-hello contract

Integration test: connect with ?requireHello=true, publish a matching
event, confirm the client receives nothing for 100ms, send a valid
options_update, publish again, confirm only the post-hello event
arrives (the pre-hello publish is dropped because Subscribe hadn't
run yet)."
```

---

### Task 6: Test — `TestHandler_RequireHello_InvalidUpdateDisconnects`

**Files:**
- Modify: `internal/subscribe/handler_test.go` (append the test)

Locks down the v1 README rule: "Invalid Options Updates in `requireHello` mode or normal operating mode will result in the client being disconnected." Three sub-cases cover each existing close-status path.

- [ ] **Step 6.1: Add the test**

Append to `handler_test.go`:

```go
func TestHandler_RequireHello_InvalidUpdateDisconnects(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		frame     []byte
		wantClose websocket.StatusCode
	}{
		{
			name:      "malformed envelope JSON",
			frame:     []byte(`{`),
			wantClose: websocket.StatusInvalidFramePayloadData,
		},
		{
			name:      "well-formed envelope with bad payload JSON",
			frame:     []byte(`{"type":"options_update","payload":"not-json"}`),
			wantClose: websocket.StatusInvalidFramePayloadData,
		},
		{
			name: "well-formed payload with bad DID",
			frame: jsonMust(t, SubscriberSourcedMessage{
				Type: SubMessageTypeOptionsUpdate,
				Payload: jsonMust(t, UpdatePayload{
					WantedDIDs: []string{"not-a-did"},
				}),
			}),
			wantClose: websocket.StatusPolicyViolation,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := newSteadyStateStore(t)
			b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			require.NoError(t, err)

			m := NewMetrics()
			h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), m)
			srv := httptest.NewServer(h)
			defer srv.Close()

			wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?requireHello=true"

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conn, resp, err := websocket.Dial(ctx, wsURL, nil)
			require.NoError(t, err)
			if resp != nil && resp.Body != nil {
				defer func() { _ = resp.Body.Close() }()
			}
			defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

			require.NoError(t, conn.Write(ctx, websocket.MessageText, tc.frame))

			// The server should close the connection. Read returns the
			// close as an error; CloseStatus extracts the code.
			_, _, rerr := conn.Read(ctx)
			require.Error(t, rerr)
			require.Equal(t, tc.wantClose, websocket.CloseStatus(rerr),
				"close status mismatch (err=%v)", rerr)
		})
	}
}
```

- [ ] **Step 6.2: Run the test, verify it passes**

```
go test ./internal/subscribe/... -run TestHandler_RequireHello_InvalidUpdateDisconnects -count=1 -race
```

Expected: PASS for all three sub-cases.

- [ ] **Step 6.3: Run the full subscribe suite**

```
go test ./internal/subscribe/... -count=1 -race
```

Expected: PASS.

- [ ] **Step 6.4: Commit**

```bash
git add internal/subscribe/handler_test.go
git commit -m "subscribe: lock down invalid-update-disconnects in requireHello mode

Three sub-cases cover the three close-status paths the reader goroutine
already implements: malformed envelope JSON, bad payload JSON, and a
well-formed payload that fails ParseUpdatePayload (e.g. invalid DID).
Each must close the connection with the expected status code."
```

---

### Task 7: Test — `TestHandler_RequireHello_FalseHasNoEffect`

**Files:**
- Modify: `internal/subscribe/handler_test.go` (append the test)

Locks down the "exactly the literal string `true`" rule end-to-end: anything else (including the absence of the param) means events flow immediately, no hello required.

- [ ] **Step 7.1: Add the test**

Append to `handler_test.go`:

```go
func TestHandler_RequireHello_FalseHasNoEffect(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// queryFragment is appended to "ws://..." with "?" already
		// present iff non-empty. "" means no query string at all.
		queryFragment string
	}{
		{"absent", ""},
		{"false", "?requireHello=false"},
		{"capitalized True", "?requireHello=True"},
		{"garbage", "?requireHello=garbage"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := newSteadyStateStore(t)
			b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			require.NoError(t, err)

			m := NewMetrics()
			h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), m)
			srv := httptest.NewServer(h)
			defer srv.Close()

			wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + tc.queryFragment

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conn, resp, err := websocket.Dial(ctx, wsURL, nil)
			require.NoError(t, err)
			if resp != nil && resp.Body != nil {
				defer func() { _ = resp.Body.Close() }()
			}
			defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

			// Wait for the handler to register the subscriber.
			time.Sleep(50 * time.Millisecond)

			// No hello sent. Publish and expect immediate delivery.
			publishIdentity(t, b, "did:plc:no-hello-needed", 1)

			frame := readOneFrame(t, ctx, conn)
			require.Contains(t, string(frame), "did:plc:no-hello-needed")
		})
	}
}
```

- [ ] **Step 7.2: Run the test**

```
go test ./internal/subscribe/... -run TestHandler_RequireHello_FalseHasNoEffect -count=1 -race
```

Expected: PASS for all four sub-cases.

- [ ] **Step 7.3: Run the full subscribe suite**

```
go test ./internal/subscribe/... -count=1 -race
```

Expected: PASS.

- [ ] **Step 7.4: Commit**

```bash
git add internal/subscribe/handler_test.go
git commit -m "subscribe: lock down requireHello-not-true is a no-op

Four sub-cases (absent, false, True, garbage) confirm that anything
other than the literal lowercase string \"true\" leaves the handler
in normal flow: events arrive immediately without a hello."
```

---

### Task 8: Test — `TestHandler_RequireHello_NoLeakOnClientDisconnect`

**Files:**
- Modify: `internal/subscribe/handler_test.go` (append the test)

Robustness: a client that connects with `?requireHello=true` and then disconnects (without sending hello) must NOT leak the handler goroutine. The wait must exit via `ctx.Done()`.

We assert by observing the broadcaster's subscriber count via the existing `Broadcaster.SubscriberCount()` method — if the handler exited cleanly, the subscriber count returns to 0 (it never went above 0 in the first place, since Subscribe is gated; but the test still validates the handler returned).

Check whether `SubscriberCount` exists; if not, fall back to a goroutine-leak detector.

- [ ] **Step 8.1: Confirm the broadcaster API**

Run:

```
grep -n "func (b \*Broadcaster)" internal/subscribe/broadcaster.go
```

If a `SubscriberCount() int` method exists, use it. If not, the assertion is a goroutine count check via `runtime.NumGoroutine` before/after with a small tolerance. The test below assumes `SubscriberCount()` exists; adapt if it doesn't.

- [ ] **Step 8.2: Add the test**

Append to `handler_test.go`:

```go
func TestHandler_RequireHello_NoLeakOnClientDisconnect(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)

	m := NewMetrics()
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), m)
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?requireHello=true"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}

	// Let the handler get into its hello wait.
	time.Sleep(50 * time.Millisecond)

	// Subscriber count should be 0 — Subscribe hasn't been called yet
	// because hello hasn't fired.
	require.Equal(t, 0, b.SubscriberCount(), "must not subscribe before hello")

	// Client closes without sending hello.
	require.NoError(t, conn.Close(websocket.StatusNormalClosure, "go away"))

	// Wait for the handler to notice and return. Poll up to 1s.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if b.SubscriberCount() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Subscriber count must still be 0 (it never went above 0).
	require.Equal(t, 0, b.SubscriberCount(),
		"handler should have returned without ever subscribing")

	// Smoke check: srv.Close() will block forever if the handler
	// goroutine is still running. The deferred srv.Close() at top of
	// function provides this assertion implicitly via the test
	// timeout, but make it explicit by closing here too.
	srv.Close()
}
```

If `Broadcaster` has no `SubscriberCount()` method, replace the count assertions with a `runtime.NumGoroutine()` snapshot:

```go
// Adapter version if SubscriberCount doesn't exist:
import "runtime"

before := runtime.NumGoroutine()
// ... connect, sleep, close conn ...
deadline := time.Now().Add(time.Second)
for time.Now().Before(deadline) {
	if runtime.NumGoroutine() <= before+1 {
		break
	}
	time.Sleep(10 * time.Millisecond)
}
require.LessOrEqual(t, runtime.NumGoroutine(), before+2,
	"handler goroutine appears to have leaked")
```

The `+2` tolerance accounts for the test runner's own bookkeeping goroutines. Pick whichever assertion the codebase supports.

- [ ] **Step 8.3: Run the test**

```
go test ./internal/subscribe/... -run TestHandler_RequireHello_NoLeakOnClientDisconnect -count=1 -race
```

Expected: PASS.

- [ ] **Step 8.4: Run the full subscribe suite**

```
go test ./internal/subscribe/... -count=1 -race
```

Expected: PASS.

- [ ] **Step 8.5: Commit**

```bash
git add internal/subscribe/handler_test.go
git commit -m "subscribe: assert no leak when client disconnects mid-hello-wait

Connect with requireHello=true, close the client side without sending
hello, confirm the handler exits cleanly (broadcaster subscriber count
stays at 0, never went up because Subscribe is hello-gated)."
```

**Note for the implementer:** As of this writing the `Broadcaster` type does NOT expose a `SubscriberCount()` method (verified by `grep "func (b \*Broadcaster)" internal/subscribe/broadcaster.go`). Use the `runtime.NumGoroutine()` fallback shown in Step 8.2. Do NOT add a `SubscriberCount()` method just for this test — that's scope creep.

---

### Task 9: Test — `TestHandler_RequireHello_MultipleUpdatesDoNotPanic`

**Files:**
- Modify: `internal/subscribe/handler_test.go` (append the test)

Robustness: a chatty client that sends multiple `options_update` frames (the second one would race with the close in Task 3 if not for the `sync.Once` guard) must not panic on a closed channel. Locks down the `Once`.

- [ ] **Step 9.1: Add the test**

Append to `handler_test.go`:

```go
func TestHandler_RequireHello_MultipleUpdatesDoNotPanic(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)

	m := NewMetrics()
	h := NewHandler(b, st, slog.New(slog.NewTextHandler(io.Discard, nil)), m)
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?requireHello=true"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	hello := SubscriberSourcedMessage{
		Type:    SubMessageTypeOptionsUpdate,
		Payload: jsonMust(t, UpdatePayload{}),
	}
	helloBytes := jsonMust(t, hello)

	// Send the first hello.
	require.NoError(t, conn.Write(ctx, websocket.MessageText, helloBytes))
	// Immediately send a second valid options_update.
	require.NoError(t, conn.Write(ctx, websocket.MessageText, helloBytes))
	// And a third, for good measure.
	require.NoError(t, conn.Write(ctx, websocket.MessageText, helloBytes))

	// Give the handler time to process all three and Subscribe.
	time.Sleep(100 * time.Millisecond)

	// Confirm normal flow works after the chatty start.
	publishIdentity(t, b, "did:plc:still-flowing", 1)
	frame := readOneFrame(t, ctx, conn)
	require.Contains(t, string(frame), "did:plc:still-flowing")
}
```

- [ ] **Step 9.2: Run the test**

```
go test ./internal/subscribe/... -run TestHandler_RequireHello_MultipleUpdatesDoNotPanic -count=1 -race
```

Expected: PASS, no race-detector report.

- [ ] **Step 9.3: Run the full subscribe suite**

```
go test ./internal/subscribe/... -count=1 -race
```

Expected: PASS.

- [ ] **Step 9.4: Commit**

```bash
git add internal/subscribe/handler_test.go
git commit -m "subscribe: assert chatty clients don't panic the hello signal

Three back-to-back valid options_update frames must not double-close
the helloCh — the sync.Once guard makes the signal idempotent. After
all three, normal event flow continues."
```

---

### Task 10: Update `internal/subscribe/doc.go`

**Files:**
- Modify: `internal/subscribe/doc.go` (move `requireHello` between sections, lines 63-67 currently)

doc.go currently lists `requireHello` in the "Out of scope" block. Now that it's implemented, it belongs in the V1 PARITY block.

- [ ] **Step 10.1: Read the current doc.go**

```
cat internal/subscribe/doc.go
```

Confirm the V1 PARITY list ends around line 62 with the "commit with empty collection" bullet and the "Out of scope" paragraph at lines 63-67 reads:

```
// Out of scope for this v1-compat surface: cursor replay, zstd
// compression, requireHello. We accept those query params
// (silently ignored for cursor; absent for requireHello) so that v1
// clients that send them aren't rejected. Future v2-native endpoints
// will live alongside this package or in a sibling.
```

- [ ] **Step 10.2: Add the V1 PARITY bullet for requireHello**

Add a new bullet to the V1 PARITY list, immediately after the "commit with an empty collection field bypasses" bullet and before the blank `//` separator that precedes the "Out of scope" paragraph:

```go
//   - ?requireHello=true blocks event delivery (the broadcaster
//     Subscribe call is delayed) until the client sends a valid
//     options_update over the websocket. Matches v1 README:
//     "a client can connect with ?requireHello=true ... to pause
//     replay/live-tail until the first Options Update message is
//     sent by the client over the socket." Locked down by
//     TestHandler_RequireHello_BlocksUntilOptionsUpdate. Invalid
//     updates during the wait disconnect the client; locked down by
//     TestHandler_RequireHello_InvalidUpdateDisconnects.
```

- [ ] **Step 10.3: Remove `requireHello` from the "Out of scope" paragraph**

Change:

```go
// Out of scope for this v1-compat surface: cursor replay, zstd
// compression, requireHello. We accept those query params
// (silently ignored for cursor; absent for requireHello) so that v1
// clients that send them aren't rejected. Future v2-native endpoints
// will live alongside this package or in a sibling.
```

to:

```go
// Out of scope for this v1-compat surface: cursor replay, zstd
// compression. We silently ignore the cursor query param so that
// v1 clients that send it aren't rejected. Future v2-native
// endpoints will live alongside this package or in a sibling.
```

- [ ] **Step 10.4: Confirm with `go doc`**

```
go doc github.com/bluesky-social/jetstream-v2/internal/subscribe
```

Expected: the package doc displays the new V1 PARITY bullet for requireHello and no longer mentions requireHello in the "Out of scope" paragraph.

- [ ] **Step 10.5: Run the test suite for sanity**

```
go test ./internal/subscribe/... -count=1 -race
```

Expected: PASS (doc-only change shouldn't break anything).

- [ ] **Step 10.6: Commit**

```bash
git add internal/subscribe/doc.go
git commit -m "subscribe: document requireHello in V1 PARITY list

Moves requireHello from 'out of scope' to the V1 PARITY block now
that it's implemented. Points readers at the lockdown tests."
```

---

### Task 11: Final verification

**Files:** none modified — verification only.

- [ ] **Step 11.1: Full project test run**

```
just test
```

Expected: all tests pass, race-clean.

- [ ] **Step 11.2: Lint the whole project**

```
just lint
```

Expected: clean.

- [ ] **Step 11.3: Verify the branch shape**

```
git log --oneline main..HEAD
```

Expected: 10 commits (Tasks 1 through 10), each with a focused message. Final commit is the doc.go update.

- [ ] **Step 11.4: Verify the diff is small and contained**

```
git diff --stat main..HEAD
```

Expected: changes only in `internal/subscribe/handler.go`, `internal/subscribe/handler_test.go`, and `internal/subscribe/doc.go`. No other files touched.

- [ ] **Step 11.5: Hand off**

Summarize the branch's work to the user, with commit count, test count added, and lint status. Ask whether to invoke `superpowers:finishing-a-development-branch` or whether the user wants to handle integration themselves.

---

## Self-review

**Spec coverage:**
- Wire surface (`requireHello == "true"` parse rule) → Task 1 helper + Task 6 sub-cases.
- Control flow change (delay Subscribe) → Tasks 2/3/4.
- Invalid-update-disconnects rule → Task 6.
- No-leak on disconnect → Task 8.
- Multiple-updates idempotence (`sync.Once`) → Task 9.
- "no events queued before hello" assertion → Task 5 (post-hello publish must not contain the pre-hello DID).
- Pre-hello no-ping behavior → matched by code (no ping ticker until after the wait); covered implicitly by Task 8 (the connection survives idle until the client closes it).
- doc.go V1 PARITY documentation → Task 10.

**Placeholder scan:** none — every step has concrete code, exact commands, and concrete commit messages.

**Type consistency:**
- `parseRequireHello(values url.Values) bool` — used identically in Tasks 1, 2, and the verification commands.
- `helloCh chan struct{}` and `helloOnce sync.Once` — defined in Task 3 and referenced unchanged in Task 4.
- Test helpers (`readOneFrame`, `publishIdentity`, `jsonMust`, `SubscriberSourcedMessage`, `UpdatePayload`, `SubMessageTypeOptionsUpdate`) — names match the existing handler_test.go and filter.go.

**Open hazards for implementer attention:**
- Task 8 references `b.SubscriberCount()` which DOES NOT exist. The fallback to `runtime.NumGoroutine()` is documented inline in Step 8.2 and re-emphasized in the post-task note. Use the fallback.
- Task 5 uses a 50ms sleep before asserting "no events" — flakiness risk on heavily loaded CI. The 100ms read deadline that follows gives enough headroom that a slow scheduler still produces `DeadlineExceeded` rather than a false-negative receive.

