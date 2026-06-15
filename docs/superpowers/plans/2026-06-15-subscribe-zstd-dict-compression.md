# subscribe: v1 custom zstd-dictionary compression — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-enable the Jetstream v1 custom-zstd-dictionary compression scheme on `/subscribe` (currently rejected with a 400) for backwards compatibility, while documenting that RFC 7692 permessage-deflate is the preferred path.

**Architecture:** A subscriber opts in via `?compress=true` or `Socket-Encoding: zstd` and receives websocket **BinaryMessage** frames, each a zstd frame compressed with the v1 custom dictionary embedded into the `subscribe` package. Compressed bytes are lazily memoized per hot-ring `Entry` (one compression per event, shared across all zstd subscribers) via a shared `*zstd.Encoder`. Opting into zstd while also offering permessage-deflate is rejected with a 400; zstd clients get `CompressionDisabled` on the websocket Accept.

**Tech Stack:** Go, `github.com/klauspost/compress/zstd` (already whitelisted), `github.com/coder/websocket`, testify. Issue: #3.

---

## File Structure

- **Create** `internal/subscribe/zstd_dictionary` — the v1 dictionary file, copied verbatim from jetstream v1 (`pkg/models/zstd_dictionary`, 113 KB, dict ID 1612007021).
- **Create** `internal/subscribe/compress.go` — `//go:embed` of the dictionary, the package-level shared `*zstd.Encoder`, and a `compressFrame([]byte) []byte` helper. Responsibility: own the dictionary + encoder; nothing else imports the raw dictionary.
- **Create** `internal/subscribe/compress_test.go` — golden round-trip test (decode a frame with a dictionary reader) pinning the dict ID and encoder config.
- **Modify** `internal/subscribe/entry.go` — add `Compressed()` / `CompressedExtended()` memoized accessors.
- **Modify** `internal/subscribe/entry_test.go` — memoization tests for the new accessors.
- **Modify** `internal/subscribe/handler.go` — replace the 400-rejection with opt-in detection, mutual-exclusion 400, `CompressionDisabled` for zstd clients, and `compress bool` threaded into `runSubscriberLoop` with binary/text framing.
- **Modify** `internal/subscribe/handler_test.go` — rewrite the two rejection tests into acceptance tests; add the both-at-once 400 test.
- **Modify** `internal/subscribe/doc.go` — rewrite the "Cursor replay and compression" section.

---

## Task 1: Copy the dictionary and embed it with a shared encoder

**Files:**
- Create: `internal/subscribe/zstd_dictionary` (binary, copied)
- Create: `internal/subscribe/compress.go`
- Create: `internal/subscribe/compress_test.go`

- [ ] **Step 1: Copy the dictionary file verbatim**

Run:
```bash
cp /home/jcalabro/go/src/github.com/bluesky-social/jetstream/pkg/models/zstd_dictionary \
   /home/jcalabro/go/src/github.com/bluesky-social/jetstream-v2/internal/subscribe/zstd_dictionary
```

Verify it is the expected dictionary:
```bash
file internal/subscribe/zstd_dictionary
```
Expected: `internal/subscribe/zstd_dictionary: Zstandard dictionary (ID 1612007021)`

- [ ] **Step 2: Write the failing golden round-trip test**

Create `internal/subscribe/compress_test.go`:

```go
package subscribe

import (
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// TestCompressFrame_RoundTripsWithDictionaryReader pins the wire contract:
// a frame produced by compressFrame must decode, using a decoder seeded
// with the SAME embedded dictionary, back to the original bytes. This is
// exactly what a v1 client does (zstd.NewReader(nil, WithDecoderDicts(dict))).
func TestCompressFrame_RoundTripsWithDictionaryReader(t *testing.T) {
	t.Parallel()

	orig := []byte(`{"did":"did:plc:example","kind":"identity","time_us":1700000000000000}`)
	frame := compressFrame(orig)
	require.NotEmpty(t, frame)
	require.NotEqual(t, orig, frame, "frame must actually be compressed/encoded, not pass-through")

	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(zstdDictionary))
	require.NoError(t, err)
	defer dec.Close()

	got, err := dec.DecodeAll(frame, nil)
	require.NoError(t, err)
	require.Equal(t, orig, got, "dictionary-decoded frame must equal the original bytes")
}

// TestZstdDictionary_IsEmbedded guards against an empty / missing embed.
func TestZstdDictionary_IsEmbedded(t *testing.T) {
	t.Parallel()
	require.Greater(t, len(zstdDictionary), 100_000,
		"the v1 zstd dictionary is ~113 KB; a tiny value means the embed broke")
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `just test ./internal/subscribe -run 'TestCompressFrame_RoundTripsWithDictionaryReader|TestZstdDictionary_IsEmbedded'`
Expected: compile failure — `undefined: compressFrame`, `undefined: zstdDictionary`.

- [ ] **Step 4: Write the implementation**

Create `internal/subscribe/compress.go`:

```go
package subscribe

import (
	_ "embed"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// zstdDictionary is the Jetstream v1 custom zstd dictionary (dict ID
// 1612007021), copied verbatim from jetstream v1
// (pkg/models/zstd_dictionary). It is trained on the atproto firehose
// JSON and gives a better ratio than generic deflate on this
// small-message, highly repetitive stream.
//
// NOT PREFERRED. This custom-dictionary scheme exists only for
// backwards compatibility with v1 clients (?compress=true /
// Socket-Encoding: zstd). New consumers should use standard RFC 7692
// permessage-deflate, which v2 negotiates automatically and which needs
// no out-of-band dictionary.
//
//go:embed zstd_dictionary
var zstdDictionary []byte

// zstdEncoder is the process-wide encoder for the v1 compatibility
// scheme. klauspost/compress's EncodeAll is safe for concurrent use on a
// shared *zstd.Encoder, so one instance serves every subscriber. The
// configuration mirrors v1 exactly (WithEncoderDict + 128 KiB window +
// single-goroutine concurrency) so frames are byte-compatible with v1
// decoders.
var zstdEncoder = mustNewZstdEncoder()

func mustNewZstdEncoder() *zstd.Encoder {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderDict(zstdDictionary),
		zstd.WithWindowSize(1<<17),
		zstd.WithEncoderConcurrency(1))
	if err != nil {
		// The dictionary is embedded at build time; a failure here is a
		// build/programmer error, not runtime input. Fail loud.
		panic(fmt.Sprintf("subscribe: build zstd encoder: %v", err))
	}
	return enc
}

// compressFrame returns src compressed as a single zstd frame using the
// v1 custom dictionary. The result is a fresh slice (EncodeAll appends to
// a nil dst), safe to hand to a websocket write without aliasing src.
func compressFrame(src []byte) []byte {
	return zstdEncoder.EncodeAll(src, nil)
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `just test ./internal/subscribe -run 'TestCompressFrame_RoundTripsWithDictionaryReader|TestZstdDictionary_IsEmbedded'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -f internal/subscribe/zstd_dictionary
git add internal/subscribe/compress.go internal/subscribe/compress_test.go
git commit -m "$(cat <<'EOF'
subscribe: embed v1 zstd dictionary + shared encoder

Copies the v1 custom zstd dictionary (dict ID 1612007021) verbatim and
adds a shared encoder + compressFrame helper. Documented as a v1-compat
scheme that is not preferred over RFC 7692 permessage-deflate.

Refs #3

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

Note: `git add -f` is required because `/docs` and possibly other paths are gitignored; the dictionary is a normal source path under `internal/` and should add cleanly, but use `-f` only if `git status` shows it ignored. Verify with `git status` first.

---

## Task 2: Memoize compressed bytes per Entry

**Files:**
- Modify: `internal/subscribe/entry.go`
- Test: `internal/subscribe/entry_test.go`

- [ ] **Step 1: Write the failing memoization test**

Add to `internal/subscribe/entry_test.go`:

```go
func TestEntry_CompressedMemoizesOnceAndDecodes(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	e := newEntry(&segment.Event{Seq: 3, Kind: segment.KindIdentity, DID: "did:plc:c"})
	e.encodeFn = func(*segment.Event) ([]byte, error) {
		calls.Add(1)
		return []byte(`{"did":"did:plc:c","kind":"identity"}`), nil
	}

	const N = 50
	var wg sync.WaitGroup
	results := make([][]byte, N)
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			body, err := e.Compressed()
			require.NoError(t, err)
			results[i] = body
		}(i)
	}
	wg.Wait()

	require.Equal(t, int64(1), calls.Load(), "underlying JSON encode must run exactly once")
	for i := 0; i < N; i++ {
		require.Equal(t, results[0], results[i], "all callers see the same memoized frame")
	}

	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(zstdDictionary))
	require.NoError(t, err)
	defer dec.Close()
	got, err := dec.DecodeAll(results[0], nil)
	require.NoError(t, err)
	require.Equal(t, []byte(`{"did":"did:plc:c","kind":"identity"}`), got)
}

// TestEntry_CompressedPropagatesSkipSentinel ensures the compressed path
// surfaces errSkipEvent unchanged (so the loop advances without sending),
// exactly like the JSON path.
func TestEntry_CompressedPropagatesSkipSentinel(t *testing.T) {
	t.Parallel()
	e := newEntry(&segment.Event{Seq: 9, Kind: segment.KindSync, DID: "did:plc:s"})
	body, err := e.Compressed()
	require.ErrorIs(t, err, errSkipEvent)
	require.Nil(t, body)
}
```

Add the `zstd` import to the test file's import block:
```go
"github.com/klauspost/compress/zstd"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `just test ./internal/subscribe -run 'TestEntry_Compressed'`
Expected: compile failure — `e.Compressed undefined`.

- [ ] **Step 3: Add the memoized fields and accessors**

In `internal/subscribe/entry.go`, add fields to the `Entry` struct (after the existing `extendedErr` field):

```go
	compressedOnce sync.Once
	compressedBody []byte
	compressedErr  error

	compressedExtendedOnce sync.Once
	compressedExtendedBody []byte
	compressedExtendedErr  error
```

Add the accessors after `EncodedExtended`:

```go
// Compressed returns the memoized v1-shape JSON encoding compressed as a
// single zstd frame with the custom dictionary. It derives from Encoded
// (so the JSON encode is never duplicated) and runs the compression at
// most once per Entry, shared across every caught-up zstd subscriber. The
// error contract matches Encoded: errSkipEvent (advance, don't send) or an
// encode error (skip + log) is returned unchanged.
func (e *Entry) Compressed() ([]byte, error) {
	e.compressedOnce.Do(func() {
		body, err := e.Encoded()
		if err != nil {
			e.compressedErr = err
			return
		}
		e.compressedBody = compressFrame(body)
	})
	return e.compressedBody, e.compressedErr
}

// CompressedExtended is Compressed for the extended (v2) wire shape.
func (e *Entry) CompressedExtended() ([]byte, error) {
	e.compressedExtendedOnce.Do(func() {
		body, err := e.EncodedExtended()
		if err != nil {
			e.compressedExtendedErr = err
			return
		}
		e.compressedExtendedBody = compressFrame(body)
	})
	return e.compressedExtendedBody, e.compressedExtendedErr
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `just test ./internal/subscribe -run 'TestEntry_Compressed'`
Expected: PASS.

- [ ] **Step 5: Run the full Entry test set to confirm no regression**

Run: `just test ./internal/subscribe -run 'TestEntry'`
Expected: PASS (all existing memoization tests still green).

- [ ] **Step 6: Commit**

```bash
git add internal/subscribe/entry.go internal/subscribe/entry_test.go
git commit -m "$(cat <<'EOF'
subscribe: memoize per-Entry zstd-compressed frames

Compressed/CompressedExtended derive from the memoized JSON encoding and
compress at most once per event, shared across all zstd subscribers.
Error contract (errSkipEvent / encode error) matches the JSON path.

Refs #3

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Handler opt-in, mutual exclusion, and binary framing

**Files:**
- Modify: `internal/subscribe/handler.go` (replace block at `:91`; Accept at `:189`; `runSubscriberLoop` signature + write at `:404`; call site at `:251`)

- [ ] **Step 1: Replace the rejection block with opt-in + mutual-exclusion detection**

In `internal/subscribe/handler.go`, replace this block (currently at ~`:91`):

```go
	if values.Get("compress") == "true" || strings.Contains(r.Header.Get("Socket-Encoding"), "zstd") {
		http.Error(w, "compression not supported: jetstream v2 does not implement the v1 zstd-with-custom-dictionary scheme; remove ?compress=true and the Socket-Encoding header", http.StatusBadRequest)
		return
	}
```

with:

```go
	// v1 custom-zstd-dictionary opt-in. NOT PREFERRED — kept only for
	// backwards compatibility with v1 clients. New consumers should use
	// RFC 7692 permessage-deflate (negotiated automatically below). A
	// client must pick ONE scheme: opting into custom zstd while ALSO
	// offering permessage-deflate would double-compress (zstd output is
	// already entropy-coded), so we reject that combination loudly rather
	// than silently disabling one.
	wantZstd := values.Get("compress") == "true" ||
		strings.Contains(r.Header.Get("Socket-Encoding"), "zstd")
	if wantZstd && strings.Contains(r.Header.Get("Sec-WebSocket-Extensions"), "permessage-deflate") {
		http.Error(w, "choose one compression scheme: custom zstd (compress=true / Socket-Encoding: zstd) or RFC 7692 permessage-deflate, not both", http.StatusBadRequest)
		return
	}
```

- [ ] **Step 2: Select the compression mode by opt-in on Accept**

Replace the `websocket.Accept` call (currently at ~`:189`):

```go
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  []string{"*"},
		CompressionMode: websocket.CompressionContextTakeover,
	})
```

with:

```go
	// zstd clients do their own framing; permessage-deflate must NOT also
	// run (the mutual-exclusion 400 above guarantees they didn't offer it,
	// but disable explicitly so an Accept default can't re-enable it).
	// Non-zstd clients keep the default: deflate negotiated when offered.
	compressionMode := websocket.CompressionContextTakeover
	if wantZstd {
		compressionMode = websocket.CompressionDisabled
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  []string{"*"},
		CompressionMode: compressionMode,
	})
```

- [ ] **Step 3: Thread `compress` into the subscriber loop call**

Replace the `runSubscriberLoop` call (currently at ~`:251`):

```go
	runSubscriberLoop(ctx, conn, deps, &filterPtr, startSeq, extended, logger)
```

with:

```go
	runSubscriberLoop(ctx, conn, deps, &filterPtr, startSeq, extended, wantZstd, logger)
```

- [ ] **Step 4: Update `runSubscriberLoop` signature and framing**

Change the signature (currently at ~`:308`) to add `compress bool` before `logger`:

```go
func runSubscriberLoop(
	ctx context.Context,
	conn *websocket.Conn,
	deps Subscription,
	filterPtr *atomic.Pointer[Filter],
	startSeq uint64,
	extended bool,
	compress bool,
	logger *slog.Logger,
) {
```

Replace the encode + write section inside the `for _, e := range batch` loop (currently `:381`–`:408`):

```go
			var body []byte
			var eerr error
			if extended {
				body, eerr = e.EncodedExtended()
			} else {
				body, eerr = e.Encoded()
			}
			if errors.Is(eerr, errSkipEvent) {
				deps.Metrics.incEventsSkippedSync()
				continue
			}
			if eerr != nil {
				deps.Metrics.incEncodeErrors()
				logger.Warn("encode error", "err", eerr, "kind", int(e.Event.Kind), "did", e.Event.DID)
				continue
			}

			if max := f.MaxMessageSizeBytes(); max > 0 && uint32(len(body)) > max {
				deps.Metrics.incEventsOversize()
				continue
			}

			writeCtx, wcancel := context.WithTimeout(ctx, frameWriteTimeout)
			werr := conn.Write(writeCtx, websocket.MessageText, body)
			wcancel()
			if werr != nil {
				return
			}
```

with:

```go
			// Size cap is enforced on the UNCOMPRESSED JSON length even for
			// zstd clients: the cap bounds the logical record size a client
			// will accept, and comparing against unpredictable compressed
			// size (v1's behavior) would let a large record slip a small cap.
			// A deliberate, documented divergence from v1.
			var body []byte
			var eerr error
			if extended {
				body, eerr = e.EncodedExtended()
			} else {
				body, eerr = e.Encoded()
			}
			if errors.Is(eerr, errSkipEvent) {
				deps.Metrics.incEventsSkippedSync()
				continue
			}
			if eerr != nil {
				deps.Metrics.incEncodeErrors()
				logger.Warn("encode error", "err", eerr, "kind", int(e.Event.Kind), "did", e.Event.DID)
				continue
			}

			if max := f.MaxMessageSizeBytes(); max > 0 && uint32(len(body)) > max {
				deps.Metrics.incEventsOversize()
				continue
			}

			// Pick the wire payload + frame type by the connection's fixed
			// compression preference. The compressed accessors derive from
			// the same memoized JSON above, so the size cap (checked on the
			// uncompressed body) and the skip/encode-error branches already
			// hold; the only remaining failure is the compress step itself.
			msgType := websocket.MessageText
			payload := body
			if compress {
				var cerr error
				if extended {
					payload, cerr = e.CompressedExtended()
				} else {
					payload, cerr = e.Compressed()
				}
				if cerr != nil {
					deps.Metrics.incEncodeErrors()
					logger.Warn("compress error", "err", cerr, "kind", int(e.Event.Kind), "did", e.Event.DID)
					continue
				}
				msgType = websocket.MessageBinary
			}

			writeCtx, wcancel := context.WithTimeout(ctx, frameWriteTimeout)
			werr := conn.Write(writeCtx, msgType, payload)
			wcancel()
			if werr != nil {
				return
			}
```

- [ ] **Step 5: Verify it compiles and the package still builds**

Run: `just test ./internal/subscribe -run 'TestEntry_Compressed'`
Expected: PASS (compiles; the new handler code is exercised by Task 4 tests).

- [ ] **Step 6: Commit**

```bash
git add internal/subscribe/handler.go
git commit -m "$(cat <<'EOF'
subscribe: serve v1 custom-zstd frames on opt-in

Removes the 400 rejection: ?compress=true / Socket-Encoding: zstd now
opt a connection into binary zstd frames. Rejects offering both custom
zstd and permessage-deflate; disables deflate for zstd clients. Size cap
stays on the uncompressed body (deliberate divergence from v1).

Refs #3

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Rewrite rejection tests into acceptance tests

**Files:**
- Modify: `internal/subscribe/handler_test.go` (replace `TestHandler_RejectsCompressQueryParam` and `TestHandler_RejectsZstdSocketEncoding`; add the both-at-once test)

- [ ] **Step 1: Add a zstd decode helper near the other test helpers**

Add to `internal/subscribe/handler_test.go` (near `readOneFrame`, ~`:305`):

```go
// readOneZstdFrame reads one websocket frame and asserts it is a BINARY
// frame, then decodes it with the v1 custom dictionary — exactly what a
// v1 client does. Returns the decoded JSON bytes.
func readOneZstdFrame(t *testing.T, ctx context.Context, conn *websocket.Conn) []byte {
	t.Helper()
	rctx, rcancel := context.WithTimeout(ctx, 1*time.Second)
	defer rcancel()
	mt, frame, err := conn.Read(rctx)
	require.NoError(t, err)
	require.Equal(t, websocket.MessageBinary, mt, "zstd clients must receive binary frames")

	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(zstdDictionary))
	require.NoError(t, err)
	defer dec.Close()
	got, err := dec.DecodeAll(frame, nil)
	require.NoError(t, err, "frame must decode with the v1 custom dictionary")
	return got
}
```

Ensure the test file imports `"github.com/klauspost/compress/zstd"` (add to the import block if not already present).

- [ ] **Step 2: Replace `TestHandler_RejectsCompressQueryParam` with an acceptance test**

Replace the whole `TestHandler_RejectsCompressQueryParam` function with:

```go
// TestHandler_ZstdQueryParam_DeliversDictCompressedFrames verifies the v1
// custom-zstd opt-in via ?compress=true: frames arrive BINARY and decode,
// with the v1 dictionary, to the same JSON the uncompressed path emits.
func TestHandler_ZstdQueryParam_DeliversDictCompressedFrames(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "?compress=true"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// CompressionDisabled: the client must NOT offer permessage-deflate,
	// or the handler rejects the both-at-once combination.
	conn, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	time.Sleep(50 * time.Millisecond)
	var seq uint64
	publishIdentity(t, b, &seq, "did:plc:zstdquery", 1)

	got := readOneZstdFrame(t, ctx, conn)
	require.Contains(t, string(got), "did:plc:zstdquery")
	require.Contains(t, string(got), `"kind":"identity"`)
}
```

- [ ] **Step 3: Replace `TestHandler_RejectsZstdSocketEncoding` with a header-path acceptance test**

Replace the whole `TestHandler_RejectsZstdSocketEncoding` function with:

```go
// TestHandler_ZstdSocketEncodingHeader_DeliversDictCompressedFrames
// verifies the alternate v1 opt-in signal: Socket-Encoding: zstd. Same
// contract as the query-param path.
func TestHandler_ZstdSocketEncodingHeader_DeliversDictCompressedFrames(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		CompressionMode: websocket.CompressionDisabled,
		HTTPHeader:      http.Header{"Socket-Encoding": []string{"zstd"}},
	})
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	time.Sleep(50 * time.Millisecond)
	var seq uint64
	publishIdentity(t, b, &seq, "did:plc:zstdheader", 1)

	got := readOneZstdFrame(t, ctx, conn)
	require.Contains(t, string(got), "did:plc:zstdheader")
}
```

- [ ] **Step 4: Add the mutual-exclusion 400 test**

Add a new test (after the header acceptance test):

```go
// TestHandler_RejectsZstdAndDeflateTogether verifies the mutual-exclusion
// rule: a client opting into custom zstd (?compress=true) while ALSO
// offering RFC 7692 permessage-deflate is rejected with a 400, rather than
// silently double-compressing or dropping one scheme.
func TestHandler_RejectsZstdAndDeflateTogether(t *testing.T) {
	t.Parallel()

	st := newSteadyStateStore(t)
	b, err := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil, nil)
	require.NoError(t, err)
	h := NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Plain HTTP GET (not a ws Dial) so we can set the exact extension
	// offer header and read the 400 body. A real ws handshake also carries
	// Upgrade headers, but the handler's mutual-exclusion check runs before
	// the upgrade, so a 400 is returned regardless.
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"?compress=true", nil)
	require.NoError(t, err)
	req.Header.Set("Sec-WebSocket-Extensions", "permessage-deflate; client_max_window_bits")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	require.Contains(t, string(body), "choose one compression scheme",
		"the 400 body should explain zstd and permessage-deflate are mutually exclusive")
}
```

- [ ] **Step 5: Run the rewritten + new tests to verify they pass**

Run: `just test ./internal/subscribe -run 'TestHandler_Zstd|TestHandler_RejectsZstdAndDeflateTogether|TestHandler_AllowsCompressFalse|TestHandler_NegotiatesCompression|TestHandler_NoCompression'`
Expected: PASS — zstd query + header paths deliver decodable binary frames, both-at-once 400s, and the existing deflate / no-compression / compress=false tests still pass.

- [ ] **Step 6: Commit**

```bash
git add internal/subscribe/handler_test.go
git commit -m "$(cat <<'EOF'
subscribe: test v1 zstd opt-in delivers decodable binary frames

Rewrites the two rejection tests into acceptance tests proving query-param
and header opt-in yield dictionary-decodable binary frames; adds the
mutual-exclusion 400 (zstd + permessage-deflate) test.

Refs #3

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Rewrite the doc.go compression section

**Files:**
- Modify: `internal/subscribe/doc.go` (the "Cursor replay and compression" section, ~`:106`–`:124`)

- [ ] **Step 1: Replace the compression paragraph**

In `internal/subscribe/doc.go`, replace this block:

```go
// RFC 7692 permessage-deflate compression is negotiated when the client
// offers it (handler.go). The v1 zstd-with-custom-dictionary scheme
// (?compress=true / Socket-Encoding: zstd) is NOT supported and is
// rejected with a 400 so a v1 client fails loudly rather than receiving
// uncompressed frames it can't decode.
```

with:

```go
// Two compression schemes are offered, and a client may use at most one:
//
//   - RFC 7692 permessage-deflate (PREFERRED) is negotiated transparently
//     when the client offers it via Sec-WebSocket-Extensions (handler.go).
//     This is the recommended path: no out-of-band dictionary, standard
//     browser support, transparent on the read path.
//
//   - The v1 custom-zstd-dictionary scheme (?compress=true or
//     Socket-Encoding: zstd) is supported only for backwards compatibility
//     with v1 clients and is NOT preferred. Opted-in connections receive
//     binary websocket frames, each a zstd frame compressed with the v1
//     custom dictionary (dict ID 1612007021, embedded in compress.go). A
//     client decodes with zstd.NewReader(nil, WithDecoderDicts(dict)).
//
// Offering BOTH at once (zstd opt-in plus a permessage-deflate extension
// offer) is rejected with a 400: the two would double-compress, so the
// client must pick one. zstd clients have permessage-deflate disabled on
// the connection; the maxMessageSizeBytes cap is enforced on the
// uncompressed JSON length for all clients.
```

- [ ] **Step 2: Verify the package still builds and doc is well-formed**

Run: `just test ./internal/subscribe -run 'TestZstdDictionary_IsEmbedded'`
Expected: PASS (compiles; doc.go is comment-only).

- [ ] **Step 3: Commit**

```bash
git add internal/subscribe/doc.go
git commit -m "$(cat <<'EOF'
docs(subscribe): document v1 zstd scheme as supported-but-discouraged

Rewrites the compression section from "NOT supported / rejected" to
describe both schemes, the preference for permessage-deflate, the
mutual-exclusion rule, and uncompressed size-cap semantics.

Refs #3

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Full-package verification and lint

**Files:** none (verification only)

- [ ] **Step 1: Run the full subscribe package test suite (race)**

Run: `just test ./internal/subscribe`
Expected: PASS, suite under ~1s (AGENTS.md performance bar). The new tests publish a handful of small events; no slow paths added.

- [ ] **Step 2: Run lint**

Run: `just lint`
Expected: no new findings. (If `strings` was already imported in handler.go it stays; confirm no unused-import errors.)

- [ ] **Step 3: Confirm no other caller of `runSubscriberLoop` was missed**

Run: `grep -rn "runSubscriberLoop" internal/subscribe/`
Expected: only the definition and the single call site in `serve`, both with the new `compress bool` arg. No test calls it directly (the loop is exercised via the HTTP handler).

- [ ] **Step 4: Final commit if lint applied any fixes**

Only if `just lint` / `just modernize` changed files:

```bash
git add -A
git commit -m "$(cat <<'EOF'
subscribe: lint/modernize cleanup for zstd compression

Refs #3

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
EOF
)"
```

---

## Notes / out of scope (kaizen)

- **cmd/client decoder** — server-side only this pass. If end-to-end round-trip validation through the load-test client is wanted, file a follow-up issue.
- **Metrics** — compress errors reuse `incEncodeErrors`; if operators want to distinguish compress failures from JSON-encode failures, a dedicated `incCompressErrors` counter is a clean follow-up (not required for parity).
- The final PR description (or the commit closing the work) should carry `Closes #3` so the issue auto-closes on merge to `main`.
