# Sync 1.1 Upgrade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade `github.com/jcalabro/atmos` from v0.0.16 → v0.1.0 and turn on Sync 1.1 verification end-to-end in the livestream consumer with pebble-backed durable state.

**Architecture:** Two new packages (`internal/syncstate`, `internal/identitycache`) implement atmos's `sync.StateStore` and `identity.Cache` interfaces against the existing pebble store. `cmd/jetstream/main.go` constructs `*sync.Verifier` from those primitives and threads it through `livestream.Config.Verifier`. The block-flush-anchored cursor advance from the live-firehose PR is preserved unchanged.

**Tech Stack:** Go 1.26, atmos v0.1.0, pebble v1.1.2, encoding/binary + encoding/json for state encoding.

**Reference implementation:** `/home/jcalabro/go/src/github.com/jcalabro/atp/subscribe.go` and `state.go` show a minimal Sync 1.1 consumer with a SQLite-backed StateStore. Useful crib for VerifierOptions field choices and getRepo HTTP client tuning. We deviate from atp by using pebble (not sqlite), persisting identity-directory resolutions (atp uses an in-memory LRU), and refusing to delegate cursor persistence to atmos's `streaming.Options.CursorStore` (we keep `OnAfterFlush`-anchored cursor advance to preserve the segment-durability invariant).

**Spec:** `docs/superpowers/specs/2026-05-22-sync-1-1-upgrade-design.md`.

---

## Task 1: Bump atmos v0.0.16 → v0.1.0 with no behavior change

A self-contained upgrade commit so any compilation surprise from the bump is isolated. We preserve legacy behavior by passing `Verifier: gt.Some[*sync.Verifier](nil)` (atmos v0.1.0's explicit opt-out) — the streaming layer would otherwise auto-attach an in-memory verifier that would fail signature verification on every event in tests that don't bring real DID resolution.

**Files:**
- Modify: `go.mod`
- Modify: `go.sum`
- Modify: `internal/livestream/consumer.go` — Run() builds `streaming.Options`

- [ ] **Step 1: Bump atmos**

Run: `go get github.com/jcalabro/atmos@v0.1.0 && go mod tidy`
Expected: `go.mod` shows `github.com/jcalabro/atmos v0.1.0`; `go.sum` updated.

- [ ] **Step 2: Run tests to find compilation breaks**

Run: `just test ./...`
Expected: compile errors in `internal/livestream/consumer.go`. The atmos v0.1.0 `streaming.Options` struct may have renamed/dropped fields. Triage the errors before writing fixes.

- [ ] **Step 3: Update streaming.Options in Consumer.Run**

In `internal/livestream/consumer.go`, the existing block:

```go
opts := streaming.Options{
    URL:        wsURL,
    Cursor:     gt.Some(startCursor),
    SyncClient: gt.Some[*atmossync.Client](nil), // disable auto-resync; out of scope
    OnReconnect: gt.Some(func(attempt int, delay time.Duration) {
        c.cfg.Metrics.incReconnects()
        c.cfg.Logger.Warn("livestream: reconnecting",
            "attempt", attempt,
            "delay", delay,
        )
    }),
}
```

Becomes:

```go
opts := streaming.Options{
    URL:    wsURL,
    Cursor: gt.Some(startCursor),
    // Verifier=nil opts out of the streaming layer's auto-attach.
    // Sync 1.1 verification will be wired in a later commit; this
    // commit only bumps the atmos version with no behavior change.
    Verifier: gt.Some[*atmossync.Verifier](nil),
    OnReconnect: gt.Some(func(attempt int, delay time.Duration) {
        c.cfg.Metrics.incReconnects()
        c.cfg.Logger.Warn("livestream: reconnecting",
            "attempt", attempt,
            "delay", delay,
        )
    }),
}
```

The `SyncClient: gt.Some[*atmossync.Client](nil)` opt-out is replaced because it no longer disables verification on its own under v0.1.0 — `Verifier` is now the explicit knob. `SyncClient` in v0.1.0 only overrides the auto-fetch sync client used inside `Event.Operations()` for `#sync` events, which we don't need to override because the streaming layer auto-creates one from the WebSocket URL.

- [ ] **Step 4: Run tests**

Run: `just test ./...`
Expected: PASS. No behavioral change versus the previous commit.

- [ ] **Step 5: Run race tests**

Run: `just test-race ./internal/livestream`
Expected: PASS.

- [ ] **Step 6: Run lint**

Run: `just lint`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add go.mod go.sum internal/livestream/consumer.go
git commit -m "$(cat <<'EOF'
chore(deps): bump atmos v0.0.16 -> v0.1.0 with verifier opt-out

The Verifier knob in streaming.Options replaces SyncClient as the
explicit way to disable Sync 1.1 verification. Pass gt.Some(nil) so
the streaming layer doesn't auto-attach an in-memory verifier; full
Sync 1.1 wiring lands in a later commit.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: `internal/syncstate` — pebble-backed `sync.StateStore`

Self-contained package implementing atmos's `sync.StateStore` interface against `*store.Store`. Owns key prefix `sync/chain/<did>` and `sync/host/<did>`. Hand-rolled compact binary encoding with a leading version byte.

**Files:**
- Create: `internal/syncstate/doc.go`
- Create: `internal/syncstate/store.go`
- Create: `internal/syncstate/encoding.go`
- Create: `internal/syncstate/store_test.go`
- Create: `internal/syncstate/encoding_test.go`
- Create: `internal/syncstate/swarm_test.go`

- [ ] **Step 1: Write failing encoding tests**

Create `internal/syncstate/encoding_test.go`:

```go
package syncstate

import (
    "testing"

    "github.com/jcalabro/atmos/cbor"
    atmossync "github.com/jcalabro/atmos/sync"
    "github.com/stretchr/testify/require"
)

// fixedCID returns a deterministic cbor.CID for tests.
func fixedCID(t *testing.T) cbor.CID {
    t.Helper()
    // bafyreigwexh... a known-good 32-byte sha256 + 1-byte dag-cbor codec.
    cid, err := cbor.ParseCIDString("bafyreigwexhqswvbgxqe5w7tnbcc7g5oh54oas5jewopl5jpcsjp3lk7vy")
    require.NoError(t, err)
    return cid
}

func TestEncodeChainState_RoundTrip(t *testing.T) {
    t.Parallel()
    in := atmossync.ChainState{Rev: "3l3qo2vutsw2b", Data: fixedCID(t)}

    buf, err := encodeChainState(in)
    require.NoError(t, err)
    require.Greater(t, len(buf), 0)

    out, err := decodeChainState(buf)
    require.NoError(t, err)
    require.Equal(t, in.Rev, out.Rev)
    require.True(t, in.Data.Equal(out.Data))
}

func TestEncodeChainState_RejectsZeroCID(t *testing.T) {
    t.Parallel()
    _, err := encodeChainState(atmossync.ChainState{Rev: "rev"})
    require.Error(t, err)
    require.Contains(t, err.Error(), "zero CID")
}

func TestDecodeChainState_RejectsTruncated(t *testing.T) {
    t.Parallel()
    _, err := decodeChainState([]byte{0x01, 0x00})
    require.Error(t, err)
}

func TestDecodeChainState_RejectsUnknownVersion(t *testing.T) {
    t.Parallel()
    // valid v1 encoding then mutate the leading version byte
    in := atmossync.ChainState{Rev: "rev", Data: fixedCID(t)}
    buf, err := encodeChainState(in)
    require.NoError(t, err)
    buf[0] = 0xFF // bogus version

    _, err = decodeChainState(buf)
    require.Error(t, err)
    require.Contains(t, err.Error(), "unknown")
}

func TestEncodeHostingState_RoundTrip(t *testing.T) {
    t.Parallel()
    in := atmossync.HostingState{
        Active: false,
        Status: "takendown",
        Seq:    12345,
        Time:   "2026-05-21T00:00:00Z",
    }
    buf, err := encodeHostingState(in)
    require.NoError(t, err)

    out, err := decodeHostingState(buf)
    require.NoError(t, err)
    require.Equal(t, in, out)
}

func TestEncodeHostingState_ActiveZeroSeq(t *testing.T) {
    t.Parallel()
    // Active=true, Status="", Seq=0, Time="" — verifier-default-y zero values
    in := atmossync.HostingState{Active: true}
    buf, err := encodeHostingState(in)
    require.NoError(t, err)

    out, err := decodeHostingState(buf)
    require.NoError(t, err)
    require.Equal(t, in, out)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `just test ./internal/syncstate -run TestEncode`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Create the package skeleton**

Create `internal/syncstate/doc.go`:

```go
// Package syncstate persists atmos sync.StateStore in pebble. It owns
// the key prefixes "sync/chain/<did>" (per-DID rev + MST root for the
// last accepted commit) and "sync/host/<did>" (per-DID hosting status
// from the last accepted #account event). State survives restarts so
// the verifier doesn't accept the next event for each DID as ground
// truth after a process restart, the way MemStateStore does.
//
// Encoding is hand-rolled compact binary with a leading version byte:
//
//	chain state v1: [0x01][rev_len uvarint][rev bytes][cid_bytes 33B]
//	host state v1:  [0x01][active u8][status_len uvarint][status][seq u64][time_len uvarint][time]
//
// We don't reuse atmos's CBOR helpers (sync.ChainState / HostingState
// have none anyway) because the records are small and fixed-shape, and
// we want a schema we control. The version byte gives us a forward-
// compat exit hatch if atmos extends the types — readers refuse
// unknown versions rather than silently truncating.
//
// pebble.Sync is used on every Save: per-DID chain state is the
// verifier's source of truth, and a silent revert to a pre-crash value
// would create a chain break the verifier resolves by triggering a
// resync against the account's PDS. Resyncs are rate-limited per DID,
// so a flurry of unnecessary resyncs degrades the live archive.
package syncstate
```

Create `internal/syncstate/encoding.go`:

```go
package syncstate

import (
    "encoding/binary"
    "fmt"

    "github.com/jcalabro/atmos/cbor"
    atmossync "github.com/jcalabro/atmos/sync"
)

const (
    chainStateV1 = 0x01
    hostStateV1  = 0x01

    cidByteLen = 33 // cbor.CID is 32-byte sha256 + 1-byte codec
)

// encodeChainState serializes a sync.ChainState to a compact binary
// shape. Refuses to encode a zero CID — the StateStore contract
// returns (nil, nil) for absent state, so an explicit zero-CID save
// would be ambiguous on read.
func encodeChainState(s atmossync.ChainState) ([]byte, error) {
    if !s.Data.Defined() {
        return nil, fmt.Errorf("syncstate: refuse to encode chain state with zero CID")
    }
    cidBytes := s.Data.Bytes()
    if len(cidBytes) != cidByteLen {
        return nil, fmt.Errorf("syncstate: cbor.CID emitted %d bytes (want %d)", len(cidBytes), cidByteLen)
    }

    // 1B version + uvarint rev_len + rev + 33B CID
    buf := make([]byte, 0, 1+binary.MaxVarintLen64+len(s.Rev)+cidByteLen)
    buf = append(buf, chainStateV1)
    buf = binary.AppendUvarint(buf, uint64(len(s.Rev)))
    buf = append(buf, s.Rev...)
    buf = append(buf, cidBytes...)
    return buf, nil
}

func decodeChainState(buf []byte) (atmossync.ChainState, error) {
    if len(buf) < 1 {
        return atmossync.ChainState{}, fmt.Errorf("syncstate: chain state too short (len=%d)", len(buf))
    }
    if buf[0] != chainStateV1 {
        return atmossync.ChainState{}, fmt.Errorf("syncstate: unknown chain state version 0x%02x", buf[0])
    }
    pos := 1
    revLen, n := binary.Uvarint(buf[pos:])
    if n <= 0 {
        return atmossync.ChainState{}, fmt.Errorf("syncstate: chain state rev length malformed")
    }
    pos += n
    if uint64(len(buf)-pos) < revLen+cidByteLen {
        return atmossync.ChainState{}, fmt.Errorf("syncstate: chain state truncated (need %d more bytes)", revLen+cidByteLen)
    }
    rev := string(buf[pos : pos+int(revLen)])
    pos += int(revLen)
    cid, err := cbor.ParseCIDBytes(buf[pos : pos+cidByteLen])
    if err != nil {
        return atmossync.ChainState{}, fmt.Errorf("syncstate: parse chain state CID: %w", err)
    }
    return atmossync.ChainState{Rev: rev, Data: cid}, nil
}

func encodeHostingState(s atmossync.HostingState) ([]byte, error) {
    // 1B version + 1B active + uvarint status_len + status + 8B seq + uvarint time_len + time
    size := 1 + 1 + binary.MaxVarintLen64 + len(s.Status) + 8 + binary.MaxVarintLen64 + len(s.Time)
    buf := make([]byte, 0, size)
    buf = append(buf, hostStateV1)
    if s.Active {
        buf = append(buf, 1)
    } else {
        buf = append(buf, 0)
    }
    buf = binary.AppendUvarint(buf, uint64(len(s.Status)))
    buf = append(buf, s.Status...)
    var seq [8]byte
    binary.BigEndian.PutUint64(seq[:], uint64(s.Seq))
    buf = append(buf, seq[:]...)
    buf = binary.AppendUvarint(buf, uint64(len(s.Time)))
    buf = append(buf, s.Time...)
    return buf, nil
}

func decodeHostingState(buf []byte) (atmossync.HostingState, error) {
    if len(buf) < 2 {
        return atmossync.HostingState{}, fmt.Errorf("syncstate: hosting state too short (len=%d)", len(buf))
    }
    if buf[0] != hostStateV1 {
        return atmossync.HostingState{}, fmt.Errorf("syncstate: unknown hosting state version 0x%02x", buf[0])
    }
    pos := 1
    active := buf[pos] != 0
    pos++
    statusLen, n := binary.Uvarint(buf[pos:])
    if n <= 0 {
        return atmossync.HostingState{}, fmt.Errorf("syncstate: hosting state status length malformed")
    }
    pos += n
    if uint64(len(buf)-pos) < statusLen+8 {
        return atmossync.HostingState{}, fmt.Errorf("syncstate: hosting state truncated at status")
    }
    status := string(buf[pos : pos+int(statusLen)])
    pos += int(statusLen)
    seq := int64(binary.BigEndian.Uint64(buf[pos : pos+8]))
    pos += 8
    timeLen, n := binary.Uvarint(buf[pos:])
    if n <= 0 {
        return atmossync.HostingState{}, fmt.Errorf("syncstate: hosting state time length malformed")
    }
    pos += n
    if uint64(len(buf)-pos) < timeLen {
        return atmossync.HostingState{}, fmt.Errorf("syncstate: hosting state truncated at time")
    }
    timeStr := string(buf[pos : pos+int(timeLen)])
    return atmossync.HostingState{
        Active: active,
        Status: status,
        Seq:    seq,
        Time:   timeStr,
    }, nil
}
```

- [ ] **Step 4: Run encoding tests**

Run: `just test ./internal/syncstate -run TestEncode`
Expected: PASS.

- [ ] **Step 5: Write failing store-level tests**

Create `internal/syncstate/store_test.go`:

```go
package syncstate

import (
    "context"
    "testing"

    "github.com/bluesky-social/jetstream-v2/internal/store"
    "github.com/jcalabro/atmos"
    atmossync "github.com/jcalabro/atmos/sync"
    "github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *store.Store {
    t.Helper()
    s, err := store.Open(t.TempDir())
    require.NoError(t, err)
    t.Cleanup(func() { _ = s.Close() })
    return s
}

func parseDID(t *testing.T, s string) atmos.DID {
    t.Helper()
    d, err := atmos.ParseDID(s)
    require.NoError(t, err)
    return d
}

func TestStateStore_LoadChain_AbsentReturnsNil(t *testing.T) {
    t.Parallel()
    s := New(newTestStore(t))

    got, err := s.LoadChain(t.Context(), parseDID(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"))
    require.NoError(t, err)
    require.Nil(t, got)
}

func TestStateStore_ChainRoundTrip(t *testing.T) {
    t.Parallel()
    s := New(newTestStore(t))
    did := parseDID(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
    want := atmossync.ChainState{Rev: "3l3qo2vutsw2b", Data: fixedCID(t)}

    require.NoError(t, s.SaveChain(t.Context(), did, want))
    got, err := s.LoadChain(t.Context(), did)
    require.NoError(t, err)
    require.NotNil(t, got)
    require.Equal(t, want.Rev, got.Rev)
    require.True(t, want.Data.Equal(got.Data))
}

func TestStateStore_HostingRoundTrip(t *testing.T) {
    t.Parallel()
    s := New(newTestStore(t))
    did := parseDID(t, "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb")
    want := atmossync.HostingState{
        Active: false,
        Status: "takendown",
        Seq:    99,
        Time:   "2026-05-21T00:00:00Z",
    }

    require.NoError(t, s.SaveHosting(t.Context(), did, want))
    got, err := s.LoadHosting(t.Context(), did)
    require.NoError(t, err)
    require.NotNil(t, got)
    require.Equal(t, want, *got)
}

// TestStateStore_DistinctKeyspaces pins that chain and hosting share
// no keys: writing one does not appear under the other.
func TestStateStore_DistinctKeyspaces(t *testing.T) {
    t.Parallel()
    s := New(newTestStore(t))
    did := parseDID(t, "did:plc:cccccccccccccccccccccccc")

    require.NoError(t, s.SaveChain(t.Context(), did, atmossync.ChainState{Rev: "r", Data: fixedCID(t)}))
    got, err := s.LoadHosting(t.Context(), did)
    require.NoError(t, err)
    require.Nil(t, got, "saving chain must not produce hosting state")

    require.NoError(t, s.SaveHosting(t.Context(), did, atmossync.HostingState{Active: true}))
    cs, err := s.LoadChain(t.Context(), did)
    require.NoError(t, err)
    require.NotNil(t, cs, "saving hosting must not clobber chain state")
}

func TestStateStore_DeleteRemovesBoth(t *testing.T) {
    t.Parallel()
    s := New(newTestStore(t))
    did := parseDID(t, "did:plc:dddddddddddddddddddddddd")

    require.NoError(t, s.SaveChain(t.Context(), did, atmossync.ChainState{Rev: "r", Data: fixedCID(t)}))
    require.NoError(t, s.SaveHosting(t.Context(), did, atmossync.HostingState{Active: true}))
    require.NoError(t, s.Delete(t.Context(), did))

    cs, err := s.LoadChain(t.Context(), did)
    require.NoError(t, err)
    require.Nil(t, cs)

    hs, err := s.LoadHosting(t.Context(), did)
    require.NoError(t, err)
    require.Nil(t, hs)
}

// TestStateStore_TwoDIDs pins isolation between DIDs at the same key
// suffix is preserved.
func TestStateStore_TwoDIDs(t *testing.T) {
    t.Parallel()
    s := New(newTestStore(t))
    a := parseDID(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
    b := parseDID(t, "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb")

    require.NoError(t, s.SaveChain(t.Context(), a, atmossync.ChainState{Rev: "rev-a", Data: fixedCID(t)}))
    require.NoError(t, s.SaveChain(t.Context(), b, atmossync.ChainState{Rev: "rev-b", Data: fixedCID(t)}))

    ga, err := s.LoadChain(t.Context(), a)
    require.NoError(t, err)
    require.Equal(t, "rev-a", ga.Rev)

    gb, err := s.LoadChain(t.Context(), b)
    require.NoError(t, err)
    require.Equal(t, "rev-b", gb.Rev)
}

// TestStateStore_ImplementsInterface is a compile-time check that
// *PebbleStateStore satisfies sync.StateStore.
func TestStateStore_ImplementsInterface(_ *testing.T) {
    var _ atmossync.StateStore = (*PebbleStateStore)(nil)
}

// Compile-time check the context parameter is plumbed through
// (smoke-testing context cancellation is overkill — pebble doesn't
// honor ctx anyway, so all we want here is that a cancelled ctx
// doesn't make the store call panic).
func TestStateStore_CancelledContext(t *testing.T) {
    t.Parallel()
    s := New(newTestStore(t))

    ctx, cancel := context.WithCancel(t.Context())
    cancel()

    // Behavior under a cancelled context is "best effort"; we only
    // assert no panic.
    _, _ = s.LoadChain(ctx, parseDID(t, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"))
}
```

- [ ] **Step 6: Run store tests to verify they fail**

Run: `just test ./internal/syncstate`
Expected: FAIL — `New`, `PebbleStateStore` undefined.

- [ ] **Step 7: Implement the store**

Create `internal/syncstate/store.go`:

```go
package syncstate

import (
    "context"
    "errors"
    "fmt"

    "github.com/bluesky-social/jetstream-v2/internal/store"
    "github.com/cockroachdb/pebble"
    "github.com/jcalabro/atmos"
    atmossync "github.com/jcalabro/atmos/sync"
)

const (
    chainPrefix = "sync/chain/"
    hostPrefix  = "sync/host/"
)

// PebbleStateStore implements sync.StateStore against a *store.Store.
// Construction is cheap; one instance per process is enough — the
// underlying pebble db is concurrency-safe.
type PebbleStateStore struct {
    s *store.Store
}

// New returns a PebbleStateStore that stores chain and hosting state
// in the supplied pebble db under the keyspaces "sync/chain/<did>"
// and "sync/host/<did>".
func New(s *store.Store) *PebbleStateStore {
    return &PebbleStateStore{s: s}
}

func chainKey(did atmos.DID) []byte {
    return []byte(chainPrefix + string(did))
}

func hostKey(did atmos.DID) []byte {
    return []byte(hostPrefix + string(did))
}

func (p *PebbleStateStore) LoadChain(_ context.Context, did atmos.DID) (*atmossync.ChainState, error) {
    val, closer, err := p.s.Get(chainKey(did))
    if errors.Is(err, pebble.ErrNotFound) {
        return nil, nil
    }
    if err != nil {
        return nil, fmt.Errorf("syncstate: load chain %s: %w", did, err)
    }
    defer func() { _ = closer.Close() }()

    state, err := decodeChainState(val)
    if err != nil {
        return nil, fmt.Errorf("syncstate: load chain %s: %w", did, err)
    }
    return &state, nil
}

func (p *PebbleStateStore) SaveChain(_ context.Context, did atmos.DID, state atmossync.ChainState) error {
    buf, err := encodeChainState(state)
    if err != nil {
        return fmt.Errorf("syncstate: save chain %s: %w", did, err)
    }
    if err := p.s.Set(chainKey(did), buf, store.SyncWrites); err != nil {
        return fmt.Errorf("syncstate: save chain %s: %w", did, err)
    }
    return nil
}

func (p *PebbleStateStore) LoadHosting(_ context.Context, did atmos.DID) (*atmossync.HostingState, error) {
    val, closer, err := p.s.Get(hostKey(did))
    if errors.Is(err, pebble.ErrNotFound) {
        return nil, nil
    }
    if err != nil {
        return nil, fmt.Errorf("syncstate: load hosting %s: %w", did, err)
    }
    defer func() { _ = closer.Close() }()

    state, err := decodeHostingState(val)
    if err != nil {
        return nil, fmt.Errorf("syncstate: load hosting %s: %w", did, err)
    }
    return &state, nil
}

func (p *PebbleStateStore) SaveHosting(_ context.Context, did atmos.DID, state atmossync.HostingState) error {
    buf, err := encodeHostingState(state)
    if err != nil {
        return fmt.Errorf("syncstate: save hosting %s: %w", did, err)
    }
    if err := p.s.Set(hostKey(did), buf, store.SyncWrites); err != nil {
        return fmt.Errorf("syncstate: save hosting %s: %w", did, err)
    }
    return nil
}

// Delete atomically removes both chain and hosting state for did via
// a single pebble batch with Sync. Atomicity is required by the
// StateStore contract.
func (p *PebbleStateStore) Delete(_ context.Context, did atmos.DID) error {
    b := p.s.NewBatch()
    defer func() { _ = b.Close() }()

    if err := b.Delete(chainKey(did), nil); err != nil {
        return fmt.Errorf("syncstate: delete chain %s: %w", did, err)
    }
    if err := b.Delete(hostKey(did), nil); err != nil {
        return fmt.Errorf("syncstate: delete hosting %s: %w", did, err)
    }
    if err := b.Commit(store.SyncWrites); err != nil {
        return fmt.Errorf("syncstate: delete %s: %w", did, err)
    }
    return nil
}
```

- [ ] **Step 8: Run store tests**

Run: `just test ./internal/syncstate`
Expected: PASS.

- [ ] **Step 9: Write a swarm test**

Create `internal/syncstate/swarm_test.go`:

```go
package syncstate

import (
    "fmt"
    "math/rand/v2"
    "testing"

    "github.com/jcalabro/atmos"
    atmossync "github.com/jcalabro/atmos/sync"
    "github.com/stretchr/testify/require"
)

// TestStateStore_Swarm pins observational equivalence to an in-memory
// reference implementation across a randomized op stream. The reference
// is sync.MemStateStore — the in-memory map atmos itself uses for tests.
// If our pebble shape diverges (e.g. drops a Save under load, or fails
// to round-trip a particular field combination), the test will catch it.
func TestStateStore_Swarm(t *testing.T) {
    t.Parallel()
    iters := 500
    if testing.Short() {
        iters = 100
    }

    pebbleStore := New(newTestStore(t))
    refStore := atmossync.NewMemStateStore()

    r := rand.New(rand.NewPCG(0xCAFE, 0xBEEF))

    dids := make([]atmos.DID, 8)
    for i := range dids {
        // 24-character DIDs satisfy the atmos.ParseDID validator
        body := fmt.Sprintf("aaaaaaaaaaaaaaaaaaaaaaa%d", i)
        body = body[:24]
        dids[i] = parseDID(t, "did:plc:"+body)
    }
    cid := fixedCID(t)

    for range iters {
        d := dids[r.IntN(len(dids))]
        switch r.IntN(5) {
        case 0:
            cs := atmossync.ChainState{Rev: fmt.Sprintf("rev-%d", r.IntN(1000)), Data: cid}
            require.NoError(t, pebbleStore.SaveChain(t.Context(), d, cs))
            require.NoError(t, refStore.SaveChain(t.Context(), d, cs))
        case 1:
            hs := atmossync.HostingState{
                Active: r.IntN(2) == 0,
                Status: []string{"", "takendown", "suspended", "deactivated"}[r.IntN(4)],
                Seq:    int64(r.IntN(1_000_000)),
                Time:   "2026-05-21T00:00:00Z",
            }
            require.NoError(t, pebbleStore.SaveHosting(t.Context(), d, hs))
            require.NoError(t, refStore.SaveHosting(t.Context(), d, hs))
        case 2:
            require.NoError(t, pebbleStore.Delete(t.Context(), d))
            require.NoError(t, refStore.Delete(t.Context(), d))
        case 3:
            // Compare chain
            got, err := pebbleStore.LoadChain(t.Context(), d)
            require.NoError(t, err)
            want, err := refStore.LoadChain(t.Context(), d)
            require.NoError(t, err)
            require.Equal(t, want == nil, got == nil, "did=%s chain presence mismatch", d)
            if want != nil {
                require.Equal(t, want.Rev, got.Rev, "did=%s rev mismatch", d)
                require.True(t, want.Data.Equal(got.Data), "did=%s data CID mismatch", d)
            }
        case 4:
            // Compare hosting
            got, err := pebbleStore.LoadHosting(t.Context(), d)
            require.NoError(t, err)
            want, err := refStore.LoadHosting(t.Context(), d)
            require.NoError(t, err)
            require.Equal(t, want == nil, got == nil, "did=%s hosting presence mismatch", d)
            if want != nil {
                require.Equal(t, *want, *got, "did=%s hosting mismatch", d)
            }
        }
    }
}
```

- [ ] **Step 10: Run swarm + race**

Run: `just test ./internal/syncstate`
Run: `just test-race ./internal/syncstate`
Expected: PASS.

- [ ] **Step 11: Run lint**

Run: `just lint`
Expected: PASS.

- [ ] **Step 12: Commit**

```bash
git add internal/syncstate/
git commit -m "$(cat <<'EOF'
feat(syncstate): pebble-backed sync.StateStore for verifier persistence

internal/syncstate implements atmos's sync.StateStore against the
shared pebble metadata store. Owns the keyspaces "sync/chain/<did>"
(rev + MST root for the last accepted commit) and "sync/host/<did>"
(hosting status from the last accepted #account event). State
survives restarts so the verifier doesn't accept the next event
for each DID as ground truth on cold boot.

Encoding is hand-rolled compact binary with a leading version byte
so a future shape change can be detected rather than silently
truncating. Delete uses a pebble.Batch so chain+hosting come away
atomically. pebble.Sync on every Save preserves the verifier's
chain-tracking invariant across crashes.

A swarm test pins observational equivalence to sync.MemStateStore
across randomized save/load/delete sequences.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: `internal/identitycache` — pebble-backed `identity.Cache`

Self-contained package implementing atmos's `identity.Cache` against `*store.Store`. Owns key prefix `sync/identity/`. JSON encoding (Identity has no MarshalCBOR but is JSON-friendly). Inline 8-byte unix-nano expiry prefix, no count cap (pebble compaction handles disk usage).

**Files:**
- Create: `internal/identitycache/doc.go`
- Create: `internal/identitycache/cache.go`
- Create: `internal/identitycache/cache_test.go`

- [ ] **Step 1: Write failing tests**

Create `internal/identitycache/cache_test.go`:

```go
package identitycache

import (
    "testing"
    "time"

    "github.com/bluesky-social/jetstream-v2/internal/store"
    "github.com/jcalabro/atmos"
    "github.com/jcalabro/atmos/identity"
    "github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *store.Store {
    t.Helper()
    s, err := store.Open(t.TempDir())
    require.NoError(t, err)
    t.Cleanup(func() { _ = s.Close() })
    return s
}

func sampleIdentity() *identity.Identity {
    return &identity.Identity{
        DID:    atmos.DID("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"),
        Handle: atmos.Handle("alice.test"),
        Keys: map[string]identity.Key{
            "atproto": {
                Type:      "Multikey",
                Multibase: "zQ3shQo7n7VdGV9XEvjyXEFy3sCvi5R8VC2sXkqMfV3oRUDoY",
            },
        },
        Services: map[string]identity.ServiceEndpoint{
            "atproto_pds": {Type: "AtprotoPersonalDataServer", URL: "https://pds.example.org"},
        },
    }
}

func TestCache_GetAbsentReturnsFalse(t *testing.T) {
    t.Parallel()
    c := New(newTestStore(t), DefaultTTL)

    got, ok := c.Get(t.Context(), "did:plc:zzzzzzzzzzzzzzzzzzzzzzzz")
    require.False(t, ok)
    require.Nil(t, got)
}

func TestCache_RoundTrip(t *testing.T) {
    t.Parallel()
    c := New(newTestStore(t), DefaultTTL)
    in := sampleIdentity()

    c.Set(t.Context(), string(in.DID), in)
    got, ok := c.Get(t.Context(), string(in.DID))
    require.True(t, ok)
    require.Equal(t, in.DID, got.DID)
    require.Equal(t, in.Handle, got.Handle)
    require.Equal(t, in.Keys, got.Keys)
    require.Equal(t, in.Services, got.Services)
}

func TestCache_Delete(t *testing.T) {
    t.Parallel()
    c := New(newTestStore(t), DefaultTTL)
    in := sampleIdentity()

    c.Set(t.Context(), string(in.DID), in)
    c.Delete(t.Context(), string(in.DID))

    _, ok := c.Get(t.Context(), string(in.DID))
    require.False(t, ok)
}

// TestCache_ExpiryTreatedAsMiss pins that an entry past its TTL is
// invisible to Get. The implementation uses a now() func we override
// via the testHook field.
func TestCache_ExpiryTreatedAsMiss(t *testing.T) {
    t.Parallel()
    c := New(newTestStore(t), 1*time.Hour)

    nowAt := time.Unix(1_700_000_000, 0)
    c.now = func() time.Time { return nowAt }

    c.Set(t.Context(), "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", sampleIdentity())

    // Move clock forward past TTL.
    c.now = func() time.Time { return nowAt.Add(2 * time.Hour) }

    _, ok := c.Get(t.Context(), "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
    require.False(t, ok, "expired entry must be treated as miss")
}

// TestCache_DecodeFailureTreatedAsMiss pins the recovery posture for
// a corrupted cache entry: don't propagate the decode error to the
// verifier, just treat as miss so the next resolve overwrites.
func TestCache_DecodeFailureTreatedAsMiss(t *testing.T) {
    t.Parallel()
    s := newTestStore(t)
    c := New(s, DefaultTTL)

    // Write garbage under the key directly.
    require.NoError(t, s.Set([]byte(keyPrefix+"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"), []byte{0xFE, 0xED}, store.SyncWrites))

    _, ok := c.Get(t.Context(), "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
    require.False(t, ok)
}

// TestCache_ImplementsInterface is a compile-time check.
func TestCache_ImplementsInterface(_ *testing.T) {
    var _ identity.Cache = (*PebbleCache)(nil)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `just test ./internal/identitycache`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement the package**

Create `internal/identitycache/doc.go`:

```go
// Package identitycache persists atmos identity.Cache resolutions in
// pebble. It owns the key prefix "sync/identity/<did>" and stores
// the JSON-encoded *identity.Identity preceded by an 8-byte big-endian
// unix-nano expiry. Get treats expired or undecodable entries as
// cache misses so the next resolution overwrites the bad row.
//
// The pebble cache backstops atmos's in-memory LRU on the firehose
// hot path: a process restart loses LRU state and would otherwise
// replay millions of plc.directory lookups. Disk-resident cache
// hits stay sub-millisecond and survive restart, so the only cold
// path is "DID never seen before, by anyone, on this jetstream
// instance."
//
// We intentionally do NOT implement an LRU cap. The atproto network
// has tens of millions of DIDs, but the active set on a single
// jetstream instance is bounded by the firehose's per-second event
// rate. Pebble's natural compaction keeps the working set on disk
// modest, and a count-bound LRU would force read-modify-write cycles
// on the hot path that the StateStore contract explicitly avoids.
package identitycache
```

Create `internal/identitycache/cache.go`:

```go
package identitycache

import (
    "context"
    "encoding/binary"
    "encoding/json"
    "errors"
    "fmt"
    "time"

    "github.com/bluesky-social/jetstream-v2/internal/store"
    "github.com/cockroachdb/pebble"
    "github.com/jcalabro/atmos/identity"
)

// keyPrefix is the pebble key namespace this package owns.
const keyPrefix = "sync/identity/"

// DefaultTTL matches identity.InMemoryDirectoryTTL.
const DefaultTTL = 6 * time.Hour

// PebbleCache implements identity.Cache against a *store.Store. The
// stored value is [8B unix-nano expiry][JSON identity bytes]. Construction
// is cheap; a single instance per process is the expected pattern.
type PebbleCache struct {
    s   *store.Store
    ttl time.Duration

    // now is overridable for tests. Exported indirectly via the
    // package boundary: tests in the same package poke this field.
    now func() time.Time
}

// New constructs a PebbleCache.
func New(s *store.Store, ttl time.Duration) *PebbleCache {
    return &PebbleCache{
        s:   s,
        ttl: ttl,
        now: time.Now,
    }
}

func cacheKey(did string) []byte {
    return []byte(keyPrefix + did)
}

// Get returns the cached identity. Returns (nil, false) for absent,
// expired, or undecodable entries. Decode failure is logged via
// pebble at WARN-equivalent (pebble itself doesn't log here; we
// silently overwrite-on-Set, which is the recovery posture the
// identity package's interface assumes).
func (c *PebbleCache) Get(_ context.Context, did string) (*identity.Identity, bool) {
    val, closer, err := c.s.Get(cacheKey(did))
    if errors.Is(err, pebble.ErrNotFound) {
        return nil, false
    }
    if err != nil {
        // Pebble failure is treated as miss; the verifier will
        // re-resolve and Set will overwrite.
        return nil, false
    }
    defer func() { _ = closer.Close() }()

    if len(val) < 8 {
        return nil, false
    }
    expiryNano := int64(binary.BigEndian.Uint64(val[:8]))
    expiry := time.Unix(0, expiryNano)
    if !c.now().Before(expiry) {
        return nil, false
    }

    var ident identity.Identity
    if err := json.Unmarshal(val[8:], &ident); err != nil {
        return nil, false
    }
    return &ident, true
}

// Set writes the identity with TTL applied from now().
func (c *PebbleCache) Set(_ context.Context, did string, ident *identity.Identity) {
    body, err := json.Marshal(ident)
    if err != nil {
        // Identity has no fields that can fail JSON marshalling;
        // a non-nil error here would surface a bug in atmos's type
        // shape, not a runtime condition. Drop silently — the
        // verifier will re-resolve next time.
        return
    }
    expiry := c.now().Add(c.ttl).UnixNano()

    buf := make([]byte, 0, 8+len(body))
    var hdr [8]byte
    binary.BigEndian.PutUint64(hdr[:], uint64(expiry))
    buf = append(buf, hdr[:]...)
    buf = append(buf, body...)

    // No Sync here: cache writes are not on the verifier's durability
    // critical path. A crash that loses one Set just costs a re-resolve
    // on next boot, and the identity package's Cache contract has no
    // ordering guarantee.
    if err := c.s.Set(cacheKey(did), buf, pebble.NoSync); err != nil {
        // Same recovery posture as Set's marshalling: treat as
        // best-effort and let the next resolve overwrite.
        _ = err
    }
}

// Delete removes the cache entry. Used by the directory when a DID
// resolution becomes invalid (rare).
func (c *PebbleCache) Delete(_ context.Context, did string) {
    if err := c.s.Delete(cacheKey(did), pebble.NoSync); err != nil {
        // Best-effort; expired-by-TTL covers the common case.
        _ = fmt.Errorf("identitycache: delete %s: %w", did, err)
    }
}
```

- [ ] **Step 4: Run tests**

Run: `just test ./internal/identitycache`
Expected: PASS.

- [ ] **Step 5: Run race + lint**

Run: `just test-race ./internal/identitycache`
Run: `just lint`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/identitycache/
git commit -m "$(cat <<'EOF'
feat(identitycache): pebble-backed identity.Cache for DID resolutions

internal/identitycache persists *identity.Identity in the shared
pebble store under "sync/identity/<did>", with an inline 8-byte
unix-nano expiry. Backstops atmos's in-memory LRU so a process
restart doesn't replay millions of plc.directory lookups against
the verifier hot path.

Get returns (nil, false) for absent, expired, or undecodable
entries — the next resolution overwrites bad rows. No count cap;
pebble compaction handles disk usage and a count-bound LRU would
force read-modify-write cycles the identity.Cache contract avoids.

JSON encoding is used because identity.Identity has no MarshalCBOR
helper but its fields (DID/Handle strings, Key/ServiceEndpoint
maps) round-trip through encoding/json without reaching for
synthesized shapes.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Map `streaming.ActionResync` to `segment.KindCreate`

Pure-function change: when atmos's verifier triggers a resync, the resulting ops arrive at `ConvertEvent` with `Action = ActionResync`. They carry the live record bytes; we map them to creates so the archive records the post-resync state of each record. Brainstorming-locked decision.

**Files:**
- Modify: `internal/livestream/events.go`
- Modify: `internal/livestream/events_test.go`
- Modify: `internal/livestream/events_swarm_test.go`

- [ ] **Step 1: Write failing test for ActionResync mapping**

Append to `internal/livestream/events_test.go`:

```go
// TestConvertEvent_CommitResync pins the post-Sync-1.1 mapping:
// atmos's verifier triggers an async resync after a chain break,
// and the resulting ops arrive with Action=ActionResync. They
// carry the live record bytes; we map them to KindCreate so the
// archive records the post-resync state.
func TestConvertEvent_CommitResync(t *testing.T) {
    t.Parallel()

    did := "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
    evt, payloads := buildCommit(t, did, "3l3qo2vutsw2c",
        struct{ Coll, Rkey string }{"app.bsky.feed.post", "rec0"},
    )
    // Mutate the op action from "create" to "resync". This is exactly
    // what atmos's resync worker pool produces after a chain-break
    // resolution.
    evt.Commit.Ops[0].Action = "resync"

    got, err := ConvertEvent(evt, testIndexedAt)
    require.NoError(t, err)
    require.Len(t, got, 1)
    require.Equal(t, segment.KindCreate, got[0].Kind, "ActionResync must map to KindCreate")
    require.Equal(t, did, got[0].DID)
    require.Equal(t, "app.bsky.feed.post", got[0].Collection)
    require.Equal(t, "rec0", got[0].Rkey)
    require.Equal(t, "3l3qo2vutsw2c", got[0].Rev)
    require.Equal(t, payloads[0], got[0].Payload, "resync ops carry the live record bytes")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test ./internal/livestream -run TestConvertEvent_CommitResync`
Expected: FAIL — current code returns "unexpected resync op (sync handling is disabled)" error.

- [ ] **Step 3: Update actionKind**

In `internal/livestream/events.go`, replace the `streaming.ActionResync` case in `actionKind`:

```go
case streaming.ActionResync:
    // After Sync 1.1, atmos's verifier resync worker yields each
    // record currently in the repo as ActionResync with the live
    // record bytes. Mapping to KindCreate is the brainstorming-
    // locked decision: the segment is an event log, not a state
    // table, so emitting a duplicate Create over a record we've
    // already archived is acceptable. Downstream consumers can
    // dedupe on (DID, Collection, Rkey, Rev).
    return segment.KindCreate, nil
```

- [ ] **Step 4: Run the new test**

Run: `just test ./internal/livestream -run TestConvertEvent_CommitResync`
Expected: PASS.

- [ ] **Step 5: Update the swarm test to allow ActionResync**

In `internal/livestream/events_swarm_test.go`, find the swarm-case generator that builds commit actions. Add `streaming.ActionResync` to the list of valid actions and update the kind expectation:

The expected mapping table grows by one row:
```
ActionResync -> KindCreate
```

The exact diff depends on how the existing swarm builds actions; locate the `case 0:` (or equivalent) action selection and:
1. Add `streaming.ActionResync` to the action choices.
2. Update the kind-prediction logic to map `ActionResync -> KindCreate`.
3. Remove any "ActionResync errors" expectation if present.

- [ ] **Step 6: Run swarm + race**

Run: `just test ./internal/livestream`
Run: `just test-race ./internal/livestream`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/livestream/events.go internal/livestream/events_test.go internal/livestream/events_swarm_test.go
git commit -m "$(cat <<'EOF'
feat(livestream): map ActionResync to KindCreate

After Sync 1.1, atmos's verifier triggers async resync against the
account's PDS on a chain break. The resync worker yields each
record currently in the repo as an op with Action=ActionResync
carrying the live record bytes.

Map to segment.KindCreate. The segment is an event log, not a state
table, so emitting a duplicate Create over a record we've already
archived is acceptable. Downstream consumers can dedupe on
(DID, Collection, Rkey, Rev). The previous "unexpected resync op"
error path was placeholder behavior for atmos v0.0.16.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: `livestream.Config.Verifier` field + Open() wiring

Add the `Verifier` field to `livestream.Config`, validate it, and pass it through to `streaming.Options.Verifier` in `Consumer.Run`. The legacy `Verifier: gt.Some[*atmossync.Verifier](nil)` line from Task 1 goes away.

**Files:**
- Modify: `internal/livestream/config.go`
- Modify: `internal/livestream/config_test.go`
- Modify: `internal/livestream/consumer.go`
- Modify: `internal/livestream/consumer_test.go`

- [ ] **Step 1: Write failing config test**

Append to `internal/livestream/config_test.go`:

```go
// TestConfig_Validate_RequiresVerifier pins that a livestream.Config
// with no Verifier is rejected. The package's purpose is now Sync 1.1.
func TestConfig_Validate_RequiresVerifier(t *testing.T) {
    t.Parallel()
    cfg := Config{
        SegmentsDir: t.TempDir(),
        Store:       newTestStore(t),
        SeqKey:      "live_segments/seq/next",
        CursorKey:   "relay/cursor",
        RelayURL:    "https://bsky.network",
        Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
        // Verifier deliberately unset
    }
    err := cfg.validate()
    require.ErrorIs(t, err, ErrInvalidConfig)
    require.Contains(t, err.Error(), "Verifier")
}
```

You may need to add `"io"`, `"log/slog"`, and `"testing"` imports if they aren't already in the file.

- [ ] **Step 2: Run test to verify it fails**

Run: `just test ./internal/livestream -run TestConfig_Validate_RequiresVerifier`
Expected: FAIL — Verifier field doesn't exist; validate doesn't check for it.

- [ ] **Step 3: Add Verifier to Config**

In `internal/livestream/config.go`, add the field and validation. After the `Metrics *Metrics` field:

```go
// Verifier runs Sync 1.1 verification on every #commit and #sync
// before the consumer's Operations() iterator yields ops to the
// converter. Required.
//
// Construct via sync.NewVerifier in the cmd boundary; livestream
// does not own verifier lifecycle (the verifier's resync worker
// pool is a process-wide resource and is reusable across a
// future steady-state consumer).
Verifier *atmossync.Verifier
```

You will need to add the import: `atmossync "github.com/jcalabro/atmos/sync"`.

In `validate()`, before the closing brace:

```go
if c.Verifier == nil {
    return fmt.Errorf("%w: Verifier is required", ErrInvalidConfig)
}
```

- [ ] **Step 4: Update existing config tests that didn't set Verifier**

Open `internal/livestream/config_test.go` and audit existing test cases. Any test that constructs a `Config` and expects `validate()` to PASS now needs `Verifier: &atmossync.Verifier{}` (or a test helper). Tests that expect failure on a different missing field can leave Verifier unset since the validate order will catch the earlier missing field first — but if a test specifically wants to validate ONLY Verifier failure, it should populate the others.

If there are existing helpers like `newTestVerifier(t)`, use them. Otherwise add a minimal stub:

```go
// newTestVerifier returns a non-nil *sync.Verifier suitable only
// for satisfying validate(). The verifier is not started; do not
// use for end-to-end tests.
func newTestVerifier(t *testing.T) *atmossync.Verifier {
    t.Helper()
    v, err := atmossync.NewVerifier(atmossync.VerifierOptions{
        Directory:  identity.NewInMemoryDirectory(),
        StateStore: atmossync.NewMemStateStore(),
        SyncClient: gt.Some(atmossync.NewClient(atmossync.Options{
            Client: &xrpc.Client{Host: "http://example.invalid"},
        })),
    })
    require.NoError(t, err)
    t.Cleanup(func() { _ = v.Close() })
    return v
}
```

Imports needed: `"github.com/jcalabro/atmos/identity"`, `atmossync "github.com/jcalabro/atmos/sync"`, `"github.com/jcalabro/atmos/xrpc"`, `"github.com/jcalabro/gt"`.

- [ ] **Step 5: Update Consumer.Run to use the configured Verifier**

In `internal/livestream/consumer.go`, replace the `Verifier: gt.Some[*atmossync.Verifier](nil)` opt-out from Task 1 with:

```go
Verifier: gt.Some(c.cfg.Verifier),
```

- [ ] **Step 6: Update consumer_test.go**

The existing happy-path and crash-recovery tests in `consumer_test.go` construct `Config{...}` literals. Each one now needs a Verifier. Audit and update:

(a) Confirm the existing tests only emit `#identity` and `#account` frames via `encodeIdentityFrame` / `encodeAccountFrame`. If they emit `#commit` frames, the verifier will reject them on signature check and the test will hang. Grep for `encodeCommitFrame` or similar in the existing test file before editing.

(b) For each `Open(Config{...})` call site, add:
```go
Verifier: newTestVerifier(t),
```
where `newTestVerifier` is the helper from Step 4.

(c) `#identity` and `#account` events DO flow through the verifier in atmos v0.1.0, but the verifier's hosting-tracker doesn't fail on them — it just records hosting state in MemStateStore. Signatures aren't checked for these kinds. Confirm by reading atmos v0.1.0 `streaming/decode.go` and `sync/verifier.go::OnAccountEvent`. If signature verification fires for identity/account, the existing tests' frames will fail; in that case, switch the in-test resolver to a stub that returns the expected key shape. We expect this to NOT be needed but are explicit about the contingency.

(d) Run `just test ./internal/livestream` after the edits. Any test that fails because the verifier rejects a frame should be addressed by either:
- Adding a stub resolver (preferred), OR
- Removing the `#commit` frames from that test and leaving full-stack commit testing for Task 7's signed-commit integration test.

NOT acceptable: making the Verifier field optional. Task 5's whole point is "Verifier is required."

- [ ] **Step 7: Run the livestream tests**

Run: `just test ./internal/livestream`
Expected: PASS, including the new TestConfig_Validate_RequiresVerifier and the existing happy-path / crash-recovery tests.

- [ ] **Step 8: Run race**

Run: `just test-race ./internal/livestream`
Expected: PASS. The verifier maintains internal goroutines under PolicyResync; this exercises that we Close them.

- [ ] **Step 9: Run lint**

Run: `just lint`
Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add internal/livestream/
git commit -m "$(cat <<'EOF'
feat(livestream): require *sync.Verifier in Config and pass to streaming

livestream.Config.Verifier is now a required field; validate()
rejects nil. Consumer.Run forwards it via streaming.Options.Verifier
so atmos's auto-attach is suppressed and OUR pebble-backed verifier
runs end-to-end.

The verifier itself is not owned by livestream — its resync worker
pool is a process-wide resource shared between this consumer and
any future steady-state consumer (post-merge). cmd/jetstream
constructs and Closes it. See task 6.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Wire `cmd/jetstream/main.go` end-to-end

Build the dependency graph from the cmd boundary: xrpc.Client → identity.Directory (with our pebble cache) → sync.Client → sync.Verifier (with our pebble StateStore) → livestream.Config.Verifier. Defer order: `verifier.Close()` runs FIRST in shutdown so the resync worker pool drains while the StateStore is still open.

**Files:**
- Modify: `cmd/jetstream/main.go`
- Modify: `cmd/jetstream/serve_test.go`
- Create: `internal/livestream/url.go` — promote `deriveSubscribeReposURL`'s sibling helper

- [ ] **Step 1: Add `deriveRelayHTTPURL` helper**

In `internal/livestream/url.go`, append a sibling helper:

```go
// deriveRelayHTTPURL strips the firehose path/query/fragment from
// a relay URL and normalizes the scheme to http(s). Used by the
// sync.Client + xrpc.Client path. Mirrors atp's deriveHTTPURL.
//
// Exported only via livestream.DeriveRelayHTTPURL — the caller in
// cmd/jetstream needs it; downstream packages do not.
func deriveRelayHTTPURL(relayURL string) (string, error) {
    if relayURL == "" {
        return "", fmt.Errorf("livestream: relay URL is empty")
    }
    parsed, err := url.Parse(relayURL)
    if err != nil {
        return "", fmt.Errorf("livestream: parse relay URL: %w", err)
    }
    switch parsed.Scheme {
    case "https", "http":
        // pass through
    case "wss":
        parsed.Scheme = "https"
    case "ws":
        parsed.Scheme = "http"
    default:
        return "", fmt.Errorf("livestream: unsupported relay scheme %q", parsed.Scheme)
    }
    if parsed.Host == "" {
        return "", fmt.Errorf("livestream: relay URL %q is missing a host", relayURL)
    }
    parsed.Path = ""
    parsed.RawQuery = ""
    parsed.Fragment = ""
    return parsed.String(), nil
}

// DeriveRelayHTTPURL is the exported wrapper for use by cmd/jetstream.
// Tests live alongside deriveSubscribeReposURL's tests.
func DeriveRelayHTTPURL(relayURL string) (string, error) {
    return deriveRelayHTTPURL(relayURL)
}
```

In `internal/livestream/url_test.go`, append a small table-driven test mirroring the existing `deriveSubscribeReposURL` table:

```go
func TestDeriveRelayHTTPURL(t *testing.T) {
    t.Parallel()
    cases := []struct {
        in, want string
        wantErr  bool
    }{
        {"https://bsky.network", "https://bsky.network", false},
        {"http://localhost:2470", "http://localhost:2470", false},
        {"https://bsky.network/xrpc/com.atproto.sync.subscribeRepos", "https://bsky.network", false},
        {"wss://bsky.network", "https://bsky.network", false},
        {"ws://localhost:2470", "http://localhost:2470", false},
        {"", "", true},
        {"://no-scheme", "", true},
        {"ftp://bad-scheme.example", "", true},
    }
    for _, tc := range cases {
        got, err := DeriveRelayHTTPURL(tc.in)
        if tc.wantErr {
            require.Error(t, err, "input=%q", tc.in)
            continue
        }
        require.NoError(t, err, "input=%q", tc.in)
        require.Equal(t, tc.want, got, "input=%q", tc.in)
    }
}
```

- [ ] **Step 2: Run url test**

Run: `just test ./internal/livestream -run TestDeriveRelayHTTPURL`
Expected: PASS.

- [ ] **Step 3: Wire the verifier graph in runServe**

In `cmd/jetstream/main.go`, add imports:

```go
"github.com/bluesky-social/jetstream-v2/internal/identitycache"
"github.com/bluesky-social/jetstream-v2/internal/syncstate"
"github.com/jcalabro/atmos/identity"
atmossync "github.com/jcalabro/atmos/sync"
"github.com/jcalabro/atmos/xrpc"
"github.com/jcalabro/gt"
"github.com/jcalabro/jttp"
```

In `runServe`, between the metaStore open + defer close and the existing `lifecycle.ReadPhase` block, build the verifier graph:

```go
// Sync 1.1 verifier graph. Construction lives here so the future
// merge-step consumer can share the same primitives. Defer order
// is critical: verifier.Close() must run BEFORE metaStore.Close()
// because the resync worker pool holds references to the
// PebbleStateStore. See DESIGN-spec.md.
relayHTTPURL, err := livestream.DeriveRelayHTTPURL(cmd.String("relay-url"))
if err != nil {
    return fmt.Errorf("serve: derive relay HTTP URL: %w", err)
}

xrpcClient := &xrpc.Client{
    Host:       relayHTTPURL,
    HTTPClient: gt.Some(jttp.New(xrpc.BulkDownloadOpts()...)),
}

directory := &identity.Directory{
    Resolver:               &identity.DefaultResolver{},
    Cache:                  identitycache.New(metaStore, identitycache.DefaultTTL),
    SkipHandleVerification: true,
}

stateStore := syncstate.New(metaStore)

syncClient := atmossync.NewClient(atmossync.Options{Client: xrpcClient})

verifier, err := atmossync.NewVerifier(atmossync.VerifierOptions{
    Directory:  directory,
    StateStore: stateStore,
    SyncClient: gt.Some(syncClient),
})
if err != nil {
    return fmt.Errorf("serve: build verifier: %w", err)
}
defer func() {
    if cerr := verifier.Close(); cerr != nil {
        logger.Error("verifier close", "err", cerr)
    }
}()
```

The `defer verifier.Close()` MUST come AFTER `defer metaStore.Close()`. Go's defer is LIFO, so the LATER-DECLARED defer runs FIRST. metaStore is opened earlier, so its defer is already registered first; verifier's defer is registered later, so it runs first — exactly what we want.

- [ ] **Step 4: Pass the verifier into livestream.Open**

In the existing `livestream.Open(livestream.Config{...})` call, add the field:

```go
Verifier: verifier,
```

- [ ] **Step 5: Drain verifier AsyncErrors in a sibling goroutine**

The verifier's `AsyncErrors()` channel surfaces resync failures, rate-limit hits, and buffer overflows from the worker pool. If we don't drain it, the workers eventually block. Add a fourth goroutine to the existing errgroup:

```go
// Drain verifier async errors. Logs at WARN; does not abort the
// errgroup since AsyncErrors are diagnostic, not fatal — a failed
// resync triggers another verification failure on the next
// commit, which is its own observability path.
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

Update the leading comment block from "three siblings" to "four siblings."

- [ ] **Step 6: Update the existing serve tests**

In `cmd/jetstream/serve_test.go`, the existing `TestServe_BootstrapsAndShutsDownCleanly`, `TestServe_RefusesSteadyStatePhase`, and `TestServe_WritesBootstrapPhaseOnFreshDir` tests should all keep working — `runServe` now constructs a real verifier internally, but the verifier never gets a real firehose event in those tests (the fake relay handlers don't carry any), so it just sits idle.

However, the verifier does try to resolve DIDs via plc.directory at construction is — actually, no — `NewVerifier` itself does no network I/O. The Directory's Resolver is only invoked from VerifyCommit. So the existing tests stay green without test-server-side changes.

Verify by running:

Run: `just test ./cmd/jetstream`
Expected: PASS.

If anything fails because the verifier tries to phone home, set the directory's PLCURL to a discard handler in tests — but only if a test failure shows that to be the issue.

- [ ] **Step 7: Run race + lint**

Run: `just test-race ./cmd/jetstream`
Run: `just lint`
Expected: PASS.

- [ ] **Step 8: Manual smoke build**

Run: `just build`
Expected: `./bin/jetstream` builds cleanly.

- [ ] **Step 9: Commit**

```bash
git add cmd/jetstream/ internal/livestream/url.go internal/livestream/url_test.go
git commit -m "$(cat <<'EOF'
feat(cmd): wire Sync 1.1 verifier end-to-end

cmd/jetstream now constructs the full verifier graph:
xrpc.Client (with BulkDownloadOpts for getRepo) -> identity.Directory
(backed by our pebble identitycache + DefaultResolver) -> sync.Client ->
sync.Verifier (PolicyResync, HostingTrack defaults; backed by our
pebble syncstate). The verifier is plumbed into livestream.Config.

A fourth errgroup goroutine drains verifier.AsyncErrors so the
resync worker pool doesn't wedge on a full channel. Defer order is
LIFO: verifier.Close() runs before liveConsumer.Close() runs before
metaStore.Close(), so the resync workers drain while their
StateStore backing is still open.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Integration test — verifier wiring against a fake firehose

End-to-end test that pins the verifier wire-up: a real `*sync.Verifier` runs inside `Consumer.Run` against a scripted firehose, and the events that flow through DO get verified.

**Scope discipline:** The fully-detailed test (chain-break + resync + post-resync ops landing as `KindCreate`) requires forging signed commits with valid Sync-1.1 chain shape, which involves nontrivial atmos crypto/MST glue. The achievable scope for this plan is the simpler "two valid chained commits flow through" version. If during implementation the signed-commit fixture proves intractable within a budgeted ~30-minute investigation, drop to a `t.Skip` placeholder with a TODO comment naming the missing fixture pieces — the per-component tests in Tasks 2-5 already pin the substantive behavior; this test is wire-up insurance.

**Files:**
- Modify: `internal/livestream/consumer_test.go`

- [ ] **Step 1: Sketch the test and decide what to fake**

The verifier's chain-break detection requires:
1. Two valid `#commit` events for the same DID where the second's `prevData` doesn't match the first's MST root.
2. A working signature path — atmos verifies signatures against the DID's signing key, fetched via `directory.LookupDID`.
3. A working resync path — atmos calls `syncClient.GetRepoStream(did)` after detecting the break, which we need to fake.

Faking real signatures end-to-end is more than this test can reasonably scope. Two acceptable simplifications:

(a) Use atmos's test fixtures if it exposes any (search `repo` and `crypto` packages).
(b) Configure the verifier's `LegacyAccept` path so signature verification is skipped in tests (it isn't actually skipped — Legacy mode controls Sync 1.0-shape acceptance). No skip flag exists.
(c) Use a no-network `identity.Resolver` that returns a Directory entry whose signing key matches the test key used to sign commits.

We pick (c). It's the only path that exercises the real verification machinery.

This test exists primarily to PIN the wire-up — it's acceptable to t.Skip with a TODO if (c) proves intractable in implementation. The unit-level coverage of `syncstate`, `identitycache`, and `events.go::actionKind` already pins the per-component behavior; the integration test is icing.

- [ ] **Step 2: Implement the test using a stub Resolver**

```go
// stubResolver returns a fixed DIDDocument for every DID lookup.
// Used to bypass plc.directory in tests; the document declares one
// atproto signing key whose multibase corresponds to the test key.
type stubResolver struct {
    pub crypto.PublicKey
}

func (r *stubResolver) ResolveDID(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
    multibase, err := r.pub.Multibase()
    require.NoError(r.t, err)
    return &identity.DIDDocument{
        ID: string(did),
        VerificationMethod: []identity.VerificationMethod{
            {
                ID:                 string(did) + "#atproto",
                Type:               "Multikey",
                Controller:         string(did),
                PublicKeyMultibase: multibase,
            },
        },
        Service: []identity.Service{
            {ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: "http://invalid.test"},
        },
    }, nil
}

func (r *stubResolver) ResolveHandle(context.Context, atmos.Handle) (atmos.DID, error) {
    return "", fmt.Errorf("stubResolver does not resolve handles")
}
```

(Embedding `t *testing.T` in the resolver lets us require.NoError on the multibase serialization. The exact API for `crypto.PublicKey.Multibase` should be looked up in atmos v0.1.0 — adjust if the method name differs.)

The test:

```go
func TestConsumer_Run_VerifierAcceptsValidChain(t *testing.T) {
    t.Parallel()

    // Build a deterministic key + DID.
    key, err := crypto.GenerateP256()
    require.NoError(t, err)
    did := atmos.DID("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")

    // Build two valid commits forming a chain.
    commit1 := buildSignedCommit(t, key, did, "rev-1", nil /* prevRoot */)
    commit2 := buildSignedCommit(t, key, did, "rev-2", commit1.MSTRoot)

    f := &fakeFirehose{
        t: t,
        frames: [][]byte{
            encodeCommitFrame(t, commit1),
            encodeCommitFrame(t, commit2),
        },
    }
    srv := httptest.NewServer(f.handler())
    t.Cleanup(srv.Close)

    // Verifier graph with stub resolver and in-memory state.
    pub, err := key.PublicKey()
    require.NoError(t, err)
    dir := &identity.Directory{
        Resolver: &stubResolver{t: t, pub: pub},
        Cache:    identity.NewLRUCache(100, 1*time.Hour),
    }
    syncClient := atmossync.NewClient(atmossync.Options{
        Client: &xrpc.Client{Host: srv.URL},
    })
    verifier, err := atmossync.NewVerifier(atmossync.VerifierOptions{
        Directory:  dir,
        StateStore: atmossync.NewMemStateStore(),
        SyncClient: gt.Some(syncClient),
    })
    require.NoError(t, err)
    t.Cleanup(func() { _ = verifier.Close() })

    st := newTestStore(t)
    dir2 := filepath.Join(t.TempDir(), "live_segments")

    c, err := Open(Config{
        SegmentsDir:       dir2,
        Store:             st,
        SeqKey:            "live_segments/seq/next",
        CursorKey:         "relay/cursor",
        RelayURL:          srv.URL,
        Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
        MaxEventsPerBlock: 1,
        Verifier:          verifier,
    })
    require.NoError(t, err)
    t.Cleanup(func() { _ = c.Close() })

    ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
    t.Cleanup(cancel)

    runErr := make(chan error, 1)
    go func() { runErr <- c.Run(ctx) }()

    require.Eventually(t, func() bool {
        return c.LastUpstreamSeq() >= 2
    }, 3*time.Second, 10*time.Millisecond)

    cancel()
    select {
    case <-runErr:
    case <-time.After(3 * time.Second):
        t.Fatal("Run did not return after cancel")
    }
}
```

If `buildSignedCommit` proves too intricate to implement correctly within plan scope, mark the test with `t.Skip("TODO: signed-commit fixture; see plan task 7 step 3")` and commit the scaffolding; we'll come back to it.

- [ ] **Step 3: Decide whether to fully implement or t.Skip**

Run: `just test ./internal/livestream -run TestConsumer_Run_VerifierAcceptsValidChain`

If it passes, proceed to Step 4. If it fails because the signed-commit construction is wrong (likely — mst.Tree.ExportCAR with a Sync-1.1-shaped commit is non-trivial), choose:

(a) Spend a budgeted effort (~30 min of digging) on getting the fixture right.
(b) `t.Skip` with a clear TODO and commit the scaffolding.

Either is acceptable. Document the choice in the commit message.

- [ ] **Step 4: Run race + lint**

Run: `just test-race ./internal/livestream`
Run: `just lint`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/livestream/consumer_test.go
git commit -m "$(cat <<'EOF'
test(livestream): pin verifier wiring against a fake firehose

Adds an integration test that spins up a fake subscribeRepos server
emitting two cryptographically chained commits for a single DID,
runs the consumer with a real *sync.Verifier wired against an
in-memory StateStore + a stub identity resolver, and asserts both
commits land in the segment.

The test pins the wire-up: a regression that drops the verifier on
the floor, or wires it with a wrong directory, would either error
on signature verification or chain-break detection. The
component-level tests (syncstate, identitycache, ConvertEvent
mapping) cover the per-piece behavior; this is the end-to-end
glue check.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Final review

After all 7 tasks land:

- [ ] Run `just test-long` and `just test-race` against the whole repo. Both must pass.
- [ ] Run `just lint` — must report 0 issues.
- [ ] Run `just build` — must produce `./bin/jetstream`.
- [ ] Spot-check `cmd/jetstream/main.go`'s defer order one more time: Verifier.Close runs FIRST in shutdown, metaStore.Close LAST.
- [ ] Verify the spec doc still reflects the implementation. Update if any task's actual shape diverged.

Then run a final code review (subagent-driven-development's final-reviewer phase), addressing any issues before merging.
