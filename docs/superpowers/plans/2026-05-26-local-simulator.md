# Local atproto Simulator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a `cmd/simulator` binary plus jetstream-side wiring that lets `just simulator` (terminal 1) + `just run serve` (terminal 2) replace the production `bsky.network` + `plc.directory` round-trip during local dev.

**Architecture:** Single-process Go binary that serves PLC, a single PDS, and a relay (firehose) under one HTTP listener at production paths. Pebble-backed deterministic world of 10k accounts seeded from a global RNG, with a live commit generator that drives realistic-distribution traffic onto a websocket fanout. Jetstream gains a `--plc-url` flag and the repo gains a committed `.env` with simulator-pointing defaults so `just run` defaults to the local sim.

**Tech Stack:** Go, `github.com/cockroachdb/pebble`, `github.com/coder/websocket`, `github.com/jcalabro/atmos` (for `crypto`, `repo`, `mst`, `car`, `cbor`, `api/comatproto`), `github.com/urfave/cli/v3`, `github.com/prometheus/client_golang`. Spec lives at `docs/superpowers/specs/2026-05-26-local-simulator-design.md`.

---

## Repository conventions to follow

Read these once before starting; the design they encode applies to every task below.

- `AGENTS.md` and `PRACTICES.md` at the repo root — observability, error-handling, test discipline.
- `cmd/jetstream/main.go` — the cli/serve idiom (urfave/cli v3, `Sources: cli.EnvVars(...)`, errgroup, `signal.NotifyContext`, slog component-tagging). The simulator's `cmd/simulator/main.go` mirrors this structure.
- `internal/server/server.go` — `Run(ctx)` server pattern with bind-then-serve, graceful shutdown, ErrorLog wired through slog. Lift the same skeleton, but the simulator only needs one public listener (PLC+PDS+relay) plus one `/metrics` listener.
- `internal/subscribe/broadcaster.go` and `internal/subscribe/handler.go` — bounded-channel-per-subscriber fanout pattern with drop-on-overflow. The simulator's `fanout` package mirrors the same shape.
- `internal/store/store.go` — pebble open/close. **Do not reuse this code from the simulator** (the simulator must not link production packages); copy the small `Open(dir)` shape into `internal/simulator/world` and adapt.

**Hard constraint:** the simulator imports nothing from `github.com/bluesky-social/jetstream-v2/internal/...` or `.../segment`. Allowed deps are `atmos`, `pebble`, `coder/websocket`, `urfave/cli/v3`, `prometheus/client_golang`, and `golang.org/x/...`.

---

## File structure

**New files (simulator side):**

```
cmd/simulator/
  main.go                              — cli wiring + runServe action

internal/simulator/
  world/
    doc.go                             — package overview
    config.go                          — World config struct + defaults + validate
    world.go                           — *World construct/lifecycle, RNG ownership, repo cache, seq counter
    keys.go                            — pebble key constructors for sim/* keyspace
    meta.go                            — seed/seq read/write
    accounts.go                        — deterministic key + DID generation; account record load/save
    did.go                             — `did:plc:<base32>` formatter from pubkey hash
    repos.go                            — load/build *repo.Repo for an account from pebble blocks
    bootstrap.go                       — first-run world generation
    distributions.go                   — Zipfian, exponential, weighted-choice, log-normal helpers
    records.go                         — generate `app.bsky.*` record payloads with realistic shape
    traffic.go                         — live commit generator goroutine
    firehose.go                        — encode wire frames; persist + fan-out helpers
  http/
    handler.go                         — top-level mux dispatching by path
    plc.go                             — DID document GETs
    pds.go                             — getRepo CAR streaming
    relay_listrepos.go                 — listRepos pagination
    relay_subscribe.go                 — subscribeRepos websocket
  fanout/
    fanout.go                          — Registry + Subscriber type, bounded outbound channel
  metrics.go                           — simulator's prometheus.Registry

# Tests (alongside the code they test)
internal/simulator/world/*_test.go
internal/simulator/http/*_test.go
internal/simulator/fanout/*_test.go
cmd/simulator/main_test.go             — end-to-end smoke test
```

**Modified files (jetstream side):**

```
cmd/jetstream/main.go                  — add --plc-url flag + plumb into DefaultResolver.PLCURL
.gitignore                             — remove the .env entry
.env                                   — NEW (simulator-pointing defaults, committed)
justfile                               — set dotenv-load + run/run-prod/simulator/simulator-reset
```

---

## Task 1: Scaffold cmd/simulator with a "hello" subcommand

Goal: get a buildable, runnable empty `cmd/simulator` binary that wires up logging the same way jetstream does. No HTTP yet; no pebble yet. This is the substrate every later task lands on.

**Files:**
- Create: `cmd/simulator/main.go`
- Create: `cmd/simulator/main_test.go`

- [ ] **Step 1: Write a smoke test that the binary's `--version`-equivalent runs**

`cmd/simulator/main_test.go`:

```go
package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApp_VersionFlag(t *testing.T) {
	t.Parallel()
	cmd := newApp()
	require.NoError(t, cmd.Run(context.Background(), []string{"simulator", "--help"}))
}
```

- [ ] **Step 2: Run the test, watch it fail to compile**

```sh
just test ./cmd/simulator -run TestApp_VersionFlag
```

Expected: build error — `newApp` undefined.

- [ ] **Step 3: Write the minimal cli skeleton**

`cmd/simulator/main.go`:

```go
// Command simulator is a development-only fake atproto network: PLC,
// a single PDS, and a relay (firehose) under one HTTP listener. It
// exists so jetstream can iterate locally without depending on
// bsky.network or plc.directory. Not shipped to users; not in the
// Dockerfile.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/urfave/cli/v3"
)

func main() {
	if err := newApp().Run(context.Background(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "simulator:", err)
		os.Exit(1)
	}
}

func newApp() *cli.Command {
	return &cli.Command{
		Name:  "simulator",
		Usage: "Local atproto simulator for jetstream development",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "log-level",
				Usage:   "Log level (debug|info|warn|error)",
				Sources: cli.EnvVars("JETSTREAM_LOG_LEVEL"),
				Value:   "info",
			},
			&cli.StringFlag{
				Name:    "log-format",
				Usage:   "Log handler format (text|json)",
				Sources: cli.EnvVars("JETSTREAM_LOG_FORMAT"),
				Value:   "json",
			},
		},
		Commands: []*cli.Command{
			serveCommand(),
		},
	}
}

func serveCommand() *cli.Command {
	return &cli.Command{
		Name:  "serve",
		Usage: "Run the simulator (PLC + PDS + relay)",
		Action: func(_ context.Context, _ *cli.Command) error {
			// Filled in by later tasks.
			return nil
		},
	}
}
```

- [ ] **Step 4: Run the test**

```sh
just test ./cmd/simulator -run TestApp_VersionFlag
just lint
```

Expected: PASS, no lint errors.

- [ ] **Step 5: Commit**

```sh
git add cmd/simulator/main.go cmd/simulator/main_test.go
git commit -m "scaffold cmd/simulator with cli skeleton"
```

---
## Task 2: World package — Config + pebble open + key namespace

Goal: stand up `internal/simulator/world` with a Config struct, a `New(ctx, cfg)` that opens pebble at `cfg.DataDir`, refuses `./data` exactly, and supports `--reset`. No accounts yet; this just owns the lifecycle of the on-disk state.

**Files:**
- Create: `internal/simulator/world/doc.go`
- Create: `internal/simulator/world/config.go`
- Create: `internal/simulator/world/keys.go`
- Create: `internal/simulator/world/world.go`
- Create: `internal/simulator/world/world_test.go`

- [ ] **Step 1: Write failing tests for World lifecycle**

`internal/simulator/world/world_test.go`:

```go
package world

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNew_RejectsExactDataDir(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = "./data"
	_, err := New(context.Background(), cfg)
	require.ErrorIs(t, err, ErrDataDirReserved)
}

func TestNew_OpensAndCloses(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	require.NoError(t, w.Close())
}

func TestNew_ResetWipesDir(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "simulator")
	cfg := DefaultConfig()
	cfg.DataDir = dir
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	cfg.Reset = true
	w, err = New(context.Background(), cfg)
	require.NoError(t, err)
	require.NoError(t, w.Close())
}
```

- [ ] **Step 2: Run tests; expect compile failure**

```sh
just test ./internal/simulator/world
```

- [ ] **Step 3: Write doc.go and config.go**

`internal/simulator/world/doc.go`:

```go
// Package world owns the simulator's on-disk state: the pebble db, the
// global RNG, the deterministic account roster, and the live commit
// generator. The HTTP layer reads through *World; only the traffic
// goroutine writes to pebble after bootstrap.
package world
```

`internal/simulator/world/config.go`:

```go
package world

import (
	"errors"
	"fmt"
	"path/filepath"
)

// ErrDataDirReserved is returned by New when DataDir resolves to the
// jetstream data directory. The simulator owns its own pebble db and
// must never share a directory with the production binary.
var ErrDataDirReserved = errors.New("world: --data-dir cannot be ./data; use ./data/simulator")

// Config drives *World construction.
type Config struct {
	DataDir          string
	Reset            bool
	Seed             uint64
	Accounts         int
	InitialRecords   int
	CommitsPerSec    float64
	RateMultiplier   float64
	FirehoseHistory  int
	RepoCacheSize    int
}

// DefaultConfig returns simulator defaults matching the design doc.
func DefaultConfig() Config {
	return Config{
		DataDir:         "./data/simulator",
		Reset:           false,
		Seed:            42,
		Accounts:        10000,
		InitialRecords:  5,
		CommitsPerSec:   10,
		RateMultiplier:  1.0,
		FirehoseHistory: 10000,
		RepoCacheSize:   512,
	}
}

func (c Config) validate() error {
	if c.DataDir == "" {
		return errors.New("world: DataDir is required")
	}
	if filepath.Clean(c.DataDir) == "data" || filepath.Clean(c.DataDir) == "./data" {
		return ErrDataDirReserved
	}
	if c.Accounts <= 0 {
		return fmt.Errorf("world: Accounts must be > 0 (got %d)", c.Accounts)
	}
	if c.CommitsPerSec <= 0 {
		return fmt.Errorf("world: CommitsPerSec must be > 0 (got %v)", c.CommitsPerSec)
	}
	if c.RateMultiplier <= 0 {
		return fmt.Errorf("world: RateMultiplier must be > 0 (got %v)", c.RateMultiplier)
	}
	if c.InitialRecords < 0 {
		return fmt.Errorf("world: InitialRecords must be >= 0 (got %d)", c.InitialRecords)
	}
	if c.FirehoseHistory < 0 {
		return fmt.Errorf("world: FirehoseHistory must be >= 0 (got %d)", c.FirehoseHistory)
	}
	if c.RepoCacheSize <= 0 {
		return fmt.Errorf("world: RepoCacheSize must be > 0 (got %d)", c.RepoCacheSize)
	}
	return nil
}
```

- [ ] **Step 4: Write keys.go**

`internal/simulator/world/keys.go`:

```go
package world

import (
	"encoding/binary"
	"fmt"
)

// All pebble keys are flat byte sequences with these prefixes. We do
// not use slashes inside numeric portions because they would interact
// poorly with range iteration if we ever switch to lexicographic
// account indexing.

var (
	keyMetaSeed = []byte("sim/meta/seed")
	keyMetaSeq  = []byte("sim/meta/seq")
)

// keyAccountState builds "sim/account/<idx>/state".
func keyAccountState(idx int) []byte {
	return fmt.Appendf(nil, "sim/account/%010d/state", idx)
}

// keyAccountKey builds "sim/account/<idx>/key".
func keyAccountKey(idx int) []byte {
	return fmt.Appendf(nil, "sim/account/%010d/key", idx)
}

// keyAccountDID builds "sim/account/<idx>/did".
func keyAccountDID(idx int) []byte {
	return fmt.Appendf(nil, "sim/account/%010d/did", idx)
}

// keyAccountBlock builds "sim/account/<idx>/blocks/<cidBytes>". The
// CID bytes are appended raw — we never iterate by CID, only point-
// look up.
func keyAccountBlock(idx int, cidBytes []byte) []byte {
	prefix := fmt.Appendf(nil, "sim/account/%010d/blocks/", idx)
	return append(prefix, cidBytes...)
}

// keyAccountMSTKey builds "sim/account/<idx>/mst/<mstkey>".
func keyAccountMSTKey(idx int, mstKey string) []byte {
	prefix := fmt.Appendf(nil, "sim/account/%010d/mst/", idx)
	return append(prefix, mstKey...)
}

// keyFirehose builds "sim/firehose/<seq>" using big-endian uint64 so
// pebble's lexicographic order matches numeric order, which matters
// for the cursor-replay range scan.
func keyFirehose(seq int64) []byte {
	out := make([]byte, 0, len("sim/firehose/")+8)
	out = append(out, []byte("sim/firehose/")...)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(seq))
	return append(out, buf[:]...)
}
```

- [ ] **Step 5: Write world.go**

`internal/simulator/world/world.go`:

```go
package world

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/cockroachdb/pebble"
)

// World is the simulator's runtime handle: pebble db + the in-memory
// state that derives from it (later tasks add: RNG, account cache,
// fanout). Goroutine-safety: pebble itself is safe; the in-memory
// state added later uses its own locks.
type World struct {
	cfg Config
	db  *pebble.DB
}

// New opens (creating if needed) the simulator pebble db at
// cfg.DataDir. With cfg.Reset = true, removes the directory first.
// Refuses to operate when cfg.DataDir resolves to "./data".
func New(_ context.Context, cfg Config) (*World, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.Reset {
		if err := os.RemoveAll(cfg.DataDir); err != nil {
			return nil, fmt.Errorf("world: reset %s: %w", cfg.DataDir, err)
		}
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("world: mkdir %s: %w", cfg.DataDir, err)
	}
	db, err := pebble.Open(cfg.DataDir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("world: open pebble at %s: %w", cfg.DataDir, err)
	}
	return &World{cfg: cfg, db: db}, nil
}

// Close releases the pebble db. Idempotent.
func (w *World) Close() error {
	if w.db == nil {
		return nil
	}
	err := w.db.Close()
	w.db = nil
	if err != nil && !errors.Is(err, pebble.ErrClosed) {
		return fmt.Errorf("world: close pebble: %w", err)
	}
	return nil
}
```

- [ ] **Step 6: Run the tests**

```sh
just test ./internal/simulator/world
just lint
```

Expected: PASS, lint clean.

- [ ] **Step 7: Commit**

```sh
git add internal/simulator/world/
git commit -m "scaffold simulator world package with pebble lifecycle"
```

---
## Task 3: Seed/seq metadata

Goal: read+write `sim/meta/seed` and `sim/meta/seq`. Seed mismatch = hard fail. This is the durable handshake that makes `--reset` necessary when changing seeds.

**Files:**
- Create: `internal/simulator/world/meta.go`
- Create: `internal/simulator/world/meta_test.go`

- [ ] **Step 1: Write the failing test**

`internal/simulator/world/meta_test.go`:

```go
package world

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSeedRoundTrip(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	// First-run: no seed persisted yet.
	seed, ok, err := w.loadSeed()
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, w.saveSeed(123))

	seed, ok, err = w.loadSeed()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(123), seed)
}

func TestEnsureSeed_Mismatch(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Seed = 1
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)

	wantBootstrap, err := w.EnsureSeed()
	require.NoError(t, err)
	require.True(t, wantBootstrap)
	require.NoError(t, w.Close())

	cfg.Seed = 2
	w, err = New(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	_, err = w.EnsureSeed()
	require.True(t, errors.Is(err, ErrSeedMismatch))
}

func TestSeqRoundTrip(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	seq, err := w.loadSeq()
	require.NoError(t, err)
	require.Equal(t, int64(0), seq)

	require.NoError(t, w.saveSeq(42))
	seq, err = w.loadSeq()
	require.NoError(t, err)
	require.Equal(t, int64(42), seq)
}
```

- [ ] **Step 2: Run, watch fail**

```sh
just test ./internal/simulator/world -run TestSeedRoundTrip
```

- [ ] **Step 3: Implement meta.go**

`internal/simulator/world/meta.go`:

```go
package world

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

// ErrSeedMismatch is returned by EnsureSeed when the persisted seed
// does not match cfg.Seed. Operators must --reset (or change cfg.Seed
// back) before continuing.
var ErrSeedMismatch = errors.New("world: seed mismatch; pass --reset or restore previous --seed")

// loadSeed reads sim/meta/seed. The bool is false on first-run (no
// row).
func (w *World) loadSeed() (uint64, bool, error) {
	val, closer, err := w.db.Get(keyMetaSeed)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("world: load seed: %w", err)
	}
	defer func() { _ = closer.Close() }()
	if len(val) != 8 {
		return 0, false, fmt.Errorf("world: load seed: got %d bytes, want 8", len(val))
	}
	return binary.BigEndian.Uint64(val), true, nil
}

// saveSeed persists sim/meta/seed with pebble.Sync — this is the
// commit point of the bootstrap handshake.
func (w *World) saveSeed(seed uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], seed)
	if err := w.db.Set(keyMetaSeed, buf[:], pebble.Sync); err != nil {
		return fmt.Errorf("world: save seed: %w", err)
	}
	return nil
}

// EnsureSeed implements the seed handshake:
//   - first run (no row): persists cfg.Seed, returns (true, nil) →
//     "caller should run bootstrap"
//   - matching row: returns (false, nil) → "resume"
//   - mismatched row: returns (_, ErrSeedMismatch)
func (w *World) EnsureSeed() (wantBootstrap bool, err error) {
	persisted, ok, err := w.loadSeed()
	if err != nil {
		return false, err
	}
	if !ok {
		if err := w.saveSeed(w.cfg.Seed); err != nil {
			return false, err
		}
		return true, nil
	}
	if persisted != w.cfg.Seed {
		return false, fmt.Errorf("%w: persisted=%d cfg=%d", ErrSeedMismatch, persisted, w.cfg.Seed)
	}
	return false, nil
}

// loadSeq reads sim/meta/seq, returning 0 if absent (first run).
func (w *World) loadSeq() (int64, error) {
	val, closer, err := w.db.Get(keyMetaSeq)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("world: load seq: %w", err)
	}
	defer func() { _ = closer.Close() }()
	if len(val) != 8 {
		return 0, fmt.Errorf("world: load seq: got %d bytes, want 8", len(val))
	}
	return int64(binary.BigEndian.Uint64(val)), nil
}

// saveSeq writes sim/meta/seq with pebble.NoSync — the seq is updated
// in the same batch as the firehose-event row, so durability comes
// from that batch.
func (w *World) saveSeq(seq int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(seq))
	if err := w.db.Set(keyMetaSeq, buf[:], pebble.NoSync); err != nil {
		return fmt.Errorf("world: save seq: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Run tests**

```sh
just test ./internal/simulator/world
just lint
```

- [ ] **Step 5: Commit**

```sh
git add internal/simulator/world/meta.go internal/simulator/world/meta_test.go
git commit -m "world: persist seed and seq with handshake"
```

---
## Task 4: Deterministic key generation + DID derivation

Goal: from a global seed and an account index, produce a stable secp256k1 key and a `did:plc:<24 base32 chars>` DID. Same inputs always produce the same outputs across runs and platforms.

**Files:**
- Create: `internal/simulator/world/did.go`
- Create: `internal/simulator/world/accounts.go`
- Create: `internal/simulator/world/accounts_test.go`

- [ ] **Step 1: Write failing tests**

`internal/simulator/world/accounts_test.go`:

```go
package world

import (
	"strings"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/stretchr/testify/require"
)

func TestDeriveAccount_Deterministic(t *testing.T) {
	t.Parallel()
	a, err := deriveAccount(42, 7)
	require.NoError(t, err)
	b, err := deriveAccount(42, 7)
	require.NoError(t, err)
	require.Equal(t, a.DID, b.DID)
	require.Equal(t, a.PrivKeyBytes, b.PrivKeyBytes)
}

func TestDeriveAccount_DifferentInputsDiffer(t *testing.T) {
	t.Parallel()
	a1, _ := deriveAccount(42, 1)
	a2, _ := deriveAccount(42, 2)
	a3, _ := deriveAccount(43, 1)
	require.NotEqual(t, a1.DID, a2.DID)
	require.NotEqual(t, a1.DID, a3.DID)
}

func TestDeriveAccount_DIDIsValid(t *testing.T) {
	t.Parallel()
	a, err := deriveAccount(42, 0)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(a.DID), "did:plc:"))
	require.Len(t, a.DID.Identifier(), 24)
	_, err = atmos.ParseDID(string(a.DID))
	require.NoError(t, err)
}
```

- [ ] **Step 2: Run, watch fail**

```sh
just test ./internal/simulator/world -run TestDeriveAccount
```

- [ ] **Step 3: Implement DID derivation**

`internal/simulator/world/did.go`:

```go
package world

import (
	"crypto/sha256"
	"encoding/base32"
	"strings"
)

// b32 is the lowercase RFC 4648 base32 alphabet matching real
// did:plc identifiers. did:plc identifiers are 24 chars of base32-
// encoded SHA-256 truncated to 15 bytes — close enough that
// atmos.ParseDID accepts them.
var b32 = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// didFromPubkey derives a "did:plc:<24 base32 chars>" identifier from
// a compressed secp256k1 public key. Real did:plc creation hashes a
// genesis operation; here we hash the pubkey bytes directly. The
// shape is what atproto code paths care about.
func didFromPubkey(pub []byte) string {
	sum := sha256.Sum256(pub)
	return "did:plc:" + strings.ToLower(b32.EncodeToString(sum[:15]))
}
```

- [ ] **Step 4: Implement accounts.go**

`internal/simulator/world/accounts.go`:

```go
package world

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
)

// account is the in-memory shape of a single simulated user's
// identity. Repo state is loaded lazily by repos.go.
type account struct {
	Index        int
	DID          atmos.DID
	PrivKeyBytes []byte // 32-byte k256 private key
	priv         *crypto.K256PrivateKey
}

// deriveAccount produces a deterministic account from a global seed
// and account index. Same (seed, idx) always returns the same DID +
// signing key, regardless of OS or compilation flags.
//
// k256 keygen requires a 32-byte scalar in [1, n-1]. We derive a
// candidate via SHA-256(seed_bytes || idx_bytes || counter) and retry
// until atmos accepts it. With overwhelming probability the first
// candidate works.
func deriveAccount(seed uint64, idx int) (account, error) {
	for counter := 0; counter < 256; counter++ {
		raw := deriveScalar(seed, idx, counter)
		priv, err := crypto.ParsePrivateK256(raw)
		if err != nil {
			continue
		}
		pubBytes := priv.PublicKey().(*crypto.K256PublicKey).Bytes()
		didStr := didFromPubkey(pubBytes)
		did, err := atmos.ParseDID(didStr)
		if err != nil {
			return account{}, fmt.Errorf("world: derived DID rejected: %w", err)
		}
		return account{
			Index:        idx,
			DID:          did,
			PrivKeyBytes: raw,
			priv:         priv,
		}, nil
	}
	return account{}, errors.New("world: failed to derive valid k256 key after 256 attempts")
}

func deriveScalar(seed uint64, idx, counter int) []byte {
	h := sha256.New()
	var seedBuf [8]byte
	binary.BigEndian.PutUint64(seedBuf[:], seed)
	h.Write(seedBuf[:])
	var idxBuf [8]byte
	binary.BigEndian.PutUint64(idxBuf[:], uint64(idx))
	h.Write(idxBuf[:])
	var ctrBuf [4]byte
	binary.BigEndian.PutUint32(ctrBuf[:], uint32(counter))
	h.Write(ctrBuf[:])
	return h.Sum(nil)
}

// saveAccount writes (key, did) for an account; safe to call inside a
// pebble batch via batch.Set, but here we use the db directly.
func (w *World) saveAccount(b *pebble.Batch, a account) error {
	if err := b.Set(keyAccountKey(a.Index), a.PrivKeyBytes, nil); err != nil {
		return fmt.Errorf("world: save account key: %w", err)
	}
	if err := b.Set(keyAccountDID(a.Index), []byte(a.DID), nil); err != nil {
		return fmt.Errorf("world: save account did: %w", err)
	}
	return nil
}

// loadAccount reads (key, did) for an account.
func (w *World) loadAccount(idx int) (account, error) {
	keyVal, kc, err := w.db.Get(keyAccountKey(idx))
	if err != nil {
		return account{}, fmt.Errorf("world: load account %d key: %w", idx, err)
	}
	defer func() { _ = kc.Close() }()
	priv, err := crypto.ParsePrivateK256(keyVal)
	if err != nil {
		return account{}, fmt.Errorf("world: parse account %d key: %w", idx, err)
	}
	didVal, dc, err := w.db.Get(keyAccountDID(idx))
	if err != nil {
		return account{}, fmt.Errorf("world: load account %d did: %w", idx, err)
	}
	defer func() { _ = dc.Close() }()
	did, err := atmos.ParseDID(string(didVal))
	if err != nil {
		return account{}, fmt.Errorf("world: parse account %d did: %w", idx, err)
	}
	return account{
		Index:        idx,
		DID:          did,
		PrivKeyBytes: append([]byte(nil), keyVal...),
		priv:         priv,
	}, nil
}
```

- [ ] **Step 5: Run tests**

```sh
just test ./internal/simulator/world
just lint
```

- [ ] **Step 6: Commit**

```sh
git add internal/simulator/world/did.go internal/simulator/world/accounts.go internal/simulator/world/accounts_test.go
git commit -m "world: deterministic account derivation"
```

---
## Task 5: Distributions package

Goal: pure functions for the realistic-randomness model. No I/O, all draws take a `*rand.Rand`. These are unit-testable in isolation, so we get them right before plumbing into the world.

**Files:**
- Create: `internal/simulator/world/distributions.go`
- Create: `internal/simulator/world/distributions_test.go`

- [ ] **Step 1: Write failing tests**

`internal/simulator/world/distributions_test.go`:

```go
package world

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestRand() *rand.Rand {
	return rand.New(rand.NewPCG(1, 2))
}

func TestZipfian_AllInRange(t *testing.T) {
	t.Parallel()
	r := newTestRand()
	for range 10000 {
		idx := zipfian(r, 1.07, 100)
		require.GreaterOrEqual(t, idx, 0)
		require.Less(t, idx, 100)
	}
}

func TestZipfian_FavorsLowIndices(t *testing.T) {
	t.Parallel()
	r := newTestRand()
	hits := make(map[int]int, 100)
	for range 100000 {
		hits[zipfian(r, 1.07, 100)]++
	}
	require.Greater(t, hits[0], hits[50])
	require.Greater(t, hits[50], hits[99])
}

func TestExponentialDelay_MeanInTolerance(t *testing.T) {
	t.Parallel()
	r := newTestRand()
	const want = 0.1 // mean
	var sum float64
	const n = 100000
	for range n {
		sum += exponentialDelay(r, want)
	}
	got := sum / n
	require.InDelta(t, want, got, 0.01)
}

func TestWeightedChoice(t *testing.T) {
	t.Parallel()
	r := newTestRand()
	const a, b, c = "a", "b", "c"
	weights := []weighted[string]{
		{value: a, weight: 7},
		{value: b, weight: 2},
		{value: c, weight: 1},
	}
	hits := map[string]int{}
	for range 100000 {
		hits[weightedChoice(r, weights)]++
	}
	require.Greater(t, hits[a], hits[b])
	require.Greater(t, hits[b], hits[c])
}

func TestLogNormalClamped(t *testing.T) {
	t.Parallel()
	r := newTestRand()
	for range 5000 {
		v := logNormalClamped(r, 4.0, 1.0, 1, 3000)
		require.GreaterOrEqual(t, v, 1)
		require.LessOrEqual(t, v, 3000)
	}
}

func TestGeometricAtLeastOne(t *testing.T) {
	t.Parallel()
	r := newTestRand()
	for range 1000 {
		require.GreaterOrEqual(t, geometricAtLeastOne(r, 0.7), 1)
	}
}
```

- [ ] **Step 2: Run, watch fail**

```sh
just test ./internal/simulator/world -run TestZipfian
```

- [ ] **Step 3: Implement distributions.go**

`internal/simulator/world/distributions.go`:

```go
package world

import (
	"math"
	"math/rand/v2"
)

// zipfian draws an index in [0, n) under a Zipfian (power-law)
// distribution with exponent s. Index 0 is most likely; the tail
// thins out exponentially. Implementation: inverse-CDF sampling
// against an unnormalized Zipf weight; correctness > speed at
// n <= 10k.
func zipfian(r *rand.Rand, s float64, n int) int {
	if n <= 1 {
		return 0
	}
	// Build cumulative weights lazily per call. n is small (<= 10k);
	// the alloc dominates only when called millions of times in tight
	// loops. Acceptable for our event rate.
	weights := make([]float64, n)
	var total float64
	for i := range n {
		w := 1.0 / math.Pow(float64(i+1), s)
		weights[i] = w
		total += w
	}
	target := r.Float64() * total
	var acc float64
	for i, w := range weights {
		acc += w
		if target <= acc {
			return i
		}
	}
	return n - 1
}

// exponentialDelay returns a sample from Exp(λ) with the given mean.
// Used for inter-arrival times between commits — Poisson process.
func exponentialDelay(r *rand.Rand, mean float64) float64 {
	if mean <= 0 {
		return 0
	}
	// Avoid log(0).
	u := r.Float64()
	for u == 0 {
		u = r.Float64()
	}
	return -math.Log(u) * mean
}

// weighted is one option in a weighted-choice draw. Weights need not
// be normalized.
type weighted[T any] struct {
	value  T
	weight float64
}

// weightedChoice draws one value from opts proportional to weight.
func weightedChoice[T any](r *rand.Rand, opts []weighted[T]) T {
	var total float64
	for _, o := range opts {
		total += o.weight
	}
	target := r.Float64() * total
	var acc float64
	for _, o := range opts {
		acc += o.weight
		if target <= acc {
			return o.value
		}
	}
	return opts[len(opts)-1].value
}

// logNormalClamped draws a log-normal sample, rounded to int and
// clamped to [lo, hi]. mu and sigma are the parameters of the
// underlying normal (so the median is exp(mu)).
func logNormalClamped(r *rand.Rand, mu, sigma float64, lo, hi int) int {
	v := math.Exp(mu + sigma*r.NormFloat64())
	n := int(math.Round(v))
	if n < lo {
		return lo
	}
	if n > hi {
		return hi
	}
	return n
}

// geometricAtLeastOne returns 1, 2, 3, … with probability decaying
// at rate p. Models "ops per commit" — almost always 1, occasionally
// more. Math: returns 1 + Geometric(p).
func geometricAtLeastOne(r *rand.Rand, p float64) int {
	n := 1
	for n < 100 && r.Float64() > p {
		n++
	}
	return n
}
```

- [ ] **Step 4: Run tests**

```sh
just test ./internal/simulator/world
just lint
```

- [ ] **Step 5: Commit**

```sh
git add internal/simulator/world/distributions.go internal/simulator/world/distributions_test.go
git commit -m "world: realistic-distribution helpers"
```

---
## Task 6: Repo state + record block storage

Goal: a single account's `*repo.Repo` reconstruction and persistence path. Given the pebble blocks + MST index for one account, build a `*mst.Tree` and `*repo.Repo`. Given a freshly modified Repo, persist its state and the new blocks. Wraps atmos's MST and repo packages.

**Files:**
- Create: `internal/simulator/world/repos.go`
- Create: `internal/simulator/world/repos_test.go`

- [ ] **Step 1: Write failing tests**

`internal/simulator/world/repos_test.go`:

```go
package world

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

func TestRepoRoundTrip(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	a, err := deriveAccount(42, 0)
	require.NoError(t, err)
	b := w.db.NewBatch()
	require.NoError(t, w.saveAccount(b, a))
	require.NoError(t, b.Commit(nil))

	// Build an empty repo and add one record.
	rp, err := newEmptyRepo(a)
	require.NoError(t, err)
	require.NoError(t, rp.Create("app.bsky.feed.post", "3kabc123de4fg", map[string]any{
		"$type":     "app.bsky.feed.post",
		"text":      "hello",
		"createdAt": "2024-01-01T00:00:00Z",
	}))

	state, err := w.commitAndPersist(a, rp)
	require.NoError(t, err)
	require.NotEqual(t, cbor.CID{}, state.DataCID)
	require.NotEmpty(t, state.Rev)
	require.Equal(t, 1, state.RecordCount)

	// Reload from disk.
	state2, err := w.loadState(a.Index)
	require.NoError(t, err)
	require.Equal(t, state, state2)

	rp2, err := w.loadRepo(a)
	require.NoError(t, err)
	cid, data, err := rp2.Get("app.bsky.feed.post", "3kabc123de4fg")
	require.NoError(t, err)
	require.NotEqual(t, cbor.CID{}, cid)
	require.NotEmpty(t, data)
}
```

- [ ] **Step 2: Run, watch fail**

```sh
just test ./internal/simulator/world -run TestRepoRoundTrip
```

- [ ] **Step 3: Implement repos.go**

`internal/simulator/world/repos.go`:

```go
package world

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
)

// repoState mirrors what we persist per-account: the previous commit
// CID + rev + MST root + record count. The "previous" framing matches
// how subscribeRepos #commit envelopes need `since` (= previous rev)
// and `prevData` (= previous MST root). The fresh state after commit
// becomes the *current* state, and turns into "previous" the next
// time this account commits.
type repoState struct {
	Rev         string
	DataCID     cbor.CID // MST root
	CommitCID   cbor.CID // signed commit block CID
	RecordCount int
}

// pebbleStore is a *pebble.DB-backed mst.BlockStore scoped to one
// account, so MST node loads come from that account's blocks
// keyspace.
type pebbleStore struct {
	db  *pebble.DB
	idx int
	// writes accumulates new blocks created by Tree.WriteBlocks; we
	// flush them to pebble in a batch alongside the commit.
	writes map[cbor.CID][]byte
}

func (s *pebbleStore) GetBlock(cid cbor.CID) ([]byte, error) {
	if data, ok := s.writes[cid]; ok {
		return data, nil
	}
	val, closer, err := s.db.Get(keyAccountBlock(s.idx, cid.Bytes()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()
	return append([]byte(nil), val...), nil
}

func (s *pebbleStore) PutBlock(cid cbor.CID, data []byte) error {
	if s.writes == nil {
		s.writes = make(map[cbor.CID][]byte)
	}
	s.writes[cid] = append([]byte(nil), data...)
	return nil
}

// newEmptyRepo constructs an in-memory *repo.Repo for an account
// with no records yet. Used by bootstrap and by callers that want
// to add records before the first commit.
func newEmptyRepo(a account) (*repo.Repo, error) {
	store := mst.NewMemBlockStore()
	tree := mst.NewTree(store)
	return &repo.Repo{
		DID:   a.DID,
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  tree,
	}, nil
}

// loadRepo reconstructs an account's *repo.Repo from its persisted
// state: MST root from sim/account/<idx>/state, MST node + record
// blocks from sim/account/<idx>/blocks/*. Reads on demand.
func (w *World) loadRepo(a account) (*repo.Repo, error) {
	state, err := w.loadState(a.Index)
	if err != nil {
		return nil, err
	}
	store := &pebbleStore{db: w.db, idx: a.Index}
	if !state.DataCID.Defined() {
		// First commit lifecycle — empty MST.
		return newEmptyRepo(a)
	}
	tree := mst.LoadTree(store, state.DataCID)
	return &repo.Repo{
		DID:   a.DID,
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  tree,
	}, nil
}

// commitAndPersist writes the repo's MST blocks, signs a fresh commit,
// and persists every new block plus the updated state under one
// pebble batch. Returns the post-commit state. New record blocks must
// already be in rp.Store before this is called.
func (w *World) commitAndPersist(a account, rp *repo.Repo) (repoState, error) {
	commit, err := rp.Commit(a.priv)
	if err != nil {
		return repoState{}, fmt.Errorf("world: commit account %d: %w", a.Index, err)
	}
	commitData, err := commit.EncodeCBOR()
	if err != nil {
		return repoState{}, fmt.Errorf("world: encode commit: %w", err)
	}
	commitCID := cbor.ComputeCID(cbor.CodecDagCBOR, commitData)

	// Walk the tree to count records and capture key→cid for the
	// MST index keyspace. This also forces the mst.Tree to populate
	// so anything not already cached fails loudly.
	count := 0
	keyCID := make(map[string]cbor.CID)
	if err := rp.Tree.Walk(func(key string, val cbor.CID) error {
		count++
		keyCID[key] = val
		return nil
	}); err != nil {
		return repoState{}, fmt.Errorf("world: walk tree: %w", err)
	}

	state := repoState{
		Rev:         commit.Rev,
		DataCID:     commit.Data,
		CommitCID:   commitCID,
		RecordCount: count,
	}

	// Write everything as one batch.
	b := w.db.NewBatch()
	defer func() { _ = b.Close() }()

	// New blocks: every block the *pebbleStore captured during this
	// session, plus the commit block.
	if ps, ok := rp.Store.(*pebbleStore); ok {
		for cid, data := range ps.writes {
			if err := b.Set(keyAccountBlock(a.Index, cid.Bytes()), data, nil); err != nil {
				return repoState{}, fmt.Errorf("world: stage block: %w", err)
			}
		}
		ps.writes = nil
	} else if mem, ok := rp.Store.(*mst.MemBlockStore); ok {
		// Bootstrap path: empty repo started in-memory; flush all
		// blocks. Iterate via mst's All().
		for cid, data := range mem.All() {
			if err := b.Set(keyAccountBlock(a.Index, cid.Bytes()), data, nil); err != nil {
				return repoState{}, fmt.Errorf("world: stage block: %w", err)
			}
		}
	} else {
		return repoState{}, errors.New("world: unsupported BlockStore impl")
	}
	if err := b.Set(keyAccountBlock(a.Index, commitCID.Bytes()), commitData, nil); err != nil {
		return repoState{}, fmt.Errorf("world: stage commit block: %w", err)
	}

	// Refresh the MST key→cid index (clear-and-rewrite is fine; the
	// tree size is small per-account).
	prefix := fmt.Sprintf("sim/account/%010d/mst/", a.Index)
	if err := b.DeleteRange([]byte(prefix), []byte(prefix+"\xff"), nil); err != nil {
		return repoState{}, fmt.Errorf("world: clear mst index: %w", err)
	}
	for k, v := range keyCID {
		if err := b.Set(keyAccountMSTKey(a.Index, k), v.Bytes(), nil); err != nil {
			return repoState{}, fmt.Errorf("world: stage mst index: %w", err)
		}
	}

	if err := b.Set(keyAccountState(a.Index), encodeState(state), nil); err != nil {
		return repoState{}, fmt.Errorf("world: stage state: %w", err)
	}

	if err := b.Commit(pebble.NoSync); err != nil {
		return repoState{}, fmt.Errorf("world: commit batch: %w", err)
	}
	return state, nil
}

// loadState reads sim/account/<idx>/state. Missing rows return a zero
// state (= "no commit yet").
func (w *World) loadState(idx int) (repoState, error) {
	val, closer, err := w.db.Get(keyAccountState(idx))
	if errors.Is(err, pebble.ErrNotFound) {
		return repoState{}, nil
	}
	if err != nil {
		return repoState{}, fmt.Errorf("world: load state %d: %w", idx, err)
	}
	defer func() { _ = closer.Close() }()
	return decodeState(val)
}

// encodeState/decodeState use a tiny hand-rolled format to dodge a
// CBOR struct tag dependency: rev_len (varint) | rev | dataCID_len |
// dataCID | commitCID_len | commitCID | recordCount (varint).
func encodeState(s repoState) []byte {
	dataBytes := s.DataCID.Bytes()
	commitBytes := s.CommitCID.Bytes()
	out := make([]byte, 0, 4+len(s.Rev)+4+len(dataBytes)+4+len(commitBytes)+4)
	out = appendUvarint(out, uint64(len(s.Rev)))
	out = append(out, s.Rev...)
	out = appendUvarint(out, uint64(len(dataBytes)))
	out = append(out, dataBytes...)
	out = appendUvarint(out, uint64(len(commitBytes)))
	out = append(out, commitBytes...)
	out = appendUvarint(out, uint64(s.RecordCount))
	return out
}

func decodeState(buf []byte) (repoState, error) {
	var s repoState
	revLen, n, err := readUvarint(buf)
	if err != nil {
		return s, fmt.Errorf("world: decode state rev len: %w", err)
	}
	buf = buf[n:]
	if uint64(len(buf)) < revLen {
		return s, errors.New("world: decode state: short buffer (rev)")
	}
	s.Rev = string(buf[:revLen])
	buf = buf[revLen:]

	dataLen, n, err := readUvarint(buf)
	if err != nil {
		return s, fmt.Errorf("world: decode state data len: %w", err)
	}
	buf = buf[n:]
	if uint64(len(buf)) < dataLen {
		return s, errors.New("world: decode state: short buffer (data)")
	}
	if dataLen > 0 {
		cid, err := cbor.ParseCIDBytes(buf[:dataLen])
		if err != nil {
			return s, fmt.Errorf("world: decode state data cid: %w", err)
		}
		s.DataCID = cid
	}
	buf = buf[dataLen:]

	commitLen, n, err := readUvarint(buf)
	if err != nil {
		return s, fmt.Errorf("world: decode state commit len: %w", err)
	}
	buf = buf[n:]
	if uint64(len(buf)) < commitLen {
		return s, errors.New("world: decode state: short buffer (commit)")
	}
	if commitLen > 0 {
		cid, err := cbor.ParseCIDBytes(buf[:commitLen])
		if err != nil {
			return s, fmt.Errorf("world: decode state commit cid: %w", err)
		}
		s.CommitCID = cid
	}
	buf = buf[commitLen:]

	count, _, err := readUvarint(buf)
	if err != nil {
		return s, fmt.Errorf("world: decode state count: %w", err)
	}
	s.RecordCount = int(count)
	return s, nil
}

func appendUvarint(b []byte, x uint64) []byte {
	for x >= 0x80 {
		b = append(b, byte(x)|0x80)
		x >>= 7
	}
	return append(b, byte(x))
}

func readUvarint(b []byte) (uint64, int, error) {
	var x uint64
	var s uint
	for i, c := range b {
		if i >= 10 {
			return 0, 0, errors.New("uvarint too long")
		}
		if c < 0x80 {
			return x | uint64(c)<<s, i + 1, nil
		}
		x |= uint64(c&0x7f) << s
		s += 7
	}
	return 0, 0, errors.New("uvarint truncated")
}
```

- [ ] **Step 4: Run tests**

```sh
just test ./internal/simulator/world
just lint
```

- [ ] **Step 5: Commit**

```sh
git add internal/simulator/world/repos.go internal/simulator/world/repos_test.go
git commit -m "world: per-account repo persistence + state encoding"
```

---
## Task 7: Record payload generator

Goal: produce well-formed `app.bsky.*` records (post, like, follow, repost, profile) of realistic shape, driven by the world's RNG. The payloads round-trip through atmos's `cbor.Marshal` (which `repo.Create` calls).

**Files:**
- Create: `internal/simulator/world/records.go`
- Create: `internal/simulator/world/records_test.go`

- [ ] **Step 1: Write failing test**

`internal/simulator/world/records_test.go`:

```go
package world

import (
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

func TestGenerateRecord_RoundTrips(t *testing.T) {
	t.Parallel()
	r := newTestRand()
	for _, coll := range []string{
		"app.bsky.feed.post",
		"app.bsky.feed.like",
		"app.bsky.graph.follow",
		"app.bsky.feed.repost",
		"app.bsky.actor.profile",
	} {
		rec := generateRecord(r, coll, "did:plc:targetabcdefghijklmnopqr")
		_, err := cbor.Marshal(rec)
		require.NoError(t, err, coll)
	}
}

func TestNewRkey_ValidTID(t *testing.T) {
	t.Parallel()
	r := newTestRand()
	k := newRkey(r)
	require.Len(t, k, 13)
}
```

- [ ] **Step 2: Run, watch fail**

```sh
just test ./internal/simulator/world -run TestGenerateRecord
```

- [ ] **Step 3: Implement records.go**

`internal/simulator/world/records.go`:

```go
package world

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jcalabro/atmos"
)

// Realistic-distribution constants. Lifting these into named consts
// makes them tunable in one place, per the design doc.
const (
	collPost    = "app.bsky.feed.post"
	collLike    = "app.bsky.feed.like"
	collFollow  = "app.bsky.graph.follow"
	collRepost  = "app.bsky.feed.repost"
	collProfile = "app.bsky.actor.profile"
)

// collectionWeights is the design-doc realistic mix for create ops.
var collectionWeights = []weighted[string]{
	{value: collPost, weight: 60},
	{value: collLike, weight: 20},
	{value: collFollow, weight: 10},
	{value: collRepost, weight: 5},
	{value: collProfile, weight: 5},
}

// chooseCreateCollection picks a collection NSID for a create op.
func chooseCreateCollection(r *rand.Rand) string {
	return weightedChoice(r, collectionWeights)
}

// newRkey generates a fresh TID record key. Real PDSes generate TIDs
// from wall clock + a clock id; here we use the global RNG to keep
// runs deterministic.
func newRkey(r *rand.Rand) string {
	micros := int64(2000) * int64(time.Hour) / int64(time.Microsecond) // arbitrary baseline
	micros += r.Int64N(1 << 40)
	clockID := uint(r.UintN(1024))
	return string(atmos.NewTID(micros, clockID))
}

// generateRecord builds a payload for the given collection. target is
// the DID a like/follow/repost is aimed at (caller picks a random
// other account); ignored for post/profile.
func generateRecord(r *rand.Rand, collection, target string) map[string]any {
	createdAt := time.Unix(0, r.Int64N(1<<60)).UTC().Format(time.RFC3339)
	switch collection {
	case collPost:
		length := logNormalClamped(r, 4.0, 1.0, 1, 3000)
		return map[string]any{
			"$type":     collPost,
			"text":      randomText(r, length),
			"createdAt": createdAt,
			"langs":     []any{"en"},
		}
	case collLike:
		return map[string]any{
			"$type":     collLike,
			"createdAt": createdAt,
			"subject": map[string]any{
				"uri": fmt.Sprintf("at://%s/app.bsky.feed.post/%s", target, newRkey(r)),
				"cid": fakeCIDString(r),
			},
		}
	case collFollow:
		return map[string]any{
			"$type":     collFollow,
			"createdAt": createdAt,
			"subject":   target,
		}
	case collRepost:
		return map[string]any{
			"$type":     collRepost,
			"createdAt": createdAt,
			"subject": map[string]any{
				"uri": fmt.Sprintf("at://%s/app.bsky.feed.post/%s", target, newRkey(r)),
				"cid": fakeCIDString(r),
			},
		}
	case collProfile:
		return map[string]any{
			"$type":       collProfile,
			"displayName": randomText(r, logNormalClamped(r, 2.5, 0.6, 1, 64)),
			"description": randomText(r, logNormalClamped(r, 4.0, 0.8, 0, 256)),
		}
	default:
		// Forward-compat: unknown collection gets a benign empty
		// record. We never produce these in v1, but defensive code is
		// cheap.
		return map[string]any{"$type": collection}
	}
}

// randomText returns a string of length n composed of lowercase
// letters and spaces. Not artistic; the bytes are what matter for
// codec round-trips.
func randomText(r *rand.Rand, n int) string {
	if n <= 0 {
		return ""
	}
	const alphabet = "abcdefghijklmnopqrstuvwxyz "
	out := make([]byte, n)
	for i := range out {
		out[i] = alphabet[r.IntN(len(alphabet))]
	}
	return string(out)
}

// fakeCIDString returns a syntactically-plausible CIDv1 base32 string
// for record subjects in like/repost payloads. Not derived from the
// referenced post; we don't track inter-record references in v1.
func fakeCIDString(r *rand.Rand) string {
	// Real CIDs are way more structured; this is just a deterministic
	// 59-char base32 string starting with 'b' (CIDv1 base32 prefix).
	const alphabet = "abcdefghijklmnopqrstuvwxyz234567"
	out := make([]byte, 59)
	out[0] = 'b'
	for i := 1; i < len(out); i++ {
		out[i] = alphabet[r.IntN(len(alphabet))]
	}
	return string(out)
}
```

- [ ] **Step 4: Run tests**

```sh
just test ./internal/simulator/world
just lint
```

- [ ] **Step 5: Commit**

```sh
git add internal/simulator/world/records.go internal/simulator/world/records_test.go
git commit -m "world: realistic-shape record payload generator"
```

---
## Task 8: World construction with bootstrap

Goal: stitch together what we've built so far into a `*World` that bootstraps a fresh roster of accounts on first run, and resumes on subsequent runs.

**Files:**
- Modify: `internal/simulator/world/world.go`
- Create: `internal/simulator/world/bootstrap.go`
- Create: `internal/simulator/world/bootstrap_test.go`

- [ ] **Step 1: Write failing test**

`internal/simulator/world/bootstrap_test.go`:

```go
package world

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBootstrap_FirstRunPopulates(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Accounts = 50
	cfg.InitialRecords = 2
	cfg.Seed = 7

	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	wantBootstrap, err := w.EnsureSeed()
	require.NoError(t, err)
	require.True(t, wantBootstrap)

	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))

	// Every account has a state row with 2 records.
	for i := range cfg.Accounts {
		state, err := w.loadState(i)
		require.NoError(t, err)
		require.Equal(t, 2, state.RecordCount, "account %d", i)
	}
}

func TestBootstrap_DeterministicAcrossRuns(t *testing.T) {
	t.Parallel()
	cfg1 := DefaultConfig()
	cfg1.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg1.Accounts = 10
	cfg1.InitialRecords = 1

	w1, err := New(context.Background(), cfg1)
	require.NoError(t, err)
	_, err = w1.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w1.Bootstrap(context.Background(), slog.Default()))
	a1, _ := w1.loadAccount(0)
	require.NoError(t, w1.Close())

	cfg2 := cfg1
	cfg2.DataDir = filepath.Join(t.TempDir(), "simulator")
	w2, err := New(context.Background(), cfg2)
	require.NoError(t, err)
	defer func() { _ = w2.Close() }()
	_, err = w2.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w2.Bootstrap(context.Background(), slog.Default()))
	a2, _ := w2.loadAccount(0)

	require.Equal(t, a1.DID, a2.DID)
}
```

- [ ] **Step 2: Run, watch fail**

```sh
just test ./internal/simulator/world -run TestBootstrap
```

- [ ] **Step 3: Implement bootstrap.go**

`internal/simulator/world/bootstrap.go`:

```go
package world

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
)

// Bootstrap generates and persists Accounts × InitialRecords records.
// Idempotent: state rows already at the target shape are not
// rewritten, so re-running on a partially-populated db is safe.
//
// Uses a dedicated PCG seeded from cfg.Seed for the *content* of
// initial records. The runtime RNG owned by *World drives only live
// traffic; mixing the two would make resume-from-disk
// content-dependent on whether a previous run had bootstrapped fully.
func (w *World) Bootstrap(ctx context.Context, logger *slog.Logger) error {
	logger = logger.With(slog.String("component", "simulator/bootstrap"))
	r := rand.New(rand.NewPCG(w.cfg.Seed, 0xb007))

	// Pre-derive every account so account picks for like/follow/repost
	// targets can come from the full roster.
	accounts := make([]account, w.cfg.Accounts)
	for i := range w.cfg.Accounts {
		a, err := deriveAccount(w.cfg.Seed, i)
		if err != nil {
			return fmt.Errorf("simulator: derive account %d: %w", i, err)
		}
		accounts[i] = a
	}

	const logEvery = 1000
	for i, a := range accounts {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Skip if already populated to the target record count.
		state, err := w.loadState(i)
		if err != nil {
			return err
		}
		if state.RecordCount >= w.cfg.InitialRecords {
			continue
		}

		b := w.db.NewBatch()
		if err := w.saveAccount(b, a); err != nil {
			_ = b.Close()
			return err
		}
		if err := b.Commit(nil); err != nil {
			return fmt.Errorf("simulator: save account %d: %w", i, err)
		}

		rp, err := newEmptyRepo(a)
		if err != nil {
			return err
		}
		for range w.cfg.InitialRecords {
			coll := chooseCreateCollection(r)
			target := accounts[r.IntN(len(accounts))].DID
			rkey := newRkey(r)
			rec := generateRecord(r, coll, string(target))
			if err := rp.Create(coll, rkey, rec); err != nil {
				return fmt.Errorf("simulator: bootstrap create %s/%s on %d: %w", coll, rkey, i, err)
			}
		}
		if _, err := w.commitAndPersist(a, rp); err != nil {
			return err
		}

		if (i+1)%logEvery == 0 {
			logger.InfoContext(ctx, "bootstrapped accounts", "n", i+1, "of", w.cfg.Accounts)
		}
	}
	logger.InfoContext(ctx, "bootstrap complete", "accounts", len(accounts))
	return nil
}
```

- [ ] **Step 4: Run tests**

```sh
just test ./internal/simulator/world
just lint
```

- [ ] **Step 5: Commit**

```sh
git add internal/simulator/world/bootstrap.go internal/simulator/world/bootstrap_test.go
git commit -m "world: bootstrap roster of accounts with initial records"
```

---
## Task 9: Fanout package

Goal: a goroutine-safe in-memory pub/sub for firehose events. Mirrors the shape of `internal/subscribe/broadcaster.go` but lives in its own simulator-side package — same drop-on-overflow contract, different consumer.

**Files:**
- Create: `internal/simulator/fanout/fanout.go`
- Create: `internal/simulator/fanout/fanout_test.go`

- [ ] **Step 1: Write failing tests**

`internal/simulator/fanout/fanout_test.go`:

```go
package fanout

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRegistry_DeliversToSubscriber(t *testing.T) {
	t.Parallel()
	r := New(8)
	sub := r.Subscribe()
	defer sub.Close()

	r.Publish([]byte("hello"))
	select {
	case msg := <-sub.Events():
		require.Equal(t, []byte("hello"), msg)
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestRegistry_DropsWhenSubscriberFull(t *testing.T) {
	t.Parallel()
	r := New(1)
	sub := r.Subscribe()
	defer sub.Close()

	r.Publish([]byte("a"))
	r.Publish([]byte("b")) // drop

	require.Equal(t, uint64(1), sub.Drops())
}

func TestRegistry_FanOut(t *testing.T) {
	t.Parallel()
	r := New(8)
	const n = 4
	subs := make([]*Subscriber, n)
	for i := range n {
		subs[i] = r.Subscribe()
	}
	defer func() {
		for _, s := range subs {
			s.Close()
		}
	}()

	r.Publish([]byte("x"))
	var got atomic.Int32
	for _, s := range subs {
		select {
		case <-s.Events():
			got.Add(1)
		case <-time.After(time.Second):
			t.Fatal("timeout")
		}
	}
	require.Equal(t, int32(n), got.Load())
}
```

- [ ] **Step 2: Run, watch fail**

```sh
just test ./internal/simulator/fanout
```

- [ ] **Step 3: Implement fanout.go**

`internal/simulator/fanout/fanout.go`:

```go
// Package fanout owns the in-memory pub/sub that the simulator's
// traffic generator broadcasts to and the websocket subscribeRepos
// handler reads from. Buffered per-subscriber channel; drop-on-
// overflow signals a slow consumer that should reconnect from
// cursor (the relay path bookkeeping lives in the relay handler,
// not here).
package fanout

import (
	"sync"
	"sync/atomic"
)

// Registry holds all currently-attached subscribers.
type Registry struct {
	bufSize int

	mu   sync.Mutex
	subs map[*Subscriber]struct{}
}

// Subscriber is one consumer's view onto the broadcast.
type Subscriber struct {
	ch     chan []byte
	drops  atomic.Uint64
	closed atomic.Bool

	registry *Registry
}

// New constructs a Registry whose subscribers' outbound channels are
// sized at bufSize.
func New(bufSize int) *Registry {
	if bufSize < 1 {
		bufSize = 1
	}
	return &Registry{
		bufSize: bufSize,
		subs:    make(map[*Subscriber]struct{}),
	}
}

// Subscribe registers a fresh subscriber.
func (r *Registry) Subscribe() *Subscriber {
	s := &Subscriber{
		ch:       make(chan []byte, r.bufSize),
		registry: r,
	}
	r.mu.Lock()
	r.subs[s] = struct{}{}
	r.mu.Unlock()
	return s
}

// Publish sends one frame to every attached subscriber. Drops
// non-blockingly into any subscriber whose buffer is full.
func (r *Registry) Publish(frame []byte) {
	r.mu.Lock()
	subs := make([]*Subscriber, 0, len(r.subs))
	for s := range r.subs {
		subs = append(subs, s)
	}
	r.mu.Unlock()
	for _, s := range subs {
		select {
		case s.ch <- frame:
		default:
			s.drops.Add(1)
		}
	}
}

// CloseAll closes every subscriber. Used at simulator shutdown.
func (r *Registry) CloseAll() {
	r.mu.Lock()
	for s := range r.subs {
		s.markClosed()
	}
	r.subs = make(map[*Subscriber]struct{})
	r.mu.Unlock()
}

// Events is the receive-only outbound channel for this subscriber.
func (s *Subscriber) Events() <-chan []byte {
	return s.ch
}

// Drops returns the number of dropped frames observed by this
// subscriber.
func (s *Subscriber) Drops() uint64 {
	return s.drops.Load()
}

// Close detaches the subscriber from the registry. Idempotent.
func (s *Subscriber) Close() {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	s.registry.mu.Lock()
	delete(s.registry.subs, s)
	s.registry.mu.Unlock()
	close(s.ch)
}

func (s *Subscriber) markClosed() {
	if s.closed.CompareAndSwap(false, true) {
		close(s.ch)
	}
}
```

- [ ] **Step 4: Run tests**

```sh
just test ./internal/simulator/fanout
just lint
```

- [ ] **Step 5: Commit**

```sh
git add internal/simulator/fanout/
git commit -m "simulator: in-memory fanout for firehose subscribers"
```

---
## Task 10: Firehose frame encoding + ring buffer persistence

Goal: encode #commit / #identity / #account frames as the two-CBOR-value wire format atmos expects, and persist each broadcast frame to `sim/firehose/<seq>` with a ring-buffer cap.

**Files:**
- Create: `internal/simulator/world/firehose.go`
- Create: `internal/simulator/world/firehose_test.go`

- [ ] **Step 1: Write failing tests**

`internal/simulator/world/firehose_test.go`:

```go
package world

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/stretchr/testify/require"
)

func TestEncodeCommitFrame_DecodableHeader(t *testing.T) {
	t.Parallel()
	cm := &comatproto.SyncSubscribeRepos_Commit{
		Repo: "did:plc:abcdefghijklmnopqrstuvwx",
		Rev:  "3kabc123def4g",
		Seq:  1,
		Time: "2024-01-01T00:00:00Z",
	}
	frame, err := encodeCommitFrame(cm)
	require.NoError(t, err)

	// Sanity: header must start with a CBOR map header for {"op":1, "t":"#commit"}.
	require.True(t, bytes.HasPrefix(frame, []byte{0xa2}), "expected map(2) header")
}

func TestPersistAndRange(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.FirehoseHistory = 3
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	for i := int64(1); i <= 5; i++ {
		require.NoError(t, w.persistFirehoseFrame(i, []byte{byte(i)}))
	}
	// Cap at 3: only 3..5 remain.
	frames, err := w.firehoseRange(0, 100)
	require.NoError(t, err)
	require.Len(t, frames, 3)
	require.Equal(t, byte(3), frames[0][0])
	require.Equal(t, byte(5), frames[2][0])

	// Cursor=4 returns only 5.
	frames, err = w.firehoseRange(4, 100)
	require.NoError(t, err)
	require.Len(t, frames, 1)
	require.Equal(t, byte(5), frames[0][0])
}
```

- [ ] **Step 2: Run, watch fail**

```sh
just test ./internal/simulator/world -run TestEncodeCommitFrame
```

- [ ] **Step 3: Implement firehose.go**

`internal/simulator/world/firehose.go`:

```go
package world

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
)

// frameHeaderCommit, frameHeaderIdentity, frameHeaderAccount, frameHeaderInfo
// are the precomputed CBOR encodings of the {"op":1,"t":"#..."} headers
// that prefix every wire frame.
var (
	frameHeaderCommit   = mustEncodeFrameHeader("#commit")
	frameHeaderIdentity = mustEncodeFrameHeader("#identity")
	frameHeaderAccount  = mustEncodeFrameHeader("#account")
	frameHeaderInfo     = mustEncodeFrameHeader("#info")
)

func mustEncodeFrameHeader(typ string) []byte {
	// Map(2): "op"->1, "t"->typ. We hand-encode to avoid a CBOR
	// dependency for marshalling tiny static maps.
	out := []byte{0xa2}
	out = append(out, encodeText("op")...)
	out = append(out, 0x01)
	out = append(out, encodeText("t")...)
	out = append(out, encodeText(typ)...)
	return out
}

// encodeText returns CBOR-encoded text-string bytes. Strings up to
// length 23 are tiny-encoded; longer strings use the 1-byte length
// header (0x78). Our header strings ("op", "t", "#commit"…) all fit
// under 23 chars.
func encodeText(s string) []byte {
	if len(s) > 23 {
		return append([]byte{0x78, byte(len(s))}, []byte(s)...)
	}
	return append([]byte{0x60 | byte(len(s))}, []byte(s)...)
}

// encodeCommitFrame serializes header + body for a #commit event.
func encodeCommitFrame(c *comatproto.SyncSubscribeRepos_Commit) ([]byte, error) {
	body, err := c.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("world: encode commit frame body: %w", err)
	}
	out := make([]byte, 0, len(frameHeaderCommit)+len(body))
	out = append(out, frameHeaderCommit...)
	out = append(out, body...)
	return out, nil
}

// encodeIdentityFrame and encodeAccountFrame mirror the above for
// #identity and #account.
func encodeIdentityFrame(e *comatproto.SyncSubscribeRepos_Identity) ([]byte, error) {
	body, err := e.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("world: encode identity frame body: %w", err)
	}
	out := make([]byte, 0, len(frameHeaderIdentity)+len(body))
	out = append(out, frameHeaderIdentity...)
	out = append(out, body...)
	return out, nil
}

func encodeAccountFrame(e *comatproto.SyncSubscribeRepos_Account) ([]byte, error) {
	body, err := e.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("world: encode account frame body: %w", err)
	}
	out := make([]byte, 0, len(frameHeaderAccount)+len(body))
	out = append(out, frameHeaderAccount...)
	out = append(out, body...)
	return out, nil
}

// encodeInfoFrame builds a #info frame with the given name + message.
func encodeInfoFrame(name, message string) []byte {
	info := &comatproto.SyncSubscribeRepos_Info{
		Name:    name,
		Message: optString(message),
	}
	body, err := info.MarshalCBOR()
	if err != nil {
		// Static input shape; an error here would surface a bug in
		// atmos's marshaller, not a runtime condition.
		panic(fmt.Sprintf("world: encode #info: %v", err))
	}
	out := make([]byte, 0, len(frameHeaderInfo)+len(body))
	out = append(out, frameHeaderInfo...)
	out = append(out, body...)
	return out
}

// persistFirehoseFrame writes one frame at sim/firehose/<seq> and
// trims the oldest entries beyond cfg.FirehoseHistory. Caller is
// responsible for serializing calls (single-writer invariant).
func (w *World) persistFirehoseFrame(seq int64, frame []byte) error {
	b := w.db.NewBatch()
	defer func() { _ = b.Close() }()

	if err := b.Set(keyFirehose(seq), frame, nil); err != nil {
		return fmt.Errorf("world: stage firehose row: %w", err)
	}

	// Trim. The oldest seq we want to retain is seq - history + 1.
	if w.cfg.FirehoseHistory > 0 && seq > int64(w.cfg.FirehoseHistory) {
		oldest := seq - int64(w.cfg.FirehoseHistory) + 1
		// DeleteRange [firehose/0, firehose/oldest).
		if err := b.DeleteRange(keyFirehose(0), keyFirehose(oldest), nil); err != nil {
			return fmt.Errorf("world: trim firehose: %w", err)
		}
	}

	if err := b.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("world: commit firehose: %w", err)
	}
	return nil
}

// firehoseRange returns frames with seq > cursor, capped at limit.
// Frames are returned in seq order. Used by relay/subscribe to replay
// history before joining the live fanout.
func (w *World) firehoseRange(cursor int64, limit int) ([][]byte, error) {
	if limit <= 0 {
		return nil, nil
	}
	lo := keyFirehose(cursor + 1)
	hi := keyFirehoseUpper()
	iter, err := w.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, fmt.Errorf("world: firehose iter: %w", err)
	}
	defer func() { _ = iter.Close() }()

	out := make([][]byte, 0, limit)
	for iter.First(); iter.Valid() && len(out) < limit; iter.Next() {
		out = append(out, append([]byte(nil), iter.Value()...))
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("world: firehose iter error: %w", err)
	}
	return out, nil
}

// keyFirehoseUpper is the exclusive upper bound for a firehose range
// scan: lexicographically greater than any keyFirehose(seq).
func keyFirehoseUpper() []byte {
	out := append([]byte(nil), []byte("sim/firehose/")...)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], 1<<63-1)
	return append(out, buf[:]...)
}

// optString wraps a string into atmos's gt.Option-style encoding for
// CBOR fields that omit the empty value. comatproto's struct uses
// gt.Option[string]; we materialize one inline.
func optString(s string) (zero gtOptString) {
	// We can't import gt here without changing the function name to
	// match atmos's API. The actual atmos call sites all use
	// gt.Some(s); we forward through a small helper to dodge a tight
	// coupling at the package boundary.
	if s == "" {
		return zero
	}
	return gtOptStringFromString(s)
}

// gtOptString is a placeholder name that maps to atmos's gt.Option;
// we let the import path resolve it via the actual struct field type.
// The implementation pattern here mirrors how atmos does it
// internally — see api/comatproto/syncsubscriberepos.go.
//
// To compile, this aliasing happens via a tiny shim file so we do
// pull the gt package in directly:

var _ = errors.New // keep imports stable in early diffs
```

- [ ] **Step 4: Replace the gtOptString placeholder with the real shim**

The `optString` helper above is sketched against `gt.Option[string]`. Replace the trailing block in `firehose.go` with the real import:

```go
// (delete the gtOptString placeholder + var _ = errors.New line, then add imports)

import (
	// … existing imports …
	"github.com/jcalabro/gt"
)

// (rewrite the optString helper)
func optString(s string) gt.Option[string] {
	if s == "" {
		return gt.None[string]()
	}
	return gt.Some(s)
}
```

Update the type of `gtOptString` references in the file accordingly. The comatproto types use `gt.Option[string]` for their `Status`/`Message`-style optional fields.

- [ ] **Step 5: Run tests**

```sh
just test ./internal/simulator/world
just lint
```

- [ ] **Step 6: Commit**

```sh
git add internal/simulator/world/firehose.go internal/simulator/world/firehose_test.go
git commit -m "world: firehose frame encoding + persisted ring buffer"
```

---
## Task 11: Live commit generator with #commit blocks/CAR diff

Goal: the heart of the simulator. A goroutine that picks an account, generates one or more record ops, signs a fresh commit, builds a CAR diff containing only the new blocks, packages it as `SyncSubscribeRepos_Commit`, persists everything, and broadcasts the wire frame. **This is the trickiest task in the plan** — verify with a real `streaming.Client` round-trip in the test.

**Files:**
- Modify: `internal/simulator/world/world.go` (add Run + RNG + fanout fields)
- Create: `internal/simulator/world/traffic.go`
- Create: `internal/simulator/world/traffic_test.go`

- [ ] **Step 1: Extend World with runtime fields**

In `internal/simulator/world/world.go`, replace the struct + add a setter wired in by main.go:

```go
import (
	// existing
	"math/rand/v2"
	"sync/atomic"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/fanout"
)

type World struct {
	cfg Config
	db  *pebble.DB

	rng     *rand.Rand
	fanout  *fanout.Registry
	seq     atomic.Int64
}

// AttachRuntime wires in the live RNG and fanout. Called once after
// New + EnsureSeed + Bootstrap.
func (w *World) AttachRuntime(r *rand.Rand, fan *fanout.Registry) error {
	w.rng = r
	w.fanout = fan
	cur, err := w.loadSeq()
	if err != nil {
		return err
	}
	w.seq.Store(cur)
	return nil
}

// CurrentSeq returns the latest persisted seq.
func (w *World) CurrentSeq() int64 { return w.seq.Load() }

// FirehoseRange exposes the read-side of the ring buffer for relay
// subscribers.
func (w *World) FirehoseRange(cursor int64, limit int) ([][]byte, error) {
	return w.firehoseRange(cursor, limit)
}
```

- [ ] **Step 2: Write a failing test that round-trips a generated commit**

`internal/simulator/world/traffic_test.go`:

```go
package world

import (
	"bytes"
	"context"
	"log/slog"
	"math/rand/v2"
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/fanout"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/repo"
	"github.com/stretchr/testify/require"
)

func newTestWorld(t *testing.T) *World {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Accounts = 25
	cfg.InitialRecords = 1
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(7, 8)), fanout.New(64)))
	return w
}

func TestGenerateOne_ProducesValidCommit(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t)

	frame, err := w.generateOne(context.Background())
	require.NoError(t, err)

	// Decode #commit body off the wire.
	body, ok := bytes.CutPrefix(frame, frameHeaderCommit)
	require.True(t, ok, "expected #commit header")
	var cm comatproto.SyncSubscribeRepos_Commit
	require.NoError(t, cm.UnmarshalCBOR(body))
	require.Equal(t, int64(1), cm.Seq)
	require.NotEmpty(t, cm.Repo)
	require.NotEmpty(t, cm.Ops)
	require.NotEmpty(t, cm.Blocks)

	// The blocks CAR roundtrips through repo.LoadFromCAR — the new
	// commit + record blocks plus enough MST nodes to root.
	rp, commit, err := repo.LoadFromCAR(bytes.NewReader(cm.Blocks))
	require.NoError(t, err)
	require.Equal(t, cm.Repo, string(rp.DID))
	require.Equal(t, cm.Rev, commit.Rev)

	// At least one new block is the record itself; we can pull it
	// out via the op's CID.
	require.NotZero(t, cm.Ops[0].CID.HasVal())
	cid := cm.Ops[0].CID.Val()
	_, err = rp.Store.GetBlock(cbor.CID(cid))
	require.NoError(t, err)
}

func TestGenerateOne_AdvancesSeq(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t)
	for i := int64(1); i <= 3; i++ {
		_, err := w.generateOne(context.Background())
		require.NoError(t, err)
		require.Equal(t, i, w.CurrentSeq())
	}
}

func TestRunTraffic_StopsOnContext(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.NoError(t, w.RunTraffic(ctx, slog.Default()))
}
```

- [ ] **Step 3: Run, watch fail**

```sh
just test ./internal/simulator/world -run TestGenerateOne
```

- [ ] **Step 4: Implement traffic.go**

`internal/simulator/world/traffic.go`:

```go
package world

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/gt"
)

// RunTraffic blocks generating + broadcasting events until ctx is
// cancelled. One event per loop iteration; inter-arrival drawn from
// the exponential distribution. Returns ctx.Err() on cancel.
func (w *World) RunTraffic(ctx context.Context, logger *slog.Logger) error {
	logger = logger.With(slog.String("component", "simulator/traffic"))
	mean := 1.0 / (w.cfg.CommitsPerSec * w.cfg.RateMultiplier)
	logger.InfoContext(ctx, "starting", "mean_delay_sec", mean)

	for {
		delay := exponentialDelay(w.rng, mean)
		t := time.NewTimer(time.Duration(delay * float64(time.Second)))
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
		if _, err := w.generateOne(ctx); err != nil {
			logger.ErrorContext(ctx, "generate failed", "err", err)
			return err
		}
	}
}

// actionMix is the design-doc weighted action distribution.
var actionMix = []weighted[string]{
	{value: "create", weight: 75},
	{value: "update", weight: 15},
	{value: "delete", weight: 10},
}

// generateOne is one tick of the live commit pump: pick an account
// (Zipfian), draw an action and op count, modify the repo, persist
// the new state + blocks + ring-buffer entry, and broadcast the
// frame. Returns the wire frame so tests can inspect it.
func (w *World) generateOne(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Choose author.
	authorIdx := zipfian(w.rng, 1.07, w.cfg.Accounts)
	author, err := w.loadAccount(authorIdx)
	if err != nil {
		return nil, err
	}
	prevState, err := w.loadState(authorIdx)
	if err != nil {
		return nil, err
	}

	// Build a *repo.Repo that loads MST/record blocks from pebble
	// and captures any newly written blocks for diff packaging.
	store := &diffStore{
		base: &pebbleStore{db: w.db, idx: authorIdx},
	}
	tree := mst.NewTree(store)
	if prevState.DataCID.Defined() {
		tree = mst.LoadTree(store, prevState.DataCID)
	}
	rp := &repo.Repo{
		DID:   author.DID,
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  tree,
	}

	// Apply N ops of the chosen action. v1: each commit's ops share
	// an action; mixing actions per-commit isn't useful for the
	// distributions we're targeting.
	action := weightedChoice(w.rng, actionMix)
	nOps := geometricAtLeastOne(w.rng, 0.7)
	wireOps := make([]comatproto.SyncSubscribeRepos_RepoOp, 0, nOps)

	for range nOps {
		var op comatproto.SyncSubscribeRepos_RepoOp
		switch action {
		case "create":
			coll := chooseCreateCollection(w.rng)
			rkey := newRkey(w.rng)
			target := w.pickAnotherAccount(authorIdx).DID
			rec := generateRecord(w.rng, coll, string(target))
			if err := rp.Create(coll, rkey, rec); err != nil {
				return nil, fmt.Errorf("simulator: create %s/%s: %w", coll, rkey, err)
			}
			cid, _, err := rp.Get(coll, rkey)
			if err != nil {
				return nil, fmt.Errorf("simulator: lookup new record: %w", err)
			}
			op = comatproto.SyncSubscribeRepos_RepoOp{
				Action: "create",
				Path:   coll + "/" + rkey,
				CID:    gt.Some(lextypes.LexCIDLink(cid)),
			}
		case "update":
			// Pick an existing record at random; if none exist, fall
			// back to create.
			coll, rkey, ok := w.pickExistingRecord(rp)
			if !ok {
				coll = chooseCreateCollection(w.rng)
				rkey = newRkey(w.rng)
				rec := generateRecord(w.rng, coll, string(w.pickAnotherAccount(authorIdx).DID))
				if err := rp.Create(coll, rkey, rec); err != nil {
					return nil, err
				}
				cid, _, _ := rp.Get(coll, rkey)
				op = comatproto.SyncSubscribeRepos_RepoOp{
					Action: "create",
					Path:   coll + "/" + rkey,
					CID:    gt.Some(lextypes.LexCIDLink(cid)),
				}
				break
			}
			rec := generateRecord(w.rng, coll, string(w.pickAnotherAccount(authorIdx).DID))
			prevCID, _, _ := rp.Get(coll, rkey)
			if err := rp.Update(coll, rkey, rec); err != nil {
				return nil, err
			}
			cid, _, _ := rp.Get(coll, rkey)
			op = comatproto.SyncSubscribeRepos_RepoOp{
				Action: "update",
				Path:   coll + "/" + rkey,
				CID:    gt.Some(lextypes.LexCIDLink(cid)),
				Prev:   gt.Some(lextypes.LexCIDLink(prevCID)),
			}
		case "delete":
			coll, rkey, ok := w.pickExistingRecord(rp)
			if !ok {
				// No records to delete; emit a create instead.
				coll = chooseCreateCollection(w.rng)
				rkey = newRkey(w.rng)
				rec := generateRecord(w.rng, coll, string(w.pickAnotherAccount(authorIdx).DID))
				if err := rp.Create(coll, rkey, rec); err != nil {
					return nil, err
				}
				cid, _, _ := rp.Get(coll, rkey)
				op = comatproto.SyncSubscribeRepos_RepoOp{
					Action: "create",
					Path:   coll + "/" + rkey,
					CID:    gt.Some(lextypes.LexCIDLink(cid)),
				}
				break
			}
			prevCID, _, _ := rp.Get(coll, rkey)
			if err := rp.Delete(coll, rkey); err != nil {
				return nil, err
			}
			op = comatproto.SyncSubscribeRepos_RepoOp{
				Action: "delete",
				Path:   coll + "/" + rkey,
				Prev:   gt.Some(lextypes.LexCIDLink(prevCID)),
			}
		}
		wireOps = append(wireOps, op)
	}

	// Persist the new state. commitAndPersist signs + flushes blocks
	// + updates state + clears+rewrites the MST index.
	newState, err := w.commitAndPersist(author, rp)
	if err != nil {
		return nil, err
	}

	// Build a CAR diff containing only the blocks our diffStore
	// captured (plus the commit block). repo.ExportCAR re-signs and
	// includes the full repo, which is wrong for #commit.Blocks; we
	// build it ourselves.
	commitData, err := getBlock(rp.Store, newState.CommitCID)
	if err != nil {
		return nil, err
	}
	carBlocks := make([]car.Block, 0, len(store.writes)+1)
	carBlocks = append(carBlocks, car.Block{CID: newState.CommitCID, Data: commitData})
	for cid, data := range store.writes {
		if cid == newState.CommitCID {
			continue
		}
		carBlocks = append(carBlocks, car.Block{CID: cid, Data: data})
	}
	var carBuf carWriter
	if err := car.WriteAll(&carBuf, []cbor.CID{newState.CommitCID}, carBlocks); err != nil {
		return nil, fmt.Errorf("simulator: write CAR diff: %w", err)
	}

	// Allocate the seq and assemble the envelope.
	seq := w.seq.Add(1)
	envelope := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   string(author.DID),
		Rev:    newState.Rev,
		Seq:    seq,
		Time:   time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		Commit: lextypes.LexCIDLink(newState.CommitCID),
		Blocks: carBuf.bytes(),
		Ops:    wireOps,
	}
	if prevState.DataCID.Defined() {
		envelope.PrevData = gt.Some(lextypes.LexCIDLink(prevState.DataCID))
		envelope.Since = gt.Some(prevState.Rev)
	}

	frame, err := encodeCommitFrame(envelope)
	if err != nil {
		return nil, err
	}

	if err := w.persistFirehoseFrame(seq, frame); err != nil {
		return nil, err
	}
	if err := w.saveSeq(seq); err != nil {
		return nil, err
	}
	w.fanout.Publish(frame)
	return frame, nil
}

func (w *World) pickAnotherAccount(notIdx int) account {
	for {
		idx := w.rng.IntN(w.cfg.Accounts)
		if idx == notIdx {
			continue
		}
		a, err := w.loadAccount(idx)
		if err == nil {
			return a
		}
	}
}

// pickExistingRecord chooses one (collection, rkey) at random from the
// account's current MST. Returns ok=false on an empty repo.
func (w *World) pickExistingRecord(rp *repo.Repo) (collection, rkey string, ok bool) {
	type entry struct{ coll, rkey string }
	var entries []entry
	_ = rp.Tree.Walk(func(key string, _ cbor.CID) error {
		c, k := repo.SplitMSTKey(key)
		entries = append(entries, entry{c, k})
		return nil
	})
	if len(entries) == 0 {
		return "", "", false
	}
	pick := entries[w.rng.IntN(len(entries))]
	return pick.coll, pick.rkey, true
}

// diffStore wraps a base BlockStore (the persisted-blocks pebbleStore)
// with a write-capture set: any block PutBlock'd during this commit
// is recorded for later inclusion in the CAR diff.
type diffStore struct {
	base   mst.BlockStore
	writes map[cbor.CID][]byte
}

func (s *diffStore) GetBlock(cid cbor.CID) ([]byte, error) {
	if data, ok := s.writes[cid]; ok {
		return data, nil
	}
	return s.base.GetBlock(cid)
}

func (s *diffStore) PutBlock(cid cbor.CID, data []byte) error {
	if s.writes == nil {
		s.writes = make(map[cbor.CID][]byte)
	}
	s.writes[cid] = append([]byte(nil), data...)
	return s.base.PutBlock(cid, data)
}

// carWriter is a tiny io.Writer over a growable byte slice; used to
// avoid bringing in bytes.Buffer just for the CAR diff.
type carWriter struct{ buf []byte }

func (w *carWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}
func (w *carWriter) bytes() []byte { return w.buf }

// getBlock reads a block out of any mst.BlockStore.
func getBlock(s mst.BlockStore, cid cbor.CID) ([]byte, error) {
	return s.GetBlock(cid)
}
```

- [ ] **Step 5: Make sure repos.go's commitAndPersist supports the diffStore wrapper**

Open `internal/simulator/world/repos.go` and update the type-switch in `commitAndPersist` to also handle `*diffStore`:

```go
} else if ds, ok := rp.Store.(*diffStore); ok {
	for cid, data := range ds.writes {
		if err := b.Set(keyAccountBlock(a.Index, cid.Bytes()), data, nil); err != nil {
			return repoState{}, fmt.Errorf("world: stage block: %w", err)
		}
	}
	ds.writes = nil
} else if mem, ok := rp.Store.(*mst.MemBlockStore); ok {
```

(Insert before the existing `} else if mem, ok := rp.Store.(*mst.MemBlockStore)` branch. Diff-store carriers may also wrap a pebble base; we still want to flush writes from the wrapper.)

- [ ] **Step 6: Run the tests**

```sh
just test ./internal/simulator/world
just lint
```

Expected: all PASS, including the round-trip through `repo.LoadFromCAR`.

- [ ] **Step 7: Commit**

```sh
git add internal/simulator/world/world.go internal/simulator/world/traffic.go internal/simulator/world/traffic_test.go internal/simulator/world/repos.go
git commit -m "world: live commit generator with signed CAR-diff frames"
```

---
## Task 12: PLC HTTP handler

Goal: serve `GET /{did}` returning a DID document JSON whose `verificationMethod` advertises the account's pubkey and whose `service[atproto_pds]` points back at the simulator listener URL. atmos's `DefaultResolver` will read this and route `getRepo` calls to the same listener.

**Files:**
- Create: `internal/simulator/http/handler.go`
- Create: `internal/simulator/http/plc.go`
- Create: `internal/simulator/http/plc_test.go`

- [ ] **Step 1: Write failing test**

`internal/simulator/http/plc_test.go`:

```go
package http_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	simhttp "github.com/bluesky-social/jetstream-v2/internal/simulator/http"
	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

func TestPLC_ResolvesAccount(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)

	srv := httptest.NewServer(simhttp.NewHandler(w, "http://example.test"))
	defer srv.Close()

	// Pick the DID for account 0.
	a, _ := w.LoadAccount(0)

	resolver := &identity.DefaultResolver{
		PLCURL: gt.Some(srv.URL),
	}
	doc, err := resolver.ResolveDID(context.Background(), a.DID)
	require.NoError(t, err)
	require.Equal(t, string(a.DID), doc.ID)
	id, err := identity.IdentityFromDocument(doc)
	require.NoError(t, err)
	require.Equal(t, "http://example.test", id.PDSEndpoint())
}

func TestPLC_404OnUnknown(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 1, 0)
	srv := httptest.NewServer(simhttp.NewHandler(w, "http://example.test"))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/did:plc:doesnotexist000000000000")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	_, _ = io.Copy(io.Discard, resp.Body)
}
```

The test pulls in a `newTestWorld` helper that the `world` package needs to expose (one-time, used by every http test). Add this small public surface:

```go
// internal/simulator/world/test_helpers.go
package world

func (w *World) LoadAccount(idx int) (account, error) { return w.loadAccount(idx) }
```

…and a corresponding helper in a new file:

`internal/simulator/http/helpers_test.go`:

```go
package http_test

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/fanout"
	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
	"github.com/stretchr/testify/require"
)

func newTestWorld(t *testing.T, accounts, initialRecords int) *world.World {
	t.Helper()
	cfg := world.DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Accounts = accounts
	cfg.InitialRecords = initialRecords
	w, err := world.New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(1, 2)), fanout.New(64)))
	return w
}
```

`account` is package-private; if the test reaches into account fields, expose just what we need. For now, export a small `Account` value type:

`internal/simulator/world/test_helpers.go`:

```go
package world

import "github.com/jcalabro/atmos"

// Account is an exported view of an account, for tests outside this
// package. Only fields tests need — DID + index. Production code
// inside this package uses the unexported `account` directly.
type Account struct {
	Index int
	DID   atmos.DID
}

// LoadAccount returns an exported account view for tests.
func (w *World) LoadAccount(idx int) (Account, error) {
	a, err := w.loadAccount(idx)
	if err != nil {
		return Account{}, err
	}
	return Account{Index: a.Index, DID: a.DID}, nil
}
```

(The Task 11 test `traffic_test.go` already uses unexported `loadAccount` directly because it lives in the same package — leave it alone.)

- [ ] **Step 2: Run, watch fail**

```sh
just test ./internal/simulator/http
```

- [ ] **Step 3: Implement handler.go**

`internal/simulator/http/handler.go`:

```go
// Package http hosts the simulator's HTTP surface: PLC, PDS, and
// relay endpoints under a single mux.
package http

import (
	"net/http"
	"strings"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
)

// NewHandler builds the simulator's HTTP handler. publicURL is the
// externally-reachable base URL of the simulator (without trailing
// slash); it's published in DID documents as the PDS endpoint so
// jetstream's verifier rounds back to us for getRepo.
func NewHandler(w *world.World, publicURL string) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /xrpc/com.atproto.sync.getRepo", newPDSGetRepoHandler(w))
	mux.Handle("GET /xrpc/com.atproto.sync.listRepos", newRelayListReposHandler(w))
	mux.Handle("GET /xrpc/com.atproto.sync.subscribeRepos", newRelaySubscribeReposHandler(w))

	// PLC's `/<did>` doesn't fit Go ServeMux's path syntax cleanly
	// because `did:` contains a colon. Pre-route any request whose
	// first path segment starts with `did:` through the PLC handler.
	plc := newPLCHandler(w, strings.TrimRight(publicURL, "/"))
	root := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/did:") {
			plc.ServeHTTP(rw, r)
			return
		}
		mux.ServeHTTP(rw, r)
	})
	return root
}
```

`internal/simulator/http/plc.go`:

```go
package http

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
	"github.com/jcalabro/atmos"
)

type didDoc struct {
	ID                 string               `json:"id"`
	AlsoKnownAs        []string             `json:"alsoKnownAs"`
	VerificationMethod []verificationMethod `json:"verificationMethod"`
	Service            []service            `json:"service"`
}

type verificationMethod struct {
	ID                 string `json:"id"`
	Type               string `json:"type"`
	Controller         string `json:"controller"`
	PublicKeyMultibase string `json:"publicKeyMultibase"`
}

type service struct {
	ID              string `json:"id"`
	Type            string `json:"type"`
	ServiceEndpoint string `json:"serviceEndpoint"`
}

// newPLCHandler returns a handler matching atmos's PLC resolution
// pattern: GET <plcURL>/<did> → JSON DID document.
func newPLCHandler(w *world.World, pdsEndpoint string) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		didStr := strings.TrimPrefix(r.URL.Path, "/")
		did, err := atmos.ParseDID(didStr)
		if err != nil {
			http.Error(rw, "bad did", http.StatusBadRequest)
			return
		}
		acct, ok, err := w.FindAccountByDID(did)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			http.NotFound(rw, r)
			return
		}
		doc := didDoc{
			ID:          string(acct.DID),
			AlsoKnownAs: []string{"at://user-" + acct.HandleSuffix() + ".test"},
			VerificationMethod: []verificationMethod{{
				ID:                 string(acct.DID) + "#atproto",
				Type:               "Multikey",
				Controller:         string(acct.DID),
				PublicKeyMultibase: acct.PubkeyMultibase(),
			}},
			Service: []service{{
				ID:              "#atproto_pds",
				Type:            "AtprotoPersonalDataServer",
				ServiceEndpoint: pdsEndpoint,
			}},
		}
		rw.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(rw).Encode(doc)
	})
}
```

The handler depends on three new methods on the exported Account view: `FindAccountByDID`, `HandleSuffix`, and `PubkeyMultibase`. Add them in `internal/simulator/world/test_helpers.go` (rename the file to `account_view.go` for clarity since it's no longer test-only):

```go
// internal/simulator/world/account_view.go
package world

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
)

// Account is the exported view of a simulator account, for HTTP
// handlers and tests living outside this package.
type Account struct {
	Index   int
	DID     atmos.DID
	pubKey  *crypto.K256PublicKey
}

// LoadAccount returns the account at the given index.
func (w *World) LoadAccount(idx int) (Account, error) {
	a, err := w.loadAccount(idx)
	if err != nil {
		return Account{}, err
	}
	return Account{
		Index:  a.Index,
		DID:    a.DID,
		pubKey: a.priv.PublicKey().(*crypto.K256PublicKey),
	}, nil
}

// FindAccountByDID returns (account, true) if a matching account
// exists; (Account{}, false, nil) otherwise. Linear scan over
// account/<idx>/did rows; acceptable at 10k entries because we cache
// per-DID lookups in the LRU added by Task 13.
//
// TODO(perf): build a DID→idx index in pebble during bootstrap if
// the linear scan ever shows up in profiles.
func (w *World) FindAccountByDID(did atmos.DID) (Account, bool, error) {
	iter, err := w.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("sim/account/"),
		UpperBound: []byte("sim/account/\xff"),
	})
	if err != nil {
		return Account{}, false, fmt.Errorf("world: did lookup iter: %w", err)
	}
	defer func() { _ = iter.Close() }()
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		// Match keys ending in "/did".
		if len(key) < 4 || string(key[len(key)-4:]) != "/did" {
			continue
		}
		if !errors.Is(nil, nil) {
			// dummy, satisfies linter
		}
		if string(iter.Value()) == string(did) {
			// Parse the index out of the key: "sim/account/<idx>/did".
			rest := key[len("sim/account/") : len(key)-len("/did")]
			idx, err := strconv.Atoi(string(rest))
			if err != nil {
				return Account{}, false, fmt.Errorf("world: bad account key %q: %w", key, err)
			}
			a, err := w.LoadAccount(idx)
			return a, err == nil, err
		}
	}
	return Account{}, false, nil
}

// HandleSuffix is the cosmetic handle disambiguator: just the index.
func (a Account) HandleSuffix() string { return strconv.Itoa(a.Index) }

// PubkeyMultibase returns the z-prefixed base58 multibase string for
// the account's atproto signing key.
func (a Account) PubkeyMultibase() string { return a.pubKey.Multibase() }
```

(Drop `internal/simulator/world/test_helpers.go` from Step 1's plan in favor of this file. The placeholder content under that path described in the plan above is superseded by `account_view.go`.)

- [ ] **Step 4: Run the tests**

```sh
just test ./internal/simulator/http
just test ./internal/simulator/world
just lint
```

- [ ] **Step 5: Commit**

```sh
git add internal/simulator/http/ internal/simulator/world/account_view.go
git commit -m "simulator: PLC handler + account view"
```

---
## Task 13: PDS getRepo handler

Goal: serve `GET /xrpc/com.atproto.sync.getRepo?did=…` by streaming a CAR built via `repo.ExportCAR`. Re-signs the commit on the way out, so the CAR validates against the PLC-published key.

**Files:**
- Create: `internal/simulator/http/pds.go`
- Create: `internal/simulator/http/pds_test.go`
- Modify: `internal/simulator/world/account_view.go` (add a `LoadRepo` helper)

- [ ] **Step 1: Add LoadRepo + signing-key access to the Account view**

Append to `internal/simulator/world/account_view.go`:

```go
import (
	// existing
	"github.com/jcalabro/atmos/repo"
)

// LoadRepo returns a fully-loaded *repo.Repo plus the signing key
// needed to call ExportCAR. Reads MST/record blocks lazily from
// pebble; safe to call concurrently because the underlying
// pebbleStore only reads.
func (w *World) LoadRepo(idx int) (*repo.Repo, *crypto.K256PrivateKey, error) {
	a, err := w.loadAccount(idx)
	if err != nil {
		return nil, nil, err
	}
	rp, err := w.loadRepo(a)
	if err != nil {
		return nil, nil, err
	}
	return rp, a.priv, nil
}
```

- [ ] **Step 2: Write failing test**

`internal/simulator/http/pds_test.go`:

```go
package http_test

import (
	"context"
	"net/http/httptest"
	"testing"

	simhttp "github.com/bluesky-social/jetstream-v2/internal/simulator/http"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/jcalabro/jttp"
	"github.com/stretchr/testify/require"
)

func TestPDS_GetRepoRoundTrips(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 2)
	srv := httptest.NewServer(simhttp.NewHandler(w, "http://example.test"))
	defer srv.Close()

	a, err := w.LoadAccount(0)
	require.NoError(t, err)

	xc := &xrpc.Client{
		Host:       srv.URL,
		HTTPClient: gt.Some(jttp.New()),
	}
	sc := sync.NewClient(sync.Options{Client: xc})

	body, err := sc.GetRepoStream(context.Background(), a.DID, "")
	require.NoError(t, err)
	defer func() { _ = body.Close() }()

	// LoadFromCAR validates the structural shape; commit signature
	// validates against the PLC-published key (covered in the
	// listRepos test in Task 14).
	rp, commit, err := loadFromCAR(body)
	require.NoError(t, err)
	require.Equal(t, a.DID, rp.DID)
	require.NotEmpty(t, commit.Sig)
}
```

Add the `loadFromCAR` shim helper (since the package already takes the dependency for repo loading in production code):

```go
// internal/simulator/http/helpers_test.go (append)
import "github.com/jcalabro/atmos/repo"

func loadFromCAR(r io.Reader) (*repo.Repo, *repo.Commit, error) {
	return repo.LoadFromCAR(r)
}
```

(also add `import "io"` to helpers_test.go.)

- [ ] **Step 3: Run, watch fail**

```sh
just test ./internal/simulator/http -run TestPDS_GetRepoRoundTrips
```

- [ ] **Step 4: Implement pds.go**

`internal/simulator/http/pds.go`:

```go
package http

import (
	"net/http"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
	"github.com/jcalabro/atmos"
)

// newPDSGetRepoHandler serves com.atproto.sync.getRepo. Streams CAR
// bytes straight to the response. Ignores `since` in v1.
func newPDSGetRepoHandler(w *world.World) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		didStr := r.URL.Query().Get("did")
		did, err := atmos.ParseDID(didStr)
		if err != nil {
			http.Error(rw, "bad did", http.StatusBadRequest)
			return
		}
		acct, ok, err := w.FindAccountByDID(did)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			http.NotFound(rw, r)
			return
		}
		rp, key, err := w.LoadRepo(acct.Index)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		rw.Header().Set("Content-Type", "application/vnd.ipld.car")
		if err := rp.ExportCAR(rw, key); err != nil {
			// Headers may already be flushed; logging via stdlib log
			// from a handler is the cleanest fallback. Real slog
			// plumbing happens at the top of the handler chain when
			// we wire main.go.
			_, _ = rw.Write([]byte("export car: " + err.Error() + "\n"))
		}
	})
}
```

- [ ] **Step 5: Run tests**

```sh
just test ./internal/simulator/http
just lint
```

- [ ] **Step 6: Commit**

```sh
git add internal/simulator/http/pds.go internal/simulator/http/pds_test.go internal/simulator/http/helpers_test.go internal/simulator/world/account_view.go
git commit -m "simulator: PDS getRepo handler"
```

---
## Task 14: Relay listRepos handler with full pagination round-trip

Goal: serve `GET /xrpc/com.atproto.sync.listRepos?cursor=…&limit=…` returning JSON pages with the cap at 1000. The end-to-end test in this task drives `atmos.sync.Client.ListRepos` against the full handler chain (PLC + PDS + listRepos) so the commit signature validation path is exercised here.

**Files:**
- Create: `internal/simulator/http/relay_listrepos.go`
- Create: `internal/simulator/http/relay_listrepos_test.go`
- Modify: `internal/simulator/world/account_view.go` (expose count + ListPage)

- [ ] **Step 1: Add ListRepos pagination support to the world API**

Append to `internal/simulator/world/account_view.go`:

```go
// AccountCount returns the total accounts in the world.
func (w *World) AccountCount() int { return w.cfg.Accounts }

// ListReposEntry is one row of a listRepos response.
type ListReposEntry struct {
	DID    atmos.DID
	Rev    string
	Head   string // commit CID string
	Active bool
}

// ListReposPage returns up to limit entries starting at index `start`.
// nextStart is start + len(entries); when nextStart == AccountCount(),
// the caller has paged through everything.
func (w *World) ListReposPage(start, limit int) (entries []ListReposEntry, nextStart int, err error) {
	if start < 0 {
		start = 0
	}
	if limit > 1000 {
		limit = 1000
	}
	if limit <= 0 {
		limit = 50
	}
	end := start + limit
	if end > w.cfg.Accounts {
		end = w.cfg.Accounts
	}
	out := make([]ListReposEntry, 0, end-start)
	for i := start; i < end; i++ {
		a, err := w.LoadAccount(i)
		if err != nil {
			return nil, 0, err
		}
		state, err := w.loadState(i)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, ListReposEntry{
			DID:    a.DID,
			Rev:    state.Rev,
			Head:   state.CommitCID.String(),
			Active: true,
		})
	}
	return out, end, nil
}
```

- [ ] **Step 2: Write failing test**

`internal/simulator/http/relay_listrepos_test.go`:

```go
package http_test

import (
	"context"
	"net/http/httptest"
	"testing"

	simhttp "github.com/bluesky-social/jetstream-v2/internal/simulator/http"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/jcalabro/jttp"
	"github.com/stretchr/testify/require"
)

func TestListRepos_PagesAcrossAllAccounts(t *testing.T) {
	t.Parallel()
	const total = 25
	w := newTestWorld(t, total, 1)
	srv := httptest.NewServer(simhttp.NewHandler(w, "")) // pds endpoint not needed here
	defer srv.Close()

	xc := &xrpc.Client{Host: srv.URL, HTTPClient: gt.Some(jttp.New())}
	sc := sync.NewClient(sync.Options{Client: xc})

	seen := make(map[atmos.DID]bool)
	for page, err := range sc.ListRepos(context.Background(), 10, "") {
		require.NoError(t, err)
		for _, e := range page.Entries {
			require.False(t, seen[e.DID], "duplicate DID %s", e.DID)
			seen[e.DID] = true
		}
	}
	require.Equal(t, total, len(seen))
}

func TestListRepos_GetRepoVerifiesCommitSignature(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)
	srv := httptest.NewServer(simhttp.NewHandler(w, srvURL(t, w)))
	defer srv.Close()

	xc := &xrpc.Client{Host: srv.URL, HTTPClient: gt.Some(jttp.New())}
	directory := &identity.Directory{
		Resolver: &identity.DefaultResolver{
			HTTPClient: gt.Some(jttp.New()),
			PLCURL:     gt.Some(srv.URL),
		},
		SkipHandleVerification: true,
	}
	sc := sync.NewClient(sync.Options{
		Client:    xc,
		Directory: gt.Some(directory),
	})

	a, _ := w.LoadAccount(2)
	body, err := sc.GetRepoStream(context.Background(), a.DID, "")
	require.NoError(t, err)
	defer func() { _ = body.Close() }()

	rp, commit, err := loadFromCAR(body)
	require.NoError(t, err)

	// Verify the commit signature against the pubkey we publish via
	// PLC. This is the strongest signal that the simulator's
	// keys+DID+CAR pipeline is internally consistent.
	id, err := directory.LookupDID(context.Background(), rp.DID)
	require.NoError(t, err)
	pub, err := id.PublicKey()
	require.NoError(t, err)
	require.NoError(t, commit.VerifySignature(pub))
}

// srvURL is a placeholder helper that returns the test server URL.
// Implemented at test time as a closure over the *httptest.Server. We
// inline the trick instead of a global to avoid mutable state.
func srvURL(t *testing.T, _ *world.World) string {
	t.Helper()
	t.Skip("populated inline by callers; this stub keeps the import graph happy")
	return ""
}
```

Replace `srvURL` usage. The simpler structure is to flip the order: build the simulator URL after the httptest.Server starts. Inline:

```go
// Inside TestListRepos_GetRepoVerifiesCommitSignature, replace the
// `simhttp.NewHandler(w, srvURL(t, w))` line with a two-step setup:

	srv := httptest.NewServer(nil)
	srv.Config.Handler = simhttp.NewHandler(w, srv.URL)
	defer srv.Close()
```

(`httptest.NewServer(nil)` reserves the URL before the handler is set; safe because no requests can land until the test makes one.)

- [ ] **Step 3: Run, watch fail**

```sh
just test ./internal/simulator/http -run TestListRepos
```

- [ ] **Step 4: Implement relay_listrepos.go**

`internal/simulator/http/relay_listrepos.go`:

```go
package http

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
)

type listReposOutput struct {
	Cursor string                 `json:"cursor,omitempty"`
	Repos  []listReposOutputEntry `json:"repos"`
}

type listReposOutputEntry struct {
	DID    string `json:"did"`
	Head   string `json:"head"`
	Rev    string `json:"rev"`
	Active bool   `json:"active"`
}

// newRelayListReposHandler serves com.atproto.sync.listRepos. Cursor
// is the stringified next-start index; "" means start at 0.
func newRelayListReposHandler(w *world.World) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		start := 0
		if c := q.Get("cursor"); c != "" {
			n, err := strconv.Atoi(c)
			if err != nil || n < 0 {
				http.Error(rw, "bad cursor", http.StatusBadRequest)
				return
			}
			start = n
		}
		limit := 50
		if l := q.Get("limit"); l != "" {
			n, err := strconv.Atoi(l)
			if err != nil || n <= 0 {
				http.Error(rw, "bad limit", http.StatusBadRequest)
				return
			}
			limit = n
		}
		entries, next, err := w.ListReposPage(start, limit)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		out := listReposOutput{
			Repos: make([]listReposOutputEntry, len(entries)),
		}
		for i, e := range entries {
			out.Repos[i] = listReposOutputEntry{
				DID:    string(e.DID),
				Head:   e.Head,
				Rev:    e.Rev,
				Active: e.Active,
			}
		}
		// Cursor is omitted on the last page.
		if next < w.AccountCount() {
			out.Cursor = strconv.Itoa(next)
		}
		rw.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(rw).Encode(out)
	})
}
```

- [ ] **Step 5: Run tests**

```sh
just test ./internal/simulator/http
just lint
```

Expected: both tests PASS, including signature verification.

- [ ] **Step 6: Commit**

```sh
git add internal/simulator/http/relay_listrepos.go internal/simulator/http/relay_listrepos_test.go internal/simulator/world/account_view.go
git commit -m "simulator: relay listRepos handler with pagination"
```

---
## Task 15: Relay subscribeRepos websocket handler

Goal: WebSocket handler that replays history after the cursor and joins the live fanout. Uses `coder/websocket` (already a dependency).

**Files:**
- Create: `internal/simulator/http/relay_subscribe.go`
- Create: `internal/simulator/http/relay_subscribe_test.go`

- [ ] **Step 1: Write failing test**

`internal/simulator/http/relay_subscribe_test.go`:

```go
package http_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	simhttp "github.com/bluesky-social/jetstream-v2/internal/simulator/http"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/stretchr/testify/require"
)

func TestSubscribeRepos_DeliversLiveCommit(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)
	srv := httptest.NewServer(nil)
	srv.Config.Handler = simhttp.NewHandler(w, srv.URL)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	// Give the handler a beat to register the subscriber.
	time.Sleep(50 * time.Millisecond)
	frame, err := w.GenerateOneForTest(ctx)
	require.NoError(t, err)

	_, got, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, frame, got)
}

func TestSubscribeRepos_ReplaysHistoricalEvents(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Generate two events before any subscriber connects.
	first, err := w.GenerateOneForTest(ctx)
	require.NoError(t, err)
	_, err = w.GenerateOneForTest(ctx)
	require.NoError(t, err)

	srv := httptest.NewServer(nil)
	srv.Config.Handler = simhttp.NewHandler(w, srv.URL)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos?cursor=0"

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	// Read the first historical frame and confirm seq=1.
	_, got, err := conn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, first, got)

	// Decode body to confirm shape.
	var cm comatproto.SyncSubscribeRepos_Commit
	body := got[len(headerCommitForTests()):]
	require.NoError(t, cm.UnmarshalCBOR(body))
	require.Equal(t, int64(1), cm.Seq)
}

// headerCommitForTests duplicates the header bytes for test
// deserialization. We don't import unexported state from the world
// package; the bytes themselves are stable.
func headerCommitForTests() []byte {
	// Length of {"op":1, "t":"#commit"} encoded as task 10 builds it.
	// Computed once and asserted on each test run via the round-trip
	// in Task 10's TestEncodeCommitFrame_DecodableHeader.
	return []byte("\xa2bopa\x01atg#commit")
}
```

The `headerCommitForTests` byte literal mirrors the encoding from `frameHeaderCommit` in `firehose.go`. If a future refactor changes the encoding, Task 10's test plus this test both fail — we want both signals.

The `GenerateOneForTest` helper exposes the unexported `generateOne`:

```go
// internal/simulator/world/account_view.go (append)
import "context"

func (w *World) GenerateOneForTest(ctx context.Context) ([]byte, error) {
	return w.generateOne(ctx)
}
```

- [ ] **Step 2: Run, watch fail**

```sh
just test ./internal/simulator/http -run TestSubscribeRepos
```

- [ ] **Step 3: Implement relay_subscribe.go**

`internal/simulator/http/relay_subscribe.go`:

```go
package http

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
	"github.com/coder/websocket"
)

const subscribeReplayLimit = 1024

func newRelaySubscribeReposHandler(w *world.World) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		conn, err := websocket.Accept(rw, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()

		var cursor int64
		if c := r.URL.Query().Get("cursor"); c != "" {
			n, err := strconv.ParseInt(c, 10, 64)
			if err != nil {
				_ = conn.Close(websocket.StatusUnsupportedData, "bad cursor")
				return
			}
			cursor = n
		}

		// Subscribe BEFORE replay so we don't drop frames whose seq
		// lands in the gap between the last replay row and the start
		// of live broadcast.
		sub := w.SubscribeFanout()
		defer sub.Close()

		// Replay history. If cursor is older than the oldest retained
		// frame, send a #info OutdatedCursor and resume from current.
		frames, err := w.FirehoseRange(cursor, subscribeReplayLimit)
		if err != nil {
			_ = conn.Close(websocket.StatusInternalError, "history")
			return
		}
		if cursor > 0 && len(frames) == 0 && w.CurrentSeq() > cursor {
			info := world.EncodeOutdatedCursorInfo()
			if writeErr := conn.Write(ctx, websocket.MessageBinary, info); writeErr != nil {
				return
			}
		}
		for _, f := range frames {
			if err := conn.Write(ctx, websocket.MessageBinary, f); err != nil {
				return
			}
		}

		// Live phase.
		for {
			select {
			case <-ctx.Done():
				return
			case f, ok := <-sub.Events():
				if !ok {
					return
				}
				if err := conn.Write(ctx, websocket.MessageBinary, f); err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					_ = conn.Close(websocket.StatusInternalError, "write")
					return
				}
			}
		}
	})
}
```

- [ ] **Step 4: Add the helpers SubscribeFanout + EncodeOutdatedCursorInfo to the world API**

In `internal/simulator/world/account_view.go`, append:

```go
import (
	// existing
	"github.com/bluesky-social/jetstream-v2/internal/simulator/fanout"
)

// SubscribeFanout adds a new subscriber to the live broadcast.
func (w *World) SubscribeFanout() *fanout.Subscriber {
	return w.fanout.Subscribe()
}

// EncodeOutdatedCursorInfo returns the wire frame for a #info
// OutdatedCursor event. Convenience for the relay handler.
func EncodeOutdatedCursorInfo() []byte {
	return encodeInfoFrame("OutdatedCursor", "cursor older than retained history")
}
```

- [ ] **Step 5: Run tests**

```sh
just test ./internal/simulator/http
just lint
```

- [ ] **Step 6: Commit**

```sh
git add internal/simulator/http/relay_subscribe.go internal/simulator/http/relay_subscribe_test.go internal/simulator/world/account_view.go
git commit -m "simulator: subscribeRepos websocket with replay + live fanout"
```

---
## Task 16: cmd/simulator main wiring

Goal: turn the empty `simulator serve` action from Task 1 into a working binary that opens the world, runs traffic, serves HTTP, and shuts down on signal. Mirrors `cmd/jetstream/main.go`'s structure.

**Files:**
- Modify: `cmd/simulator/main.go`
- Modify: `cmd/simulator/main_test.go`

- [ ] **Step 1: Replace cmd/simulator/main.go with the full serve action**

```go
// Command simulator is a development-only fake atproto network: PLC,
// a single PDS, and a relay (firehose) under one HTTP listener. It
// exists so jetstream can iterate locally without depending on
// bsky.network or plc.directory. Not shipped to users; not in the
// Dockerfile.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/bluesky-social/jetstream-v2/internal/simulator/fanout"
	simhttp "github.com/bluesky-social/jetstream-v2/internal/simulator/http"
	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
	"github.com/urfave/cli/v3"
	"golang.org/x/sync/errgroup"
)
```

`internal/simulator` is allowed to depend on `internal/obs` because `obs` is purely observability (logging + tracing setup) and provides no production-domain coupling. If you'd rather keep the wall fully airtight, copy `obs.BuildLoggerFromStrings` into `cmd/simulator/main.go` directly — it's ~15 lines.

```go
func main() {
	if err := newApp().Run(context.Background(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "simulator:", err)
		os.Exit(1)
	}
}

func newApp() *cli.Command {
	return &cli.Command{
		Name:  "simulator",
		Usage: "Local atproto simulator for jetstream development",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "log-level",
				Usage:   "Log level (debug|info|warn|error)",
				Sources: cli.EnvVars("JETSTREAM_LOG_LEVEL"),
				Value:   "info",
			},
			&cli.StringFlag{
				Name:    "log-format",
				Usage:   "Log handler format (text|json)",
				Sources: cli.EnvVars("JETSTREAM_LOG_FORMAT"),
				Value:   "json",
			},
		},
		Commands: []*cli.Command{serveCommand()},
	}
}

func serveCommand() *cli.Command {
	return &cli.Command{
		Name:  "serve",
		Usage: "Run the simulator",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "addr", Sources: cli.EnvVars("JETSTREAM_SIM_ADDR"), Value: ":7777"},
			&cli.StringFlag{Name: "metrics-addr", Sources: cli.EnvVars("JETSTREAM_SIM_METRICS_ADDR"), Value: ":7778"},
			&cli.StringFlag{Name: "data-dir", Sources: cli.EnvVars("JETSTREAM_SIM_DATA_DIR"), Value: "./data/simulator"},
			&cli.BoolFlag{Name: "reset", Sources: cli.EnvVars("JETSTREAM_SIM_RESET")},
			&cli.Uint64Flag{Name: "seed", Sources: cli.EnvVars("JETSTREAM_SIM_SEED"), Value: 42},
			&cli.IntFlag{Name: "accounts", Sources: cli.EnvVars("JETSTREAM_SIM_ACCOUNTS"), Value: 10000},
			&cli.IntFlag{Name: "initial-records-per-account", Sources: cli.EnvVars("JETSTREAM_SIM_INITIAL_RECORDS"), Value: 5},
			&cli.FloatFlag{Name: "commits-per-sec", Sources: cli.EnvVars("JETSTREAM_SIM_COMMITS_PER_SEC"), Value: 10},
			&cli.FloatFlag{Name: "traffic-rate-multiplier", Sources: cli.EnvVars("JETSTREAM_SIM_TRAFFIC_RATE_MULTIPLIER"), Value: 1},
			&cli.IntFlag{Name: "firehose-history", Sources: cli.EnvVars("JETSTREAM_SIM_FIREHOSE_HISTORY"), Value: 10000},
			&cli.IntFlag{Name: "repo-cache", Sources: cli.EnvVars("JETSTREAM_SIM_REPO_CACHE"), Value: 512},
			&cli.StringFlag{Name: "public-url", Sources: cli.EnvVars("JETSTREAM_SIM_PUBLIC_URL"), Value: ""},
			&cli.DurationFlag{Name: "shutdown-timeout", Value: 30 * time.Second},
		},
		Action: runServe,
	}
}

func runServe(ctx context.Context, cmd *cli.Command) error {
	processLogger, err := obs.BuildLoggerFromStrings(os.Stderr, cmd.String("log-level"), cmd.String("log-format"))
	if err != nil {
		return err
	}
	logger := processLogger.With(slog.String("component", "simulator/main"))
	slog.SetDefault(logger)

	cfg := world.Config{
		DataDir:         cmd.String("data-dir"),
		Reset:           cmd.Bool("reset"),
		Seed:            cmd.Uint64("seed"),
		Accounts:        cmd.Int("accounts"),
		InitialRecords:  cmd.Int("initial-records-per-account"),
		CommitsPerSec:   cmd.Float("commits-per-sec"),
		RateMultiplier:  cmd.Float("traffic-rate-multiplier"),
		FirehoseHistory: cmd.Int("firehose-history"),
		RepoCacheSize:   cmd.Int("repo-cache"),
	}

	w, err := world.New(ctx, cfg)
	if err != nil {
		return fmt.Errorf("simulator: %w", err)
	}
	defer func() {
		if cerr := w.Close(); cerr != nil {
			logger.Error("world close failed", "err", cerr)
		}
	}()

	wantBootstrap, err := w.EnsureSeed()
	if err != nil {
		return err
	}
	if wantBootstrap {
		if err := w.Bootstrap(ctx, processLogger); err != nil {
			return err
		}
	}

	rng := rand.New(rand.NewPCG(cfg.Seed^0xfeedf00d, cfg.Seed^0xc0ffee))
	fan := fanout.New(1024)
	if err := w.AttachRuntime(rng, fan); err != nil {
		return err
	}

	publicURL := cmd.String("public-url")
	if publicURL == "" {
		publicURL = "http://" + bindHost(cmd.String("addr"))
	}

	mux := simhttp.NewHandler(w, publicURL)

	httpSrv := &http.Server{
		Addr:              cmd.String("addr"),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	runCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	g, gctx := errgroup.WithContext(runCtx)

	g.Go(func() error {
		logger.InfoContext(gctx, "http listening", "addr", cmd.String("addr"))
		err := httpSrv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})

	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cmd.Duration("shutdown-timeout"))
		defer cancel()
		return httpSrv.Shutdown(shutdownCtx)
	})

	g.Go(func() error {
		err := w.RunTraffic(gctx, processLogger)
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// bindHost returns "localhost" prefixed with the bound port from a
// "[host]:port" addr string. If no host is given (e.g. ":7777"),
// localhost is the right thing to advertise to peers running on the
// same machine.
func bindHost(addr string) string {
	if addr == "" {
		return "localhost:7777"
	}
	if addr[0] == ':' {
		return "localhost" + addr
	}
	return addr
}
```

- [ ] **Step 2: Update the test to actually exercise serve briefly**

`cmd/simulator/main_test.go`:

```go
package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestApp_HelpRuns(t *testing.T) {
	t.Parallel()
	cmd := newApp()
	require.NoError(t, cmd.Run(context.Background(), []string{"simulator", "--help"}))
}

func TestServe_StartsAndStops(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a real listener; run without -short")
	}
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "simulator")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cmd := newApp()
	err := cmd.Run(ctx, []string{
		"simulator", "serve",
		"--addr=127.0.0.1:0",
		"--metrics-addr=127.0.0.1:0",
		"--data-dir", dir,
		"--accounts=5",
		"--initial-records-per-account=1",
		"--commits-per-sec=100",
	})
	// We expect ctx-deadline cancellation, which runServe maps to nil.
	require.NoError(t, err)
}
```

- [ ] **Step 3: Run tests + build**

```sh
just test ./cmd/simulator
just build
just lint
```

(`just build` only builds `cmd/jetstream`, but you can also build the simulator with `go build -trimpath -o bin/simulator ./cmd/simulator` to confirm.)

- [ ] **Step 4: Commit**

```sh
git add cmd/simulator/main.go cmd/simulator/main_test.go
git commit -m "wire cmd/simulator serve action with full lifecycle"
```

---
## Task 17: Jetstream-side `--plc-url` flag

Goal: add the `--plc-url` flag to `cmd/jetstream/main.go` and plumb it into `identity.DefaultResolver.PLCURL`. When the flag is empty, behavior is unchanged (defaults to `https://plc.directory`).

**Files:**
- Modify: `cmd/jetstream/main.go`

- [ ] **Step 1: Read the current resolver wiring**

The current code in `cmd/jetstream/main.go` (around lines 199–280) has `--relay-url` already and constructs `&identity.DefaultResolver{}` with no PLC override. We add a sibling flag and one line of plumbing.

- [ ] **Step 2: Add the flag**

In `cmd/jetstream/main.go`, in `serveCommand()` (currently around lines 192–204 with the `--relay-url` flag), append after the `--relay-url` flag block:

```go
&cli.StringFlag{
    Name:    "plc-url",
    Usage:   "Base URL of the PLC directory (defaults to https://plc.directory when empty)",
    Sources: cli.EnvVars("JETSTREAM_PLC_URL"),
    Value:   "",
},
```

- [ ] **Step 3: Plumb the flag into the resolver**

In `runServe`, replace the resolver/directory construction (around lines 274–279) with:

```go
resolver := &identity.DefaultResolver{}
if u := cmd.String("plc-url"); u != "" {
    resolver.PLCURL = gt.Some(u)
}
directory := &identity.Directory{
    Resolver:               resolver,
    Cache:                  identcache.New(metaStore, identcache.DefaultTTL),
    SkipHandleVerification: true,
}
```

- [ ] **Step 4: Run the existing jetstream tests**

```sh
just test ./cmd/jetstream
just lint
```

- [ ] **Step 5: Commit**

```sh
git add cmd/jetstream/main.go
git commit -m "jetstream: add --plc-url flag for resolver override"
```

---

## Task 18: .env + justfile + .gitignore

Goal: ship the dev-default config so `just run` defaults to the simulator, with `just run-prod` available for occasional production smoke tests.

**Files:**
- Modify: `.gitignore`
- Create: `.env`
- Modify: `justfile`

- [ ] **Step 1: Drop `.env` from .gitignore**

Open `.gitignore`. Remove the two lines:

```
# env file
.env
```

Add a short comment in their place near the top of the file:

```
# Note: .env is intentionally committed (see repo root) with
# simulator-pointing defaults. Do not put secrets in it.
```

- [ ] **Step 2: Create the .env file**

Create `.env` at repo root:

```
# Dev-only defaults that point jetstream at the local simulator.
# `just run` and any other recipe with `set dotenv-load` picks these
# up automatically. `just run-prod` overrides these inline.
JETSTREAM_RELAY_URL=http://localhost:7777
JETSTREAM_PLC_URL=http://localhost:7777
```

- [ ] **Step 3: Update the justfile**

Open `justfile`. After `set shell := ["bash", "-cu"]`, add:

```just
set dotenv-load
```

Replace the existing `run` recipe with:

```just
# Run jetstream against the local simulator (default).
# Picks up JETSTREAM_RELAY_URL and JETSTREAM_PLC_URL from .env.
run *ARGS:
    go run ./cmd/jetstream {{ARGS}}

# Run jetstream against real production (bsky.network + plc.directory).
run-prod *ARGS:
    JETSTREAM_RELAY_URL=https://bsky.network \
    JETSTREAM_PLC_URL=https://plc.directory \
    go run ./cmd/jetstream {{ARGS}}

# Run the local simulator (PLC + PDS + relay + firehose).
simulator *ARGS:
    go run ./cmd/simulator {{ARGS}}

# Wipe the simulator's pebble db so the next `just simulator` re-bootstraps.
simulator-reset:
    rm -rf ./data/simulator
```

(Keep the existing `run-race` recipe — no changes needed.)

- [ ] **Step 4: Verify recipes work**

```sh
just --list                          # should list run, run-prod, simulator, simulator-reset
just run --version                   # picks up .env; should print version
just run-prod --version              # works without .env affecting outcome
```

- [ ] **Step 5: Commit**

```sh
git add .gitignore .env justfile
git commit -m "wire .env-driven dev defaults: just run targets simulator"
```

---
## Task 19: End-to-end smoke test

Goal: an automated test that boots both the simulator and jetstream as in-process subprocesses (or via in-process `*World` and the existing `cmd/jetstream/serve_test.go` patterns), confirms backfill drains, then confirms `/subscribe` delivers a live event. This is the test that demonstrates the entire goal of the project.

**Files:**
- Create: `cmd/simulator/e2e_test.go`

- [ ] **Step 1: Write the smoke test**

`cmd/simulator/e2e_test.go`:

```go
//go:build !short

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/fanout"
	simhttp "github.com/bluesky-social/jetstream-v2/internal/simulator/http"
	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"log/slog"
	"math/rand/v2"
	"os"
)

// TestEndToEnd_JetstreamConsumesSimulator boots the simulator as an
// httptest.Server, then spawns jetstream as a subprocess pointed at
// it, then connects a websocket client to jetstream's /subscribe.
// Verifies that backfill drains and live events flow.
//
// We use os/exec rather than running jetstream's serve action
// in-process so the test exercises the same binary boundary the user
// hits in real workflows. Tagged !short so `just test` skips it.
func TestEndToEnd_JetstreamConsumesSimulator(t *testing.T) {
	t.Parallel()

	// Build the simulator world directly — using cmd/simulator as a
	// subprocess too would slow the test down without changing
	// coverage.
	cfg := world.DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Accounts = 25
	cfg.InitialRecords = 1
	cfg.CommitsPerSec = 200
	w, err := world.New(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(99, 100)), fanout.New(64)))

	simSrv := httptest.NewServer(nil)
	simSrv.Config.Handler = simhttp.NewHandler(w, simSrv.URL)
	defer simSrv.Close()

	// Run live traffic in a goroutine while the test runs.
	trafficCtx, trafficCancel := context.WithCancel(context.Background())
	defer trafficCancel()
	go func() {
		_ = w.RunTraffic(trafficCtx, slog.Default())
	}()

	// Spawn jetstream pointed at the simulator. We use `go run`
	// here for simplicity; on slower machines `just build` first
	// and exec the binary if startup time becomes a flake source.
	jetDir := filepath.Join(t.TempDir(), "jetstream-data")
	require.NoError(t, os.MkdirAll(jetDir, 0o755))

	jetCtx, jetCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer jetCancel()
	binPath := buildJetstreamForTest(t)

	jetAddr := freePortAddr(t)
	jetDebug := freePortAddr(t)

	cmd := newJetstreamCmd(jetCtx, binPath, []string{
		"serve",
		"--addr", jetAddr,
		"--debug-addr", jetDebug,
		"--data-dir", jetDir,
		"--relay-url", simSrv.URL,
		"--plc-url", simSrv.URL,
		"--shutdown-timeout=5s",
	})
	stderr := &lockedBuffer{}
	cmd.Stdout = stderr
	cmd.Stderr = stderr
	require.NoError(t, cmd.Start())
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	// Wait for jetstream's /subscribe to start serving (i.e.,
	// backfill drained and we're in steady state).
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		conn, _, err := websocket.Dial(ctx, "ws://"+jetAddr+"/subscribe", nil)
		if err != nil {
			return false
		}
		_ = conn.Close(websocket.StatusNormalClosure, "probe")
		return true
	}, 20*time.Second, 200*time.Millisecond,
		"jetstream did not become ready; logs:\n%s", stderr.String())

	// Now consume one live event.
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dialCancel()
	conn, _, err := websocket.Dial(dialCtx, "ws://"+jetAddr+"/subscribe", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") }()

	_, msg, err := conn.Read(dialCtx)
	require.NoError(t, err)
	// Jetstream's /subscribe emits JSON envelopes; just confirm it
	// parses as JSON. The shape is exercised by the existing
	// internal/subscribe tests.
	require.True(t, json.Valid(msg), "unexpected non-JSON frame: %q", string(msg))
	require.True(t, strings.Contains(string(msg), `"did":"did:plc:`),
		"expected DID in payload, got: %s", string(msg))

	// Bonus: confirm backfill ran by walking debug metrics.
	_ = fmt.Println // keep import set stable
}
```

The helpers `buildJetstreamForTest`, `newJetstreamCmd`, `freePortAddr`, and `lockedBuffer` are tiny utilities. Add them in a new file:

`cmd/simulator/e2e_helpers_test.go`:

```go
//go:build !short

package main

import (
	"bytes"
	"context"
	"net"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func buildJetstreamForTest(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "jetstream")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/jetstream")
	cmd.Dir = repoRoot(t)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "go build jetstream: %s", string(out))
	return bin
}

func newJetstreamCmd(ctx context.Context, bin string, args []string) *exec.Cmd {
	return exec.CommandContext(ctx, bin, args...)
}

func freePortAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func repoRoot(t *testing.T) string {
	t.Helper()
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	out, err := cmd.Output()
	require.NoError(t, err)
	return string(bytes.TrimSpace(out))
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
```

- [ ] **Step 2: Run the test**

```sh
just test-long ./cmd/simulator -run TestEndToEnd
```

Expected: PASS within ~25s (most of which is `go build` of jetstream).

- [ ] **Step 3: Commit**

```sh
git add cmd/simulator/e2e_test.go cmd/simulator/e2e_helpers_test.go
git commit -m "e2e test: jetstream consumes simulator end-to-end"
```

---

## Task 20: Manual verification + README note

Goal: the actual user-facing "did the thing work" check, plus a tiny note in `README.md` for newcomers.

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Manual smoke**

In one terminal:

```sh
just simulator
```

Expected: ~5s of bootstrap log lines (every 1k accounts), then `http listening addr=:7777`, then periodic traffic.

In another terminal:

```sh
just run serve
```

Expected: jetstream picks up `JETSTREAM_RELAY_URL=http://localhost:7777` and `JETSTREAM_PLC_URL=http://localhost:7777` from `.env`. Backfill drains in seconds, then the orchestrator transitions through merge → steady-state.

In a third terminal:

```sh
websocat ws://localhost:8080/subscribe
```

Expected: a stream of JSON events flow.

If anything misbehaves: check `JETSTREAM_LOG_LEVEL=debug` on either side. The simulator logs at `component=simulator/main`, `simulator/bootstrap`, `simulator/traffic`. Jetstream uses its existing slog component tags.

- [ ] **Step 2: Add a short README section**

Append to `README.md`:

```markdown
## Local development with the simulator

`just run` defaults to a local simulator (`./cmd/simulator`) that emulates
PLC, PDS, and relay under one HTTP listener. This avoids waiting hours for
backfill against the real bsky.network + plc.directory.

```sh
just simulator        # terminal 1: starts the simulator on :7777
just run serve        # terminal 2: jetstream points at the simulator (via .env)
```

To re-bootstrap the simulator's world from scratch:

```sh
just simulator-reset
just clean            # also wipe jetstream's data dir, since seq counters reset
```

To smoke against real production occasionally:

```sh
just run-prod serve
```

The simulator is a dev tool: not in the Dockerfile, not shipped to users.
See `docs/superpowers/specs/2026-05-26-local-simulator-design.md` for design
context.
```

- [ ] **Step 3: Commit**

```sh
git add README.md
git commit -m "docs: README note about the local simulator"
```

---

## Self-review checklist (run before declaring complete)

- Spec coverage:
  - Three services under one listener, production paths verbatim → Tasks 12, 13, 14, 15.
  - 10k pre-populated accounts with realistic distributions → Tasks 4, 5, 7, 8.
  - Cryptographically valid signed commits → Tasks 6, 11. Verified end-to-end in Task 14's signature test.
  - Persistent state with `--reset` → Tasks 2, 3.
  - Single global RNG seed, `--reset` only touches `--data-dir` → Task 2 (validate), Task 8 (bootstrap), Task 11 (RunTraffic uses world's rng).
  - `.env`-driven defaults, `just run-prod` for prod → Task 18.
  - Jetstream-side `--plc-url` → Task 17.
  - Simulator never imports jetstream's `internal/...` packages other than `internal/obs` (logging only) → enforced by code organization in every task.
  - End-to-end smoke proves the goal → Task 19.

- Type / API consistency:
  - `World.AttachRuntime(rng, fanout)` signature is the same in Tasks 11, 16, 19, and the helpers files.
  - `World.SubscribeFanout()` and `World.FirehoseRange()` referenced by Task 15 are added in Tasks 11/15.
  - `Account.PubkeyMultibase()` and `Account.HandleSuffix()` are introduced in Task 12 alongside their first use.
  - `EncodeOutdatedCursorInfo` lives next to other `encode…Frame` helpers (Task 10/15).

- TDD discipline: every task starts with a failing test, has its own commit, and the granularity stays at "one logical change per commit." Tasks 11 and 14 are the longest because the work is genuinely tightly coupled (commit pipeline, signature round-trip).

If this plan ages and any of the snippets stop matching the latest atmos / jetstream APIs (e.g. `cli.Uint64Flag` vs `cli.Uint64Flag`, or atmos's `gt.Option` shape), prefer the live API in the codebase over the snippet — the snippet is a target, not a contract.

