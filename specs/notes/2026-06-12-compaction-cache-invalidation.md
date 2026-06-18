# Compaction Cache Invalidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure compaction-refresh serving paths cannot reuse stale manifest metadata or stale subscribe decoded block cache entries after a sealed segment rewrite.

**Architecture:** Keep manifest refresh per segment and add generation-aware per-segment invalidation to the subscribe decoded block cache. Runtime composes the compaction hook so a successful manifest refresh invalidates the matching cold replay cache segment before the compaction watermark can advance.

**Tech Stack:** Go, `segment.Rewrite`, `internal/manifest`, `internal/subscribe`, `internal/jetstreamd`, `internal/oracle`, `just test`.

---

## File Structure

- Modify `internal/subscribe/blockcache.go`: add segment generation tracking and `invalidateSegment`.
- Modify `internal/subscribe/blockcache_test.go`: add red/green tests for purge and in-flight decode invalidation.
- Modify `internal/subscribe/replay.go`: convert the returned cold reader from a bare function to `*ColdReader` with `Read` and `InvalidateSegment`.
- Modify subscribe tests that call `NewColdReader`: pass `cold.Read` to `subscribe.New` and call `cold.Read` directly where needed.
- Modify `internal/jetstreamd/runtime.go`: compose `mft.OnSegmentCompacted` with `coldRd.InvalidateSegment`.
- Modify `internal/manifest/manifest_test.go`: add a regression test proving `OnSegmentCompacted` replaces resident metadata after a rewrite.
- Modify `internal/oracle/harness_test.go`: add serving-side replay assertions that warm the cold cache before compaction and replay after compaction.

## Task 1: Block Cache Segment Invalidation

**Files:**
- Modify: `internal/subscribe/blockcache_test.go`
- Modify: `internal/subscribe/blockcache.go`

- [ ] **Step 1: Write the failing purge test**

Add this test to `internal/subscribe/blockcache_test.go`:

```go
func TestBlockCache_InvalidateSegmentForcesRedecode(t *testing.T) {
	t.Parallel()
	c := newBlockCache(1 << 20)
	key := c.keyForBlock(7, 11, 0)

	var calls atomic.Int64
	evs, err := c.getOrDecode(key, func() ([]segment.Event, error) {
		calls.Add(1)
		return decodedFixture(1), nil
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), evs[0].Seq)

	c.invalidateSegment(7)

	key = c.keyForBlock(7, 11, 0)
	evs, err = c.getOrDecode(key, func() ([]segment.Event, error) {
		calls.Add(1)
		return decodedFixture(2), nil
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), evs[0].Seq)
	require.Equal(t, int64(2), calls.Load(), "invalidated segment must re-decode")
}
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
just test ./internal/subscribe -run TestBlockCache_InvalidateSegmentForcesRedecode -v
```

Expected: FAIL to compile because `keyForBlock` and `invalidateSegment` do not exist.

- [ ] **Step 3: Implement minimal generation keys and invalidation**

Modify `internal/subscribe/blockcache.go`:

```go
type blockKey struct {
	segIdx     uint64
	checksum   uint64
	blockIdx   uint64
	generation uint64
}

type blockCache struct {
	mu       sync.Mutex
	maxBytes int
	curBytes int
	ll       *list.List
	items    map[blockKey]*list.Element

	inFlight map[blockKey]*inflight

	generationBySegment map[uint64]uint64
}

func newBlockCache(maxBytes int) *blockCache {
	if maxBytes <= 0 {
		panic("subscribe: blockCache maxBytes must be > 0")
	}
	return &blockCache{
		maxBytes:             maxBytes,
		ll:                   list.New(),
		items:                make(map[blockKey]*list.Element),
		inFlight:             make(map[blockKey]*inflight),
		generationBySegment: make(map[uint64]uint64),
	}
}

func (c *blockCache) keyForBlock(segIdx, checksum uint64, blockIdx int) blockKey {
	c.mu.Lock()
	defer c.mu.Unlock()
	return blockKey{
		segIdx:     segIdx,
		checksum:   checksum,
		blockIdx:   uint64(blockIdx),
		generation: c.generationBySegment[segIdx],
	}
}

func (c *blockCache) invalidateSegment(segIdx uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.generationBySegment[segIdx]++
	for el := c.ll.Front(); el != nil; {
		next := el.Next()
		item := itemOf(el)
		if item.key.segIdx == segIdx {
			c.ll.Remove(el)
			delete(c.items, item.key)
			c.curBytes -= item.bytes
		}
		el = next
	}
}
```

Then update `decodeSealedBlock` in `internal/subscribe/replay.go` to call `cache.keyForBlock(segIdx, r.Header().Checksum, blockIdx)`.

- [ ] **Step 4: Run the test and verify GREEN**

Run:

```bash
just test ./internal/subscribe -run TestBlockCache_InvalidateSegmentForcesRedecode -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

Skip commit unless these ignored plan files are intentionally tracked later. This repo currently ignores `/docs`.

## Task 2: In-Flight Decode Race

**Files:**
- Modify: `internal/subscribe/blockcache_test.go`
- Modify: `internal/subscribe/blockcache.go`

- [ ] **Step 1: Write the failing race test**

Add this test:

```go
func TestBlockCache_InvalidateSegmentPreventsInflightInsert(t *testing.T) {
	t.Parallel()
	c := newBlockCache(1 << 20)
	key := c.keyForBlock(9, 22, 0)

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)
		_, err := c.getOrDecode(key, func() ([]segment.Event, error) {
			close(started)
			<-release
			return decodedFixture(1), nil
		})
		require.NoError(t, err)
	}()

	<-started
	c.invalidateSegment(9)
	close(release)
	<-done

	key = c.keyForBlock(9, 22, 0)
	var fresh atomic.Bool
	evs, err := c.getOrDecode(key, func() ([]segment.Event, error) {
		fresh.Store(true)
		return decodedFixture(2), nil
	})
	require.NoError(t, err)
	require.True(t, fresh.Load(), "old in-flight decode must not populate the new generation")
	require.Equal(t, uint64(2), evs[0].Seq)
}
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
just test ./internal/subscribe -run TestBlockCache_InvalidateSegmentPreventsInflightInsert -v
```

Expected: FAIL because the old in-flight decode inserts after invalidation.

- [ ] **Step 3: Guard insertion by current generation**

In `getOrDecode`, after decode completes and the lock is reacquired, check the key generation before inserting:

```go
	if err != nil {
		return nil, err
	}
	if c.generationBySegment[key.segIdx] != key.generation {
		return evs, nil
	}
```

This check must happen after `delete(c.inFlight, key)` and before inserting into `c.items`.

- [ ] **Step 4: Run the race test and the block cache package tests**

Run:

```bash
just test ./internal/subscribe -run 'TestBlockCache_' -v
```

Expected: PASS.

## Task 3: ColdReader Invalidation API

**Files:**
- Modify: `internal/subscribe/replay.go`
- Modify: `internal/subscribe/coldreader_test.go`
- Modify: `internal/subscribe/handler_integration_test.go`
- Modify: `internal/jetstreamd/runtime.go`

- [ ] **Step 1: Write the failing cold reader test**

Add this test to `internal/subscribe/coldreader_test.go`:

```go
func TestColdReader_InvalidateSegmentPurgesDecodedBlocks(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	segDir := filepath.Join(dir, "segments")
	mustWriteSealedSegment(t, filepath.Join(segDir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 9, minIndexedAt: 1_000, maxIndexedAt: 9_999, eventCount: 10,
	})
	m := mustOpenManifest(t, segDir)
	st, w := openWriterAtTip(t, dir, 10)
	t.Cleanup(func() { _ = w.Close(); _ = st.Close() })

	var writerPtr atomic.Pointer[ingest.Writer]
	writerPtr.Store(w)
	rd := subscribe.NewColdReader(subscribe.ColdReaderConfig{
		Manifest: m, WriterRef: &writerPtr, BlockCacheBytes: 1 << 20,
	})

	batch, _, err := rd.Read(context.Background(), 0, 10)
	require.NoError(t, err)
	require.Len(t, batch, 10)

	rd.InvalidateSegment(0)

	batch, _, err = rd.Read(context.Background(), 0, 10)
	require.NoError(t, err)
	require.Len(t, batch, 10)
	require.Equal(t, uint64(0), batch[0].Event.Seq)
}
```

- [ ] **Step 2: Run and verify RED**

Run:

```bash
just test ./internal/subscribe -run TestColdReader_InvalidateSegmentPurgesDecodedBlocks -v
```

Expected: FAIL to compile because `NewColdReader` returns a function and has no `Read` or `InvalidateSegment`.

- [ ] **Step 3: Implement `*ColdReader`**

In `internal/subscribe/replay.go`, replace `NewColdReader` with:

```go
type ColdReader struct {
	manifest  *manifest.Manifest
	writerRef *atomic.Pointer[ingest.Writer]
	cache     *blockCache
}

func NewColdReader(cfg ColdReaderConfig) *ColdReader {
	bytes := cfg.BlockCacheBytes
	if bytes <= 0 {
		bytes = DefaultBlockCacheBytes
	}
	return &ColdReader{
		manifest:  cfg.Manifest,
		writerRef: cfg.WriterRef,
		cache:     newBlockCache(bytes),
	}
}

func (r *ColdReader) InvalidateSegment(idx uint64) {
	if r == nil || r.cache == nil {
		return
	}
	r.cache.invalidateSegment(idx)
}

func (r *ColdReader) Read(ctx context.Context, cursor uint64, max int) ([]*Entry, uint64, error) {
	w := r.writerRef.Load()
	if w == nil {
		return nil, cursor, errColdUnavailable
	}
	batch := make([]*Entry, 0, max)
	next := cursor
	err := WalkFromCursor(ctx, WalkInput{
		StartSeq:   cursor,
		Manifest:   r.manifest,
		Writer:     w,
		BlockCache: r.cache,
	}, func(ev *segment.Event) error {
		cp := *ev
		batch = append(batch, newEntry(&cp))
		next = ev.Seq + 1
		if len(batch) >= max {
			return errBatchFull
		}
		return nil
	})
	if err != nil && !errors.Is(err, errBatchFull) {
		return nil, cursor, err
	}
	return batch, next, nil
}
```

Update call sites in `internal/subscribe/coldreader_test.go`,
`internal/subscribe/handler_integration_test.go`, and
`internal/jetstreamd/runtime.go`:

```go
cold := subscribe.NewColdReader(...)
tail, err := subscribe.New(cfg, cold.Read, nextSeq)
```

In tests that call the reader directly, use `rd.Read(...)`.

- [ ] **Step 4: Run subscribe tests**

Run:

```bash
just test ./internal/subscribe
```

Expected: PASS.

## Task 4: Runtime Compaction Hook Wiring

**Files:**
- Modify: `internal/jetstreamd/runtime.go`
- Modify: `internal/jetstreamd/options.go`
- Modify: `internal/jetstreamd/runtime_test.go`

- [ ] **Step 1: Write a failing runtime construction test**

Add a tiny test for a public address accessor so oracle tests can dial the
actual listener without reaching into private fields:

```go
func TestRuntimePublicAddrBeforeRunIsEmpty(t *testing.T) {
	t.Parallel()
	rt := &Runtime{}
	require.Empty(t, rt.PublicAddr())
}
```

Add it to `internal/jetstreamd/runtime_test.go`.

Run:

```bash
just test ./internal/jetstreamd -run TestRuntimePublicAddrBeforeRunIsEmpty -v
```

Expected: FAIL to compile because `Runtime.PublicAddr` does not exist.

- [ ] **Step 2: Add the runtime accessor**

Add to `internal/jetstreamd/runtime.go`:

```go
// PublicAddr returns the bound public listener address, or "" before Run binds.
func (r *Runtime) PublicAddr() string {
	if r == nil || r.server == nil {
		return ""
	}
	return r.server.PublicAddr()
}
```

- [ ] **Step 3: Verify RED-to-GREEN for the accessor**

Run:

```bash
just test ./internal/jetstreamd -run TestRuntimePublicAddrBeforeRunIsEmpty -v
```

Expected: PASS.

- [ ] **Step 4: Confirm direct compaction hook wiring needs change**

Run:

```bash
rg -n "NewColdReader|OnSegmentCompacted" internal/jetstreamd internal/subscribe
```

Expected before code change: `runtime.go` still passes `mft.OnSegmentCompacted` directly.

- [ ] **Step 5: Compose manifest refresh and cache invalidation**

In `internal/jetstreamd/runtime.go`, change cold reader construction and hook wiring:

```go
coldRd := subscribe.NewColdReader(subscribe.ColdReaderConfig{
	Manifest:        mft,
	WriterRef:       &writerPtr,
	BlockCacheBytes: opts.SubscribeBlockCacheBytes,
})
```

Then pass `coldRd.Read` to `subscribe.New`:

```go
}, coldRd.Read, func() uint64 {
```

Before `orchestrator.New`, add:

```go
onSegmentCompacted := func(idx uint64, path string) error {
	if err := mft.OnSegmentCompacted(idx, path); err != nil {
		return err
	}
	coldRd.InvalidateSegment(idx)
	return nil
}
```

And wire:

```go
OnSegmentCompacted:       onSegmentCompacted,
```

- [ ] **Step 6: Run compile tests for runtime and subscribe**

Run:

```bash
just test ./internal/jetstreamd ./internal/subscribe
```

Expected: PASS.

## Task 5: Manifest Refresh Regression

**Files:**
- Modify: `internal/manifest/manifest_test.go`

- [ ] **Step 1: Write the failing manifest metadata test**

Add:

```go
func TestOnSegmentCompacted_ReplacesResidentMetadata(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "seg_0000000000.jss")
	mustWriteSealedSegment(t, path, sealedFixture{
		minSeq: 0, maxSeq: 9, minIndexedAt: 1_000, maxIndexedAt: 9_999, eventCount: 10,
	})
	m := mustOpenManifest(t, dir)
	before, _, _ := m.ListFrom(0, 1)
	require.Len(t, before, 1)
	require.EqualValues(t, 10, before[0].EventCount)

	_, err := segment.Rewrite(path, func(ev *segment.Event) segment.RowDecision {
		if ev.Seq < 5 {
			return segment.RowDrop
		}
		return segment.RowKeep
	}, segment.RewriteOptions{})
	require.NoError(t, err)

	require.NoError(t, m.OnSegmentCompacted(0, path))

	after, _, _ := m.ListFrom(0, 1)
	require.Len(t, after, 1)
	require.EqualValues(t, 5, after[0].EventCount)
	require.NotEqual(t, before[0].Checksum, after[0].Checksum)

	blocks, err := m.BlockIndex(0)
	require.NoError(t, err)
	var events uint32
	for _, b := range blocks {
		events += b.EventCount
	}
	require.EqualValues(t, 5, events)
}
```

Also import `github.com/bluesky-social/jetstream-v2/segment`.

- [ ] **Step 2: Run and verify RED**

Run:

```bash
just test ./internal/manifest -run TestOnSegmentCompacted_ReplacesResidentMetadata -v
```

Expected: If current manifest refresh is already correct, this may PASS. If it passes, keep it as regression coverage and proceed. If it fails, fix `refreshSegment`.

- [ ] **Step 3: Run manifest tests**

Run:

```bash
just test ./internal/manifest
```

Expected: PASS.

## Task 6: Orchestrator Refresh Failure Contract

**Files:**
- Modify: `internal/ingest/orchestrator/compact_deletes_test.go`

- [ ] **Step 1: Preserve the existing refresh-failure regression**

Confirm this existing test is still present:

```bash
rg -n "TestRunDeleteCompaction_ManifestRefreshFailureReconcilesOnRetry|manifest refresh failed|require.Zero\\(t, watermark\\)" internal/ingest/orchestrator/compact_deletes_test.go
```

Expected: output includes `TestRunDeleteCompaction_ManifestRefreshFailureReconcilesOnRetry`,
`manifest refresh failed`, and `require.Zero(t, watermark)`.

Keep its assertions intact while changing runtime hook wiring:

```go
require.ErrorIs(t, err, refreshErr)
watermark, _, loadErr := loadCompactionWatermark(st)
require.NoError(t, loadErr)
require.Zero(t, watermark)
```

- [ ] **Step 2: Run orchestrator compaction tests**

Run:

```bash
just test ./internal/ingest/orchestrator -run 'Test.*Compaction|Test.*Manifest' -v
```

Expected: PASS.

## Task 7: Oracle Serving Coverage

**Files:**
- Modify: `internal/oracle/harness_test.go`
- Create: `internal/oracle/subscribe_replay_test.go`

- [ ] **Step 1: Add helpers to collect websocket replay events**

Add imports where the helper lives:

```go
import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/gt"
)
```

Add helper code that dials the runtime public URL with a cursor and
`extended=true`, then reads until `targetSeq`:

```go
func collectSubscribeReplay(t *testing.T, baseURL string, cursor, targetSeq uint64) []ObservedEvent {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(baseURL, "http") +
		"/subscribe?extended=true&cursor=" + strconv.FormatUint(cursor, 10)
	conn, resp, err := websocket.Dial(context.Background(), wsURL, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	defer func() { _ = conn.CloseNow() }()

	var out []ObservedEvent
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, body, err := conn.Read(ctx)
		cancel()
		require.NoError(t, err)

		var msg subscribeReplayEvent
		require.NoError(t, json.Unmarshal(body, &msg))
		ev := observedEventFromSubscribeReplay(t, msg)
		out = append(out, ev)
		if ev.Seq >= targetSeq {
			return out
		}
	}
}

type subscribeReplayEvent struct {
	DID     string                  `json:"did"`
	TimeUS  int64                   `json:"time_us"`
	Cursor  uint64                  `json:"cursor"`
	Kind    string                  `json:"kind"`
	Commit  *subscribeReplayCommit  `json:"commit,omitempty"`
	Account *subscribeReplayAccount `json:"account,omitempty"`
}

type subscribeReplayCommit struct {
	Rev        string `json:"rev"`
	Operation  string `json:"operation"`
	Collection string `json:"collection"`
	RKey       string `json:"rkey"`
	RecordCBOR string `json:"record_cbor,omitempty"`
}

type subscribeReplayAccount struct {
	DID    string  `json:"did"`
	Seq    int64   `json:"seq"`
	Time   string  `json:"time"`
	Active bool    `json:"active"`
	Status *string `json:"status,omitempty"`
}

func observedEventFromSubscribeReplay(t *testing.T, msg subscribeReplayEvent) ObservedEvent {
	t.Helper()
	ev := ObservedEvent{
		Seq:       msg.Cursor,
		IndexedAt: msg.TimeUS,
		DID:       msg.DID,
	}
	switch msg.Kind {
	case "commit":
		require.NotNil(t, msg.Commit)
		ev.Collection = msg.Commit.Collection
		ev.Rkey = msg.Commit.RKey
		ev.Rev = msg.Commit.Rev
		switch msg.Commit.Operation {
		case "create":
			ev.Kind = segment.KindCreate
		case "update":
			ev.Kind = segment.KindUpdate
		case "delete":
			ev.Kind = segment.KindDelete
		default:
			t.Fatalf("unknown commit operation %q", msg.Commit.Operation)
		}
		if msg.Commit.RecordCBOR != "" {
			payload, err := base64.StdEncoding.DecodeString(msg.Commit.RecordCBOR)
			require.NoError(t, err)
			ev.Payload = payload
		}
	case "account":
		require.NotNil(t, msg.Account)
		ev.Kind = segment.KindAccount
		status := gt.None[string]()
		if msg.Account.Status != nil {
			status = gt.Some(*msg.Account.Status)
		}
		payload, err := (&comatproto.SyncSubscribeRepos_Account{
			DID:    msg.Account.DID,
			Seq:    msg.Account.Seq,
			Time:   msg.Account.Time,
			Active: msg.Account.Active,
			Status: status,
		}).MarshalCBOR()
		require.NoError(t, err)
		ev.Payload = payload
	default:
		t.Fatalf("unexpected replay kind %q", msg.Kind)
	}
	return ev
}
```

- [ ] **Step 2: Warm the cold cache before steady compaction**

In `TestOracle_DefaultLifecycle`, after `afterMerge.Release()` and after enough
steady-state events are durably observed, wait for `rt.PublicAddr()` to become
non-empty and call:

```go
publicURL := "http://" + rt.PublicAddr()
_ = collectSubscribeReplay(t, publicURL, 0, uint64(targetSeq))
```

Keep the returned events only to force block decode and cache residency.

- [ ] **Step 3: Replay after compaction and check serving-side compacted model**

After `compaction.Last(t)` reports a successful watermark, run a second replay from the same cursor and assert:

```go
lastCompaction := compaction.Last(t)
served := collectSubscribeReplay(t, publicURL, 0, lastCompaction.Watermark)
require.NoError(t, CheckCompacted(served, lastCompaction.Watermark))
```

- [ ] **Step 4: Run fast oracle**

Run:

```bash
just test ./internal/oracle -run TestOracle_DefaultLifecycle -v
```

Expected: PASS in short/default configuration.

- [ ] **Step 5: Run targeted long restart oracle if serving helpers touch lifecycle**

Run:

```bash
just test-long ./internal/oracle -run TestOracle_Restart -v
```

Expected: PASS.

## Task 8: Full Verification

**Files:**
- No code edits.

- [ ] **Step 1: Run focused packages**

Run:

```bash
just test ./internal/subscribe ./internal/manifest ./internal/jetstreamd ./internal/ingest/orchestrator ./internal/oracle
```

Expected: PASS.

- [ ] **Step 2: Run default test target**

Run:

```bash
just test
```

Expected: PASS.

- [ ] **Step 3: Inspect diff**

Run:

```bash
git diff --stat
git diff -- internal/subscribe internal/manifest internal/jetstreamd internal/ingest/orchestrator internal/oracle
```

Expected: only scoped cache invalidation and tests.

## Self-Review

- Spec coverage: Tasks 1-3 cover generation-aware subscribe cache invalidation, Task 4 covers runtime composition, Task 5 covers manifest refresh, Task 6 covers watermark failure behavior, Task 7 covers oracle serving behavior.
- Placeholder scan: no deferred implementation markers remain. Task 7 uses the concrete extended subscribe JSON shape from `internal/subscribe/encoder.go`.
- Type consistency: `ColdReader.Read`, `ColdReader.InvalidateSegment`, `blockCache.keyForBlock`, and `blockCache.invalidateSegment` are introduced before use by runtime and tests.
