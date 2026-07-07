// enqueue.go implements LiveEnqueuer, the steady-state
// firehose hook that detects net-new DIDs and schedules a full repo backfill
// for them (issue #188).
//
// During steady state a DID can appear on the firehose that we never
// backfilled — e.g. a PDS that was unreachable or firewalled during the
// bootstrap listRepos sweep becomes reachable and replays its backlog. We
// archive that DID's live events but are missing all of its historical
// records. LiveEnqueuer notices the first event from an unknown DID and
// durably creates a StatusPending repo row; the steady-state failed-repo retry
// loop then performs a full getRepo on its next pass, reusing the exact same
// download → verify → complete machinery as a failed-repo retry.
//
// Hot-path budget. Observe runs synchronously on the live consumer's single
// processBatch goroutine, once per archived event — 500+ events/sec steady,
// and during a #sync-driven repo replay potentially millions of events/sec for
// one DID in a short burst. It must never touch pebble or block on that path:
//
//  1. A lock-free last-DID fast path absorbs same-DID bursts (the #sync replay
//     case) with a single atomic load and a string compare — no lock, no map.
//  2. A bounded sharded-LRU "seen" cache absorbs recently-observed distinct
//     DIDs with one sharded-mutex map lookup and no pebble read.
//  3. A genuinely unknown DID is handed to a bounded channel drained by a
//     single background worker that performs the idempotent durable
//     EnqueueNetNewRepo off the firehose goroutine. If the channel is full the
//     candidate is dropped (and intentionally NOT cached) so a later event for
//     the same still-active DID retries — self-healing under backpressure.
//
// The seen cache is an optimization, never the source of truth: the worker's
// EnqueueNetNewRepo re-checks pebble and is a no-op when a row already exists,
// so an eviction (or a cold start) costs at most one redundant point read.
//
// Durability window (accepted limitation). Observe is deliberately async and
// pebble-free, so the durable StatusPending write lags the live consumer. The
// relay cursor can advance past a DID's archived event (a block flush inside
// writer.Append persists relay/cursor) before the worker drains that DID and
// writes its row. A process crash — or a transient EnqueueNetNewRepo failure
// racing the DID's last event — in that window loses the in-memory candidate,
// and the relay does not redeliver. This is only unrecoverable for a DID that
// emits exactly ONE event in that window: any DID that emits another event
// re-observes and re-enqueues (the fast-path last and seen cache are cleared on
// failure precisely so a later event retries). We accept this rather than make
// the per-event hot path take a synchronous pebble write at firehose rate; a
// genuinely active repo that we missed will almost always emit again.

package backfill

import (
	"context"
	"hash/maphash"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/jcalabro/atmos"
)

const (
	// DefaultLiveEnqueueQueueSize bounds the in-memory candidate channel
	// between Observe and the background worker. Sized so a transient worker
	// stall (a slow pebble write) does not immediately start dropping
	// candidates, while still capping memory and keeping the drop path bounded.
	DefaultLiveEnqueueQueueSize = 4096

	// DefaultLiveEnqueueCacheSize is the total number of recently-seen DIDs
	// held across all seen-cache shards. This is a working-set cache, not a
	// full-network index: at ~50 bytes/DID this defaults to a few tens of MB.
	// An evicted-but-still-active DID simply triggers one redundant
	// already-known pebble read on its next event.
	DefaultLiveEnqueueCacheSize = 500_000

	// DefaultLiveEnqueueCacheShards is the seen-cache shard count. Rounded up
	// to a power of two so shard selection is a mask. Observe is called from a
	// single goroutine today; sharding bounds per-shard map/eviction cost and
	// keeps the cache correct if the hook is ever driven concurrently.
	DefaultLiveEnqueueCacheShards = 64
)

// LiveEnqueuerConfig configures a LiveEnqueuer. Store is required.
type LiveEnqueuerConfig struct {
	Store   *Store
	Metrics *Metrics
	Logger  *slog.Logger

	// QueueSize, CacheSize, CacheShards override the package defaults when > 0.
	QueueSize   int
	CacheSize   int
	CacheShards int
}

// LiveEnqueuer detects net-new DIDs on the steady-state firehose and schedules
// them for a full repo backfill. Construct with NewLiveEnqueuer; call Observe
// on the hot path and Run from a background goroutine.
type LiveEnqueuer struct {
	store   *Store
	metrics *Metrics
	logger  *slog.Logger

	// last is the lock-free burst absorber: the most recently observed DID.
	// A #sync replay emits a long run of events for one DID, so checking this
	// first collapses that run to a single cache/queue interaction.
	last atomic.Pointer[string]

	cache *seenCache
	queue chan string
}

// NewLiveEnqueuer builds a LiveEnqueuer. It panics if Store is nil — that is a
// construction-time wiring bug, not a runtime condition.
func NewLiveEnqueuer(cfg LiveEnqueuerConfig) *LiveEnqueuer {
	if cfg.Store == nil {
		panic("backfill: NewLiveEnqueuer: Store is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = DefaultLiveEnqueueQueueSize
	}
	cacheSize := cfg.CacheSize
	if cacheSize <= 0 {
		cacheSize = DefaultLiveEnqueueCacheSize
	}
	shards := cfg.CacheShards
	if shards <= 0 {
		shards = DefaultLiveEnqueueCacheShards
	}
	return &LiveEnqueuer{
		store:   cfg.Store,
		metrics: cfg.Metrics,
		logger:  logger.With(slog.String("component", "backfill/live-enqueuer")),
		cache:   newSeenCache(cacheSize, shards),
		queue:   make(chan string, queueSize),
	}
}

// Observe records that an event for did was archived. It is safe to call with
// an empty DID (ignored). It never blocks and never touches pebble; an unknown
// DID is handed to the background worker via a non-blocking send.
func (e *LiveEnqueuer) Observe(did string) {
	if did == "" {
		return
	}

	// Lock-free fast path: collapse a run of events for the same DID (the
	// #sync replay burst) to nothing after the first. last only ever holds a
	// DID we have fully handled (cached or successfully enqueued), so a dropped
	// DID never poisons this path — it is retried on its next event.
	if p := e.last.Load(); p != nil && *p == did {
		e.metrics.incEnqueueSeenCacheHit()
		return
	}

	if e.cache.seen(did) {
		e.setLast(did)
		e.metrics.incEnqueueSeenCacheHit()
		return
	}

	// Drop malformed DIDs here, on the hot path, before they consume a queue
	// slot or a durable write. #identity events (and #account verification
	// failures) are not DID-syntax-verified upstream, so a malformed DID can
	// reach Observe. A bad DID is non-retryable — re-validating it forever would
	// be pure churn — so cache it (and set last) exactly like a handled DID: the
	// fast path and seen cache then absorb every later event for it instead of
	// re-running validation. EnqueueNetNewRepo re-checks at the durable boundary
	// as defense in depth, so a malformed key can never become a repo/ row that
	// wedges the retry scan.
	if err := atmos.DID(did).Validate(); err != nil {
		e.cache.store(did)
		e.setLast(did)
		e.metrics.incEnqueueInvalidDID()
		return
	}

	// Unknown DID: hand it to the worker without blocking the firehose.
	select {
	case e.queue <- did:
		// Mark seen only after a successful enqueue so a still-active DID is
		// not enqueued repeatedly while its candidate sits in the channel or
		// is being written. If the durable write later fails, the worker
		// evicts it so a future event retries.
		e.cache.store(did)
		e.setLast(did)
		e.metrics.setEnqueueQueueDepth(len(e.queue))
	default:
		// Channel full: drop, do NOT cache, and do NOT set last. The DID is
		// still emitting events, so its next event retries once the worker
		// drains — and the unset last means that retry is not short-circuited.
		e.metrics.incEnqueueDropped()
	}
}

func (e *LiveEnqueuer) setLast(did string) {
	d := did
	e.last.Store(&d)
}

// Run drains the candidate queue and performs the idempotent durable enqueue
// for each net-new DID until ctx is cancelled. Returns ctx.Err() on shutdown.
//
// A per-DID durable write failure is logged and the DID is evicted from the
// seen cache (so a later event retries) rather than tearing down ingest: a
// missed pending row is recoverable on the next observation, and a genuinely
// broken pebble would fail the main archive writer's own append path.
func (e *LiveEnqueuer) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case did := <-e.queue:
			e.metrics.setEnqueueQueueDepth(len(e.queue))
			e.process(ctx, did)
		}
	}
}

func (e *LiveEnqueuer) process(ctx context.Context, did string) {
	// A net-new DID that is actively emitting events is, by definition, active;
	// mark the row Active so the retry loop's scanDue picks it up. If the repo
	// turns out to be deactivated/taken-down, the getRepo attempt records it as
	// StatusUnavailable (terminal, non-retryable) — the self-correcting path.
	if _, err := e.store.EnqueueNetNewRepo(ctx, atmos.DID(did), true); err != nil {
		if ctx.Err() != nil {
			return
		}
		// Evict the seen cache FIRST, then clear the burst-absorber, so a later
		// event re-enqueues this DID. Order matters and is concurrency-critical:
		// Observe (firehose goroutine) checks last before the cache. If we
		// cleared last first, a same-DID Observe racing between the clear and the
		// cache eviction would find last==nil, hit the still-present cache entry,
		// and call setLast(did) — repopulating last with the failed DID and
		// re-suppressing the retry forever. Removing the cache entry first means
		// that once last is cleared, Observe can no longer repopulate it from the
		// cache, so the next same-DID event genuinely re-enqueues.
		e.cache.remove(did)
		if last := e.last.Load(); last != nil && *last == did {
			e.last.CompareAndSwap(last, nil)
		}
		e.logger.WarnContext(ctx, "failed to enqueue net-new repo for backfill",
			"did", did,
			"err", err,
		)
	}
}

// QueueLen reports the current depth of the candidate channel. Test-only
// visibility into backpressure.
func (e *LiveEnqueuer) QueueLen() int {
	return len(e.queue)
}

// seenCache is a bounded, sharded LRU set of recently-observed DIDs. Each shard
// is an independent LRU guarded by its own mutex; shard selection hashes the
// DID so a given DID always maps to the same shard.
type seenCache struct {
	shards []*seenShard
	mask   uint64
	seed   maphash.Seed
}

func newSeenCache(totalCap, shardCount int) *seenCache {
	if shardCount < 1 {
		shardCount = 1
	}
	// Round shard count up to a power of two for mask-based selection.
	n := 1
	for n < shardCount {
		n <<= 1
	}
	perShard := max(totalCap/n, 1)
	shards := make([]*seenShard, n)
	for i := range shards {
		shards[i] = newSeenShard(perShard)
	}
	return &seenCache{
		shards: shards,
		mask:   uint64(n - 1),
		seed:   maphash.MakeSeed(),
	}
}

func (c *seenCache) shardFor(did string) *seenShard {
	h := maphash.String(c.seed, did)
	return c.shards[h&c.mask]
}

// seen reports whether did is present, bumping it to most-recently-used when it
// is. A miss does not insert — callers insert via store only after a successful
// enqueue.
func (c *seenCache) seen(did string) bool {
	return c.shardFor(did).seen(did)
}

func (c *seenCache) store(did string) {
	c.shardFor(did).store(did)
}

func (c *seenCache) remove(did string) {
	c.shardFor(did).remove(did)
}

// seenNode is an intrusive doubly-linked-list node in a shard's LRU order.
type seenNode struct {
	did        string
	prev, next *seenNode
}

// seenShard is one LRU shard. head.next is the most-recently-used node;
// tail.prev is the least-recently-used (next to evict). head and tail are
// sentinels so insert/remove need no nil checks on the boundaries.
type seenShard struct {
	mu    sync.Mutex
	cap   int
	items map[string]*seenNode
	head  *seenNode
	tail  *seenNode
}

func newSeenShard(capacity int) *seenShard {
	head := &seenNode{}
	tail := &seenNode{}
	head.next = tail
	tail.prev = head
	return &seenShard{
		cap:   capacity,
		items: make(map[string]*seenNode),
		head:  head,
		tail:  tail,
	}
}

func (s *seenShard) seen(did string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.items[did]
	if !ok {
		return false
	}
	s.moveToFront(n)
	return true
}

func (s *seenShard) store(did string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n, ok := s.items[did]; ok {
		s.moveToFront(n)
		return
	}
	n := &seenNode{did: did}
	s.items[did] = n
	s.insertFront(n)
	if len(s.items) > s.cap {
		s.evictLRU()
	}
}

func (s *seenShard) remove(did string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n, ok := s.items[did]; ok {
		s.unlink(n)
		delete(s.items, did)
	}
}

func (s *seenShard) moveToFront(n *seenNode) {
	s.unlink(n)
	s.insertFront(n)
}

func (s *seenShard) insertFront(n *seenNode) {
	n.prev = s.head
	n.next = s.head.next
	s.head.next.prev = n
	s.head.next = n
}

func (s *seenShard) unlink(n *seenNode) {
	n.prev.next = n.next
	n.next.prev = n.prev
	n.prev = nil
	n.next = nil
}

func (s *seenShard) evictLRU() {
	lru := s.tail.prev
	if lru == s.head {
		return
	}
	s.unlink(lru)
	delete(s.items, lru.did)
}
