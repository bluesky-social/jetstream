package subscribe

import (
	"container/list"
	"sync"

	"github.com/bluesky-social/jetstream/segment"
)

// blockKey identifies one immutable decoded block.
type blockKey struct {
	segIdx     uint64
	checksum   uint64
	blockIdx   uint64
	generation uint64
}

// blockCache is a byte-bounded LRU of decoded blocks shared by all cold-path
// subscriber goroutines. Concurrency-safe. Only immutable blocks (sealed +
// active-flushed) are ever keyed; the pending in-memory block is never cached.
type blockCache struct {
	mu       sync.Mutex
	maxBytes int
	curBytes int
	ll       *list.List // front = most recently used
	items    map[blockKey]*list.Element

	generationBySegment map[uint64]uint64

	// Single-flight: in-flight decodes keyed by blockKey. Each inflight holds
	// the decode result or error, populated by the first goroutine to hit the
	// key, and a completion channel closed when decode finishes.
	inFlight map[blockKey]*inflight
}

type blockCacheItem struct {
	key    blockKey
	events []segment.Event
	bytes  int
}

// inflight tracks one in-progress decode so concurrent getOrDecode calls for
// the same key wait for the first decode rather than redundantly decoding.
type inflight struct {
	done   chan struct{}
	events []segment.Event
	err    error
}

func newBlockCache(maxBytes int) *blockCache {
	if maxBytes <= 0 {
		panic("subscribe: blockCache maxBytes must be > 0")
	}
	return &blockCache{
		maxBytes: maxBytes,
		ll:       list.New(),
		items:    make(map[blockKey]*list.Element),
		inFlight: make(map[blockKey]*inflight),

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

// getOrDecode returns the cached decoded block for key, or runs decode,
// caches the result, and returns it. A decode error is propagated and NOT
// cached. The returned slice is shared and read-only: callers must not
// mutate it (events alias a pinned decompressed buffer).
//
// When multiple goroutines concurrently call getOrDecode for the same key,
// only the first runs decode; the others wait for completion and share the
// result. This single-flight ensures a slow zstd decode doesn't serialize
// all readers while guaranteeing exactly-once decode per key.
func (c *blockCache) getOrDecode(key blockKey, decode func() ([]segment.Event, error)) ([]segment.Event, error) {
	c.mu.Lock()
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		evs := itemOf(el).events
		c.mu.Unlock()
		return evs, nil
	}

	// Check if another goroutine is already decoding this key.
	if inf, ok := c.inFlight[key]; ok {
		c.mu.Unlock()
		<-inf.done // wait for the first goroutine to finish decode
		return inf.events, inf.err
	}

	// We are the first goroutine for this key: create inflight, unlock, decode.
	inf := &inflight{done: make(chan struct{})}
	c.inFlight[key] = inf
	c.mu.Unlock()

	// Decode outside the main lock so slow zstd doesn't block concurrent reads.
	evs, err := decode()
	inf.events = evs
	inf.err = err
	close(inf.done)

	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inFlight, key)

	if err != nil {
		// Decode error is not cached; remove inflight so next call retries.
		return nil, err
	}

	if c.generationBySegment[key.segIdx] != key.generation {
		return evs, nil
	}

	// Cache the result if not already present (a concurrent decode could have
	// raced and inserted; last writer wins and both callers get valid blocks).
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		return itemOf(el).events, nil
	}
	item := &blockCacheItem{key: key, events: evs, bytes: blockBytes(evs)}
	el := c.ll.PushFront(item)
	c.items[key] = el
	c.curBytes += item.bytes
	c.evictLocked()
	return evs, nil
}

func (c *blockCache) evictLocked() {
	for c.curBytes > c.maxBytes && c.ll.Len() > 0 {
		el := c.ll.Back()
		item := itemOf(el)
		c.ll.Remove(el)
		delete(c.items, item.key)
		c.curBytes -= item.bytes
	}
}

// itemOf extracts the typed cache item from a list element. The list only
// ever holds *blockCacheItem, so the assertion cannot fail; the comma-ok
// form keeps errcheck satisfied without an unchecked panic path.
func itemOf(el *list.Element) *blockCacheItem {
	item, _ := el.Value.(*blockCacheItem)
	return item
}

func (c *blockCache) bytes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curBytes
}

// blockBytes estimates the decoded footprint of a block. Payload plus the
// string fields dominate; a fixed per-event overhead bounds tiny events.
func blockBytes(evs []segment.Event) int {
	total := 0
	for i := range evs {
		e := &evs[i]
		total += len(e.Payload) + len(e.DID) + len(e.Collection) +
			len(e.Rkey) + len(e.Rev) + entryFixedOverhead
	}
	return total
}
