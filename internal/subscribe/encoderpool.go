package subscribe

import (
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
)

// encoderPool bounds a free list of identically configured zstd encoders. A
// single encoder with concurrency 1 serializes EncodeAll calls (~37k
// messages/s in #295). Separate encoders permit concurrent compression
// without changing frame encoding.
//
// The pool cannot use sync.Pool: encoders lazily create channels that bind to
// a testing/synctest bubble. WarmEncoder creates every encoder outside the
// bubble; GC eviction would recreate them inside it and cause cross-bubble
// channel failures.
//
// Each encoder allocates roughly 2 MB on its first EncodeAll. Production
// creates them on demand up to limit; WarmEncoder eagerly creates them for
// tests.
type encoderPool struct {
	free    chan *zstd.Encoder
	created atomic.Int64
	limit   int64
	build   func() *zstd.Encoder
}

// newEncoderPool returns a pool that lazily creates up to limit encoders
// via build. build must return a ready encoder or panic (encoder
// construction failures here are build/programmer errors, not runtime
// input — see mustNewZstdEncoder).
func newEncoderPool(limit int, build func() *zstd.Encoder) *encoderPool {
	if limit <= 0 {
		panic("subscribe: encoderPool limit must be > 0")
	}
	return &encoderPool{
		free:  make(chan *zstd.Encoder, limit),
		limit: int64(limit),
		build: build,
	}
}

// get returns an idle encoder, creating one if the pool is not yet at its
// limit. When all limit encoders are busy it blocks until one is returned;
// EncodeAll calls are microsecond-scale and every caller returns its
// encoder via defer, so the wait is short and cannot deadlock.
func (p *encoderPool) get() *zstd.Encoder {
	select {
	case enc := <-p.free:
		return enc
	default:
	}
	if p.created.Add(1) <= p.limit {
		return p.build()
	}
	p.created.Add(-1)
	return <-p.free
}

// put returns an encoder to the free list.
func (p *encoderPool) put(enc *zstd.Encoder) {
	select {
	case p.free <- enc:
	default:
		// Unreachable by construction: at most limit encoders exist and
		// the channel has capacity for all of them. Dropping is still the
		// safe fallback (the encoder is simply collected).
	}
}

// encodeAll compresses src as a single zstd frame using a pooled encoder.
// The result is a fresh slice (EncodeAll appends to a nil dst), safe to
// hand to a websocket write without aliasing src.
func (p *encoderPool) encodeAll(src []byte) []byte {
	enc := p.get()
	defer p.put(enc)
	return enc.EncodeAll(src, nil)
}

// warm eagerly creates and first-uses every encoder up to the pool limit.
// Test support for testing/synctest bubbles: an encoder first used inside
// a bubble binds its lazily-created internal channel to that bubble, and a
// later out-of-bubble EncodeAll fatals the process. Pre-creating (and
// EncodeAll-ing once) every encoder outside the bubble means in-bubble
// callers only ever draw bubble-safe encoders from the free list. Cheap
// relative to a test process; idempotent; safe concurrently with get/put.
func (p *encoderPool) warm() {
	for p.created.Add(1) <= p.limit {
		enc := p.build()
		_ = enc.EncodeAll(nil, nil)
		p.put(enc)
	}
	p.created.Add(-1)
}
