package subscribe

import (
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
)

// encoderPool is a bounded free list of identically-configured zstd
// encoders. It exists because a single shared *zstd.Encoder built with
// WithEncoderConcurrency(1) serializes every EncodeAll in the process
// behind one encoder state — a measured ~37k msgs/s ceiling regardless of
// cores (#295). A pool of independent encoders gives compression one lane
// per concurrent caller, bounded by limit, while each individual encoder
// keeps the exact per-instance configuration (dictionary, window,
// concurrency 1, level) so the frames it produces are byte-identical to
// the single-encoder era.
//
// Deliberately NOT a sync.Pool: klauspost encoders create an internal
// channel lazily on first EncodeAll, and a channel created inside a
// testing/synctest bubble fatals the process when used from outside it
// (see WarmEncoder). A free list lets WarmEncoder pre-create every
// encoder outside any bubble, so in-bubble compressions only ever reuse
// bubble-safe encoders; sync.Pool's GC-driven eviction would defeat that
// guarantee by dropping warmed encoders and lazily rebuilding them
// in-bubble.
//
// Memory: each encoder state (dictionary match tables + window) is on the
// order of ~2 MB, allocated lazily at its first EncodeAll. Production
// creates encoders on demand up to limit; only WarmEncoder (test support)
// eagerly materializes all of them.
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
