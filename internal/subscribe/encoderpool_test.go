package subscribe

import (
	"fmt"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// TestCompressFrame_PooledOutputMatchesReferenceEncoder pins the v1 wire
// contract across the #295 pooling change: a pooled encoder must produce
// byte-identical frames to a reference encoder built with the exact
// jetstream-legacy configuration (WithEncoderDict + 128 KiB window +
// concurrency 1 + default level). Pooling may change how many encoder
// instances exist — never the bytes any one of them produces.
func TestCompressFrame_PooledOutputMatchesReferenceEncoder(t *testing.T) {
	t.Parallel()

	ref, err := zstd.NewWriter(nil,
		zstd.WithEncoderDict(zstdDictionary),
		zstd.WithWindowSize(1<<17),
		zstd.WithEncoderConcurrency(1))
	require.NoError(t, err)
	defer func() { _ = ref.Close() }()

	refV2, err := zstd.NewWriter(nil,
		zstd.WithEncoderDict(zstdDictionaryV2),
		zstd.WithWindowSize(1<<17),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderLevel(zstd.SpeedFastest))
	require.NoError(t, err)
	defer func() { _ = refV2.Close() }()

	for i := range 32 {
		msg := fmt.Appendf(nil,
			`{"did":"did:plc:example%d","time_us":%d,"kind":"commit","commit":{"rev":"aaa","operation":"create","collection":"app.bsky.feed.post","rkey":"r%d"}}`,
			i, 1_700_000_000_000_000+i, i)
		require.Equal(t, ref.EncodeAll(msg, nil), compressFrame(msg),
			"pooled v1 frame must be byte-identical to the reference encoder's")
		require.Equal(t, refV2.EncodeAll(msg, nil), compressFrameV2(msg),
			"pooled v2 frame must be byte-identical to the reference encoder's")
	}
}

// TestEncoderPool_ConcurrentUseIsCorrect drives many goroutines through a
// small pool (forcing reuse and get/put contention) and verifies every
// frame round-trips. Run with -race this is the pool's data-race check.
func TestEncoderPool_ConcurrentUseIsCorrect(t *testing.T) {
	t.Parallel()

	pool := newEncoderPool(4, mustNewZstdEncoder)
	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(zstdDictionary))
	require.NoError(t, err)
	defer dec.Close()

	const goroutines, perG = 16, 200
	frames := make([][][]byte, goroutines)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			out := make([][]byte, perG)
			for i := range perG {
				out[i] = pool.encodeAll(fmt.Appendf(nil, `{"g":%d,"i":%d,"pad":"aaaaaaaaaaaaaaaaaaaaaaaa"}`, g, i))
			}
			frames[g] = out
		})
	}
	wg.Wait()

	for g := range goroutines {
		for i := range perG {
			got, derr := dec.DecodeAll(frames[g][i], nil)
			require.NoError(t, derr)
			require.Equal(t, fmt.Sprintf(`{"g":%d,"i":%d,"pad":"aaaaaaaaaaaaaaaaaaaaaaaa"}`, g, i), string(got))
		}
	}
}

// TestEncoderPool_NeverExceedsLimit floods a tiny pool from many goroutines
// and asserts the created-encoder count stays at the cap — the memory bound
// the limit exists for.
func TestEncoderPool_NeverExceedsLimit(t *testing.T) {
	t.Parallel()

	pool := newEncoderPool(2, mustNewZstdEncoder)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for range 50 {
				_ = pool.encodeAll([]byte(`{"x":"yyyyyyyyyyyyyyyy"}`))
			}
		})
	}
	wg.Wait()
	require.LessOrEqual(t, pool.created.Load(), int64(2))
}

// TestEncoderPool_WarmFillsToCapacity verifies warm() materializes every
// encoder up front (the synctest-bubble safety property WarmEncoder
// provides) and stays idempotent.
func TestEncoderPool_WarmFillsToCapacity(t *testing.T) {
	t.Parallel()

	pool := newEncoderPool(3, mustNewZstdEncoder)
	pool.warm()
	require.Equal(t, int64(3), pool.created.Load())
	require.Len(t, pool.free, 3, "all warmed encoders must be on the free list")

	pool.warm() // idempotent
	require.Equal(t, int64(3), pool.created.Load())
	require.Len(t, pool.free, 3)
}
