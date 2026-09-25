// Package blobtest is the contract suite every objstore.Blob implementation
// must pass. memblob runs it in `just`; the S3 Blob runs it against real
// object stores behind `just test-storage`, so the fake cannot drift from the
// backends it stands in for.
package blobtest

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/objstore"
)

// Config describes the Blob under test.
type Config struct {
	// New returns a Blob to test. It is called once per subtest. The Blob
	// need not be empty (a real bucket rarely is): the suite writes only
	// fresh random keys.
	New func(t *testing.T) objstore.Blob

	// NewWrongBytes returns a Blob whose first GetKey and first GetKeyRange
	// of key return wrong bytes without error, as a flaky network or disk
	// would. Nil skips the wrong-bytes cases, for a backend with no way to
	// inject them.
	NewWrongBytes func(t *testing.T, key string) objstore.Blob
}

// Run runs the contract suite. Subtests run in parallel.
func Run(t *testing.T, cfg Config) {
	t.Helper()
	cases := []struct {
		name string
		fn   func(t *testing.T, cfg Config)
	}{
		{"RoundTrip", testRoundTrip},
		{"EmptyObject", testEmptyObject},
		{"RangeReads", testRangeReads},
		{"InvalidRange", testInvalidRange},
		{"GetMissing", testGetMissing},
		{"DeleteMissing", testDeleteMissing},
		{"DeleteThenGet", testDeleteThenGet},
		{"CanceledContext", testCanceledContext},
		{"Concurrent", testConcurrent},
		{"WrongBytesIsStoreLayer", testWrongBytesIsStoreLayer},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.fn(t, cfg)
		})
	}
}

// newKey returns a fresh key in the design's §7.1 shape, so a real backend
// sees realistic keys and parallel subtests never collide.
func newKey(t *testing.T) string {
	t.Helper()
	var b [16]byte
	_, err := rand.Read(b[:])
	require.NoError(t, err)
	return "blobtest/objects/" + hex.EncodeToString(b[:])
}

func testData(n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i*7 + 3)
	}
	return data
}

// testRoundTrip also pins the copy contract: neither the caller's input nor
// a returned slice aliases what the Blob stores.
func testRoundTrip(t *testing.T, cfg Config) {
	ctx := t.Context()
	b := cfg.New(t)
	key := newKey(t)
	want := testData(1000)

	in := bytes.Clone(want)
	require.NoError(t, b.PutKey(ctx, key, in))
	in[0] ^= 0xff

	got, err := b.GetKey(ctx, key)
	require.NoError(t, err)
	require.Equal(t, want, got)
	got[1] ^= 0xff

	got, err = b.GetKey(ctx, key)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// testEmptyObject checks that a zero-length object exists rather than being
// confused with a missing one. The catalog never records one (byte_length >
// 0), but S3 allows them and the fake must agree.
func testEmptyObject(t *testing.T, cfg Config) {
	ctx := t.Context()
	b := cfg.New(t)
	key := newKey(t)
	require.NoError(t, b.PutKey(ctx, key, nil))
	got, err := b.GetKey(ctx, key)
	require.NoError(t, err)
	require.Empty(t, got)

	_, err = b.GetKeyRange(ctx, key, 0, 1)
	require.ErrorIs(t, err, objstore.ErrInvalidRange)
}

func testRangeReads(t *testing.T, cfg Config) {
	ctx := t.Context()
	b := cfg.New(t)
	key := newKey(t)
	data := testData(100)
	require.NoError(t, b.PutKey(ctx, key, data))

	cases := []struct {
		name   string
		off, n int64
	}{
		{"whole", 0, 100},
		{"prefix", 0, 10},
		{"middle", 40, 20},
		{"suffix", 90, 10},
		{"last byte", 99, 1},
		{"first byte", 0, 1},
		{"past end truncates", 90, 50},
		{"far past end truncates", 0, 1 << 40},
	}
	for _, c := range cases {
		got, err := b.GetKeyRange(ctx, key, c.off, c.n)
		require.NoError(t, err, c.name)
		wantLen, err := objstore.RangeLen(int64(len(data)), c.off, c.n)
		require.NoError(t, err, c.name)
		require.Equal(t, data[c.off:c.off+wantLen], got, c.name)
	}
}

// testInvalidRange pins S3's 416 behavior: a range that starts at or past
// the end is an error, not an empty read.
func testInvalidRange(t *testing.T, cfg Config) {
	ctx := t.Context()
	b := cfg.New(t)
	key := newKey(t)
	require.NoError(t, b.PutKey(ctx, key, testData(100)))

	cases := []struct {
		name   string
		off, n int64
	}{
		{"offset at end", 100, 1},
		{"offset past end", 500, 1},
		{"negative offset", -1, 10},
		{"zero length", 10, 0},
		{"negative length", 10, -1},
	}
	for _, c := range cases {
		_, err := b.GetKeyRange(ctx, key, c.off, c.n)
		require.ErrorIs(t, err, objstore.ErrInvalidRange, c.name)
	}
}

func testGetMissing(t *testing.T, cfg Config) {
	ctx := t.Context()
	b := cfg.New(t)
	key := newKey(t)
	_, err := b.GetKey(ctx, key)
	require.ErrorIs(t, err, objstore.ErrNotFound)
	_, err = b.GetKeyRange(ctx, key, 0, 10)
	require.ErrorIs(t, err, objstore.ErrNotFound)
}

// testDeleteMissing matters because GC retries deletes whose first attempt
// had an unknown result.
func testDeleteMissing(t *testing.T, cfg Config) {
	b := cfg.New(t)
	require.NoError(t, b.DeleteKey(t.Context(), newKey(t)))
}

func testDeleteThenGet(t *testing.T, cfg Config) {
	ctx := t.Context()
	b := cfg.New(t)
	key := newKey(t)
	require.NoError(t, b.PutKey(ctx, key, testData(10)))
	require.NoError(t, b.DeleteKey(ctx, key))
	_, err := b.GetKey(ctx, key)
	require.ErrorIs(t, err, objstore.ErrNotFound)
	require.NoError(t, b.DeleteKey(ctx, key), "second delete")
}

func testCanceledContext(t *testing.T, cfg Config) {
	b := cfg.New(t)
	key := newKey(t)
	require.NoError(t, b.PutKey(t.Context(), key, testData(10)))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, b.PutKey(ctx, newKey(t), testData(10)), context.Canceled)
	_, err := b.GetKey(ctx, key)
	require.ErrorIs(t, err, context.Canceled)
	_, err = b.GetKeyRange(ctx, key, 0, 1)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, b.DeleteKey(ctx, key), context.Canceled)
}

func testConcurrent(t *testing.T, cfg Config) {
	ctx := t.Context()
	b := cfg.New(t)
	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := range workers {
		key := newKey(t)
		wg.Go(func() {
			data := testData(100 + i)
			if err := b.PutKey(ctx, key, data); err != nil {
				errs <- err
				return
			}
			got, err := b.GetKey(ctx, key)
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, data) {
				errs <- fmt.Errorf("key %s: read back different bytes", key)
				return
			}
			if err := b.DeleteKey(ctx, key); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

// testWrongBytesIsStoreLayer pins where corruption is caught. A Blob hands
// back whatever the backend returned without complaint; only the Store's
// objstore.Verify against the catalog's length and SHA-256 rejects it.
func testWrongBytesIsStoreLayer(t *testing.T, cfg Config) {
	if cfg.NewWrongBytes == nil {
		t.Skip("backend cannot inject wrong bytes")
	}
	ctx := t.Context()
	key := newKey(t)
	b := cfg.NewWrongBytes(t, key)
	data := testData(100)
	sum := sha256.Sum256(data)
	require.NoError(t, b.PutKey(ctx, key, data))

	got, err := b.GetKey(ctx, key)
	require.NoError(t, err, "a Blob does not verify bytes")
	require.NotEqual(t, data, got)
	require.ErrorIs(t, objstore.Verify(got, int64(len(data)), sum), objstore.ErrCorrupt)

	got, err = b.GetKey(ctx, key)
	require.NoError(t, err)
	require.NoError(t, objstore.Verify(got, int64(len(data)), sum), "wrong bytes were transient")

	// A range read keeps its length, so the Store's length-only check passes
	// and the zstd frame checksum is what catches it downstream.
	got, err = b.GetKeyRange(ctx, key, 10, 20)
	require.NoError(t, err, "a Blob does not verify bytes")
	require.Len(t, got, 20)
	require.NotEqual(t, data[10:30], got)
}
