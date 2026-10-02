// Package storetest is the metastore.Store contract suite. Every
// implementation runs it, so they cannot drift apart on the semantics the
// ingest code depends on: ordered batches, [lower, upper) bytewise iteration,
// copy-on-read, and atomic commit failure.
package storetest

import (
	"context"
	"errors"
	"testing"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/stretchr/testify/require"
)

// Run runs the contract suite. newStore must return an empty store that is
// cleaned up with t.
func Run(t *testing.T, newStore func(t *testing.T) metastore.Store) {
	t.Helper()
	for _, tc := range []struct {
		name string
		fn   func(t *testing.T, s metastore.Store)
	}{
		{"GetSetDelete", testGetSetDelete},
		{"EmptyValue", testEmptyValue},
		{"GetReturnsCopy", testGetReturnsCopy},
		{"BatchInvisibleUntilCommit", testBatchInvisibleUntilCommit},
		{"BatchOrderedSameKey", testBatchOrderedSameKey},
		{"BatchDeleteRangeEndsRun", testBatchDeleteRangeEndsRun},
		{"BatchCopiesArguments", testBatchCopiesArguments},
		{"EmptyBatchCommit", testEmptyBatchCommit},
		{"BatchLen", testBatchLen},
		{"BatchSingleUse", testBatchSingleUse},
		{"IterBounds", testIterBounds},
		{"IterUnbounded", testIterUnbounded},
		{"IterEmptyRange", testIterEmptyRange},
		{"CommitErrorIsAtomic", testCommitErrorIsAtomic},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newStore(t))
		})
	}
}

// KV is one key/value pair.
type KV struct {
	Key, Value []byte
}

// Scan returns every pair in [lower, upper), copied.
func Scan(t testing.TB, s metastore.Store, lower, upper []byte) []KV {
	t.Helper()
	it, err := s.NewIter(context.Background(), lower, upper)
	require.NoError(t, err)
	var out []KV
	for it.Next() {
		out = append(out, KV{Key: append([]byte{}, it.Key()...), Value: append([]byte{}, it.Value()...)})
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	return out
}

func requireAbsent(t *testing.T, s metastore.Store, key string) {
	t.Helper()
	_, err := s.Get(context.Background(), []byte(key))
	require.ErrorIs(t, err, metastore.ErrNotFound, "key %q", key)
}

func requireValue(t *testing.T, s metastore.Store, key, want string) {
	t.Helper()
	got, err := s.Get(context.Background(), []byte(key))
	require.NoError(t, err, "key %q", key)
	require.Equal(t, want, string(got), "key %q", key)
}

func keys(kvs []KV) []string {
	out := make([]string, len(kvs))
	for i, kv := range kvs {
		out[i] = string(kv.Key)
	}
	return out
}

func testGetSetDelete(t *testing.T, s metastore.Store) {
	ctx := context.Background()
	requireAbsent(t, s, "a")
	require.NoError(t, s.Set(ctx, []byte("a"), []byte("1")))
	requireValue(t, s, "a", "1")
	require.NoError(t, s.Set(ctx, []byte("a"), []byte("2")))
	requireValue(t, s, "a", "2")
	require.NoError(t, s.Delete(ctx, []byte("a")))
	requireAbsent(t, s, "a")
	// Deleting a missing key is not an error.
	require.NoError(t, s.Delete(ctx, []byte("never")))
}

func testEmptyValue(t *testing.T, s metastore.Store) {
	ctx := context.Background()
	require.NoError(t, s.Set(ctx, []byte("e"), nil))
	got, err := s.Get(ctx, []byte("e"))
	require.NoError(t, err, "an empty value is present, not absent")
	require.Empty(t, got)
	require.Len(t, Scan(t, s, nil, nil), 1)
}

func testGetReturnsCopy(t *testing.T, s metastore.Store) {
	ctx := context.Background()
	require.NoError(t, s.Set(ctx, []byte("k"), []byte("value")))
	got, err := s.Get(ctx, []byte("k"))
	require.NoError(t, err)
	got[0] = 'X'
	requireValue(t, s, "k", "value")
}

func testBatchInvisibleUntilCommit(t *testing.T, s metastore.Store) {
	ctx := context.Background()
	b := s.NewBatch()
	b.Set([]byte("k"), []byte("v"))
	requireAbsent(t, s, "k")
	require.Empty(t, Scan(t, s, nil, nil))
	require.NoError(t, b.Commit(ctx))
	requireValue(t, s, "k", "v")
}

func testBatchOrderedSameKey(t *testing.T, s metastore.Store) {
	ctx := context.Background()
	b := s.NewBatch()
	b.Set([]byte("k"), []byte("1"))
	b.Delete([]byte("k"))
	b.Set([]byte("k"), []byte("3"))
	b.Set([]byte("gone"), []byte("x"))
	b.Delete([]byte("gone"))
	require.NoError(t, b.Commit(ctx))
	requireValue(t, s, "k", "3")
	requireAbsent(t, s, "gone")
}

// A DeleteRange removes keys staged before it and existing keys, but not keys
// staged after it. This is the "DeleteRange ends a run" rule the PG impl
// must preserve when it coalesces consecutive Sets.
func testBatchDeleteRangeEndsRun(t *testing.T, s metastore.Store) {
	ctx := context.Background()
	require.NoError(t, s.Set(ctx, []byte("r/old"), []byte("o")))
	require.NoError(t, s.Set(ctx, []byte("r0"), []byte("outside")))

	b := s.NewBatch()
	b.Set([]byte("r/a"), []byte("a"))
	b.Set([]byte("r/b"), []byte("b"))
	b.DeleteRange([]byte("r/"), []byte("r0"))
	b.Set([]byte("r/b"), []byte("b2"))
	require.NoError(t, b.Commit(ctx))

	requireAbsent(t, s, "r/old")
	requireAbsent(t, s, "r/a")
	requireValue(t, s, "r/b", "b2")
	requireValue(t, s, "r0", "outside")
}

func testBatchCopiesArguments(t *testing.T, s metastore.Store) {
	ctx := context.Background()
	k := []byte("key")
	v := []byte("val")
	b := s.NewBatch()
	b.Set(k, v)
	k[0], v[0] = 'X', 'X'
	require.NoError(t, b.Commit(ctx))
	requireValue(t, s, "key", "val")
	requireAbsent(t, s, "Xey")
}

func testEmptyBatchCommit(t *testing.T, s metastore.Store) {
	ctx := context.Background()
	require.NoError(t, s.Set(ctx, []byte("k"), []byte("v")))
	b := s.NewBatch()
	require.Equal(t, 0, b.Len())
	require.NoError(t, b.Commit(ctx))
	requireValue(t, s, "k", "v")
}

func testBatchLen(t *testing.T, s metastore.Store) {
	b := s.NewBatch()
	b.Set([]byte("a"), []byte("1"))
	b.Delete([]byte("b"))
	b.DeleteRange([]byte("c"), []byte("d"))
	b.Set([]byte("a"), []byte("2"))
	require.Equal(t, 4, b.Len())
	require.NoError(t, b.Commit(context.Background()))
}

func testBatchSingleUse(t *testing.T, s metastore.Store) {
	ctx := context.Background()
	b := s.NewBatch()
	b.Set([]byte("a"), []byte("1"))
	require.NoError(t, b.Commit(ctx))
	require.ErrorIs(t, b.Commit(ctx), metastore.ErrBatchCommitted)
}

// Keys containing 0x00 and 0xff sort bytewise, which is what both Pebble's
// default comparer and PostgreSQL's bytea comparison give.
func testIterBounds(t *testing.T, s metastore.Store) {
	ctx := context.Background()
	all := []string{
		"a", "a\x00", "a\x00\x00", "a\x01", "ab", "a\xff", "a\xff\xff", "b", "\xff", "\xff\xff",
	}
	// Insert out of order to catch implementations that rely on insertion
	// order.
	for i := len(all) - 1; i >= 0; i-- {
		require.NoError(t, s.Set(ctx, []byte(all[i]), []byte{byte(i)}))
	}

	require.Equal(t, all, keys(Scan(t, s, nil, nil)))
	require.Equal(t, all[:7], keys(Scan(t, s, []byte("a"), []byte("b"))))
	// Lower is inclusive, upper exclusive.
	require.Equal(t, []string{"a\x00", "a\x00\x00", "a\x01", "ab"},
		keys(Scan(t, s, []byte("a\x00"), []byte("a\xff"))))
	require.Equal(t, []string{"a\xff", "a\xff\xff", "b"},
		keys(Scan(t, s, []byte("a\xff"), []byte("b\x00"))))
	require.Equal(t, all[1:7], keys(Scan(t, s, []byte("a\x00"), metastore.PrefixUpperBound([]byte("a")))))
	// An all-0xff prefix has no upper bound; nil means unbounded.
	require.Nil(t, metastore.PrefixUpperBound([]byte("\xff")))
	require.Equal(t, []string{"\xff", "\xff\xff"}, keys(Scan(t, s, []byte("\xff"), nil)))

	// Values come back paired with their keys.
	for _, kv := range Scan(t, s, nil, nil) {
		want := -1
		for i, k := range all {
			if k == string(kv.Key) {
				want = i
			}
		}
		require.Equal(t, []byte{byte(want)}, kv.Value)
	}
}

func testIterUnbounded(t *testing.T, s metastore.Store) {
	ctx := context.Background()
	for _, k := range []string{"x", "\x00", "m"} {
		require.NoError(t, s.Set(ctx, []byte(k), []byte("v")))
	}
	require.Equal(t, []string{"\x00", "m", "x"}, keys(Scan(t, s, nil, nil)))
	require.Equal(t, []string{"m", "x"}, keys(Scan(t, s, []byte("m"), nil)))
	require.Equal(t, []string{"\x00"}, keys(Scan(t, s, nil, []byte("m"))))
}

func testIterEmptyRange(t *testing.T, s metastore.Store) {
	ctx := context.Background()
	require.NoError(t, s.Set(ctx, []byte("k"), []byte("v")))
	require.Empty(t, Scan(t, s, []byte("l"), []byte("z")))
	require.Empty(t, Scan(t, s, []byte("a"), []byte("k")))

	// Next keeps returning false after exhaustion.
	it, err := s.NewIter(ctx, []byte("l"), []byte("z"))
	require.NoError(t, err)
	require.False(t, it.Next())
	require.False(t, it.Next())
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
}

var errContractInjected = errors.New("storetest: injected commit failure")

// A failed commit applies nothing. The implementation's own commit paths
// rarely fail on demand, so the failure is injected with the WithFaults
// wrapper, which is what every fault-injection test in the tree uses.
func testCommitErrorIsAtomic(t *testing.T, s metastore.Store) {
	ctx := context.Background()
	require.NoError(t, s.Set(ctx, []byte("keep"), []byte("v")))
	fault := &metastore.KeyPrefixFault{Prefix: []byte("f/"), Ordinal: 1, Err: errContractInjected}
	fs := metastore.WithFaults(s, fault)

	b := fs.NewBatch()
	b.Set([]byte("a"), []byte("1"))
	b.Delete([]byte("keep"))
	b.Set([]byte("f/x"), []byte("2"))
	require.ErrorIs(t, b.Commit(ctx), errContractInjected)
	requireAbsent(t, s, "a")
	requireAbsent(t, s, "f/x")
	requireValue(t, s, "keep", "v")

	require.NoError(t, fs.Set(ctx, []byte("f/y"), []byte("v")), "fault is one-shot")
	requireValue(t, s, "f/y", "v")
}
