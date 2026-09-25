package memblob_test

import (
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/objstore/blobtest"
	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
)

var errInjected = errors.New("injected blob fault")

func TestContract(t *testing.T) {
	t.Parallel()
	blobtest.Run(t, blobtest.Config{
		New: func(*testing.T) objstore.Blob { return memblob.New() },
		NewWrongBytes: func(_ *testing.T, key string) objstore.Blob {
			return memblob.New(memblob.WithFaultInjector(memblob.Faults{
				&memblob.KeyPrefixFault{Prefix: key, Op: memblob.OpGet, Ordinal: 1, Kind: memblob.FaultWrongBytes},
				&memblob.KeyPrefixFault{Prefix: key, Op: memblob.OpGetRange, Ordinal: 1, Kind: memblob.FaultWrongBytes},
			}))
		},
	})
}

// TestKeyPrefixFaultOrdinal checks that only the Ordinal-th matching op
// fails, that non-matching keys and ops do not advance the count, and that
// the fault is one-shot.
func TestKeyPrefixFaultOrdinal(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	b := memblob.New(memblob.WithFaultInjector(&memblob.KeyPrefixFault{
		Prefix: "a/", Op: memblob.OpPut, Ordinal: 2, Err: errInjected,
	}))

	require.NoError(t, b.PutKey(ctx, "b/1", []byte("x")), "other prefix")
	require.NoError(t, b.DeleteKey(ctx, "a/0"), "other op")
	require.NoError(t, b.PutKey(ctx, "a/1", []byte("x")), "first match")
	require.ErrorIs(t, b.PutKey(ctx, "a/2", []byte("x")), errInjected, "second match")
	require.NoError(t, b.PutKey(ctx, "a/3", []byte("x")), "third match")

	// The failed put stored nothing.
	require.Equal(t, []string{"a/1", "a/3", "b/1"}, b.Keys())
}

// TestFaultDefaults checks that an empty Kind means FaultError and that a
// nil Err still produces an error.
func TestFaultDefaults(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	b := memblob.New(memblob.WithFaultInjector(&memblob.KeyPrefixFault{Ordinal: 1}))
	err := b.PutKey(ctx, "k", []byte("x"))
	require.Error(t, err)
	require.Empty(t, b.Keys())
}

// TestDropPut models an acknowledged PUT that never landed: the upload
// protocol's read-back (§7.3 step 5) is what notices.
func TestDropPut(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	b := memblob.New(memblob.WithFaultInjector(&memblob.KeyPrefixFault{
		Op: memblob.OpPut, Ordinal: 1, Kind: memblob.FaultDropPut,
	}))
	require.NoError(t, b.PutKey(ctx, "k", []byte("x")))
	_, err := b.GetKey(ctx, "k")
	require.ErrorIs(t, err, objstore.ErrNotFound)
}

// TestErrorAfter models a lost response: the caller sees an error but the
// operation took effect.
func TestErrorAfter(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	b := memblob.New(memblob.WithFaultInjector(memblob.Faults{
		&memblob.KeyPrefixFault{Op: memblob.OpPut, Ordinal: 1, Kind: memblob.FaultErrorAfter, Err: errInjected},
		&memblob.KeyPrefixFault{Op: memblob.OpDelete, Ordinal: 1, Kind: memblob.FaultErrorAfter, Err: errInjected},
	}))
	require.ErrorIs(t, b.PutKey(ctx, "k", []byte("x")), errInjected)
	require.Equal(t, []string{"k"}, b.Keys())
	require.ErrorIs(t, b.DeleteKey(ctx, "k"), errInjected)
	require.Empty(t, b.Keys())
}

// TestWrongBytesOnPut models a corrupted write: unlike a bad read, every
// later read sees the damage, and only Verify notices.
func TestWrongBytesOnPut(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	b := memblob.New(memblob.WithFaultInjector(&memblob.KeyPrefixFault{
		Op: memblob.OpPut, Ordinal: 1, Kind: memblob.FaultWrongBytes,
	}))
	data := []byte("some object bytes")
	sum := sha256.Sum256(data)
	require.NoError(t, b.PutKey(ctx, "k", data))
	require.Equal(t, []byte("some object bytes"), data, "caller's slice untouched")
	for range 2 {
		got, err := b.GetKey(ctx, "k")
		require.NoError(t, err)
		require.ErrorIs(t, objstore.Verify(got, int64(len(data)), sum), objstore.ErrCorrupt)
	}
}

// TestInapplicableFault checks that a fault kind with no meaning for the op
// fails loudly instead of letting the test pass without its fault firing.
func TestInapplicableFault(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	b := memblob.New(memblob.WithFaultInjector(&memblob.KeyPrefixFault{
		Op: memblob.OpGet, Ordinal: 1, Kind: memblob.FaultDropPut,
	}))
	require.NoError(t, b.PutKey(ctx, "k", []byte("x")))
	_, err := b.GetKey(ctx, "k")
	require.ErrorContains(t, err, "does not apply")
}

// TestFaultsCountIndependently checks that every injector in a Faults list
// sees every op, so a later injector's ordinal is not skewed by an earlier
// one firing.
func TestFaultsCountIndependently(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	errFirst, errSecond := errors.New("first"), errors.New("second")
	b := memblob.New(memblob.WithFaultInjector(memblob.Faults{
		&memblob.KeyPrefixFault{Op: memblob.OpPut, Ordinal: 1, Err: errFirst},
		&memblob.KeyPrefixFault{Op: memblob.OpPut, Ordinal: 2, Err: errSecond},
	}))
	require.ErrorIs(t, b.PutKey(ctx, "a", []byte("x")), errFirst)
	require.ErrorIs(t, b.PutKey(ctx, "b", []byte("x")), errSecond)
	require.NoError(t, b.PutKey(ctx, "c", []byte("x")))
}
