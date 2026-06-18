package live

import (
	"encoding/binary"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

// FuzzLoadUpstreamCursor pins panic-freedom on arbitrary bytes:
// LoadUpstreamCursor must always return either a non-negative int64
// or an error, never panic and never produce a negative value (atmos
// would silently treat a negative cursor as "no cursor → live tail",
// which would skip historical events on resume).
func FuzzLoadUpstreamCursor(f *testing.F) {
	// Seed corpus: the well-formed v1 shape, every existing rejection
	// case, and a few off-by-one length variants. Seeds run as ordinary
	// unit tests when -fuzz is not set, so this corpus also acts as a
	// no-op regression net in CI.
	f.Add([]byte{})
	f.Add([]byte{0x01})
	f.Add([]byte{0x01, 0x02, 0x03})

	var ok [9]byte
	ok[0] = 0x01
	binary.LittleEndian.PutUint64(ok[1:], 12345)
	f.Add(ok[:])

	var negative [9]byte
	negative[0] = 0x01
	for i := 1; i < 9; i++ {
		negative[i] = 0xff
	}
	f.Add(negative[:])

	var unknownVer [9]byte
	unknownVer[0] = 0xFF
	f.Add(unknownVer[:])

	short := make([]byte, 8)
	short[0] = 0x01
	f.Add(short)

	long := make([]byte, 10)
	long[0] = 0x01
	f.Add(long)

	f.Fuzz(func(t *testing.T, raw []byte) {
		// Build an in-memory test store, inject the raw bytes, and
		// drive LoadUpstreamCursor. We bypass SaveUpstreamCursor on
		// purpose so we can exercise the decode-rejection paths.
		st, err := store.Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer func() { _ = st.Close() }()

		require.NoError(t, st.Set([]byte("relay/cursor"), raw, store.SyncWrites))
		cur, err := LoadUpstreamCursor(st, "relay/cursor")
		if err != nil {
			return
		}
		if cur < 0 {
			t.Fatalf("LoadUpstreamCursor returned negative cursor %d (input=%x)", cur, raw)
		}
	})
}

// FuzzUpstreamCursorRoundTrip pins the encode→decode equality property
// for non-negative cursor values: anything SaveUpstreamCursor would
// accept must come back unchanged through LoadUpstreamCursor.
func FuzzUpstreamCursorRoundTrip(f *testing.F) {
	f.Add(int64(0))
	f.Add(int64(1))
	f.Add(int64(12345))
	f.Add(int64(1) << 62)

	f.Fuzz(func(t *testing.T, v int64) {
		if v < 0 {
			t.Skip()
		}
		st, err := store.Open(t.TempDir(), nil)
		require.NoError(t, err)
		defer func() { _ = st.Close() }()

		require.NoError(t, SaveUpstreamCursor(st, "relay/cursor", v))
		got, err := LoadUpstreamCursor(st, "relay/cursor")
		if err != nil {
			t.Fatalf("load rejected freshly-saved v=%d: %v", v, err)
		}
		if got != v {
			t.Fatalf("round-trip mismatch: in=%d out=%d", v, got)
		}
	})
}
