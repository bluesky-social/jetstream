package live

import (
	"encoding/binary"
	"testing"
)

// FuzzDecodeUpstreamCursor pins panic-freedom on arbitrary bytes:
// decodeUpstreamCursor must always return either a non-negative int64
// or an error, never panic and never produce a negative value (atmos
// would silently treat a negative cursor as "no cursor → live tail",
// which would skip historical events on resume).
func FuzzDecodeUpstreamCursor(f *testing.F) {
	// Seed corpus: the well-formed v1 shape, every existing rejection
	// case, and a few off-by-one length variants. Seeds run as ordinary
	// unit tests when -fuzz is not set, so this corpus also acts as a
	// no-op regression net in CI.
	f.Add([]byte{})
	f.Add([]byte{cursorV1})
	f.Add([]byte{0x01, 0x02, 0x03})

	var ok [cursorV1Len]byte
	ok[0] = cursorV1
	binary.LittleEndian.PutUint64(ok[1:], 12345)
	f.Add(ok[:])

	var negative [cursorV1Len]byte
	negative[0] = cursorV1
	for i := 1; i < cursorV1Len; i++ {
		negative[i] = 0xff
	}
	f.Add(negative[:])

	var unknownVer [cursorV1Len]byte
	unknownVer[0] = 0xFF
	f.Add(unknownVer[:])

	short := make([]byte, cursorV1Len-1)
	short[0] = cursorV1
	f.Add(short)

	long := make([]byte, cursorV1Len+1)
	long[0] = cursorV1
	f.Add(long)

	f.Fuzz(func(t *testing.T, val []byte) {
		cur, err := decodeUpstreamCursor(val)
		if err != nil {
			return
		}
		if cur < 0 {
			t.Fatalf("decodeUpstreamCursor returned negative cursor %d (input=%x)", cur, val)
		}
	})
}

// FuzzUpstreamCursorRoundTrip pins the encode→decode equality property
// for non-negative cursor values: anything SaveUpstreamCursor would
// accept must come back unchanged through decodeUpstreamCursor.
//
// Save itself is store-coupled, so we replicate its on-disk shape
// here and feed the bytes through the pure decoder. If anyone ever
// changes the on-wire layout in one direction but not the other, this
// catches the drift.
func FuzzUpstreamCursorRoundTrip(f *testing.F) {
	f.Add(int64(0))
	f.Add(int64(1))
	f.Add(int64(12345))
	f.Add(int64(1) << 62)

	f.Fuzz(func(t *testing.T, v int64) {
		if v < 0 {
			t.Skip()
		}
		var buf [cursorV1Len]byte
		buf[0] = cursorV1
		binary.LittleEndian.PutUint64(buf[1:], uint64(v))

		got, err := decodeUpstreamCursor(buf[:])
		if err != nil {
			t.Fatalf("decode rejected freshly-encoded v=%d: %v", v, err)
		}
		if got != v {
			t.Fatalf("round-trip mismatch: in=%d out=%d", v, got)
		}
	})
}
