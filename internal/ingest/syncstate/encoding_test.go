package syncstate

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

// fixedCID returns a deterministic cbor.CID for tests.
func fixedCID(t *testing.T) cbor.CID {
	t.Helper()
	cid, err := cbor.ParseCIDString("bafyreigwexhqswvbgxqe5w7tnbcc7g5oh54oas5jewopl5jpcsjp3lk7vy")
	require.NoError(t, err)
	return cid
}

func TestEncodeChainState_RoundTrip(t *testing.T) {
	t.Parallel()
	in := atmossync.ChainState{Rev: "3l3qo2vutsw2b", Data: fixedCID(t)}

	buf, err := encodeChainState(in)
	require.NoError(t, err)
	require.Greater(t, len(buf), 0)

	out, err := decodeChainState(buf)
	require.NoError(t, err)
	require.Equal(t, in.Rev, out.Rev)
	require.True(t, in.Data.Equal(out.Data))
}

func TestEncodeChainState_RejectsZeroCID(t *testing.T) {
	t.Parallel()
	_, err := encodeChainState(atmossync.ChainState{Rev: "rev"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "zero CID")
}

func TestDecodeChainState_RejectsTruncated(t *testing.T) {
	t.Parallel()
	_, err := decodeChainState([]byte{0x01, 0x00})
	require.Error(t, err)
}

func TestDecodeChainState_RejectsUnknownVersion(t *testing.T) {
	t.Parallel()
	in := atmossync.ChainState{Rev: "rev", Data: fixedCID(t)}
	buf, err := encodeChainState(in)
	require.NoError(t, err)
	buf[0] = 0xFF // bogus version

	_, err = decodeChainState(buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown")
}

func TestEncodeHostingState_RoundTrip(t *testing.T) {
	t.Parallel()
	in := atmossync.HostingState{
		Active: false,
		Status: "takendown",
		Seq:    12345,
		Time:   "2026-05-21T00:00:00Z",
	}
	buf, err := encodeHostingState(in)
	require.NoError(t, err)

	out, err := decodeHostingState(buf)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestEncodeHostingState_ActiveZeroSeq(t *testing.T) {
	t.Parallel()
	in := atmossync.HostingState{Active: true}
	buf, err := encodeHostingState(in)
	require.NoError(t, err)

	out, err := decodeHostingState(buf)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

// TestDecode_RejectsOverflowingLengths pins that decode functions
// reject corrupt buffers whose declared length fields would overflow
// when added to other lengths during the bounds check. Without an
// explicit cap, MaxUint64-style values wrap silently and the bounds
// check passes, then the slice expression panics.
//
// Build a chain state buffer with revLen = MaxUint64 in the varint
// slot. Decode must return a structured error, not panic. The buffer
// includes enough trailing bytes that a wrap-around bounds check
// (MaxUint64 + cidByteLen wraps to 35) would silently succeed.
func TestDecodeChainState_RejectsOverflowingRevLen(t *testing.T) {
	t.Parallel()

	// [version byte][MaxUint64 as uvarint][>=35 trailing bytes]
	var buf []byte
	buf = append(buf, chainStateV1)
	buf = binary.AppendUvarint(buf, math.MaxUint64)
	// MaxUint64 + cidByteLen (36) wraps to 35; pad past that so the
	// faulty bounds check passes and the slice expression panics.
	buf = append(buf, make([]byte, 64)...)

	require.NotPanics(t, func() {
		_, _ = decodeChainState(buf)
	})
	_, err := decodeChainState(buf)
	require.Error(t, err)
}

func TestDecodeHostingState_RejectsOverflowingStatusLen(t *testing.T) {
	t.Parallel()

	var buf []byte
	buf = append(buf, hostStateV1)
	buf = append(buf, 0x01) // active = true
	buf = binary.AppendUvarint(buf, math.MaxUint64)
	// MaxUint64 + 8 wraps to 7; pad past that.
	buf = append(buf, make([]byte, 32)...)

	require.NotPanics(t, func() {
		_, _ = decodeHostingState(buf)
	})
	_, err := decodeHostingState(buf)
	require.Error(t, err)
}

func TestDecodeHostingState_RejectsOverflowingTimeLen(t *testing.T) {
	t.Parallel()

	var buf []byte
	buf = append(buf, hostStateV1)
	buf = append(buf, 0x01)
	buf = binary.AppendUvarint(buf, 0) // empty status
	var seq [8]byte                    // zero seq
	buf = append(buf, seq[:]...)
	buf = binary.AppendUvarint(buf, math.MaxUint64) // overflowing time len
	// timeLen alone (no addition) is checked against remaining; the
	// slice expression `buf[pos : pos+int(timeLen)]` with int(MaxUint64)
	// = -1 still panics on amd64 even without an additive overflow.
	buf = append(buf, make([]byte, 32)...)

	require.NotPanics(t, func() {
		_, _ = decodeHostingState(buf)
	})
	_, err := decodeHostingState(buf)
	require.Error(t, err)
}
