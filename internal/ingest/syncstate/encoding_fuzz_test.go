package syncstate

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	atmossync "github.com/jcalabro/atmos/sync"
)

// fuzzCID parses the same fixed CID used by the unit tests. A helper
// rather than calling fixedCID(t) because *testing.F seed setup runs
// outside a *testing.T.
func fuzzCID(tb testing.TB) cbor.CID {
	tb.Helper()
	cid, err := cbor.ParseCIDString("bafyreigwexhqswvbgxqe5w7tnbcc7g5oh54oas5jewopl5jpcsjp3lk7vy")
	if err != nil {
		tb.Fatalf("parse fixture CID: %v", err)
	}
	return cid
}

// FuzzDecodeChainState pins the panic-freedom property: decodeChainState
// must return a value or a structured error for any byte slice, never
// panic. Seeded with a valid encoding, several truncations, and the
// MaxUint64-revLen overflow case from encoding_test.go so the seed
// corpus alone covers what the hand-authored tests cover.
func FuzzDecodeChainState(f *testing.F) {
	valid, err := encodeChainState(atmossync.ChainState{Rev: "rev", Data: fuzzCID(f)})
	if err != nil {
		f.Fatalf("seed encode: %v", err)
	}
	f.Add(valid)
	f.Add([]byte{})
	f.Add([]byte{chainStateV1})
	f.Add([]byte{chainStateV1, 0x00})
	f.Add([]byte{0xFF, 0x00, 0x00})

	var overflow []byte
	overflow = append(overflow, chainStateV1)
	overflow = binary.AppendUvarint(overflow, math.MaxUint64)
	overflow = append(overflow, make([]byte, 64)...)
	f.Add(overflow)

	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = decodeChainState(b)
	})
}

// FuzzDecodeHostingState mirrors FuzzDecodeChainState for the hosting
// state decoder. Seeds include both overflow cases pinned by
// encoding_test.go (statusLen and timeLen).
func FuzzDecodeHostingState(f *testing.F) {
	valid, err := encodeHostingState(atmossync.HostingState{
		Active: true,
		Status: "takendown",
		Seq:    42,
		Time:   "2026-05-21T00:00:00Z",
	})
	if err != nil {
		f.Fatalf("seed encode: %v", err)
	}
	f.Add(valid)
	f.Add([]byte{})
	f.Add([]byte{hostStateV1})
	f.Add([]byte{hostStateV1, 0x01})
	f.Add([]byte{0xFF, 0x00, 0x00})

	var statusOverflow []byte
	statusOverflow = append(statusOverflow, hostStateV1, 0x01)
	statusOverflow = binary.AppendUvarint(statusOverflow, math.MaxUint64)
	statusOverflow = append(statusOverflow, make([]byte, 32)...)
	f.Add(statusOverflow)

	var timeOverflow []byte
	timeOverflow = append(timeOverflow, hostStateV1, 0x01)
	timeOverflow = binary.AppendUvarint(timeOverflow, 0)
	timeOverflow = append(timeOverflow, make([]byte, 8)...)
	timeOverflow = binary.AppendUvarint(timeOverflow, math.MaxUint64)
	timeOverflow = append(timeOverflow, make([]byte, 32)...)
	f.Add(timeOverflow)

	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = decodeHostingState(b)
	})
}

// FuzzChainStateRoundTrip pins encode→decode equality across arbitrary
// Rev strings. The CID is fixed because cbor.CID is not directly
// fuzzable — encode rejects the zero CID and only well-formed CIDs
// survive parsing.
func FuzzChainStateRoundTrip(f *testing.F) {
	cid := fuzzCID(f)

	f.Add("")
	f.Add("rev")
	f.Add("3l3qo2vutsw2b")
	f.Add("\x00\x01\x02")

	f.Fuzz(func(t *testing.T, rev string) {
		in := atmossync.ChainState{Rev: rev, Data: cid}
		buf, err := encodeChainState(in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		out, err := decodeChainState(buf)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if out.Rev != in.Rev {
			t.Fatalf("rev mismatch: in=%q out=%q", in.Rev, out.Rev)
		}
		if !out.Data.Equal(in.Data) {
			t.Fatalf("cid mismatch: in=%s out=%s", in.Data, out.Data)
		}
	})
}

// FuzzHostingStateRoundTrip pins encode→decode equality across all
// four HostingState fields. Catches encoder/decoder drift — e.g. if
// someone widens Seq or changes a varint to a fixed width on one side
// but not the other.
func FuzzHostingStateRoundTrip(f *testing.F) {
	f.Add(true, "", int64(0), "")
	f.Add(false, "takendown", int64(12345), "2026-05-21T00:00:00Z")
	f.Add(true, "suspended", int64(-1), "x")
	f.Add(false, "deactivated", int64(math.MaxInt64), "2026-05-21T00:00:00Z")
	f.Add(true, "\x00\x01\x02", int64(math.MinInt64), "\xff")

	f.Fuzz(func(t *testing.T, active bool, status string, seq int64, timeStr string) {
		in := atmossync.HostingState{Active: active, Status: status, Seq: seq, Time: timeStr}
		buf, err := encodeHostingState(in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		out, err := decodeHostingState(buf)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if in != out {
			t.Fatalf("round-trip mismatch:\n  in=%+v\n  out=%+v", in, out)
		}
	})
}
