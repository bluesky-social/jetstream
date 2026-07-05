package syncstate

import (
	"encoding/binary"
	"fmt"

	"github.com/jcalabro/atmos/cbor"
	atmossync "github.com/jcalabro/atmos/sync"
)

const (
	chainStateV1 = 0x01
	hostStateV1  = 0x01
	identSeqV1   = 0x01

	// cidByteLen is the on-the-wire size of cbor.CID.Bytes() output:
	// version varint (1B = 0x01) + codec varint (1B) + sha2-256
	// multihash prefix (2B = 0x12 0x20) + 32B hash = 36 bytes.
	cidByteLen = 36
)

// encodeIdentitySeq serializes the applied #identity seq ratchet
// (#234): version byte + big-endian int64.
func encodeIdentitySeq(seq int64) []byte {
	buf := make([]byte, 9)
	buf[0] = identSeqV1
	binary.BigEndian.PutUint64(buf[1:], uint64(seq))
	return buf
}

func decodeIdentitySeq(buf []byte) (int64, error) {
	if len(buf) != 9 {
		return 0, fmt.Errorf("syncstate: identity seq has %d bytes, want 9", len(buf))
	}
	if buf[0] != identSeqV1 {
		return 0, fmt.Errorf("syncstate: unknown identity seq version 0x%02x", buf[0])
	}
	return int64(binary.BigEndian.Uint64(buf[1:])), nil
}

// encodeChainState serializes a sync.ChainState to a compact binary
// shape. Refuses to encode a zero CID — the StateStore contract
// returns (nil, nil) for absent state, since an explicit zero-CID save
// would be ambiguous on read.
func encodeChainState(s atmossync.ChainState) ([]byte, error) {
	if !s.Data.Defined() {
		return nil, fmt.Errorf("syncstate: refuse to encode chain state with zero CID")
	}
	cidBytes := s.Data.Bytes()
	if len(cidBytes) != cidByteLen {
		return nil, fmt.Errorf("syncstate: cbor.CID emitted %d bytes (want %d)", len(cidBytes), cidByteLen)
	}

	buf := make([]byte, 0, 1+binary.MaxVarintLen64+len(s.Rev)+cidByteLen)
	buf = append(buf, chainStateV1)
	buf = binary.AppendUvarint(buf, uint64(len(s.Rev)))
	buf = append(buf, s.Rev...)
	buf = append(buf, cidBytes...)
	return buf, nil
}

func decodeChainState(buf []byte) (atmossync.ChainState, error) {
	if len(buf) < 1 {
		return atmossync.ChainState{}, fmt.Errorf("syncstate: chain state too short (len=%d)", len(buf))
	}
	if buf[0] != chainStateV1 {
		return atmossync.ChainState{}, fmt.Errorf("syncstate: unknown chain state version 0x%02x", buf[0])
	}
	pos := 1
	revLen, n := binary.Uvarint(buf[pos:])
	if n <= 0 {
		return atmossync.ChainState{}, fmt.Errorf("syncstate: chain state rev length malformed")
	}
	pos += n
	remaining := uint64(len(buf) - pos)
	if revLen > remaining {
		return atmossync.ChainState{}, fmt.Errorf("syncstate: chain state rev length %d exceeds remaining buffer (%d)", revLen, remaining)
	}
	if remaining-revLen < cidByteLen {
		return atmossync.ChainState{}, fmt.Errorf("syncstate: chain state truncated (need %d more bytes for CID)", cidByteLen)
	}
	rev := string(buf[pos : pos+int(revLen)])
	pos += int(revLen)
	cid, err := cbor.ParseCIDBytes(buf[pos : pos+cidByteLen])
	if err != nil {
		return atmossync.ChainState{}, fmt.Errorf("syncstate: parse chain state CID: %w", err)
	}
	return atmossync.ChainState{Rev: rev, Data: cid}, nil
}

func encodeHostingState(s atmossync.HostingState) ([]byte, error) {
	size := 1 + 1 + binary.MaxVarintLen64 + len(s.Status) + 8 + binary.MaxVarintLen64 + len(s.Time)
	buf := make([]byte, 0, size)
	buf = append(buf, hostStateV1)
	if s.Active {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = binary.AppendUvarint(buf, uint64(len(s.Status)))
	buf = append(buf, s.Status...)
	var seq [8]byte
	binary.BigEndian.PutUint64(seq[:], uint64(s.Seq))
	buf = append(buf, seq[:]...)
	buf = binary.AppendUvarint(buf, uint64(len(s.Time)))
	buf = append(buf, s.Time...)
	return buf, nil
}

func decodeHostingState(buf []byte) (atmossync.HostingState, error) {
	if len(buf) < 2 {
		return atmossync.HostingState{}, fmt.Errorf("syncstate: hosting state too short (len=%d)", len(buf))
	}
	if buf[0] != hostStateV1 {
		return atmossync.HostingState{}, fmt.Errorf("syncstate: unknown hosting state version 0x%02x", buf[0])
	}
	pos := 1
	active := buf[pos] != 0
	pos++
	statusLen, n := binary.Uvarint(buf[pos:])
	if n <= 0 {
		return atmossync.HostingState{}, fmt.Errorf("syncstate: hosting state status length malformed")
	}
	pos += n
	remaining := uint64(len(buf) - pos)
	if statusLen > remaining {
		return atmossync.HostingState{}, fmt.Errorf("syncstate: hosting state status length %d exceeds remaining buffer (%d)", statusLen, remaining)
	}
	if remaining-statusLen < 8 {
		return atmossync.HostingState{}, fmt.Errorf("syncstate: hosting state truncated (need 8 more bytes for seq)")
	}
	status := string(buf[pos : pos+int(statusLen)])
	pos += int(statusLen)
	seq := int64(binary.BigEndian.Uint64(buf[pos : pos+8]))
	pos += 8
	timeLen, n := binary.Uvarint(buf[pos:])
	if n <= 0 {
		return atmossync.HostingState{}, fmt.Errorf("syncstate: hosting state time length malformed")
	}
	pos += n
	remaining = uint64(len(buf) - pos)
	if timeLen > remaining {
		return atmossync.HostingState{}, fmt.Errorf("syncstate: hosting state time length %d exceeds remaining buffer (%d)", timeLen, remaining)
	}
	timeStr := string(buf[pos : pos+int(timeLen)])
	return atmossync.HostingState{
		Active: active,
		Status: status,
		Seq:    seq,
		Time:   timeStr,
	}, nil
}
