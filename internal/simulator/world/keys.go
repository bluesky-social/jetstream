package world

import (
	"encoding/binary"
	"fmt"
)

// All pebble keys are flat byte sequences with these prefixes. We do
// not use slashes inside numeric portions because they would interact
// poorly with range iteration if we ever switch to lexicographic
// account indexing.

var (
	keyMetaSeed = []byte("sim/meta/seed")
	keyMetaSeq  = []byte("sim/meta/seq")
)

// keyAccountState builds "sim/account/<idx>/state".
func keyAccountState(idx int) []byte {
	return fmt.Appendf(nil, "sim/account/%010d/state", idx)
}

// keyAccountKey builds "sim/account/<idx>/key".
func keyAccountKey(idx int) []byte {
	return fmt.Appendf(nil, "sim/account/%010d/key", idx)
}

// keyAccountDID builds "sim/account/<idx>/did".
func keyAccountDID(idx int) []byte {
	return fmt.Appendf(nil, "sim/account/%010d/did", idx)
}

// keyAccountBlock builds "sim/account/<idx>/blocks/<cidBytes>". The
// CID bytes are appended raw — we never iterate by CID, only point-
// look up.
func keyAccountBlock(idx int, cidBytes []byte) []byte {
	prefix := fmt.Appendf(nil, "sim/account/%010d/blocks/", idx)
	return append(prefix, cidBytes...)
}

// keyAccountMSTKey builds "sim/account/<idx>/mst/<mstkey>".
func keyAccountMSTKey(idx int, mstKey string) []byte {
	prefix := fmt.Appendf(nil, "sim/account/%010d/mst/", idx)
	return append(prefix, mstKey...)
}

// keyFirehose builds "sim/firehose/<seq>" using big-endian uint64 so
// pebble's lexicographic order matches numeric order, which matters
// for the cursor-replay range scan.
func keyFirehose(seq int64) []byte {
	out := make([]byte, 0, len("sim/firehose/")+8)
	out = append(out, []byte("sim/firehose/")...)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(seq))
	return append(out, buf[:]...)
}
