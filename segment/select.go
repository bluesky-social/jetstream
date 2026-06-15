package segment

import "github.com/jcalabro/gloom"

// SelectBlocksForDID returns the ascending indices of blocks that may
// contain an event for did, given a segment's DID bloom filters. It is
// the single source of the bloom-pruning decision, shared by the
// file-backed Reader.BlocksContainingDID and the manifest's in-memory
// resident-bloom selection so the two can never diverge.
//
// The contract is one-sided, exactly as bloom filters allow: there are
// never false negatives, so every block that actually holds did is
// included; there may be false positives, so a returned block is not
// guaranteed to hold did (the caller filters per-event after decode).
//
// segBloom is the whole-segment DID bloom; a non-nil filter that does
// not contain did short-circuits to no blocks (the property that turns
// a full-archive scan into a near-no-op). A nil segBloom does not
// short-circuit. A nil entry in blockBlooms is conservatively included
// rather than risk dropping a record when a bloom is unavailable.
func SelectBlocksForDID(segBloom *gloom.Filter, blockBlooms []*gloom.Filter, did string) []int {
	if len(blockBlooms) == 0 {
		return nil
	}
	if segBloom != nil && !segBloom.TestString(did) {
		return nil
	}
	out := make([]int, 0, len(blockBlooms))
	for i, bloom := range blockBlooms {
		if bloom == nil || bloom.TestString(did) {
			out = append(out, i)
		}
	}
	return out
}
