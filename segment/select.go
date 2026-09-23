package segment

import "github.com/jcalabro/gloom"

// SelectBlocksForDID returns ascending block indices that may contain did,
// shared by Reader and manifest selection. Bloom filters allow false
// positives; callers must filter decoded rows.
//
// A negative segBloom skips the segment. A nil segBloom does not prune, and
// nil block blooms are included to avoid false negatives.
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
