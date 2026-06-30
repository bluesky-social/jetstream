package manifest

import (
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// seg builds a minimal SegmentMetadata with the given Idx and seq envelope.
// eventCount==0 marks a compacted-to-empty segment whose (stale) seq envelope
// must be ignored by the monotonicity check.
func seg(idx, minSeq, maxSeq uint64, eventCount uint32) SegmentMetadata {
	return SegmentMetadata{
		SegmentBounds: SegmentBounds{Idx: idx, MinSeq: minSeq, MaxSeq: maxSeq},
		Header:        segment.Header{EventCount: eventCount, MinSeq: minSeq, MaxSeq: maxSeq},
	}
}

func TestValidateSegmentSeqMonotonicity(t *testing.T) {
	t.Parallel()

	t.Run("ascending disjoint is ok", func(t *testing.T) {
		t.Parallel()
		segs := []SegmentMetadata{
			seg(0, 1, 100, 100),
			seg(1, 101, 200, 100),
			seg(2, 201, 300, 100),
		}
		require.NoError(t, validateSegmentSeqMonotonicity(segs))
	})

	t.Run("empty and single are ok", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, validateSegmentSeqMonotonicity(nil))
		require.NoError(t, validateSegmentSeqMonotonicity([]SegmentMetadata{seg(0, 1, 50, 50)}))
	})

	t.Run("out-of-order seq across Idx is rejected", func(t *testing.T) {
		t.Parallel()
		// Idx ascending, but seq ranges disagree: idx 0 is [100,200], idx 1 is
		// [50,80]. This is the exact silent-loss trap — a paginating client
		// would learn a too-low SealedTipSeq and skip idx 0's tail. Crash loud.
		segs := []SegmentMetadata{
			seg(0, 100, 200, 100),
			seg(1, 50, 80, 30),
		}
		err := validateSegmentSeqMonotonicity(segs)
		require.ErrorIs(t, err, ErrSegmentSeqOverlap)
	})

	t.Run("overlapping ranges are rejected", func(t *testing.T) {
		t.Parallel()
		segs := []SegmentMetadata{
			seg(0, 1, 100, 100),
			seg(1, 100, 200, 100), // MinSeq 100 == prior MaxSeq 100: not strictly greater.
		}
		require.ErrorIs(t, validateSegmentSeqMonotonicity(segs), ErrSegmentSeqOverlap)
	})

	t.Run("adjacent equal seqs are rejected", func(t *testing.T) {
		t.Parallel()
		segs := []SegmentMetadata{
			seg(0, 1, 100, 100),
			seg(1, 99, 150, 51), // MinSeq 99 <= prior MaxSeq 100.
		}
		require.ErrorIs(t, validateSegmentSeqMonotonicity(segs), ErrSegmentSeqOverlap)
	})

	t.Run("empty segment between non-empty does not break the chain", func(t *testing.T) {
		t.Parallel()
		// A compacted-to-empty segment retains a stale [1,100] envelope but owns
		// no rows; it must be skipped so the real [1,100] -> [101,200] adjacency
		// is what's checked, not the stale empty envelope.
		segs := []SegmentMetadata{
			seg(0, 1, 100, 100),
			seg(1, 1, 100, 0), // compacted-to-empty, stale envelope overlaps both
			seg(2, 101, 200, 100),
		}
		require.NoError(t, validateSegmentSeqMonotonicity(segs))
	})

	t.Run("leading empty segment is skipped", func(t *testing.T) {
		t.Parallel()
		segs := []SegmentMetadata{
			seg(0, 0, 0, 0), // never held an event
			seg(1, 1, 100, 100),
		}
		require.NoError(t, validateSegmentSeqMonotonicity(segs))
	})
}
