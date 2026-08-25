// Package seqspace owns the shared Jetstream sequence-number namespace and
// the immutable set of durable, explicitly-authorized sequence vacancies.
package seqspace

import (
	"fmt"
	"math"
	"slices"
)

// CursorSeqMaxThreshold splits sequence cursors from legacy unix-microsecond
// cursors. Valid event sequences and exclusive reservation ends must not exceed
// this value.
const CursorSeqMaxThreshold uint64 = 1_000_000_000_000_000

// Gap is a half-open interval [Start, End) containing no events.
type Gap struct {
	Start uint64
	End   uint64
}

// BlockRange is the inclusive sequence envelope of a non-empty durable block.
type BlockRange struct {
	Min uint64
	Max uint64
}

// Gaps is an immutable, sorted, disjoint set of half-open intervals.
type Gaps struct {
	ranges []Gap
	width  uint64
}

// NewGaps validates, sorts, and coalesces overlapping or adjacent intervals.
func NewGaps(in []Gap) (*Gaps, error) {
	if len(in) == 0 {
		return &Gaps{}, nil
	}
	ranges := slices.Clone(in)
	for _, gap := range ranges {
		if gap.Start == 0 || gap.End <= gap.Start {
			return nil, fmt.Errorf("seqspace: invalid gap [%d,%d)", gap.Start, gap.End)
		}
		if gap.End > CursorSeqMaxThreshold {
			return nil, fmt.Errorf("seqspace: gap end %d exceeds cursor ceiling %d", gap.End, CursorSeqMaxThreshold)
		}
	}
	slices.SortFunc(ranges, func(a, b Gap) int {
		if a.Start < b.Start {
			return -1
		}
		if a.Start > b.Start {
			return 1
		}
		if a.End < b.End {
			return -1
		}
		if a.End > b.End {
			return 1
		}
		return 0
	})
	out := ranges[:0]
	for _, gap := range ranges {
		if len(out) == 0 || gap.Start > out[len(out)-1].End {
			out = append(out, gap)
			continue
		}
		out[len(out)-1].End = max(out[len(out)-1].End, gap.End)
	}
	var width uint64
	for _, gap := range out {
		width += gap.End - gap.Start
	}
	return &Gaps{ranges: out, width: width}, nil
}

// Add returns a new set containing gap.
func (g *Gaps) Add(gap Gap) (*Gaps, error) {
	ranges := g.Ranges()
	ranges = append(ranges, gap)
	return NewGaps(ranges)
}

// Ranges returns a copy of the normalized intervals.
func (g *Gaps) Ranges() []Gap {
	if g == nil {
		return nil
	}
	return slices.Clone(g.ranges)
}

// Count returns the number of normalized intervals.
func (g *Gaps) Count() int {
	if g == nil {
		return 0
	}
	return len(g.ranges)
}

// Width returns the total number of vacant sequence values.
func (g *Gaps) Width() uint64 {
	if g == nil {
		return 0
	}
	return g.width
}

// EndContaining returns the exclusive end of the gap containing seq.
func (g *Gaps) EndContaining(seq uint64) (uint64, bool) {
	if g == nil {
		return 0, false
	}
	i, found := slices.BinarySearchFunc(g.ranges, seq, func(gap Gap, seq uint64) int {
		switch {
		case gap.End <= seq:
			return -1
		case gap.Start > seq:
			return 1
		default:
			return 0
		}
	})
	if !found {
		return 0, false
	}
	return g.ranges[i].End, true
}

// ValidateVacant rejects a registered gap that intersects a non-empty durable
// block envelope. Segment envelopes are intentionally not used: a valid gap
// may sit between blocks within one segment.
func (g *Gaps) ValidateVacant(blocks []BlockRange) error {
	if g == nil || len(g.ranges) == 0 {
		return nil
	}
	ordered := slices.Clone(blocks)
	for _, block := range ordered {
		if block.Min == 0 || block.Max < block.Min {
			return fmt.Errorf("seqspace: invalid durable block range [%d,%d]", block.Min, block.Max)
		}
	}
	slices.SortFunc(ordered, func(a, b BlockRange) int {
		if a.Min < b.Min {
			return -1
		}
		if a.Min > b.Min {
			return 1
		}
		return 0
	})
	gapIdx := 0
	for _, block := range ordered {
		for gapIdx < len(g.ranges) && g.ranges[gapIdx].End <= block.Min {
			gapIdx++
		}
		if gapIdx < len(g.ranges) && g.ranges[gapIdx].Start <= block.Max {
			gap := g.ranges[gapIdx]
			return fmt.Errorf("seqspace: registered gap [%d,%d) overlaps durable block [%d,%d]", gap.Start, gap.End, block.Min, block.Max)
		}
	}
	return nil
}

// ReserveEnd returns next+count after checking overflow and the cursor ceiling.
func ReserveEnd(next uint64, count int) (uint64, error) {
	if next == 0 || count <= 0 {
		return 0, fmt.Errorf("seqspace: invalid reservation start=%d count=%d", next, count)
	}
	n := uint64(count)
	if next > math.MaxUint64-n {
		return 0, fmt.Errorf("seqspace: reservation overflow at %d + %d", next, n)
	}
	end := next + n
	if end > CursorSeqMaxThreshold {
		return 0, fmt.Errorf("seqspace: reservation end %d exceeds cursor ceiling %d", end, CursorSeqMaxThreshold)
	}
	return end, nil
}
