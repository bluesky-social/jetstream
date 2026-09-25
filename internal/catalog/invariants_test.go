package catalog_test

import (
	"errors"
	"maps"
	"slices"
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/stretchr/testify/require"
)

// validSnapshot builds a catalog with a sealed segment, an active segment
// with a block, and hot batches above it.
func validSnapshot(t *testing.T) *catalog.Snapshot {
	h := newHarness(t)
	h.hot(1, 2)
	h.hot(3, 5)
	b1 := h.fold(1, 2)
	b2 := h.fold(3, 5)
	_, err := h.s.Seal(t.Context(), h.sealOf(0, []builtBlock{b1, b2}))
	require.NoError(t, err)
	h.hot(6, 7)
	h.fold(6, 7)
	h.hot(8, 8)
	h.hot(9, 12)
	s, err := h.db.Snapshot()
	require.NoError(t, err)
	require.NoError(t, catalog.CheckInvariants(s, catalog.InvariantOptions{}))
	return s
}

func clone(s *catalog.Snapshot) *catalog.Snapshot {
	c := *s
	c.Segments = slices.Clone(s.Segments)
	c.Generations = maps.Clone(s.Generations)
	c.GenerationBlocks = map[uint64][]catalog.GenerationBlockRow{}
	for k, v := range s.GenerationBlocks {
		c.GenerationBlocks[k] = slices.Clone(v)
	}
	c.ActiveBlocks = slices.Clone(s.ActiveBlocks)
	c.HotBatches = slices.Clone(s.HotBatches)
	c.Objects = maps.Clone(s.Objects)
	c.Meta = maps.Clone(s.Meta)
	return &c
}

func TestCheckInvariants(t *testing.T) {
	t.Parallel()
	base := validSnapshot(t)
	cases := map[string]func(s *catalog.Snapshot){
		"inv1 seq key ahead":        func(s *catalog.Snapshot) { s.Meta[catalog.MainSeqKey] = catalog.EncodeSeq(14) },
		"inv1 seq key behind":       func(s *catalog.Snapshot) { s.Meta[catalog.MainSeqKey] = catalog.EncodeSeq(12) },
		"inv1 active block gap":     func(s *catalog.Snapshot) { s.ActiveBlocks[0].MinSeq++; s.ActiveBlocks[0].EventCount-- },
		"inv1 block count mismatch": func(s *catalog.Snapshot) { s.ActiveBlocks[0].EventCount++ },
		"inv1 sealed header blocks": func(s *catalog.Snapshot) {
			id := s.Segments[0].GenerationID
			s.GenerationBlocks[id] = s.GenerationBlocks[id][:1]
		},
		"inv2 two active": func(s *catalog.Snapshot) { s.Segments[0].State = catalog.Active },
		"inv2 index gap":  func(s *catalog.Snapshot) { s.Segments[1].Index = 2 },
		"inv2 foreign generation": func(s *catalog.Snapshot) {
			g := s.Generations[s.Segments[0].GenerationID]
			g.Segment = 7
			s.Generations[g.ID] = g
		},
		"inv3 ordinal gap": func(s *catalog.Snapshot) { s.ActiveBlocks[0].Ordinal = 1 },
		"inv3 block in sealed segment": func(s *catalog.Snapshot) {
			s.ActiveBlocks[0].Segment = 0
		},
		"inv4 uploading object": func(s *catalog.Snapshot) {
			o := s.Objects[s.ActiveBlocks[0].ObjectID]
			o.State = catalog.ObjectUploading
			s.Objects[o.ID] = o
		},
		"inv4 missing object": func(s *catalog.Snapshot) { delete(s.Objects, s.ActiveBlocks[0].ObjectID) },
		"inv5 hot gap":        func(s *catalog.Snapshot) { s.HotBatches = s.HotBatches[1:] },
		"inv5 hot overlap":    func(s *catalog.Snapshot) { s.HotBatches[0].LastSeq = 9; s.HotBatches[0].EventCount = 2 },
		"inv5 hot below blocks": func(s *catalog.Snapshot) {
			s.HotBatches = append([]catalog.HotBatchRow{{FirstSeq: 7, LastSeq: 7, EventCount: 1, Inline: true}}, s.HotBatches...)
		},
		"hot both frame and object": func(s *catalog.Snapshot) { s.HotBatches[0].ObjectID = s.ActiveBlocks[0].ObjectID },
		"seq key garbage":           func(s *catalog.Snapshot) { s.Meta[catalog.MainSeqKey] = []byte{1} },
		"hot but no segments": func(s *catalog.Snapshot) {
			s.Segments, s.ActiveBlocks, s.Generations, s.GenerationBlocks = nil, nil, nil, nil
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := clone(base)
			mutate(s)
			err := catalog.CheckInvariants(s, catalog.InvariantOptions{})
			require.Error(t, err)
			_, ok := catalog.IsCorruption(err)
			require.True(t, ok, "%v", err)
		})
	}

	t.Run("inv6 batch larger than a block", func(t *testing.T) {
		t.Parallel()
		err := catalog.CheckInvariants(base, catalog.InvariantOptions{MaxEventsPerBlock: 3})
		require.ErrorContains(t, err, "invariant 6")
	})
	t.Run("inv7 relay cursor", func(t *testing.T) {
		t.Parallel()
		boom := errors.New("cursor past a partial commit")
		err := catalog.CheckInvariants(base, catalog.InvariantOptions{RelayCursor: func([]byte) error { return boom }})
		require.ErrorContains(t, err, "invariant 7")
	})
	t.Run("cheap skips headers", func(t *testing.T) {
		t.Parallel()
		s := clone(base)
		g := s.Generations[s.Segments[0].GenerationID]
		g.Header = []byte("garbage")
		s.Generations[g.ID] = g
		require.Error(t, catalog.CheckInvariants(s, catalog.InvariantOptions{}))
		require.NoError(t, catalog.CheckInvariants(s, catalog.InvariantOptions{Cheap: true}))
	})
}
