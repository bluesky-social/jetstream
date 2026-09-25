package local_test

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// TestLocal_SealedMetadataPinsGeneration checks that SealedMetadata reads
// the footer of the view's generation, decodes blocks through the catalog,
// and reports a rewritten or removed file as stale rather than serving
// another generation's footer.
func TestLocal_SealedMetadataPinsGeneration(t *testing.T) {
	t.Parallel()

	s := newSwarm(t, 7)
	for len(s.sealed) == 0 {
		s.step()
	}
	require.NoError(t, s.w.Close())

	var sealed, active catalog.SegmentView
	for _, v := range s.cat.Snapshot().Segments(catalog.Main) {
		if v.State == catalog.Sealed && sealed.Generation == 0 {
			sealed = v
		}
		if v.State == catalog.Active {
			active = v
		}
	}
	require.NotZero(t, sealed.Generation)

	r, err := s.cat.SealedMetadata(sealed)
	require.NoError(t, err)
	require.Equal(t, sealed.Blocks, r.Blocks())
	first, err := r.DecodeBlock(0)
	require.NoError(t, err)
	require.NotEmpty(t, first)
	blocks, err := r.BlocksContainingDID(first[0].DID)
	require.NoError(t, err)
	require.Contains(t, blocks, 0, "one-sided bloom: the DID's own block is a candidate")
	require.NoError(t, r.Close())

	if active.State == catalog.Active {
		_, err = s.cat.SealedMetadata(active)
		require.Error(t, err)
		require.NotErrorIs(t, err, catalog.ErrStaleRef)
	}

	path := s.cat.Path(catalog.Main, sealed.Index)
	res, err := segment.Rewrite(path, func(ev *segment.Event) segment.RowDecision {
		if ev.Seq == sealed.MinSeq() {
			return segment.RowDrop
		}
		return segment.RowKeep
	}, segment.RewriteOptions{FS: s.fs})
	require.NoError(t, err)
	require.True(t, res.Rewritten)
	_, err = s.cat.SealedMetadata(sealed)
	require.ErrorIs(t, err, catalog.ErrStaleRef)

	require.NoError(t, s.fs.Remove(path))
	_, err = s.cat.SealedMetadata(sealed)
	require.ErrorIs(t, err, catalog.ErrStaleRef)
}
