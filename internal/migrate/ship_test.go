package migrate

import (
	"errors"
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// viewCatalog is a LocalCatalog over fixed main segments.
type viewCatalog struct {
	catalog.CatalogView
	segs []catalog.SegmentView
}

func (v *viewCatalog) Snapshot() catalog.CatalogView         { return v }
func (v *viewCatalog) Path(catalog.Namespace, uint64) string { return "" }

func (v *viewCatalog) Segments(ns catalog.Namespace) []catalog.SegmentView {
	if ns != catalog.Main {
		return nil
	}
	return v.segs
}

func TestReconcileSealed(t *testing.T) {
	t.Parallel()
	b, err := segment.NewBlockBuilder(1)
	require.NoError(t, err)
	_, err = b.Append(segment.Event{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "r"})
	require.NoError(t, err)
	frame, _ := b.Encode()
	header, _, hdr, err := segment.BuildSealed(segment.SliceFrameSource([][]byte{frame}))
	require.NoError(t, err)

	snap := &catalog.Snapshot{
		Segments: []catalog.SegmentRow{
			{Namespace: catalog.Main, Index: 0, State: catalog.Sealed, GenerationID: 7},
			{Namespace: catalog.Main, Index: 1, State: catalog.Active},
		},
		Generations: map[uint64]catalog.GenerationRow{7: {ID: 7, Namespace: catalog.Main, Header: header}},
	}
	source := func(gen uint64, st catalog.SegmentState) *Migrator {
		return &Migrator{cfg: Config{Catalog: &viewCatalog{segs: []catalog.SegmentView{
			{Namespace: catalog.Main, Index: 0, State: st, Generation: gen},
			{Namespace: catalog.Main, Index: 1, State: catalog.Active},
		}}}}
	}
	isFatal := func(err error) bool {
		var f fatalError
		return errors.As(err, &f)
	}

	segs, err := source(hdr.Checksum, catalog.Sealed).reconcileSealed(snap)
	require.NoError(t, err)
	require.Len(t, segs, 2)

	// The source rewrote segment 0: a new generation.
	_, err = source(hdr.Checksum+1, catalog.Sealed).reconcileSealed(snap)
	require.True(t, isFatal(err), "%v", err)

	// The source does not have the segment sealed at all.
	_, err = source(hdr.Checksum, catalog.Active).reconcileSealed(snap)
	require.True(t, isFatal(err), "%v", err)
	_, err = (&Migrator{cfg: Config{Catalog: &viewCatalog{}}}).reconcileSealed(snap)
	require.True(t, isFatal(err), "%v", err)

	// A sealed segment with no generation is catalog corruption, not a
	// source change.
	delete(snap.Generations, 7)
	_, err = source(hdr.Checksum, catalog.Sealed).reconcileSealed(snap)
	require.Error(t, err)
	require.False(t, isFatal(err))
}
