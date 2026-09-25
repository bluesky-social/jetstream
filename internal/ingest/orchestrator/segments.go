package orchestrator

import (
	"fmt"
	"iter"

	"github.com/bluesky-social/jetstream/internal/catalog"
	localcatalog "github.com/bluesky-social/jetstream/internal/catalog/local"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
)

// SegmentCatalog is the segment storage the orchestrator drives. The ingest
// writers publish to it, merge and compaction list and read segments
// through its snapshots, and the storage methods do the work that differs
// between backends. catalog/local implements it.
type SegmentCatalog interface {
	ingest.SegmentCatalog
	catalog.Catalog

	// Fetcher reads the blocks the catalog's refs point at.
	Fetcher() catalog.Fetcher

	// Path is segment idx's file path, for the manifest hook and logs.
	Path(ns catalog.Namespace, idx uint64) string

	// RewriteSegment rewrites a sealed segment in place and returns its
	// new view when res.Rewritten; see localcatalog.Catalog.RewriteSegment.
	RewriteSegment(ns catalog.Namespace, idx uint64, decide func(*segment.Event) segment.RowDecision, opts segment.RewriteOptions) (res segment.RewriteResult, rewritten catalog.SegmentView, err error)

	// RemoveStaleTemps reclaims what a crashed rewrite left behind in ns.
	RemoveStaleTemps(ns catalog.Namespace) error

	// DeleteNamespace durably removes ns and everything in it. Idempotent.
	DeleteNamespace(ns catalog.Namespace) error
}

var _ SegmentCatalog = (*localcatalog.Catalog)(nil)

// segments returns cfg.Catalog, or a local catalog over cfg.DataDir when
// none is configured. It is lazy because tests build an Orchestrator
// literal and call phases directly, bypassing New.
func (o *Orchestrator) segments() SegmentCatalog {
	o.segmentsOnce.Do(func() {
		if o.cfg.Catalog != nil {
			o.segmentCatalog = o.cfg.Catalog
			return
		}
		c, err := localcatalog.New(localcatalog.DataDirConfig(o.cfg.FS, o.cfg.DataDir))
		if err != nil {
			// DataDirConfig names only known namespaces.
			panic(fmt.Sprintf("orchestrator: default segment catalog: %v", err))
		}
		o.segmentCatalog = c
	})
	return o.segmentCatalog
}

// segmentRefs yields the refs of segment v's blocks from view, which v must
// come from.
func segmentRefs(view catalog.CatalogView, v catalog.SegmentView) iter.Seq[catalog.BlockRef] {
	return func(yield func(catalog.BlockRef) bool) {
		if len(v.Blocks) == 0 {
			return
		}
		for ref := range view.RefsFrom(v.Namespace, v.MinSeq()) {
			if ref.Segment != v.Index || !yield(ref) {
				return
			}
		}
	}
}
