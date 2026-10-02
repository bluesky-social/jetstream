package ingest

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/segment"
)

// SegmentCatalog is told about the writer's segments: the active segment,
// through an ActiveSource it can sample, and each segment the writer seals.
// catalog/local implements it for the serving catalog; OnSealed adapts a
// plain function for callers that only care about seals.
type SegmentCatalog interface {
	// AttachActive registers src as ns's active segment, replacing any
	// earlier source. The Writer calls it once from Open.
	AttachActive(ns catalog.Namespace, src catalog.ActiveSource)

	// Sealed publishes a segment the writer just sealed. The segment is
	// durable (footer and header fsynced) before the call. It runs under the
	// writer mutex, so it must not call back into the Writer or do unbounded
	// I/O. An error leaves the writer without a usable active segment, as a
	// failed seal does: callers should Close and reopen.
	Sealed(v catalog.SegmentView) error
}

// OnSealed adapts fn to a SegmentCatalog that ignores the active segment.
type OnSealed func(v catalog.SegmentView) error

func (OnSealed) AttachActive(catalog.Namespace, catalog.ActiveSource) {}

func (fn OnSealed) Sealed(v catalog.SegmentView) error { return fn(v) }

// SealedPathFunc adapts a callback that takes a sealed segment's index and
// file path, for a writer whose segments live in dir.
func SealedPathFunc(dir string, fn func(idx uint64, path string) error) SegmentCatalog {
	return OnSealed(func(v catalog.SegmentView) error {
		return fn(v.Index, filepath.Join(dir, SegmentFilename(v.Index)))
	})
}

// SealedView builds the catalog view of a segment from the metadata Seal
// wrote, parsing the footer rather than re-reading the file.
func SealedView(ns catalog.Namespace, idx uint64, res segment.SealResult) (catalog.SegmentView, error) {
	r, err := segment.OpenReaderParts(res.HeaderBytes, res.Footer, func(int) ([]byte, error) {
		return nil, fmt.Errorf("ingest: sealed view %d has no block source", idx)
	}, segment.ReaderOptions{Name: SegmentFilename(idx)})
	if err != nil {
		return catalog.SegmentView{}, fmt.Errorf("ingest: parse sealed metadata of segment %d: %w", idx, err)
	}
	defer func() { _ = r.Close() }()
	hdr := r.Header()
	return catalog.SegmentView{
		Namespace:  ns,
		Index:      idx,
		State:      catalog.Sealed,
		Generation: hdr.Checksum,
		Header:     hdr,
		Blocks:     r.Blocks(),
		Size:       int64(hdr.FooterOffset) + int64(len(res.Footer)),
	}, nil
}

// blockCommitter is the Writer's storage backend (design D6): where segment
// bytes live, how a durable metadata batch becomes visible, and who hears
// about seals. The Writer keeps the ordering rules (a block is durable
// before the batch describing it commits; a segment is sealed before it is
// published) and the committer supplies the mechanics.
//
// localCommitter is the only implementation. Disaggregated hot mode (S2.8)
// adds the object-store committer and exports the seam.
type blockCommitter interface {
	// openSegment creates segment idx, or resumes it if an active file
	// already exists at that index.
	openSegment(idx uint64) (*segment.Writer, error)
	// segmentBytes is segment idx's size past the reserved header.
	segmentBytes(idx uint64) (int64, error)
	// commitBatch makes a durable metadata batch visible.
	commitBatch(ctx context.Context, b metastore.Batch) error
	// sealed publishes segment idx after Seal returned res.
	sealed(idx uint64, res segment.SealResult) error
}

// localCommitter keeps segments as files in cfg.SegmentsDir and commits
// metadata to cfg.Store. Block durability is segment.Writer.Flush's fsync;
// batch visibility is the metastore commit, which syncs.
type localCommitter struct {
	cfg *Config
}

var _ blockCommitter = localCommitter{}

// Segment paths use filepath.Join, not cfg.FS's PathJoin, even when a
// vfs.FS is injected. The two agree on every '/'-separated filesystem
// (all our prod targets and the strict-mem oracle FS, which uses
// path.Join); they diverge only on Windows separators, which we do not
// support. Keeping filepath.Join here matches the rest of the package.
func (c localCommitter) path(idx uint64) string {
	return filepath.Join(c.cfg.SegmentsDir, SegmentFilename(idx))
}

func (c localCommitter) openSegment(idx uint64) (*segment.Writer, error) {
	return segment.New(segment.Config{
		Path:              c.path(idx),
		FS:                c.cfg.FS,
		MaxEventsPerBlock: c.cfg.MaxEventsPerBlock,
		Metrics:           c.cfg.SegmentMetrics,
		IOFaultInjector:   c.cfg.SegmentIOFaultInjector,
	})
}

func (c localCommitter) segmentBytes(idx uint64) (int64, error) {
	info, err := statFS(c.cfg.FS, c.path(idx))
	if err != nil {
		return 0, err
	}
	return info.Size() - int64(segment.ReservedHeaderBytes), nil
}

func (c localCommitter) commitBatch(ctx context.Context, b metastore.Batch) error {
	return b.Commit(ctx)
}

func (c localCommitter) sealed(idx uint64, res segment.SealResult) error {
	if c.cfg.Catalog == nil {
		return nil
	}
	v, err := SealedView(c.cfg.Namespace, idx, res)
	if err != nil {
		return err
	}
	if err := c.cfg.Catalog.Sealed(v); err != nil {
		return fmt.Errorf("ingest: publish sealed segment %d: %w", idx, err)
	}
	return nil
}

// ActiveSegment reports the active segment and its flushed blocks, sampled
// together under the writer mutex. After Close it keeps reporting the
// blocks the writer made durable, since the file is still the namespace's
// unsealed tail; after SealActiveAndClose, or while no active segment is
// open, it reports false.
func (w *Writer) ActiveSegment() (catalog.SegmentView, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		if w.closedActive == nil {
			return catalog.SegmentView{}, false
		}
		return *w.closedActive, true
	}
	if w.active == nil {
		return catalog.SegmentView{}, false
	}
	return w.activeViewLocked(w.active.Blocks()), true
}

func (w *Writer) activeViewLocked(blocks []segment.BlockInfo) catalog.SegmentView {
	return catalog.SegmentView{
		Namespace: w.cfg.Namespace,
		Index:     w.activeIdx,
		State:     catalog.Active,
		Blocks:    blocks,
	}
}

// closeActiveLocked flushes and closes the active segment, keeping its block
// index for ActiveSegment. Flush then Close does the same I/O as Close alone,
// which flushes the pending block itself, but only the open writer can
// report its blocks.
func (w *Writer) closeActiveLocked() error {
	if err := w.active.Flush(); err != nil {
		_ = w.active.Close()
		return err
	}
	v := w.activeViewLocked(w.active.Blocks())
	if err := w.active.Close(); err != nil {
		return err
	}
	w.closedActive = &v
	return nil
}
