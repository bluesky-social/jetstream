package oracle

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/local"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble/vfs"
)

// observedSegment is one segment's events in physical order, as one observer
// saw them.
type observedSegment struct {
	Index  uint64
	Sealed bool
	Events []ObservedEvent
}

// observeOptions says what the caller needs and what may change under the
// observation.
type observeOptions struct {
	// sealedOnly skips the active segment.
	sealedOnly bool
	// allowMissing treats a missing namespace directory as an empty
	// namespace instead of an error.
	allowMissing bool
	// live means a writer may append and seal between the catalog read and
	// the path cross-check that follows it. The later path read may then
	// hold newer segments, and the catalog's active segment may have grown
	// or been sealed since.
	live bool
	// compactionMayRace means a compaction rewrite may land between the two
	// reads, so the later read of a segment may miss rows the earlier one
	// saw.
	compactionMayRace bool
}

// liveObserveAttempts bounds how often a live catalog observation starts over
// after the writer sealed the tail under it.
const liveObserveAttempts = 3

// observeNamespaceFS reads ns through the local catalog (CatalogView plus
// Fetcher, the read path every storage backend shares) and returns its events
// in physical order. It then walks the same directory with the path observer
// and fails if the two disagree: the oracle must not silently trust either
// one. The path observer also runs the sealed structure and footer checks, and
// its errors win, so a corrupt segment is reported by the check that
// diagnoses it.
func observeNamespaceFS(fs vfs.FS, dataDir string, ns catalog.Namespace, opts observeOptions) ([]ObservedEvent, error) {
	dir := namespaceDir(fs, dataDir, ns)
	fromCatalog, catErr := observeCatalogFS(fs, dir, ns, opts)

	fromPaths, err := observeSegmentDirFS(fs, dir, opts.sealedOnly)
	if err != nil {
		if !opts.allowMissing || !isOracleNotExist(err) {
			return nil, err
		}
		fromPaths = nil
	}
	if catErr != nil {
		// A catalog failure over files the path observer accepted is itself
		// a disagreement. Add the path stream's invariant diagnosis when it
		// has one: overlapping seqs make the catalog refuse the view, and
		// the invariant error names the offending events.
		catErr = fmt.Errorf("oracle: catalog observer %s: %w", ns, catErr)
		if invErr := CheckStructuralInvariants(flattenObserved(fromPaths)); invErr != nil {
			return nil, errors.Join(catErr, invErr)
		}
		return nil, catErr
	}
	if err := crossCheckObservers(ns, fromCatalog, fromPaths, opts); err != nil {
		return nil, err
	}
	return flattenObserved(fromCatalog), nil
}

func flattenObserved(segs []observedSegment) []ObservedEvent {
	var out []ObservedEvent
	for _, s := range segs {
		out = append(out, s.Events...)
	}
	return out
}

// observeCatalogFS builds a fresh local catalog over dir, as a restarted
// reader would, and decodes every ref from seq 0.
func observeCatalogFS(fs vfs.FS, dir string, ns catalog.Namespace, opts observeOptions) ([]observedSegment, error) {
	for attempt := 1; ; attempt++ {
		segs, err := observeCatalogOnce(fs, dir, ns, opts)
		// A live writer can seal the tail between the directory listing and
		// the active scan, or between the snapshot and a fetch of an active
		// block (ErrStaleRef). Both mean "take a fresh view", which is the
		// contract every catalog reader follows.
		retry := errors.Is(err, segment.ErrSegmentSealed) || errors.Is(err, catalog.ErrStaleRef)
		if err == nil || !opts.live || !retry || attempt == liveObserveAttempts {
			return segs, err
		}
	}
}

func observeCatalogOnce(fs vfs.FS, dir string, ns catalog.Namespace, opts observeOptions) ([]observedSegment, error) {
	ctx := context.Background()
	cat, err := local.New(local.Config{FS: fs, Dirs: map[catalog.Namespace]string{ns: dir}})
	if err != nil {
		return nil, err
	}
	if err := cat.Refresh(ctx); err != nil {
		return nil, err
	}
	view, err := catalogSnapshot(cat)
	if err != nil {
		return nil, err
	}

	segs := view.Segments(ns)
	out := make([]observedSegment, 0, len(segs))
	pos := make(map[uint64]int, len(segs))
	for _, v := range segs {
		if opts.sealedOnly && v.State != catalog.Sealed {
			continue
		}
		pos[v.Index] = len(out)
		out = append(out, observedSegment{Index: v.Index, Sealed: v.State == catalog.Sealed})
	}

	fetch := cat.Fetcher()
	for ref := range view.RefsFrom(ns, 0) {
		i, ok := pos[ref.Segment]
		if !ok {
			continue // the active segment of a sealed-only observation
		}
		events, err := catalog.DecodeRef(ctx, fetch, ref)
		if err != nil {
			return nil, fmt.Errorf("decode segment %d block %d: %w", ref.Segment, ref.Block, err)
		}
		for _, ev := range events {
			// RefsFrom seeks by envelope, so an event outside its ref's
			// envelope would be skipped or misordered by a seq-cursor read.
			if ev.Seq < ref.MinSeq || ev.Seq > ref.MaxSeq {
				return nil, fmt.Errorf("oracle: segment %d block %d seq %d outside ref envelope [%d,%d]",
					ref.Segment, ref.Block, ev.Seq, ref.MinSeq, ref.MaxSeq)
			}
			out[i].Events = append(out[i].Events, observedEventFromSegment(ev))
		}
	}
	return out, nil
}

// catalogSnapshot takes a view, turning local.Catalog's panic on overlapping
// seq envelopes into an error. A server should crash on that corruption; the
// oracle has to report it as a failed observation.
func catalogSnapshot(cat *local.Catalog) (view catalog.CatalogView, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("snapshot: %v", r)
		}
	}()
	return cat.Snapshot(), nil
}

// crossCheckObservers compares the catalog observation with the path
// observation taken after it. Quiescent directories must match exactly,
// segment by segment. Live ones may differ only in ways a running writer
// explains; see observeOptions.
func crossCheckObservers(ns catalog.Namespace, fromCatalog, fromPaths []observedSegment, opts observeOptions) error {
	fail := func(format string, args ...any) error {
		return fmt.Errorf("oracle: %s catalog observer disagrees with path observer: "+format, append([]any{ns}, args...)...)
	}

	if !opts.live && len(fromCatalog) != len(fromPaths) {
		return fail("catalog has %d segments %v, paths have %d %v",
			len(fromCatalog), segmentIndexes(fromCatalog), len(fromPaths), segmentIndexes(fromPaths))
	}
	if len(fromPaths) < len(fromCatalog) {
		return fail("catalog has segments %v, paths only %v", segmentIndexes(fromCatalog), segmentIndexes(fromPaths))
	}
	for i, c := range fromCatalog {
		p := fromPaths[i]
		if c.Index != p.Index {
			return fail("segment #%d is %d in the catalog but %d on disk", i, c.Index, p.Index)
		}
		if !opts.live {
			if c.Sealed != p.Sealed {
				return fail("segment %d sealed=%v in the catalog, sealed=%v on disk", c.Index, c.Sealed, p.Sealed)
			}
			if j, ok := firstEventMismatch(c.Events, p.Events); ok {
				return fail("segment %d: %s", c.Index, describeMismatch(c.Events, p.Events, j))
			}
			continue
		}
		if c.Sealed && !p.Sealed {
			return fail("segment %d is sealed in the catalog but active on disk", c.Index)
		}
		if err := checkLiveSegment(c, p, opts.compactionMayRace); err != nil {
			return fail("segment %d: %v", c.Index, err)
		}
	}
	return nil
}

// checkLiveSegment checks that the later path read p of a segment is one the
// writer could have produced from the earlier catalog read c. Without
// compaction a sealed segment is unchanged and an active one only grows. A
// compaction rewrite only removes rows, so with one racing, the rows p shares
// c's range with must be a subsequence of c's.
func checkLiveSegment(c, p observedSegment, compactionMayRace bool) error {
	if !compactionMayRace {
		if c.Sealed {
			if j, ok := firstEventMismatch(c.Events, p.Events); ok {
				return errors.New(describeMismatch(c.Events, p.Events, j))
			}
			return nil
		}
		if len(p.Events) < len(c.Events) {
			return fmt.Errorf("active segment shrank from %d to %d events", len(c.Events), len(p.Events))
		}
		if j, ok := firstEventMismatch(c.Events, p.Events[:len(c.Events)]); ok {
			return errors.New(describeMismatch(c.Events, p.Events, j))
		}
		return nil
	}

	var maxSeq uint64
	if len(c.Events) > 0 {
		maxSeq = c.Events[len(c.Events)-1].Seq
	}
	shared := p.Events
	for len(shared) > 0 && shared[len(shared)-1].Seq > maxSeq {
		shared = shared[:len(shared)-1]
	}
	if c.Sealed && len(shared) != len(p.Events) {
		return fmt.Errorf("sealed segment gained events above seq %d", maxSeq)
	}
	k := 0
	for _, ev := range shared {
		for k < len(c.Events) && !observedEventEqual(c.Events[k], ev) {
			k++
		}
		if k == len(c.Events) {
			return fmt.Errorf("event seq=%d on disk is not in the catalog read", ev.Seq)
		}
		k++
	}
	return nil
}

func segmentIndexes(segs []observedSegment) []uint64 {
	out := make([]uint64, len(segs))
	for i, s := range segs {
		out[i] = s.Index
	}
	return out
}

// firstEventMismatch returns the first index where a and b differ, including
// one running out before the other.
func firstEventMismatch(a, b []ObservedEvent) (int, bool) {
	for i := range min(len(a), len(b)) {
		if !observedEventEqual(a[i], b[i]) {
			return i, true
		}
	}
	if len(a) != len(b) {
		return min(len(a), len(b)), true
	}
	return 0, false
}

func describeMismatch(fromCatalog, fromPaths []ObservedEvent, i int) string {
	at := func(evs []ObservedEvent) string {
		if i >= len(evs) {
			return "<none>"
		}
		ev := evs[i]
		return fmt.Sprintf("seq=%d kind=%v did=%s %s/%s rev=%s", ev.Seq, ev.Kind, ev.DID, ev.Collection, ev.Rkey, ev.Rev)
	}
	return fmt.Sprintf("%d vs %d events, first difference at #%d: catalog %s, disk %s",
		len(fromCatalog), len(fromPaths), i, at(fromCatalog), at(fromPaths))
}

func observedEventEqual(a, b ObservedEvent) bool {
	return a.Seq == b.Seq && a.WitnessedAt == b.WitnessedAt && a.Kind == b.Kind &&
		a.DID == b.DID && a.Collection == b.Collection && a.Rkey == b.Rkey &&
		a.Rev == b.Rev && bytes.Equal(a.Payload, b.Payload)
}
