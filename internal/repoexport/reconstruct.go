// Package repoexport reconstructs local atproto repo snapshots from
// Jetstream's segment archive.
package repoexport

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
)

// ErrNoLocalRepo is returned when no local create, update, or delete
// events exist for the requested DID.
var ErrNoLocalRepo = errors.New("repoexport: no local commit events for DID")

// maxStaleRetries bounds how many fresh catalog snapshots one namespace pass
// takes after catalog.ErrStaleRef. Each retry needs a concurrent seal or
// compaction rewrite of a block the pass had not reached yet, so hitting the
// bound means something rewrites the archive faster than we can read it.
const maxStaleRetries = 8

// Selection maps a segment index to the ascending indices of its blocks
// that may hold the requested DID. A segment present with no blocks was
// checked and holds nothing for the DID; a segment absent from the
// selection was not checked, and Reconstruct decodes all of its blocks.
type Selection map[uint64][]int

// Selector prunes a namespace's segments to the blocks that may hold a DID,
// using DID blooms. The interface keeps reconstruction independent of the
// manifest implementation.
type Selector interface {
	// SelectBlocksForDID returns the candidate blocks of the segments in ns
	// the selector can check. One-sided contract: no false negatives,
	// possible false positives. Block indices stay valid across compaction,
	// which preserves block topology and only drops rows, so a selection
	// read from any generation of a segment prunes every other generation.
	SelectBlocksForDID(ns catalog.Namespace, did string) (Selection, error)
}

// Archive is where reconstruction reads segment data: a catalog snapshot per
// pass, a fetcher for the blocks it names, and the bloom selector that
// prunes them.
type Archive struct {
	Catalog  catalog.Catalog
	Fetcher  catalog.Fetcher
	Selector Selector

	// Ready, when set, blocks until Catalog has loaded the archive, so a
	// reconstruction racing startup does not read a partial catalog and
	// report a missing history as a root mismatch.
	Ready func(context.Context) error
}

func (a Archive) validate() error {
	switch {
	case a.Catalog == nil:
		return errors.New("repoexport: Archive.Catalog is required")
	case a.Fetcher == nil:
		return errors.New("repoexport: Archive.Fetcher is required")
	case a.Selector == nil:
		return errors.New("repoexport: Archive.Selector is required")
	}
	return nil
}

// Config controls local repo reconstruction.
type Config struct {
	Archive Archive
	DID     string

	// PendingEvents are events buffered in the live writer's in-memory
	// pending block that have not yet been flushed to a segment file on
	// disk. They are replayed after all on-disk segments so a record
	// created moments ago (e.g. a like) is reflected in the snapshot
	// immediately, rather than only after the next compaction-driven
	// flush rotates the active segment. Optional; nil when no live writer
	// is available. The slice is the caller's already-copied result and is
	// not mutated.
	PendingEvents []segment.Event
}

// Snapshot is the reconstructed repo state for one DID.
type Snapshot struct {
	DID         string
	LatestRev   string
	Root        cbor.CID
	RecordCount int
	Blocks      *mst.MemBlockStore
}

// Reconstruct rebuilds cfg.DID's current repo snapshot from local storage.
//
// It replays, in seq order within each namespace so the last-writer-wins
// rev tracking matches a full scan:
//  1. the main namespace, sealed and active segments alike, decoding only
//     the blocks cfg.Archive.Selector's blooms name for the DID;
//  2. the bootstrap_live namespace (bootstrap-only; empty at steady
//     state), skipping events at or below the newest rev main held;
//  3. the live writer's in-memory pending block (cfg.PendingEvents).
func Reconstruct(ctx context.Context, cfg Config) (Snapshot, error) {
	if err := cfg.Archive.validate(); err != nil {
		return Snapshot{}, err
	}
	if cfg.DID == "" {
		return Snapshot{}, errors.New("repoexport: DID is required")
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if cfg.Archive.Ready != nil {
		if err := cfg.Archive.Ready(ctx); err != nil {
			return Snapshot{}, fmt.Errorf("repoexport: wait for archive: %w", err)
		}
	}

	state := replayState{
		did:     cfg.DID,
		records: make(map[string][]byte),
	}

	if err := replayNamespace(ctx, cfg.Archive, catalog.Main, "", &state); err != nil {
		return Snapshot{}, err
	}
	primaryWatermark := state.latestRev
	if err := replayNamespace(ctx, cfg.Archive, catalog.BootstrapLive, primaryWatermark, &state); err != nil {
		return Snapshot{}, err
	}

	// Pending events live in the live writer's active in-memory block,
	// after every block already flushed to disk, so they carry the newest
	// revs and need no watermark filter. replayEvents applies the same
	// DID/kind filtering as the on-disk path.
	if err := replayEvents(ctx, cfg.PendingEvents, "", &state); err != nil {
		return Snapshot{}, err
	}

	if !state.seenCommit {
		return Snapshot{}, fmt.Errorf("%w: %s", ErrNoLocalRepo, cfg.DID)
	}

	root, blocks, err := buildSnapshotBlocks(state.records)
	if err != nil {
		return Snapshot{}, err
	}

	return Snapshot{
		DID:         cfg.DID,
		LatestRev:   state.latestRev,
		Root:        root,
		RecordCount: len(state.records),
		Blocks:      blocks,
	}, nil
}

// replayNamespace replays ns's selected blocks in seq order. A fetch that
// fails with catalog.ErrStaleRef (a seal or compaction rewrite since the
// snapshot) takes a fresh snapshot and selection and resumes from the first
// seq not yet replayed, so no event is applied twice.
func replayNamespace(ctx context.Context, a Archive, ns catalog.Namespace, watermark string, state *replayState) error {
	var next uint64
	for attempt := 0; ; attempt++ {
		// Snapshot before selecting: a segment that seals in between is
		// active in the view and still covered, by its selection if the
		// selector saw it sealed or by a full decode if not.
		view := a.Catalog.Snapshot()
		sel, err := a.Selector.SelectBlocksForDID(ns, state.did)
		if err != nil {
			return fmt.Errorf("repoexport: select %s blocks: %w", ns, err)
		}
		err = replayView(ctx, view, a.Fetcher, ns, sel, watermark, state, &next)
		if !errors.Is(err, catalog.ErrStaleRef) {
			return err
		}
		if attempt == maxStaleRetries {
			return fmt.Errorf("repoexport: %s still stale after %d fresh snapshots: %w", ns, attempt, err)
		}
	}
}

// replayView replays ns's blocks in view from seq *next on, advancing *next
// past each block it replays or prunes.
func replayView(ctx context.Context, view catalog.CatalogView, f catalog.Fetcher, ns catalog.Namespace, sel Selection, watermark string, state *replayState, next *uint64) error {
	for _, v := range view.Segments(ns) {
		if len(v.Blocks) == 0 || v.MaxSeq() < *next {
			continue
		}
		blocks, checked := sel[v.Index]
		if !checked {
			for ref := range view.RefsFrom(ns, max(*next, v.MinSeq())) {
				if ref.Segment != v.Index {
					break
				}
				if err := replayRef(ctx, f, ref, watermark, state); err != nil {
					return err
				}
				*next = ref.MaxSeq + 1
			}
			continue
		}
		for _, i := range blocks {
			if err := ctx.Err(); err != nil {
				return err
			}
			// Topology is stable across generations, so an index past the
			// end only comes from a selector bug; skipping it matches the
			// one-sided contract rather than failing the page.
			if i < 0 || i >= len(v.Blocks) || v.Blocks[i].MaxSeq < *next {
				continue
			}
			ref, ok := refAt(view, ns, v.Blocks[i].MinSeq)
			if !ok || ref.Segment != v.Index || ref.Block != i {
				return fmt.Errorf("repoexport: %s segment %d block %d: view has no ref for its envelope", ns, v.Index, i)
			}
			if err := replayRef(ctx, f, ref, watermark, state); err != nil {
				return err
			}
			*next = ref.MaxSeq + 1
		}
		*next = max(*next, v.MaxSeq()+1)
	}
	return nil
}

// refAt returns the ref of the block whose envelope holds seq, or the first
// block after it.
func refAt(view catalog.CatalogView, ns catalog.Namespace, seq uint64) (catalog.BlockRef, bool) {
	for ref := range view.RefsFrom(ns, seq) {
		return ref, true
	}
	return catalog.BlockRef{}, false
}

func replayRef(ctx context.Context, f catalog.Fetcher, ref catalog.BlockRef, watermark string, state *replayState) error {
	events, err := catalog.DecodeRef(ctx, f, ref)
	if err != nil {
		return fmt.Errorf("repoexport: decode %s segment %d block %d: %w", ref.Namespace, ref.Segment, ref.Block, err)
	}
	return replayEvents(ctx, events, watermark, state)
}

// FooterSelector prunes by the DID blooms in each sealed segment's footer,
// read through Source. Primary, when set, is asked first (e.g. the
// manifest's resident blooms), and footers are read only for the sealed
// segments it did not check. Active segments are left unchecked: their
// blooms are not written until seal.
type FooterSelector struct {
	Source interface {
		Snapshot() catalog.CatalogView
		SealedMetadata(v catalog.SegmentView) (*segment.Reader, error)
	}
	Primary Selector
}

// SelectBlocksForDID implements Selector.
func (s FooterSelector) SelectBlocksForDID(ns catalog.Namespace, did string) (Selection, error) {
	sel := Selection{}
	if s.Primary != nil {
		primary, err := s.Primary.SelectBlocksForDID(ns, did)
		if err != nil {
			return nil, err
		}
		for idx, blocks := range primary {
			sel[idx] = blocks
		}
	}
	for _, v := range s.Source.Snapshot().Segments(ns) {
		if _, ok := sel[v.Index]; ok || v.State != catalog.Sealed {
			continue
		}
		blocks, err := footerBlocks(s.Source, v, did)
		if errors.Is(err, catalog.ErrStaleRef) {
			// Rewritten or removed since the snapshot: leave it unchecked
			// and let the reader's own view decide what to decode.
			continue
		}
		if err != nil {
			return nil, err
		}
		sel[v.Index] = blocks
	}
	return sel, nil
}

func footerBlocks(src interface {
	SealedMetadata(v catalog.SegmentView) (*segment.Reader, error)
}, v catalog.SegmentView, did string) ([]int, error) {
	r, err := src.SealedMetadata(v)
	if err != nil {
		return nil, fmt.Errorf("repoexport: open %s segment %d metadata: %w", v.Namespace, v.Index, err)
	}
	defer func() { _ = r.Close() }()
	blocks, err := r.BlocksContainingDID(did)
	if err != nil {
		return nil, fmt.Errorf("repoexport: select blocks in %s segment %d: %w", v.Namespace, v.Index, err)
	}
	return blocks, nil
}

type replayState struct {
	did        string
	records    map[string][]byte
	latestRev  string
	seenCommit bool
}

func replayEvents(ctx context.Context, events []segment.Event, watermark string, state *replayState) error {
	for _, ev := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := replayEvent(ev, watermark, state); err != nil {
			return err
		}
	}
	return nil
}

func replayEvent(ev segment.Event, watermark string, state *replayState) error {
	if ev.DID != state.did {
		return nil
	}

	switch ev.Kind {
	case segment.KindCreate, segment.KindUpdate, segment.KindCreateResync:
		if watermark != "" && ev.Rev <= watermark {
			return nil
		}
		key := ev.Collection + "/" + ev.Rkey
		state.records[key] = append([]byte(nil), ev.Payload...)
	case segment.KindDelete:
		if watermark != "" && ev.Rev <= watermark {
			return nil
		}
		key := ev.Collection + "/" + ev.Rkey
		delete(state.records, key)
	default:
		return nil
	}

	state.seenCommit = true
	state.latestRev = strings.Clone(ev.Rev)
	return nil
}

func buildSnapshotBlocks(records map[string][]byte) (cbor.CID, *mst.MemBlockStore, error) {
	blocks := mst.NewMemBlockStore()
	tree := mst.NewTree(blocks)

	keys := make([]string, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		payload := records[key]
		cid := cbor.ComputeCID(cbor.CodecDagCBOR, payload)
		if err := blocks.PutBlock(cid, append([]byte(nil), payload...)); err != nil {
			return cbor.CID{}, nil, fmt.Errorf("repoexport: store record block %s: %w", cid.String(), err)
		}
		if err := tree.Insert(key, cid); err != nil {
			return cbor.CID{}, nil, fmt.Errorf("repoexport: insert %s: %w", key, err)
		}
	}

	root, err := tree.WriteBlocks(blocks)
	if err != nil {
		return cbor.CID{}, nil, fmt.Errorf("repoexport: write MST blocks: %w", err)
	}
	return root, blocks, nil
}
