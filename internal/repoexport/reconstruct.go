// Package repoexport reconstructs local atproto repo snapshots from
// Jetstream segment files.
package repoexport

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
)

// ErrNoLocalRepo is returned when no local create, update, or delete
// events exist for the requested DID.
var ErrNoLocalRepo = errors.New("repoexport: no local commit events for DID")

// BlockSelection identifies, for one sealed segment, the blocks that may
// contain the requested DID. Path locates the file; Blocks holds ascending
// block indices into the segment's on-disk block array.
type BlockSelection struct {
	Path   string
	Blocks []int
}

// Selector is the bloom-pruning front door reconstruction uses instead of
// scanning every segment file. The production implementation is backed by
// the in-memory manifest, which already holds every sealed segment's DID
// blooms resident -- so pruning happens entirely in memory and only the
// segments an account actually touches are opened.
//
// Keeping this an interface (rather than depending on *manifest.Manifest
// directly) keeps repoexport decoupled from the manifest package and makes
// the selection trivially fakeable in tests.
type Selector interface {
	// SelectBlocksForDID returns, for every sealed segment that may hold
	// did, the candidate blocks within it. One-sided contract: no false
	// negatives, possible false positives. Ascending by segment index.
	SelectBlocksForDID(did string) ([]BlockSelection, error)

	// ActiveSegmentPaths returns the seg_*.jss files not yet resident in
	// the manifest -- the active (unsealed) segment plus any just-sealed
	// file the manifest has not absorbed. Their flushed blocks are
	// invisible to SelectBlocksForDID, so reconstruction scans them
	// directly. Callers MUST query this BEFORE SelectBlocksForDID so a
	// segment sealing mid-call is double-covered, never missed.
	ActiveSegmentPaths() ([]string, error)
}

// Config controls local repo reconstruction.
type Config struct {
	DataDir string
	DID     string

	// Selector prunes which sealed segments/blocks to decode via the
	// in-memory manifest blooms, and reports the active (unsealed)
	// segment files that need a direct scan. Required: reconstruction is
	// only invoked from the live /status path, which always has a
	// manifest.
	Selector Selector

	// PendingEvents are events buffered in the live writer's in-memory
	// pending block that have not yet been flushed to a segment file on
	// disk. They are replayed after all on-disk segments so a record
	// created moments ago (e.g. a like) is reflected in the snapshot
	// immediately, rather than only after the next compaction-driven
	// flush rotates the active segment. Optional; nil when no live writer
	// is available. The slice is the caller's already-copied
	// SnapshotPending() result and is not mutated.
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
// It replays, in seq/index-ascending order so the last-writer-wins rev
// tracking matches a full scan:
//  1. sealed segments, pruned to candidate blocks via cfg.Selector's
//     in-memory manifest blooms (only the few segments the DID touches are
//     opened and decoded);
//  2. the active (unsealed) segment files, scanned directly because their
//     flushed blocks are not yet resident in the manifest;
//  3. backfill/live_segments, scanned directly (bootstrap-only; absent at
//     steady state);
//  4. the live writer's in-memory pending block (cfg.PendingEvents).
func Reconstruct(ctx context.Context, cfg Config) (Snapshot, error) {
	if cfg.DataDir == "" {
		return Snapshot{}, errors.New("repoexport: DataDir is required")
	}
	if cfg.DID == "" {
		return Snapshot{}, errors.New("repoexport: DID is required")
	}
	if cfg.Selector == nil {
		return Snapshot{}, errors.New("repoexport: Selector is required")
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}

	state := replayState{
		did:     cfg.DID,
		records: make(map[string][]byte),
	}

	// Snapshot the active (unsealed) paths BEFORE asking the selector for
	// sealed blocks. If a segment seals in the gap between the two calls it
	// is then both reported active here AND resident in the selection --
	// idempotent double-coverage. The reverse order could miss it in both,
	// dropping records (the spurious-mismatch bug class).
	activePaths, err := cfg.Selector.ActiveSegmentPaths()
	if err != nil {
		return Snapshot{}, fmt.Errorf("repoexport: list active segments: %w", err)
	}

	selections, err := cfg.Selector.SelectBlocksForDID(cfg.DID)
	if err != nil {
		return Snapshot{}, fmt.Errorf("repoexport: select blocks: %w", err)
	}
	for _, sel := range selections {
		if err := replaySelectedBlocks(ctx, sel, &state); err != nil {
			return Snapshot{}, err
		}
	}

	// Active segment files carry the newest sealed-tree revs; scan them
	// after the sealed selection so ascending order is preserved.
	for _, path := range activePaths {
		if err := ctx.Err(); err != nil {
			return Snapshot{}, err
		}
		if err := replayFile(ctx, path, "", &state); err != nil {
			return Snapshot{}, err
		}
	}

	primaryWatermark := state.latestRev
	if err := replayDir(ctx, filepath.Join(cfg.DataDir, "backfill", "live_segments"), primaryWatermark, &state); err != nil {
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

// replaySelectedBlocks decodes only the selector-chosen blocks of one
// sealed segment. The segment may have been compacted (and thus rewritten)
// since the manifest snapshot, but compaction preserves block count and is
// purely subtractive, so a stored block index never points past the file
// and never yields a false negative for the DID.
func replaySelectedBlocks(ctx context.Context, sel BlockSelection, state *replayState) error {
	if len(sel.Blocks) == 0 {
		return nil
	}
	reader, err := segment.Open(segment.ReaderConfig{Path: sel.Path})
	if err != nil {
		// A segment selected from the manifest can race a seal/compaction
		// rename. ErrActiveSegment means it is mid-rotation; the active-path
		// scan covers it, so skip rather than fail the whole reconstruction.
		if errors.Is(err, segment.ErrActiveSegment) {
			return nil
		}
		return fmt.Errorf("repoexport: open segment %s: %w", sel.Path, err)
	}
	defer func() { _ = reader.Close() }()

	blockCount := len(reader.Blocks())
	for _, i := range sel.Blocks {
		if err := ctx.Err(); err != nil {
			return err
		}
		if i < 0 || i >= blockCount {
			continue
		}
		events, err := reader.DecodeBlock(i)
		if err != nil {
			return fmt.Errorf("repoexport: decode block %d in %s: %w", i, sel.Path, err)
		}
		if err := replayEvents(ctx, events, "", state); err != nil {
			return err
		}
	}
	return nil
}

type replayState struct {
	did        string
	records    map[string][]byte
	latestRev  string
	seenCommit bool
}

func replayDir(ctx context.Context, dir, watermark string, state *replayState) error {
	files, err := ingest.SegmentFiles(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("repoexport: list segments in %s: %w", dir, err)
	}
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := replayFile(ctx, file.Path, watermark, state); err != nil {
			return err
		}
	}
	return nil
}

func replayFile(ctx context.Context, path, watermark string, state *replayState) error {
	reader, err := segment.Open(segment.ReaderConfig{Path: path})
	if err != nil {
		if errors.Is(err, segment.ErrActiveSegment) {
			return replayActive(ctx, path, watermark, state)
		}
		return fmt.Errorf("repoexport: open segment %s: %w", path, err)
	}

	closeReader := true
	defer func() {
		if closeReader {
			_ = reader.Close()
		}
	}()

	// Prune by DID before decoding. A full-network segment holds events
	// for every DID on the network; without this, reconstructing one
	// account would zstd-decompress and decode the entire archive and
	// then discard nearly every event in replayEvent's DID filter. The
	// bloom-backed selection has no false negatives, so every block that
	// actually holds state.did is still decoded.
	selected, err := reader.BlocksContainingDID(state.did)
	if err != nil {
		return fmt.Errorf("repoexport: select blocks in %s: %w", path, err)
	}
	for _, i := range selected {
		if err := ctx.Err(); err != nil {
			return err
		}
		events, err := reader.DecodeBlock(i)
		if err != nil {
			return fmt.Errorf("repoexport: decode block %d in %s: %w", i, path, err)
		}
		if err := replayEvents(ctx, events, watermark, state); err != nil {
			return err
		}
	}

	closeReader = false
	if err := reader.Close(); err != nil {
		return fmt.Errorf("repoexport: close segment %s: %w", path, err)
	}
	return nil
}

func replayActive(ctx context.Context, path, watermark string, state *replayState) error {
	if err := segment.WalkActive(path, func(events []segment.Event) error {
		return replayEvents(ctx, events, watermark, state)
	}); err != nil {
		return fmt.Errorf("repoexport: walk active segment %s: %w", path, err)
	}
	return nil
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
