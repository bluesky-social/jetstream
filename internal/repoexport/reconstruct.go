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

// Config controls local repo reconstruction.
type Config struct {
	DataDir string
	DID     string

	// PendingEvents are events buffered in the live writer's in-memory
	// pending block that have not yet been flushed to a segment file on
	// disk. They are replayed after all on-disk segments so a record
	// created moments ago (e.g. a like) is reflected in the snapshot
	// immediately, rather than only after the next compaction-driven
	// flush rotates the active segment. Optional; nil for offline
	// reconstruction with no live writer. The slice is the caller's
	// already-copied SnapshotPending() result and is not mutated.
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

// Reconstruct replays local segment files and returns the current repo
// snapshot for cfg.DID.
func Reconstruct(ctx context.Context, cfg Config) (Snapshot, error) {
	if cfg.DataDir == "" {
		return Snapshot{}, errors.New("repoexport: DataDir is required")
	}
	if cfg.DID == "" {
		return Snapshot{}, errors.New("repoexport: DID is required")
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}

	state := replayState{
		did:     cfg.DID,
		records: make(map[string][]byte),
	}

	if err := replayDir(ctx, filepath.Join(cfg.DataDir, "segments"), "", &state); err != nil {
		return Snapshot{}, err
	}
	primaryWatermark := state.latestRev
	if err := replayDir(ctx, filepath.Join(cfg.DataDir, "backfill", "live_segments"), primaryWatermark, &state); err != nil {
		return Snapshot{}, err
	}

	// Pending events live in the live writer's active segment, after every
	// block already flushed to disk, so they carry the newest revs and need
	// no watermark filter. replayEvents applies the same DID/kind filtering
	// as the on-disk path.
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
	case segment.KindCreate, segment.KindUpdate:
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
