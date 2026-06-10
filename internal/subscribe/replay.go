package subscribe

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/manifest"
	"github.com/bluesky-social/jetstream-v2/segment"
)

// WalkInput is the parameter bundle for WalkFromCursor.
type WalkInput struct {
	// StartSeq is the smallest seq the walker will emit. Events with
	// Seq < StartSeq are skipped silently.
	StartSeq uint64

	// Manifest is the in-memory segment manifest. May be nil; callers
	// without sealed segments still walk the active segment + pending.
	Manifest *manifest.Manifest

	// Writer is the ingest writer; the walker reads its active
	// segment's flushed blocks and SnapshotPending() events to extend
	// past the sealed-segment region. Required.
	Writer *ingest.Writer

	// BlockCache, when non-nil, serves sealed-block decodes through the shared
	// cache instead of decoding directly. Optional; nil preserves direct decode.
	BlockCache *blockCache
}

// WalkFromCursor invokes emit for every durable event with
// Seq >= input.StartSeq, in seq order, across:
//
//  1. the sealed-segment region from the manifest,
//  2. the active segment's flushed blocks,
//  3. the active segment's in-memory pending block.
//
// Halts when emit returns a non-nil error and surfaces the error
// (errors.Is is honored).
//
// Pure-function design: WalkFromCursor holds no subscriber state. The
// bounded cold reader (NewColdReader) composes it with a batch limit and
// the shared block cache to serve Tail's cold-path reads.
func WalkFromCursor(ctx context.Context, input WalkInput, emit func(*segment.Event) error) error {
	current := input.StartSeq

	// 1. Sealed segments.
	if input.Manifest != nil {
		if err := input.Manifest.Wait(ctx); err != nil {
			return err
		}
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			bounds, ok := input.Manifest.SegmentForSeq(current)
			if !ok {
				break
			}
			next, err := walkSealedSegment(input.Manifest, bounds, current, input.BlockCache, emit)
			if err != nil {
				return err
			}
			current = next
		}
	}

	// 2. Active segment's flushed blocks.
	activeIdx := input.Writer.ActiveIndex()
	activePath := filepath.Join(input.Writer.SegmentsDir(), ingest.SegmentFilename(activeIdx))
	walkErr := segment.WalkActive(activePath, func(events []segment.Event) error {
		for i := range events {
			if events[i].Seq < current {
				continue
			}
			ev := events[i]
			if err := emit(&ev); err != nil {
				return err
			}
			current = events[i].Seq + 1
		}
		return nil
	})
	if walkErr != nil && !errors.Is(walkErr, os.ErrNotExist) {
		return fmt.Errorf("walk active: %w", walkErr)
	}

	// 3. Pending in-memory block.
	pending := input.Writer.SnapshotPending()
	for i := range pending {
		if pending[i].Seq < current {
			continue
		}
		if err := emit(&pending[i]); err != nil {
			return err
		}
		current = pending[i].Seq + 1
	}
	return nil
}

func decodeSealedBlock(cache *blockCache, segIdx uint64, blockIdx int, r *segment.Reader) ([]segment.Event, error) {
	if cache == nil {
		return r.DecodeBlock(blockIdx)
	}
	return cache.getOrDecode(
		blockKey{segIdx: segIdx, checksum: r.Header().Checksum, blockIdx: uint64(blockIdx)},
		func() ([]segment.Event, error) { return r.DecodeBlock(blockIdx) },
	)
}

func walkSealedSegment(m *manifest.Manifest, bounds manifest.SegmentBounds, current uint64, cache *blockCache, emit func(*segment.Event) error) (uint64, error) {
	blocks, err := m.BlockIndex(bounds.Idx)
	if err != nil {
		return current, fmt.Errorf("block index for seg %d: %w", bounds.Idx, err)
	}

	r, err := segment.Open(segment.ReaderConfig{Path: bounds.Path, SkipChecksum: true})
	if err != nil {
		return current, fmt.Errorf("open seg %d: %w", bounds.Idx, err)
	}
	defer func() { _ = r.Close() }()

	for i, block := range blocks {
		if block.MaxSeq < current {
			continue
		}
		events, err := decodeSealedBlock(cache, bounds.Idx, i, r)
		if err != nil {
			return current, fmt.Errorf("decode seg %d block %d: %w", bounds.Idx, i, err)
		}
		for j := range events {
			if events[j].Seq < current {
				continue
			}
			ev := events[j]
			if err := emit(&ev); err != nil {
				return current, err
			}
			current = events[j].Seq + 1
		}
	}
	return current, nil
}

// DefaultBlockCacheBytes bounds the shared decoded-block cache for the cold
// (disk replay) path. Smaller than the hot ring: the cold path is the
// less-common case. Operator-tunable via --subscribe-block-cache-bytes.
const DefaultBlockCacheBytes = 64 << 20

// ColdReaderConfig wires the cold (disk) read path. The writer is held by
// reference (atomic.Pointer) because cmd/jetstream publishes it after
// steady-state begins; before then a cold read returns errColdUnavailable.
type ColdReaderConfig struct {
	Manifest        *manifest.Manifest
	WriterRef       *atomic.Pointer[ingest.Writer]
	BlockCacheBytes int // 0 -> DefaultBlockCacheBytes
}

// errBatchFull is the sentinel the bounded collector returns to stop the
// walk once max entries are gathered. Never escapes NewColdReader's closure.
var errBatchFull = errors.New("subscribe: cold batch full")

// NewColdReader returns a coldReader that serves a bounded batch from disk
// via WalkFromCursor, routing sealed-block decodes through a shared, byte-
// bounded block cache owned by this closure. It stops after max events and
// reports the next cursor so the subscriber loop resumes contiguously.
func NewColdReader(cfg ColdReaderConfig) coldReader {
	bytes := cfg.BlockCacheBytes
	if bytes <= 0 {
		bytes = DefaultBlockCacheBytes
	}
	cache := newBlockCache(bytes)
	return func(ctx context.Context, cursor uint64, max int) ([]*Entry, uint64, error) {
		w := cfg.WriterRef.Load()
		if w == nil {
			return nil, cursor, errColdUnavailable
		}
		batch := make([]*Entry, 0, max)
		next := cursor
		err := WalkFromCursor(ctx, WalkInput{
			StartSeq:   cursor,
			Manifest:   cfg.Manifest,
			Writer:     w,
			BlockCache: cache,
		}, func(ev *segment.Event) error {
			cp := *ev
			batch = append(batch, newEntry(&cp))
			next = ev.Seq + 1
			if len(batch) >= max {
				return errBatchFull
			}
			return nil
		})
		if err != nil && !errors.Is(err, errBatchFull) {
			return nil, cursor, err
		}
		return batch, next, nil
	}
}
