package ingest

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/cockroachdb/pebble"
)

// seqNextKey is the pebble key holding the next seq value to allocate.
// Encoded as 8 little-endian bytes; missing means 0.
const seqNextKey = "seq/next"

// Writer owns the active segment file and the seq counter. It is
// safe for concurrent use.
type Writer struct {
	cfg Config

	mu          sync.Mutex
	active      *segment.Writer
	activeBytes int64
	activeIdx   uint64
	nextSeq     uint64
	closed      bool
}

// Open scans cfg.SegmentsDir, resumes or creates the active segment,
// and reconciles seq/next against any events in the resumed file so
// a crash between block fsync and pebble batch commit can never
// produce duplicate seq numbers.
func Open(cfg Config) (*Writer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.applyDefaults()

	if err := os.MkdirAll(cfg.SegmentsDir, 0o755); err != nil {
		return nil, fmt.Errorf("ingest: mkdir %s: %w", cfg.SegmentsDir, err)
	}

	idx, hasExisting, err := scanSegmentsDir(cfg.SegmentsDir)
	if err != nil {
		return nil, err
	}

	w := &Writer{cfg: cfg, activeIdx: idx}

	var maxSeq uint64
	var foundEvents bool

	if hasExisting {
		path := filepath.Join(cfg.SegmentsDir, segmentFilename(idx))
		seg, segErr := segment.New(segment.Config{
			Path:              path,
			MaxEventsPerBlock: cfg.MaxEventsPerBlock,
		})
		switch {
		case segErr == nil:
			w.active = seg
			info, statErr := os.Stat(path)
			if statErr != nil {
				_ = seg.Close()
				return nil, fmt.Errorf("ingest: stat %s: %w", path, statErr)
			}
			w.activeBytes = info.Size() - int64(segment.ReservedHeaderBytes)

			maxSeq, foundEvents, err = segment.ScanMaxSeq(path)
			if err != nil {
				_ = seg.Close()
				return nil, fmt.Errorf("ingest: scan_max_seq %s: %w", path, err)
			}
		case errors.Is(segErr, segment.ErrSegmentSealed):
			w.activeIdx = idx + 1
			path = filepath.Join(cfg.SegmentsDir, segmentFilename(w.activeIdx))
			seg, segErr = segment.New(segment.Config{
				Path:              path,
				MaxEventsPerBlock: cfg.MaxEventsPerBlock,
			})
			if segErr != nil {
				return nil, fmt.Errorf("ingest: open next segment %s: %w", path, segErr)
			}
			w.active = seg
			w.activeBytes = 0
		default:
			return nil, fmt.Errorf("ingest: open existing %s: %w", path, segErr)
		}
	} else {
		path := filepath.Join(cfg.SegmentsDir, segmentFilename(0))
		seg, segErr := segment.New(segment.Config{
			Path:              path,
			MaxEventsPerBlock: cfg.MaxEventsPerBlock,
		})
		if segErr != nil {
			return nil, fmt.Errorf("ingest: create %s: %w", path, segErr)
		}
		w.active = seg
		w.activeBytes = 0
	}

	pebbleSeq, err := loadNextSeq(cfg.Store)
	if err != nil {
		_ = w.active.Close()
		return nil, err
	}

	reconciled := pebbleSeq
	if foundEvents && maxSeq+1 > reconciled {
		reconciled = maxSeq + 1
	}
	if reconciled > pebbleSeq {
		if err := saveNextSeq(cfg.Store, reconciled); err != nil {
			_ = w.active.Close()
			return nil, err
		}
	}
	w.nextSeq = reconciled

	w.cfg.Metrics.setActiveSegBytes(w.activeBytes)
	w.cfg.Metrics.setNextSeq(w.nextSeq)

	w.cfg.Logger.Info("ingest: opened",
		"segments_dir", cfg.SegmentsDir,
		"active_index", w.activeIdx,
		"active_bytes", w.activeBytes,
		"next_seq", w.nextSeq,
	)

	return w, nil
}

// Close flushes any pending events, persists nextSeq to pebble, and
// closes the active writer file. Idempotent. Does NOT seal — that's
// a rotation-time concern.
//
// Durability ordering matches DESIGN.md §3.1.1: segment.Writer.Close
// fsyncs any buffered block before we commit the pebble batch. If
// the pebble write fails after a successful flush, Open's
// ScanMaxSeq reconciliation will recover the correct nextSeq on
// next start.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.active == nil {
		return nil
	}
	if err := w.active.Close(); err != nil {
		return fmt.Errorf("ingest: close active segment: %w", err)
	}
	if err := saveNextSeq(w.cfg.Store, w.nextSeq); err != nil {
		return err
	}
	return nil
}

// Append writes one event into the active segment. On success,
// mutates ev.Seq in place to the allocated value; on error ev.Seq
// is left untouched so callers can safely retry without observing
// a phantom allocation. Goroutine-safe.
func (w *Writer) Append(ctx context.Context, ev *segment.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		w.cfg.Metrics.incAppendErrors()
		return ErrClosed
	}

	candidate := *ev
	candidate.Seq = w.nextSeq
	full, err := w.active.Append(candidate)
	if err != nil {
		w.cfg.Metrics.incAppendErrors()
		return fmt.Errorf("ingest: append: %w", err)
	}
	ev.Seq = candidate.Seq
	w.nextSeq++
	w.cfg.Metrics.incEventsAppended()
	w.cfg.Metrics.setNextSeq(w.nextSeq)

	if full {
		if err := w.flushAndRotateLocked(ctx); err != nil {
			return err
		}
	}
	return nil
}

// flushAndRotateLocked is the post-Append durability commit. The
// caller MUST hold w.mu.
//
// Order matters per DESIGN.md §3.1.1:
//  1. segment.Writer.Flush fsyncs the block.
//  2. We pebble.Sync the new seq/next.
//  3. If activeBytes >= MaxSegmentBytes, seal the current segment
//     and open the next index. segment.Writer.Seal handles its own
//     torn-tail recovery on partial-seal failure.
//
// Step 1 first ensures a crash between (1) and (2) leaves seq/next
// lagging at most one block; Open's ScanMaxSeq reconciles it.
//
// Spans are emitted at the block-flush and rotation granularity
// (~one per 4096 events / one per ~256MB respectively); per-Append
// spans would balloon to ~1B/day at full network scale.
func (w *Writer) flushAndRotateLocked(ctx context.Context) error {
	tracer := obs.Tracer("ingest")
	ctx, span := tracer.Start(ctx, "ingest.flush_block")
	defer span.End()

	if err := w.active.Flush(); err != nil {
		span.RecordError(err)
		return fmt.Errorf("ingest: flush block: %w", err)
	}
	w.cfg.Metrics.incBlocksFlushed()

	if err := saveNextSeq(w.cfg.Store, w.nextSeq); err != nil {
		span.RecordError(err)
		return err
	}

	path := filepath.Join(w.cfg.SegmentsDir, segmentFilename(w.activeIdx))
	info, err := os.Stat(path)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("ingest: stat active segment: %w", err)
	}
	w.activeBytes = info.Size() - int64(segment.ReservedHeaderBytes)
	w.cfg.Metrics.setActiveSegBytes(w.activeBytes)

	if w.activeBytes < w.cfg.MaxSegmentBytes {
		return nil
	}

	_, rotSpan := tracer.Start(ctx, "ingest.rotate_segment")
	defer rotSpan.End()

	if _, err := w.active.Seal(); err != nil {
		rotSpan.RecordError(err)
		return fmt.Errorf("ingest: seal segment %d: %w", w.activeIdx, err)
	}

	w.activeIdx++
	nextPath := filepath.Join(w.cfg.SegmentsDir, segmentFilename(w.activeIdx))
	next, err := segment.New(segment.Config{
		Path:              nextPath,
		MaxEventsPerBlock: w.cfg.MaxEventsPerBlock,
	})
	if err != nil {
		rotSpan.RecordError(err)
		return fmt.Errorf("ingest: open new active segment %s: %w", nextPath, err)
	}
	w.active = next
	w.activeBytes = 0
	w.cfg.Metrics.setActiveSegBytes(0)
	w.cfg.Metrics.incSegmentsRotated()

	w.cfg.Logger.Info("ingest: rotated segment",
		"new_index", w.activeIdx,
	)
	return nil
}

// NextSeq returns the next seq value the writer will allocate.
// Exposed for tests and observability; production callers should
// not rely on this value being stable across goroutines.
func (w *Writer) NextSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextSeq
}

// ActiveIndex returns the numeric index of the current active segment.
func (w *Writer) ActiveIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.activeIdx
}

// scanSegmentsDir lists cfg.SegmentsDir and returns the highest seg_*
// index seen and whether any matching files exist. Files that don't
// match the seg_<10 base36>.jss pattern are silently skipped — the
// directory may legitimately contain other operator-placed files.
func scanSegmentsDir(dir string) (idx uint64, has bool, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, false, fmt.Errorf("ingest: readdir %s: %w", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		i, ok := parseSegmentIndex(e.Name())
		if !ok {
			continue
		}
		if !has || i > idx {
			idx = i
			has = true
		}
	}
	return idx, has, nil
}

// loadNextSeq reads the persisted seq/next counter. A missing key is
// not an error; it means "fresh data dir" and reads as zero.
func loadNextSeq(st *store.Store) (uint64, error) {
	val, closer, err := st.Get([]byte(seqNextKey))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("ingest: load %s: %w", seqNextKey, err)
	}
	defer func() { _ = closer.Close() }()

	if len(val) != 8 {
		return 0, fmt.Errorf("ingest: %s has wrong length %d (want 8)", seqNextKey, len(val))
	}
	return binary.LittleEndian.Uint64(val), nil
}

// saveNextSeq durably persists the seq counter via pebble.Sync. The
// fsync is the durability anchor for the per-block ordering DESIGN.md
// §3.1.1 calls out.
func saveNextSeq(st *store.Store, v uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	if err := st.Set([]byte(seqNextKey), buf[:], pebble.Sync); err != nil {
		return fmt.Errorf("ingest: save %s: %w", seqNextKey, err)
	}
	return nil
}
