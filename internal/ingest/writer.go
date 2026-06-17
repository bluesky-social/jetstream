package ingest

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// seqNextKey is the legacy default value for Config.SeqKey. Kept
// as a constant to anchor the back-compat behavior of fresh data
// dirs created before SeqKey existed.
const seqNextKey = "seq/next"

// Writer owns the active segment file and the seq counter. It is
// safe for concurrent use.
type Writer struct {
	cfg Config

	// drainMu is an admission barrier for appends and explicit flushes. Acquire
	// it before mu, and hold it through async job submission so DrainDurability
	// can safely wait for all already-admitted jobs without racing future Add.
	drainMu        sync.Mutex
	mu             sync.Mutex
	active         *segment.Writer
	activeBytes    int64
	activeIdx      uint64
	nextSeq        uint64
	durableNextSeq uint64
	closed         bool

	async            *asyncFlushPipeline
	asyncPrepared    int
	asyncJobs        sync.WaitGroup
	nextAsyncFlushID uint64
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
	cfg.Logger = cfg.Logger.With(slog.String("component", "ingest/writer"))

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
		path := filepath.Join(cfg.SegmentsDir, SegmentFilename(idx))
		seg, segErr := segment.New(segment.Config{
			Path:              path,
			MaxEventsPerBlock: cfg.MaxEventsPerBlock,
			Metrics:           cfg.SegmentMetrics,
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
			path = filepath.Join(cfg.SegmentsDir, SegmentFilename(w.activeIdx))
			seg, segErr = segment.New(segment.Config{
				Path:              path,
				MaxEventsPerBlock: cfg.MaxEventsPerBlock,
				Metrics:           cfg.SegmentMetrics,
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
		path := filepath.Join(cfg.SegmentsDir, SegmentFilename(0))
		seg, segErr := segment.New(segment.Config{
			Path:              path,
			MaxEventsPerBlock: cfg.MaxEventsPerBlock,
			Metrics:           cfg.SegmentMetrics,
		})
		if segErr != nil {
			return nil, fmt.Errorf("ingest: create %s: %w", path, segErr)
		}
		w.active = seg
		w.activeBytes = 0
	}

	pebbleSeq, err := loadNextSeq(cfg.Store, cfg.SeqKey)
	if err != nil {
		_ = w.active.Close()
		return nil, err
	}

	reconciled := pebbleSeq
	if foundEvents && maxSeq+1 > reconciled {
		reconciled = maxSeq + 1
	}
	if reconciled > pebbleSeq {
		if err := saveNextSeq(cfg.Store, cfg.SeqKey, reconciled); err != nil {
			_ = w.active.Close()
			return nil, err
		}
	}
	w.nextSeq = reconciled
	w.durableNextSeq = reconciled

	w.cfg.Metrics.setActiveSegBytes(w.activeBytes)
	w.cfg.Metrics.setNextSeq(w.nextSeq)

	if cfg.AsyncFlushWorkers > 0 {
		w.async = newAsyncFlushPipeline(w, cfg.AsyncFlushWorkers)
	}

	w.cfg.Logger.Info("opened",
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
	if w.async != nil {
		return w.closeAsync()
	}

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
	if err := w.commitTerminalDurableBatchLocked(); err != nil {
		return err
	}
	return nil
}

// SealActiveAndClose flushes any pending block, seals the active
// segment file (writes the variable-length footer and finalizes the
// 256-byte fixed header), persists nextSeq, publishes OnAfterSeal,
// and closes the writer. Idempotent.
//
// Used by the orchestrator at cutover time to finalize the
// bootstrap-phase live_segments writer's trailing active file so the
// `backfill/live_segments/` tree is fully sealed once steady-state
// begins. Steady-state callers should continue to use Close()
// instead — sealing during normal operation is a rotation-time
// concern handled inside flushAndRotateLocked.
func (w *Writer) SealActiveAndClose() error {
	if w.async != nil {
		return w.sealActiveAndCloseAsync()
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.active == nil {
		return nil
	}
	// segment.Writer.Seal handles flushing any pending events itself
	// (and is a no-op on an empty pending block), so we don't have to
	// pre-flush here. Seal closes the underlying file on success;
	// failure paths leave the file open with stickyErr latched, so we
	// release the fd ourselves below.
	//
	// Durability ordering matches Close (DESIGN.md §3.1.1): the segment
	// fsyncs first, then we pebble.Sync nextSeq. A crash between the
	// two leaves nextSeq lagging at most one block, which Open's
	// ScanMaxSeq reconciles on next start.
	if _, err := w.active.Seal(); err != nil {
		if cerr := w.active.Close(); cerr != nil {
			w.cfg.Logger.Warn("close after failed seal", "err", cerr)
		}
		return fmt.Errorf("ingest: seal active segment: %w", err)
	}
	if err := w.commitTerminalDurableBatchLocked(); err != nil {
		return err
	}
	sealedIdx := w.activeIdx
	sealedPath := filepath.Join(w.cfg.SegmentsDir, SegmentFilename(sealedIdx))
	if err := w.onAfterSealLocked(sealedIdx, sealedPath); err != nil {
		return err
	}
	return nil
}

// Append writes one event into the active segment. On success,
// mutates ev.Seq in place to the allocated value; on error ev.Seq
// is left untouched so callers can safely retry without observing
// a phantom allocation. Goroutine-safe.
func (w *Writer) Append(ctx context.Context, ev *segment.Event) error {
	if w.async != nil {
		w.drainMu.Lock()
		w.mu.Lock()
		job, err := w.appendLocked(ctx, ev)
		w.mu.Unlock()
		if err == nil {
			w.submitAsyncFlushes([]*asyncFlushJob{job})
		}
		w.drainMu.Unlock()
		if err != nil {
			return err
		}
		return w.waitSubmittedAsyncFlushes([]*asyncFlushJob{job})
	}

	w.drainMu.Lock()
	defer w.drainMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()

	_, err := w.appendLocked(ctx, ev)
	return err
}

// AppendBatch writes a bounded caller-provided event batch while holding the
// writer lock once. On success, mutates each event's Seq in place to the
// allocated value. On an error before an event is appended, that event and all
// later events are left untouched. If a flush or hook fails after appending an
// event, the error semantics match Append.
func (w *Writer) AppendBatch(ctx context.Context, events []segment.Event) error {
	if len(events) == 0 {
		return nil
	}

	w.drainMu.Lock()
	w.mu.Lock()
	var jobs []*asyncFlushJob
	var appendErr error

	for i := range events {
		job, err := w.appendLocked(ctx, &events[i])
		if job != nil {
			jobs = append(jobs, job)
		}
		if err != nil {
			appendErr = err
			break
		}
	}
	w.mu.Unlock()
	w.submitAsyncFlushes(jobs)
	w.drainMu.Unlock()

	flushErr := w.waitSubmittedAsyncFlushes(jobs)
	if appendErr != nil {
		return appendErr
	}
	return flushErr
}

func (w *Writer) appendLocked(ctx context.Context, ev *segment.Event) (*asyncFlushJob, error) {
	if w.closed {
		w.cfg.Metrics.incAppendErrors()
		return nil, ErrClosed
	}

	candidate := *ev
	candidate.Seq = w.nextSeq
	full, err := w.active.Append(candidate)
	if err != nil {
		w.cfg.Metrics.incAppendErrors()
		return nil, fmt.Errorf("ingest: append: %w", err)
	}
	ev.Seq = candidate.Seq
	w.nextSeq++
	w.cfg.Metrics.incEventsAppended()
	w.cfg.Metrics.setNextSeq(w.nextSeq)

	if w.cfg.OnAppend != nil {
		if err := w.cfg.OnAppend(ev); err != nil {
			return nil, fmt.Errorf("ingest: on_append: %w", err)
		}
	}

	if full {
		if w.async != nil {
			job, err := w.prepareAsyncFlushLocked()
			if err != nil {
				return nil, err
			}
			return job, nil
		}
		if err := w.flushAndRotateLocked(ctx); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// Flush forces any pending block to fsync. Goroutine-safe. Used by
// the merge phase to order destination-segment durability before
// its cursor commit (DESIGN.md §3.1.1 / merge spec §5.2). Steady-
// state callers do not need to call this — the per-block-fill path
// inside Append handles ordinary flush + rotation.
//
// A no-op when nothing is buffered.
func (w *Writer) Flush(ctx context.Context) error {
	if w.async != nil {
		w.drainMu.Lock()
		job, err := w.prepareAsyncFlushForFlush()
		if err == nil {
			w.submitAsyncFlushes([]*asyncFlushJob{job})
		}
		w.drainMu.Unlock()
		if err != nil {
			return err
		}
		return w.waitSubmittedAsyncFlushes([]*asyncFlushJob{job})
	}

	w.drainMu.Lock()
	defer w.drainMu.Unlock()
	return w.flushSync(ctx)
}

func (w *Writer) prepareAsyncFlushForFlush() (*asyncFlushJob, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, ErrClosed
	}
	if w.active == nil {
		return nil, nil
	}
	return w.prepareAsyncFlushLocked()
}

func (w *Writer) flushSync(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.active == nil {
		return nil
	}
	return w.flushAndRotateLocked(ctx)
}

func (w *Writer) drainAsync(ctx context.Context) error {
	job, err := w.prepareAsyncFlushForFlush()
	if err != nil {
		return err
	}
	w.submitAsyncFlushes([]*asyncFlushJob{job})
	if err := w.waitSubmittedAsyncFlushes([]*asyncFlushJob{job}); err != nil {
		return err
	}
	w.asyncJobs.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	return w.commitDurableBatchLocked(ctx, w.durableNextSeq, true)
}

func (w *Writer) drainSync(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.active == nil {
		return nil
	}
	if w.active.Pending() > 0 {
		if err := w.flushAndRotateLocked(ctx); err != nil {
			return err
		}
	}
	return w.commitDurableBatchLocked(ctx, w.durableNextSeq, true)
}

// DrainDurability forces pending event-backed metadata to its block durability
// point and commits metadata-only durable hooks even when no events are pending.
func (w *Writer) DrainDurability(ctx context.Context) error {
	w.drainMu.Lock()
	defer w.drainMu.Unlock()

	if w.async != nil {
		return w.drainAsync(ctx)
	}
	return w.drainSync(ctx)
}

// SetDurableBatchHook installs or replaces the metadata hook invoked when the
// writer commits durable block metadata. Backfill Run owns this hook for the
// backfill writer and intentionally replaces any prior hook. Callers should
// wire this before starting producers.
func (w *Writer) SetDurableBatchHook(h DurableBatchHook) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cfg.OnDurableBatch = h
}

func (w *Writer) commitTerminalDurableBatchLocked() error {
	return w.commitDurableBatchLocked(context.Background(), w.nextSeq, true)
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
	return obs.Span(ctx, func(ctx context.Context) error {
		if err := w.flushBlockLocked(ctx); err != nil {
			return err
		}

		path := filepath.Join(w.cfg.SegmentsDir, SegmentFilename(w.activeIdx))
		info, statErr := os.Stat(path)
		if statErr != nil {
			return fmt.Errorf("ingest: stat active segment: %w", statErr)
		}
		w.activeBytes = info.Size() - int64(segment.ReservedHeaderBytes)
		w.cfg.Metrics.setActiveSegBytes(w.activeBytes)

		if w.activeBytes < w.cfg.MaxSegmentBytes {
			return nil
		}

		return w.rotateLocked(ctx)
	})
}

// flushBlockLocked is the shared flush-side of the durability commit:
// fsync the pending block, pebble.Sync seq/next, fire OnAfterFlush.
// The caller MUST hold w.mu.
func (w *Writer) flushBlockLocked(ctx context.Context) error {
	if err := w.active.Flush(); err != nil {
		return fmt.Errorf("ingest: flush block: %w", err)
	}
	w.cfg.Metrics.incBlocksFlushed()

	if err := w.commitDurableBatchLocked(ctx, w.nextSeq, false); err != nil {
		return err
	}

	if w.cfg.OnAfterFlush != nil {
		if err := w.cfg.OnAfterFlush(ctx); err != nil {
			return fmt.Errorf("ingest: on_after_flush: %w", err)
		}
	}
	return nil
}

// ForceRotate flushes any pending block and rotates the active segment
// regardless of its size, so the just-sealed file becomes visible to
// the delete compactor's sealed-segment sweep. A no-op when the active
// segment holds zero events — rotating an empty file would churn out
// empty sealed segments (e.g. every compaction interval while the
// upstream relay is down) for no compliance benefit. Goroutine-safe;
// concurrent Appends serialize against the rotation on w.mu.
func (w *Writer) ForceRotate(ctx context.Context) error {
	if w.async != nil {
		return fmt.Errorf("ingest: force rotate is not supported with async flush")
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.active.Pending() == 0 && len(w.active.Blocks()) == 0 {
		return nil
	}
	if err := w.flushBlockLocked(ctx); err != nil {
		return err
	}
	return w.rotateLocked(ctx)
}

// rotateLocked seals the active segment and opens the next one. The
// caller MUST hold w.mu. Split out from flushAndRotateLocked so its
// span (rotateLocked) is a clear child rather than buried inside the
// parent.
func (w *Writer) rotateLocked(ctx context.Context) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		sealedIdx := w.activeIdx
		sealedPath := filepath.Join(w.cfg.SegmentsDir, SegmentFilename(sealedIdx))
		if _, err := w.active.Seal(); err != nil {
			return fmt.Errorf("ingest: seal segment %d: %w", sealedIdx, err)
		}

		if err := w.onAfterSealLocked(sealedIdx, sealedPath); err != nil {
			return err
		}

		w.activeIdx++
		trace.SpanFromContext(ctx).SetAttributes(attribute.Int64("active_idx", int64(w.activeIdx)))

		nextPath := filepath.Join(w.cfg.SegmentsDir, SegmentFilename(w.activeIdx))
		next, err := segment.New(segment.Config{
			Path:              nextPath,
			MaxEventsPerBlock: w.cfg.MaxEventsPerBlock,
			Metrics:           w.cfg.SegmentMetrics,
		})
		if err != nil {
			return fmt.Errorf("ingest: open new active segment %s: %w", nextPath, err)
		}
		w.active = next
		w.activeBytes = 0
		w.cfg.Metrics.setActiveSegBytes(0)
		w.cfg.Metrics.incSegmentsRotated()

		w.cfg.Logger.InfoContext(ctx, "rotated segment", "new_index", w.activeIdx)
		return nil
	})
}

// onAfterSealLocked publishes a newly sealed segment. The caller MUST
// hold w.mu; hooks must not call back into Writer.
func (w *Writer) onAfterSealLocked(idx uint64, path string) error {
	if w.cfg.OnAfterSeal == nil {
		return nil
	}
	if err := w.cfg.OnAfterSeal(idx, path); err != nil {
		return fmt.Errorf("ingest: on_after_seal: %w", err)
	}
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

// SnapshotPending returns a copy of every event currently buffered in
// the active segment's pending block (not yet flushed to disk). The
// snapshot is taken under w.mu so it is consistent with concurrent
// Append calls; the returned events have their variable-length fields
// copied out of the writer's column buffers and are safe to retain.
//
// Used by the lookback replay engine in internal/subscribe to bridge
// disk events to live events at a cursor handoff without forcing a
// flush.
func (w *Writer) SnapshotPending() []segment.Event {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.active == nil {
		return nil
	}
	return w.active.SnapshotPending()
}

// ActiveIndex returns the numeric index of the current active segment.
func (w *Writer) ActiveIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.activeIdx
}

// SegmentsDir returns the configured segments directory. The cfg is
// read-only after Open, so no mutex is needed.
func (w *Writer) SegmentsDir() string {
	return w.cfg.SegmentsDir
}

// SegmentFile is one entry in a SegmentFiles result.
type SegmentFile struct {
	Idx  uint64
	Path string
}

// SegmentFiles returns every seg_*.jss file under dir, sorted by
// numeric index ascending. Non-segment files and subdirectories are
// silently skipped — the directory may legitimately contain other
// operator-placed files.
//
// Used by every consumer that needs the full segment manifest in
// creation order: the merge phase draining live_segments/, the
// delete/update compactor, and inspect tooling.
func SegmentFiles(dir string) ([]SegmentFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("ingest: readdir %s: %w", dir, err)
	}
	out := make([]SegmentFile, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		idx, ok := ParseSegmentIndex(e.Name())
		if !ok {
			continue
		}
		out = append(out, SegmentFile{Idx: idx, Path: filepath.Join(dir, e.Name())})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Idx < out[j].Idx })
	return out, nil
}

// scanSegmentsDir lists cfg.SegmentsDir and returns the highest seg_*
// index seen and whether any matching files exist. Thin wrapper over
// SegmentFiles preserved for the writer-open path.
func scanSegmentsDir(dir string) (idx uint64, has bool, err error) {
	files, err := SegmentFiles(dir)
	if err != nil {
		return 0, false, err
	}
	if len(files) == 0 {
		return 0, false, nil
	}
	last := files[len(files)-1]
	return last.Idx, true, nil
}

// loadNextSeq reads the persisted seq/next counter for key. A missing
// key is not an error; it means "fresh data dir" and reads as zero.
func loadNextSeq(st *store.Store, key string) (uint64, error) {
	v, _, err := st.GetUint64LE(key)
	return v, err
}

// saveNextSeq durably persists the seq counter for key via pebble.Sync.
func saveNextSeq(st *store.Store, key string, v uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	if err := st.Set([]byte(key), buf[:], store.SyncWrites); err != nil {
		return fmt.Errorf("ingest: save %s: %w", key, err)
	}
	return nil
}

func stageNextSeq(b *pebble.Batch, key string, v uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	if err := b.Set([]byte(key), buf[:], nil); err != nil {
		return fmt.Errorf("ingest: stage %s: %w", key, err)
	}
	return nil
}

func (w *Writer) commitDurableBatchLocked(ctx context.Context, nextSeq uint64, force bool) error {
	b := w.cfg.Store.NewBatch()
	defer func() { _ = b.Close() }()

	if err := stageNextSeq(b, w.cfg.SeqKey, nextSeq); err != nil {
		return err
	}
	var afterCommit func()
	var afterDone func(error)
	var commitErr error
	if w.cfg.OnDurableBatch != nil {
		cb, done, err := w.cfg.OnDurableBatch(ctx, b, nextSeq, force)
		if err != nil {
			return fmt.Errorf("ingest: on_durable_batch: %w", err)
		}
		afterCommit = cb
		afterDone = done
	}
	defer func() {
		if afterDone != nil {
			afterDone(commitErr)
		}
	}()

	commitErr = w.cfg.Store.Commit(b, store.SyncWrites)
	if commitErr != nil {
		return fmt.Errorf("ingest: commit durable batch: %w", commitErr)
	}
	w.durableNextSeq = nextSeq
	if afterCommit != nil {
		afterCommit()
	}
	return nil
}
