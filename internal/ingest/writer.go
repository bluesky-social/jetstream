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

	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
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
	readLog        *ReadableLog
	closed         bool

	async            *asyncFlushPipeline
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
		return nil, cfg.wrapSegmentPersistenceError("creating segments directory", fmt.Errorf("ingest: mkdir %s: %w", cfg.SegmentsDir, err))
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
			IOFaultInjector:   cfg.SegmentIOFaultInjector,
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
			// The highest-index segment is already sealed (e.g. the
			// orchestrator sealed it at cutover). Read its header MaxSeq so
			// the reconcile below can floor nextSeq past it — symmetric with
			// the active-branch ScanMaxSeq above. Normally seq/next was
			// persisted before the seal (rotateLocked flushes the counter,
			// then seals), so this is a no-op; but if the counter is missing
			// or an illegal 0, this is what stops the next append from reusing
			// a seq the sealed segment already contains.
			//
			// Open via the checksum-verifying Reader, NOT ReadSealedHeader:
			// this floor IS a corruption safety net, so it must not itself
			// trust an unverified header. ReadSealedHeader only validates
			// magic/version; it does not verify the xxh3 over header+footer,
			// which covers MaxSeq/EventCount. A corrupt MaxSeq read blindly
			// would floor nextSeq off garbage (silent seq reuse or a seq gap).
			// segment.Open fails loud on a bad checksum — crash > corruption.
			r, openErr := segment.Open(segment.ReaderConfig{Path: path})
			if openErr != nil {
				return nil, fmt.Errorf("ingest: open sealed %s: %w", path, openErr)
			}
			hdr := r.Header()
			if closeErr := r.Close(); closeErr != nil {
				return nil, fmt.Errorf("ingest: close sealed %s: %w", path, closeErr)
			}
			// Gate on MaxSeq, not EventCount: a fully-compacted segment validly
			// has EventCount==0 (every row dropped) while a rewrite PRESERVES the
			// original MaxSeq envelope (segment/rewrite.go restores MinSeq/MaxSeq
			// even when all rows are dropped). That historical envelope still owns
			// seqs (MinSeq..MaxSeq], so the floor must advance past it. Seqs start
			// at 1 (design §R8), so MaxSeq==0 is the unambiguous "never held an
			// event" sentinel — a truly empty fresh-sealed segment, which correctly
			// falls through to the nextSeq=1 floor below. Without this an
			// EventCount==0/MaxSeq>0 highest segment plus a missing/illegal-0
			// seq/next would floor nextSeq to 1 and reuse the envelope's seqs.
			if hdr.MaxSeq > 0 {
				maxSeq, foundEvents = hdr.MaxSeq, true
			}

			w.activeIdx = idx + 1
			path = filepath.Join(cfg.SegmentsDir, SegmentFilename(w.activeIdx))
			seg, segErr = segment.New(segment.Config{
				Path:              path,
				MaxEventsPerBlock: cfg.MaxEventsPerBlock,
				Metrics:           cfg.SegmentMetrics,
				IOFaultInjector:   cfg.SegmentIOFaultInjector,
			})
			if segErr != nil {
				return nil, cfg.wrapSegmentPersistenceError("opening next active segment", fmt.Errorf("ingest: open next segment %s: %w", path, segErr))
			}
			w.active = seg
			w.activeBytes = 0
		default:
			return nil, cfg.wrapSegmentPersistenceError("opening existing active segment", fmt.Errorf("ingest: open existing %s: %w", path, segErr))
		}
	} else {
		path := filepath.Join(cfg.SegmentsDir, SegmentFilename(0))
		seg, segErr := segment.New(segment.Config{
			Path:              path,
			MaxEventsPerBlock: cfg.MaxEventsPerBlock,
			Metrics:           cfg.SegmentMetrics,
			IOFaultInjector:   cfg.SegmentIOFaultInjector,
		})
		if segErr != nil {
			return nil, cfg.wrapSegmentPersistenceError("creating active segment", fmt.Errorf("ingest: create %s: %w", path, segErr))
		}
		w.active = seg
		w.activeBytes = 0
	}

	// If the highest segment yielded no seq envelope — a truly empty active
	// segment (ScanMaxSeq found nothing) OR an empty / compacted-to-empty sealed
	// segment (MaxSeq==0) — the recovery floor must still account for seqs owned
	// by LOWER sealed segments. Segments are seq-monotonic in creation order, so
	// the highest non-empty segment below idx holds the global max envelope.
	// Without this, a missing or illegal-0 seq/next would floor nextSeq to 1 and
	// reuse seqs those lower segments already own (the exact corruption this
	// recovery path defends against). Only runs in that rare recovery case: a
	// healthy highest segment with events sets foundEvents=true above, and a
	// fresh dir has no existing segments.
	if !foundEvents && hasExisting {
		tailMax, tailFound, tailErr := recoverSealedTailMaxSeq(cfg.SegmentsDir, idx)
		if tailErr != nil {
			_ = w.active.Close()
			return nil, tailErr
		}
		if tailFound {
			maxSeq, foundEvents = tailMax, true
		}
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
			return nil, cfg.wrapSegmentPersistenceError("reconciling durable seq metadata", err)
		}
	}
	// nextSeq is never 0: seq 0 is the pure "nothing yet" sentinel (design §R8)
	// and is never allocated to an event. reconciled == 0 here means no recovered
	// events AND a persisted counter of 0 — either a fresh dir (absent counter,
	// GetUint64LE -> 0) or an illegal persisted seq/next=0 (no current build
	// writes one; only a pre-seed build or on-disk corruption could). Both floor
	// to 1, so the first-ever event is seq 1. Floored in memory only — placed
	// after the reconcile save above, so Open still never writes pebble for a
	// fresh dir; the first block flush (or Close) persists the counter as usual.
	// The crash-recovery reconcile (maxSeq+1) yields >= 2 when foundEvents, so
	// this is a no-op on any dir that recovered events.
	if reconciled < 1 {
		reconciled = 1
	}
	w.nextSeq = reconciled
	w.durableNextSeq = reconciled
	w.readLog = newReadableLog(reconciled, cfg.ReadLogRetentionBytes, cfg.Metrics)

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
		return w.wrapSegmentPersistenceError("closing active segment", fmt.Errorf("ingest: close active segment: %w", err))
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
		return w.wrapSegmentPersistenceError("sealing active segment", fmt.Errorf("ingest: seal active segment: %w", err))
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
		if err := w.waitSubmittedAsyncFlushes([]*asyncFlushJob{job}); err != nil {
			return err
		}
		return w.rotateIfFull(ctx)
	}

	w.drainMu.Lock()
	defer w.drainMu.Unlock()
	err := func() error {
		w.mu.Lock()
		defer w.mu.Unlock()
		_, err := w.appendLocked(ctx, ev)
		return err
	}()
	if err != nil {
		return err
	}
	return nil
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
	if flushErr != nil {
		return flushErr
	}
	return w.rotateIfFull(ctx)
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
	w.readLog.append(&candidate)

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
// Flush deliberately does not rotate: size-based rotation is the
// concern of Append/AppendBatch (sync: flushAndRotateLocked; async:
// rotateIfFull). A segment pushed over MaxSegmentBytes by an explicit
// Flush alone stays active until the next append, which then rotates
// it — bounded overshoot, since appends dominate in every caller.
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
	return w.commitDurableBatchLocked(ctx, w.durableNextSeq, true, w.sampleDurableBatchPrepareValueLocked())
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
	return w.commitDurableBatchLocked(ctx, w.durableNextSeq, true, w.sampleDurableBatchPrepareValueLocked())
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
	return w.commitDurableBatchLocked(context.Background(), w.nextSeq, true, w.sampleDurableBatchPrepareValueLocked())
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
// fsync the pending block, then pebble.Sync seq/next and durable-batch
// metadata.
// The caller MUST hold w.mu.
func (w *Writer) flushBlockLocked(ctx context.Context) error {
	prepareValue := w.sampleDurableBatchPrepareValueLocked()
	if err := w.active.Flush(); err != nil {
		return w.wrapSegmentPersistenceError("flushing active segment block", fmt.Errorf("ingest: flush block: %w", err))
	}
	w.cfg.Metrics.incBlocksFlushed()

	if err := w.commitDurableBatchLocked(ctx, w.nextSeq, false, prepareValue); err != nil {
		return err
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
	w.drainMu.Lock()
	defer w.drainMu.Unlock()

	if w.async != nil {
		w.asyncJobs.Wait()
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
			return w.wrapSegmentPersistenceError("sealing active segment", fmt.Errorf("ingest: seal segment %d: %w", sealedIdx, err))
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
			IOFaultInjector:   w.cfg.SegmentIOFaultInjector,
		})
		if err != nil {
			return w.wrapSegmentPersistenceError("opening new active segment", fmt.Errorf("ingest: open new active segment %s: %w", nextPath, err))
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

// ReadLog returns the writer-owned readable log.
func (w *Writer) ReadLog() *ReadableLog {
	return w.readLog
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

// recoverSealedTailMaxSeq finds the highest seq envelope among the sealed
// segments STRICTLY BELOW highestIdx, scanning newest-first and returning the
// first non-empty one (segments are seq-monotonic in creation order, so the
// highest-index non-empty segment holds the global max). It is the fallback for
// Open's recovery floor when the highest segment carries no envelope of its own
// (a truly empty active segment, or an empty/compacted-empty sealed segment):
// without it a missing/illegal-0 seq/next would reuse seqs a lower segment owns.
//
// Each candidate is opened with the checksum-verifying segment.Open (not
// ReadSealedHeader): like the highest-segment branch, this is a corruption
// safety net and must not floor nextSeq off an unverified header. A still-active
// (unsealed) lower segment is impossible here — only the highest index is ever
// the active file — but ErrActiveSegment is treated as "no usable envelope" and
// skipped rather than failing the open, so a partially-rotated dir still
// recovers. Any other open error is returned: a corrupt lower segment must
// crash Open, not silently lower the floor.
func recoverSealedTailMaxSeq(dir string, highestIdx uint64) (maxSeq uint64, found bool, err error) {
	files, err := SegmentFiles(dir)
	if err != nil {
		return 0, false, err
	}
	for i := len(files) - 1; i >= 0; i-- {
		if files[i].Idx >= highestIdx {
			continue
		}
		r, openErr := segment.Open(segment.ReaderConfig{Path: files[i].Path})
		if openErr != nil {
			if errors.Is(openErr, segment.ErrActiveSegment) {
				continue // an unsealed file carries no committed envelope; skip it
			}
			return 0, false, fmt.Errorf("ingest: open sealed tail %s: %w", files[i].Path, openErr)
		}
		hdr := r.Header()
		if closeErr := r.Close(); closeErr != nil {
			return 0, false, fmt.Errorf("ingest: close sealed tail %s: %w", files[i].Path, closeErr)
		}
		if hdr.MaxSeq > 0 {
			return hdr.MaxSeq, true, nil
		}
	}
	return 0, false, nil
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

// loadNextSeq reads the persisted seq/next counter for key, returning 0 for a
// fresh data dir (absent key). The caller floors nextSeq to 1 either way, so the
// first-ever event is seq 1 and seq 0 stays a pure "nothing yet" sentinel
// (design §R8); the absent-vs-zero distinction is therefore irrelevant here.
func loadNextSeq(st *store.Store, key string) (val uint64, err error) {
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

func (w *Writer) sampleDurableBatchPrepareValueLocked() any {
	if w.cfg.DurableBatchPrepareValue == nil {
		return nil
	}
	return w.cfg.DurableBatchPrepareValue()
}

func (w *Writer) commitDurableBatchLocked(ctx context.Context, nextSeq uint64, force bool, prepareValue any) error {
	// The block this commit describes is already fsynced by the time we get
	// here (flushBlockLocked / commitAsyncFlush / the drain paths all flush
	// first). The durable metadata commit must therefore run to completion
	// regardless of caller cancellation: aborting now would leave seq/next and
	// any OnDurableBatch metadata (e.g. queued repo completions) behind durable
	// segment data, and would surface a benign run-cancel (backfill MaxRepos,
	// graceful shutdown) as a spurious fatal "on_durable_batch: context
	// canceled". WithoutCancel keeps trace/span lineage while detaching
	// cancellation, matching the async path's context.Background() intent.
	ctx = context.WithoutCancel(ctx)

	b := w.cfg.Store.NewBatch()
	defer func() { _ = b.Close() }()

	if err := stageNextSeq(b, w.cfg.SeqKey, nextSeq); err != nil {
		return err
	}
	var afterCommit func()
	var afterDone func(error)
	var commitErr error
	if w.cfg.OnDurableBatch != nil {
		cb, done, err := w.cfg.OnDurableBatch(ctx, b, nextSeq, force, prepareValue)
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
		return w.wrapSegmentPersistenceError("committing durable metadata batch", fmt.Errorf("ingest: commit durable batch: %w", commitErr))
	}
	w.durableNextSeq = nextSeq
	w.readLog.advanceDurable(nextSeq)
	if afterCommit != nil {
		afterCommit()
	}
	return nil
}
