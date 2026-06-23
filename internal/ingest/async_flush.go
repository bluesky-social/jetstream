package ingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/segment"
	"go.opentelemetry.io/otel/trace"
)

type asyncFlushJob struct {
	id       uint64
	prepared *segment.PreparedBlock
	nextSeq  uint64
	done     chan asyncFlushDone
}

type asyncFlushDone struct {
	err error
}

type asyncFlushResult struct {
	job   *asyncFlushJob
	frame []byte
}

type asyncFlushPipeline struct {
	writer *Writer

	jobs    chan *asyncFlushJob
	results chan asyncFlushResult
	done    chan struct{}

	workers sync.WaitGroup
}

func newAsyncFlushPipeline(w *Writer, workers int) *asyncFlushPipeline {
	p := &asyncFlushPipeline{
		writer:  w,
		jobs:    make(chan *asyncFlushJob, workers),
		results: make(chan asyncFlushResult, workers),
		done:    make(chan struct{}),
	}
	p.workers.Add(workers)
	for range workers {
		go p.compress()
	}
	go p.commit()
	return p
}

func (p *asyncFlushPipeline) close() {
	close(p.jobs)
	p.workers.Wait()
	close(p.results)
	<-p.done
}

func (p *asyncFlushPipeline) compress() {
	defer p.workers.Done()
	for job := range p.jobs {
		p.results <- asyncFlushResult{
			job:   job,
			frame: segment.CompressPreparedBlock(job.prepared),
		}
	}
}

func (p *asyncFlushPipeline) commit() {
	defer close(p.done)

	nextID := uint64(0)
	pending := make(map[uint64]asyncFlushResult)
	for result := range p.results {
		pending[result.job.id] = result
		for {
			ready, ok := pending[nextID]
			if !ok {
				break
			}
			delete(pending, nextID)
			p.finish(ready)
			nextID++
		}
	}

	for _, result := range pending {
		result.job.done <- asyncFlushDone{err: fmt.Errorf("ingest: async flush stopped before job %d committed", result.job.id)}
		close(result.job.done)
	}
}

func (p *asyncFlushPipeline) finish(result asyncFlushResult) {
	err := p.writer.commitAsyncFlush(context.Background(), result.job, result.frame)
	result.job.done <- asyncFlushDone{err: err}
	close(result.job.done)
}

func (w *Writer) prepareAsyncFlushLocked() (*asyncFlushJob, error) {
	prepared, err := w.active.PrepareFlush()
	if err != nil {
		return nil, fmt.Errorf("ingest: prepare async flush: %w", err)
	}
	if prepared == nil {
		return nil, nil
	}
	job := &asyncFlushJob{
		id:       w.nextAsyncFlushID,
		prepared: prepared,
		nextSeq:  prepared.MaxSeq() + 1,
		done:     make(chan asyncFlushDone, 1),
	}
	w.nextAsyncFlushID++
	w.asyncJobs.Add(1)
	return job, nil
}

func (w *Writer) submitAsyncFlushes(jobs []*asyncFlushJob) {
	for _, job := range jobs {
		if job == nil {
			continue
		}
		w.async.jobs <- job
	}
}

func (w *Writer) waitSubmittedAsyncFlushes(jobs []*asyncFlushJob) error {
	var firstErr error
	for _, job := range jobs {
		if job == nil {
			continue
		}
		done := <-job.done
		w.asyncJobs.Done()
		if done.err != nil && firstErr == nil {
			firstErr = done.err
		}
	}
	return firstErr
}

func (w *Writer) commitAsyncFlush(ctx context.Context, job *asyncFlushJob, frame []byte) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		w.mu.Lock()
		defer w.mu.Unlock()

		if w.active == nil {
			return ErrClosed
		}

		if err := w.active.CommitPreparedFlush(job.prepared, frame); err != nil {
			return fmt.Errorf("ingest: async flush block: %w", err)
		}
		w.cfg.Metrics.incBlocksFlushed()

		if err := w.commitDurableBatchLocked(ctx, job.nextSeq, false); err != nil {
			return err
		}

		path := filepath.Join(w.cfg.SegmentsDir, SegmentFilename(w.activeIdx))
		info, statErr := os.Stat(path)
		if statErr != nil {
			return fmt.Errorf("ingest: stat active segment: %w", statErr)
		}
		w.activeBytes = info.Size() - int64(segment.ReservedHeaderBytes)
		w.cfg.Metrics.setActiveSegBytes(w.activeBytes)

		// Rotation deliberately does NOT happen here. A prepared block may
		// still be in flight (preparedOutstanding > 0), which segment.Seal
		// refuses, and the pipeline is rarely quiescent mid-commit. The
		// post-append rotateIfFull path performs the rotation at a provably
		// quiescent point instead. See rotateIfFull.
		return nil
	})
}

// rotateIfFull seals the active segment and opens the next one when the
// active segment has grown to MaxSegmentBytes. It is the async writer's
// size-driven rotation lever, called by Append / AppendBatch after their
// flush jobs have been submitted.
//
// The check is keyed on accumulated bytes (the value commitAsyncFlush
// freshly re-stats after every block), not on transient pipeline depth,
// so segments rotate at ~MaxSegmentBytes regardless of backfill batch
// shape. Overshoot is bounded to one append/batch worth of events past
// the threshold.
//
// Crash-safety mirrors the sync flushAndRotateLocked path exactly. We
// acquire drainMu (the admission barrier every Append/AppendBatch holds
// while submitting jobs) and then asyncJobs.Wait(), which together
// guarantee the flush pipeline has drained to depth zero:
// segment.preparedOutstanding == 0, so Seal's precondition holds, and
// w.nextSeq is the trailing pending block's max seq + 1 with no
// higher-seq block in flight. We then flush the trailing pending block
// (fsync) and commit seq/next BEFORE sealing, so a crash between any two
// steps leaves seq/next lagging at most one block, which Open's
// ScanMaxSeq reconciliation recovers.
//
// Async-only: the sync path rotates inline in flushAndRotateLocked. A
// no-op (cheap, no drain) when the active segment is below threshold.
func (w *Writer) rotateIfFull(ctx context.Context) error {
	if w.async == nil {
		return nil
	}

	w.mu.Lock()
	full := w.active != nil && !w.closed && w.activeBytes >= w.cfg.MaxSegmentBytes
	w.mu.Unlock()
	if !full {
		return nil
	}

	return obs.Span(ctx, func(ctx context.Context) error {
		w.drainMu.Lock()
		defer w.drainMu.Unlock()

		// Drain the flush pipeline to quiescence so no prepared block is
		// outstanding against the segment we are about to seal.
		w.asyncJobs.Wait()

		w.mu.Lock()
		defer w.mu.Unlock()

		// Re-check under the lock: a concurrent rotateIfFull (or the drain
		// path) may have already rotated, or the writer may have closed.
		if w.closed || w.active == nil || w.activeBytes < w.cfg.MaxSegmentBytes {
			return nil
		}

		// Flush any trailing sub-block remainder durably first. flushBlockLocked
		// fsyncs the block, then commits seq/next — the same fsync-before-seq
		// ordering the sync rotation path uses (DESIGN.md §3.1.1). At full
		// drain w.nextSeq equals this block's max seq + 1, so the committed
		// seq never leads durable data.
		if w.active.Pending() > 0 {
			if err := w.flushBlockLocked(ctx); err != nil {
				return err
			}
		}

		trace.SpanFromContext(ctx).AddEvent("async_flush_rotate")
		return w.rotateLocked(ctx)
	})
}

func (w *Writer) closeAsync() error {
	w.drainMu.Lock()
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		w.drainMu.Unlock()
		return nil
	}
	job, err := w.prepareAsyncFlushLocked()
	w.closed = true
	w.mu.Unlock()
	if err == nil {
		w.submitAsyncFlushes([]*asyncFlushJob{job})
	}
	w.drainMu.Unlock()
	if err != nil {
		return err
	}

	flushErr := w.waitSubmittedAsyncFlushes([]*asyncFlushJob{job})
	w.asyncJobs.Wait()
	w.async.close()

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active == nil {
		return flushErr
	}
	closeErr := w.active.Close()
	if flushErr != nil {
		return flushErr
	}
	if closeErr != nil {
		return fmt.Errorf("ingest: close active segment: %w", closeErr)
	}
	return w.commitTerminalDurableBatchLocked()
}

func (w *Writer) sealActiveAndCloseAsync() error {
	if err := w.Flush(context.Background()); err != nil {
		return err
	}

	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.mu.Unlock()

	w.asyncJobs.Wait()
	w.async.close()

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active == nil {
		return nil
	}
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
