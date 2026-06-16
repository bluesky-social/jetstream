package ingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/bluesky-social/jetstream-v2/internal/obs"
	"github.com/bluesky-social/jetstream-v2/segment"
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
	w.asyncPrepared++
	w.asyncJobs.Add(1)
	return job, nil
}

func (w *Writer) waitAsyncFlushes(jobs []*asyncFlushJob) error {
	var firstErr error
	for _, job := range jobs {
		if job == nil {
			continue
		}
		w.async.jobs <- job
	}
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
		if w.asyncPrepared > 0 {
			defer func() { w.asyncPrepared-- }()
		}

		if err := w.active.CommitPreparedFlush(job.prepared, frame); err != nil {
			return fmt.Errorf("ingest: async flush block: %w", err)
		}
		w.cfg.Metrics.incBlocksFlushed()

		if err := saveNextSeq(w.cfg.Store, w.cfg.SeqKey, job.nextSeq); err != nil {
			return err
		}

		path := filepath.Join(w.cfg.SegmentsDir, SegmentFilename(w.activeIdx))
		info, statErr := os.Stat(path)
		if statErr != nil {
			return fmt.Errorf("ingest: stat active segment: %w", statErr)
		}
		w.activeBytes = info.Size() - int64(segment.ReservedHeaderBytes)
		w.cfg.Metrics.setActiveSegBytes(w.activeBytes)

		if w.activeBytes < w.cfg.MaxSegmentBytes || w.asyncPrepared > 1 || w.active.Pending() > 0 {
			return nil
		}

		trace.SpanFromContext(ctx).AddEvent("async_flush_rotate")
		return w.rotateLocked(ctx)
	})
}

func (w *Writer) closeAsync() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	job, err := w.prepareAsyncFlushLocked()
	w.closed = true
	w.mu.Unlock()
	if err != nil {
		return err
	}

	flushErr := w.waitAsyncFlushes([]*asyncFlushJob{job})
	w.asyncJobs.Wait()
	w.async.close()

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active == nil {
		return flushErr
	}
	closeErr := w.active.Close()
	saveErr := saveNextSeq(w.cfg.Store, w.cfg.SeqKey, w.nextSeq)
	if flushErr != nil {
		return flushErr
	}
	if closeErr != nil {
		return fmt.Errorf("ingest: close active segment: %w", closeErr)
	}
	return saveErr
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
	if err := saveNextSeq(w.cfg.Store, w.cfg.SeqKey, w.nextSeq); err != nil {
		return err
	}
	sealedIdx := w.activeIdx
	sealedPath := filepath.Join(w.cfg.SegmentsDir, SegmentFilename(sealedIdx))
	if err := w.onAfterSealLocked(sealedIdx, sealedPath); err != nil {
		return err
	}
	return nil
}
