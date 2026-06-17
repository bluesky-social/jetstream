package backfill

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/repo"
)

type completionBatcher struct {
	mu         sync.Mutex
	store      *Store
	metrics    *Metrics
	watermarks map[atmos.DID]completionWatermark
	queued     []queuedCompletion
}

type completionWatermark struct {
	lastSeq  uint64
	appended bool
}

type queuedCompletion struct {
	did       atmos.DID
	commit    *repo.Commit
	completed time.Time
	watermark completionWatermark
}

func NewCompletionBatcher(st *Store, m *Metrics) *completionBatcher {
	return &completionBatcher{
		store:      st,
		metrics:    m,
		watermarks: make(map[atmos.DID]completionWatermark),
	}
}

func (b *completionBatcher) RecordWatermark(did atmos.DID, lastSeq uint64, appended bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.watermarks[did] = completionWatermark{lastSeq: lastSeq, appended: appended}
}

func (b *completionBatcher) QueueComplete(ctx context.Context, did atmos.DID, commit *repo.Commit) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	var queuedCommit *repo.Commit
	if commit != nil {
		commitCopy := *commit
		queuedCommit = &commitCopy
	}
	watermark, ok := b.watermarks[did]
	if !ok {
		return fmt.Errorf("backfill: queue complete %s: missing watermark", did)
	}
	delete(b.watermarks, did)
	completion := queuedCompletion{
		did:       did,
		commit:    queuedCommit,
		completed: timeNow(),
		watermark: watermark,
	}
	for i := range b.queued {
		if b.queued[i].did == did {
			b.queued[i] = completion
			return nil
		}
	}
	b.queued = append(b.queued, completion)
	return nil
}

func (b *completionBatcher) StageDurable(ctx context.Context, batch *pebble.Batch, nextSeq uint64, force bool) (func(), func(error), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	b.mu.Lock()
	staged := make([]queuedCompletion, 0, len(b.queued))
	for _, completion := range b.queued {
		if !completion.watermark.appended || completion.watermark.lastSeq < nextSeq {
			staged = append(staged, completion)
		}
	}
	b.mu.Unlock()

	if len(staged) == 0 {
		return nil, nil, nil
	}
	afterDone, err := b.store.stageCompleteBatch(ctx, batch, staged)
	if err != nil {
		return nil, nil, err
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			b.queued = removeQueuedCompletions(b.queued, staged)
			b.mu.Unlock()

			for range staged {
				b.metrics.incCompleted()
			}
		})
	}, afterDone, nil
}

func removeQueuedCompletions(queued, staged []queuedCompletion) []queuedCompletion {
	if len(staged) == 0 {
		return queued
	}
	out := queued[:0]
	for _, completion := range queued {
		if removeFirstQueuedCompletion(&staged, completion) {
			continue
		}
		out = append(out, completion)
	}
	return out
}

func removeFirstQueuedCompletion(staged *[]queuedCompletion, completion queuedCompletion) bool {
	for i, candidate := range *staged {
		if queuedCompletionEqual(candidate, completion) {
			*staged = append((*staged)[:i], (*staged)[i+1:]...)
			return true
		}
	}
	return false
}

func queuedCompletionEqual(a, b queuedCompletion) bool {
	return a.did == b.did &&
		a.commit == b.commit &&
		a.completed.Equal(b.completed) &&
		a.watermark == b.watermark
}
