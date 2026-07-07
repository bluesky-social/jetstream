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
	host      string
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

func (b *completionBatcher) QueueComplete(ctx context.Context, did atmos.DID, host string, commit *repo.Commit) error {
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
		host:      host,
		commit:    queuedCommit,
		completed: timeNow(),
		watermark: watermark,
	}
	for i := range b.queued {
		if b.queued[i].did == did {
			b.queued[i] = completion
			b.metrics.incCompletionQueued()
			b.metrics.setCompletionQueueDepth(len(b.queued))
			return nil
		}
	}
	b.queued = append(b.queued, completion)
	b.metrics.incCompletionQueued()
	b.metrics.setCompletionQueueDepth(len(b.queued))
	return nil
}

func (b *completionBatcher) StageDurable(ctx context.Context, batch *pebble.Batch, nextSeq uint64, force bool, _ any) (func(), func(error), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	b.mu.Lock()
	staged := make([]queuedCompletion, 0, len(b.queued))
	for _, completion := range b.queued {
		if !completion.watermark.appended || completion.watermark.lastSeq < nextSeq {
			staged = append(staged, completion)
			continue
		}
		// force=true is the drain/terminal commit: the writer has already
		// flushed every pending block and waited for in-flight async jobs,
		// so nextSeq must cover every appended event. An event-backed
		// completion excluded here means its final event is not durable at
		// a forced checkpoint — which would let saveBatchCursor advance the
		// listRepos cursor past a non-durable completion (silent data loss).
		// Crash rather than corrupt (AGENTS.md / docs/README.md §3.1.1 ordering).
		if force {
			b.mu.Unlock()
			b.metrics.incCompletionStageErrors()
			return nil, nil, fmt.Errorf(
				"backfill: forced durable batch at seq %d excludes appended completion %s with lastSeq %d (events not durable)",
				nextSeq, completion.did, completion.watermark.lastSeq)
		}
	}
	b.mu.Unlock()

	if len(staged) == 0 {
		return nil, nil, nil
	}
	afterDone, err := b.store.stageCompleteBatch(ctx, batch, staged)
	if err != nil {
		b.metrics.incCompletionStageErrors()
		return nil, nil, err
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			b.queued = removeQueuedCompletions(b.queued, staged)
			b.metrics.setCompletionQueueDepth(len(b.queued))
			b.mu.Unlock()

			b.metrics.observeCompletionDurableBatch(len(staged))
			now := timeNow()
			for _, c := range staged {
				b.metrics.observeCompletionQueueWait(now.Sub(c.completed))
				b.metrics.incCompleted()
			}
		})
	}, afterDone, nil
}

// removeQueuedCompletions drops from queued exactly the entries that were
// staged, leaving any entry re-queued for the same DID after staging (a newer
// commit/watermark) in place. Both slices are DID-unique because QueueComplete
// dedupes by DID in place, so a single map keyed by DID gives an O(len(queued))
// filter; the full-identity queuedCompletionEqual check preserves the
// replaced-after-stage invariant (a re-queued entry differs in commit pointer,
// completed time, or watermark and therefore survives).
func removeQueuedCompletions(queued, staged []queuedCompletion) []queuedCompletion {
	if len(staged) == 0 {
		return queued
	}
	stagedByDID := make(map[atmos.DID]queuedCompletion, len(staged))
	for _, completion := range staged {
		stagedByDID[completion.did] = completion
	}
	out := queued[:0]
	for _, completion := range queued {
		if candidate, ok := stagedByDID[completion.did]; ok && queuedCompletionEqual(candidate, completion) {
			continue
		}
		out = append(out, completion)
	}
	return out
}

func queuedCompletionEqual(a, b queuedCompletion) bool {
	return a.did == b.did &&
		a.host == b.host &&
		a.commit == b.commit &&
		a.completed.Equal(b.completed) &&
		a.watermark == b.watermark
}
