package backfill

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/repo"
)

type completionBatcher struct {
	mu           sync.Mutex
	store        *Store
	metrics      *Metrics
	watermarks   map[atmos.DID]completionWatermark
	queued       []queuedCompletion
	cursors      map[string]queuedHostCursor
	nextCursorID uint64
}

type queuedHostCursor struct {
	id           uint64
	host         string
	cursor       string
	deps         map[atmos.DID]queuedCompletion
	state        atmosbackfill.HostState
	lastNonEmpty string
	attempts     int
	lastError    string
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
		cursors:    make(map[string]queuedHostCursor),
	}
}

// QueueHostCursor records a per-host checkpoint and the not-yet-durable repo
// completions it covers. StageDurable only writes the cursor in a Pebble batch
// that also makes every remaining dependency durable.
func (b *completionBatcher) QueueHostCursor(ctx context.Context, host, cursor string) error {
	return b.queueHostState(ctx, queuedHostCursor{host: host, cursor: cursor, state: atmosbackfill.HostStateRunning})
}

func (b *completionBatcher) QueueHostDrained(ctx context.Context, host, lastNonEmpty string) error {
	return b.queueHostState(ctx, queuedHostCursor{host: host, state: atmosbackfill.HostStateDrained, lastNonEmpty: lastNonEmpty})
}

func (b *completionBatcher) QueueHostExhausted(ctx context.Context, host string, cause error, attempts int) error {
	lastError := ""
	if cause != nil {
		lastError = truncateErrorString(cause.Error())
	}
	return b.queueHostState(ctx, queuedHostCursor{host: host, state: atmosbackfill.HostStateExhausted, attempts: attempts, lastError: lastError})
}

func (b *completionBatcher) queueHostState(ctx context.Context, next queuedHostCursor) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextCursorID++
	deps := make(map[atmos.DID]queuedCompletion)
	for _, completion := range b.queued {
		if completion.host == next.host {
			deps[completion.did] = completion
		}
	}
	next.id = b.nextCursorID
	next.deps = deps
	b.cursors[next.host] = next
	return nil
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
	stagedByDID := make(map[atmos.DID]queuedCompletion, len(staged))
	for _, completion := range staged {
		stagedByDID[completion.did] = completion
	}
	stagedCursors := make([]queuedHostCursor, 0, len(b.cursors))
	for _, cursor := range b.cursors {
		ready := true
		for did, dep := range cursor.deps {
			current, stillQueued := queuedCompletionForDID(b.queued, did)
			if !stillQueued {
				continue
			}
			stagedDep, included := stagedByDID[did]
			if !included || !queuedCompletionEqual(dep, current) || !queuedCompletionEqual(dep, stagedDep) {
				ready = false
				break
			}
		}
		if ready {
			stagedCursors = append(stagedCursors, cursor)
			continue
		}
		if force {
			b.mu.Unlock()
			b.metrics.incCompletionStageErrors()
			return nil, nil, fmt.Errorf("backfill: forced durable batch cannot stage host cursor %s: covered completions are not durable", cursor.host)
		}
	}
	b.mu.Unlock()

	if len(staged) == 0 && len(stagedCursors) == 0 {
		return nil, nil, nil
	}
	durableDone, err := b.store.stageDurableBatch(ctx, batch, staged, stagedCursors)
	if err != nil {
		b.metrics.incCompletionStageErrors()
		return nil, nil, err
	}

	var once sync.Once
	return func() {
			once.Do(func() {
				b.mu.Lock()
				b.queued = removeQueuedCompletions(b.queued, staged)
				for _, cursor := range stagedCursors {
					if current, ok := b.cursors[cursor.host]; ok && current.id == cursor.id {
						delete(b.cursors, cursor.host)
					}
				}
				b.metrics.setCompletionQueueDepth(len(b.queued))
				b.mu.Unlock()

				b.metrics.observeCompletionDurableBatch(len(staged))
				now := timeNow()
				for _, c := range staged {
					b.metrics.observeCompletionQueueWait(now.Sub(c.completed))
					b.metrics.incCompleted()
				}
			})
		}, func(err error) {
			if durableDone != nil {
				durableDone(err)
			}
		}, nil
}

func queuedCompletionForDID(queued []queuedCompletion, did atmos.DID) (queuedCompletion, bool) {
	for _, completion := range queued {
		if completion.did == did {
			return completion, true
		}
	}
	return queuedCompletion{}, false
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
