package ingest

import (
	"context"
	"slices"
	"sync"
)

// waitQueue holds goroutines that wait, under their owner's mutex, for a
// condition the owner tracks, oldest first. signal wakes only the oldest
// waiter, and only once the condition holds. A woken waiter that took what
// it waited for signals again when it is done, so the next waiter wakes
// only if something is left for it.
//
// It replaces a broadcast that woke every waiter on every change. With
// hundreds of backfill appends waiting on the direct writer, each committed
// block woke all of them, one could proceed, and the rest queued on the
// mutex the committer needed for its next block (pop2, 2026-10-03).
//
// Every method runs with mu held.
type waitQueue struct {
	mu *sync.Mutex
	// ready reports whether the oldest waiter may proceed.
	ready   func() bool
	waiters []chan struct{}
}

// wait releases mu until signal or wakeAll wakes this waiter, or ctx ends,
// and takes mu again. front queues it ahead of every other waiter: it was
// woken, and the condition changed again before it could act.
//
// A waiter whose ctx ends after a signal woke it passes the turn on before
// returning ctx's error, or nobody would take it.
func (q *waitQueue) wait(ctx context.Context, front bool) error {
	ch := make(chan struct{})
	if front {
		q.waiters = slices.Insert(q.waiters, 0, ch)
	} else {
		q.waiters = append(q.waiters, ch)
	}
	q.mu.Unlock()
	var err error
	select {
	case <-ch:
	case <-ctx.Done():
		err = ctx.Err()
	}
	q.mu.Lock()
	if err != nil {
		if i := slices.Index(q.waiters, ch); i >= 0 {
			q.waiters = slices.Delete(q.waiters, i, i+1)
		} else {
			q.signal()
		}
	}
	return err
}

// signal wakes the oldest waiter if the condition holds.
func (q *waitQueue) signal() {
	if len(q.waiters) == 0 || !q.ready() {
		return
	}
	close(q.waiters[0])
	q.waiters[0] = nil
	q.waiters = q.waiters[1:]
}

// wakeAll wakes every waiter, for a change that ends every wait, such as
// the owner failing or closing.
func (q *waitQueue) wakeAll() {
	for _, ch := range q.waiters {
		close(ch)
	}
	q.waiters = nil
}

// len is the number of waiters not yet woken.
func (q *waitQueue) len() int { return len(q.waiters) }
