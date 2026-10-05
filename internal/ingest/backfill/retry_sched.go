package backfill

import (
	"context"
	"sync"
	"time"
)

// retryScheduler hands a retry pass's candidates to its workers from
// per-host queues: at most limit in flight per host, hosts taken round
// robin. With waitParks set (the pending pass) a host parked by a rate
// limit is not dispatched until the park ends, and a candidate the worker
// requeues goes back on its host's queue; the pass ends when every
// candidate is done. Without it (the steady loop) parked hosts' candidates
// are still dispatched, for the worker to defer.
type retryScheduler struct {
	limit     int
	waitParks bool
	parked    func(host string, now time.Time) (time.Time, bool)
	now       func() time.Time
	metrics   *Metrics

	mu          sync.Mutex
	changed     chan struct{} // closed and replaced on every state change
	hosts       map[string]*schedHost
	ready       []*schedHost // FIFO of hosts that may have work to dispatch
	parkedHosts []*schedHost // hosts set aside until wake
	wake        time.Time
	outstanding int // candidates queued or in flight
}

type schedHost struct {
	name     string
	queue    []retryCandidate
	inflight int
	listed   bool // in ready or parkedHosts
}

func newRetryScheduler(cands []retryCandidate, limit int, waitParks bool, parked func(string, time.Time) (time.Time, bool), now func() time.Time, m *Metrics) *retryScheduler {
	s := &retryScheduler{
		limit:       max(limit, 1),
		waitParks:   waitParks,
		parked:      parked,
		now:         now,
		metrics:     m,
		changed:     make(chan struct{}),
		hosts:       make(map[string]*schedHost),
		outstanding: len(cands),
	}
	for _, c := range cands {
		h := s.hosts[c.Host]
		if h == nil {
			h = &schedHost{name: c.Host}
			s.hosts[c.Host] = h
		}
		h.queue = append(h.queue, c)
	}
	for _, h := range s.hosts {
		s.listLocked(h)
	}
	m.setRetryQueueRemaining(s.outstanding)
	return s
}

// next returns the next candidate to attempt, or ok=false once every
// candidate is done. It blocks while all remaining work is in flight or
// parked.
func (s *retryScheduler) next(ctx context.Context) (retryCandidate, bool, error) {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		s.mu.Lock()
		if s.outstanding == 0 {
			s.mu.Unlock()
			return retryCandidate{}, false, nil
		}
		now := s.now()
		if len(s.parkedHosts) > 0 && !now.Before(s.wake) {
			for _, h := range s.parkedHosts {
				h.listed = false
				s.listLocked(h)
			}
			s.parkedHosts = s.parkedHosts[:0]
			s.wake = time.Time{}
		}
		for n := len(s.ready); n > 0; n-- {
			h := s.ready[0]
			s.ready = s.ready[1:]
			h.listed = false
			if len(h.queue) == 0 || h.inflight >= s.limit {
				continue // listed again when a done frees it
			}
			if s.waitParks {
				if until, ok := s.parked(h.name, now); ok {
					h.listed = true
					s.parkedHosts = append(s.parkedHosts, h)
					if s.wake.IsZero() || until.Before(s.wake) {
						s.wake = until
					}
					continue
				}
			}
			c := h.queue[0]
			h.queue = h.queue[1:]
			h.inflight++
			s.listLocked(h)
			s.mu.Unlock()
			return c, true, nil
		}
		changed, wake := s.changed, s.wake
		s.mu.Unlock()

		var fire <-chan time.Time
		if !wake.IsZero() {
			d := max(wake.Sub(now), time.Millisecond)
			if timer == nil {
				timer = time.NewTimer(d)
			} else {
				timer.Reset(d)
			}
			fire = timer.C
		}
		select {
		case <-ctx.Done():
			return retryCandidate{}, false, ctx.Err()
		case <-changed:
		case <-fire:
		}
		if timer != nil && !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}

// done reports that the worker finished c. A requeued candidate goes back
// on its host's queue, to be dispatched again.
func (s *retryScheduler) done(c retryCandidate, requeue bool) {
	s.mu.Lock()
	h := s.hosts[c.Host]
	h.inflight--
	if requeue {
		h.queue = append(h.queue, c)
		s.metrics.incRetryRequeued()
	} else {
		s.outstanding--
		s.metrics.setRetryQueueRemaining(s.outstanding)
	}
	s.listLocked(h)
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
}

// listLocked queues h for dispatch if it has work it may start.
func (s *retryScheduler) listLocked(h *schedHost) {
	if h.listed || len(h.queue) == 0 || h.inflight >= s.limit {
		return
	}
	h.listed = true
	s.ready = append(s.ready, h)
}

// retryWriteBatch is how many failures or deferrals a pass collects before
// it writes them in one transaction.
const retryWriteBatch = 256

// retryWrites collects a pass's failure and deferral writes and commits
// them in batches. Every metadata write is a fenced transaction behind one
// lock, so one per repo would cap the pass at a few dozen repos a second.
// Rows still unwritten if the process dies keep their prior status, which
// the next pass selects again.
type retryWrites struct {
	store *Store

	mu     sync.Mutex
	fails  []retryFailure
	defers []retryDeferral
}

func (w *retryWrites) fail(f retryFailure) {
	w.mu.Lock()
	w.fails = append(w.fails, f)
	w.mu.Unlock()
}

func (w *retryWrites) deferAttempt(d retryDeferral) {
	w.mu.Lock()
	w.defers = append(w.defers, d)
	w.mu.Unlock()
}

// flush writes what is collected; with onlyFull it writes only when a
// batch's worth is waiting.
func (w *retryWrites) flush(ctx context.Context, onlyFull bool) error {
	w.mu.Lock()
	var fails []retryFailure
	var defers []retryDeferral
	if !onlyFull || len(w.fails) >= retryWriteBatch {
		fails, w.fails = w.fails, nil
	}
	if !onlyFull || len(w.defers) >= retryWriteBatch {
		defers, w.defers = w.defers, nil
	}
	w.mu.Unlock()
	if err := w.store.recordRetryFailures(ctx, fails); err != nil {
		return err
	}
	return w.store.deferRetryAttempts(ctx, defers)
}
