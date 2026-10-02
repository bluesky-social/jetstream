package storagefake

import (
	"cmp"
	"context"
	"math/rand/v2"
	"slices"
	"sync"
	"testing/synctest"
)

// Scheduler is the plan D4 hook: storage calls announce themselves at a
// named point and wait until the scheduler admits them. Production code
// never installs one.
type Scheduler interface {
	Yield(ctx context.Context, point string) error
}

type actorKey struct{}

// WithActor labels ctx's storage calls with the calling component (for
// example "pod-2/follower"). Seeded orders parked calls by actor and point,
// so the choice does not depend on which goroutine happened to park first.
func WithActor(ctx context.Context, actor string) context.Context {
	return context.WithValue(ctx, actorKey{}, actor)
}

// Actor returns ctx's actor label.
func Actor(ctx context.Context) string {
	a, _ := ctx.Value(actorKey{}).(string)
	return a
}

// Seeded admits one parked storage call at a time, chosen from a PCG stream,
// once every other goroutine in the synctest bubble is durably blocked. With
// the rest of the program deterministic between yields, a seed replays the
// same interleaving at the storage boundary.
//
// Run owns the bubble's synctest.Wait; nothing else in the bubble may call
// Wait while it runs.
type Seeded struct {
	mu      sync.Mutex
	rng     *rand.Rand
	parked  []*parkedCall
	arrived uint64
	trace   []string
	stopped bool
	arrive  chan struct{}
}

type parkedCall struct {
	key     string
	order   uint64
	release chan struct{}
}

// NewSeeded returns a scheduler seeded with seed.
func NewSeeded(seed uint64) *Seeded {
	return &Seeded{
		rng:    rand.New(rand.NewPCG(seed, seed^0x5eed_5c4e_d01e_0000)),
		arrive: make(chan struct{}, 1),
	}
}

// Yield implements Scheduler.
func (s *Seeded) Yield(ctx context.Context, point string) error {
	c := &parkedCall{key: Actor(ctx) + " " + point, release: make(chan struct{})}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.arrived++
	c.order = s.arrived
	s.parked = append(s.parked, c)
	s.mu.Unlock()
	select {
	case s.arrive <- struct{}{}:
	default:
	}
	select {
	case <-c.release:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		if i := slices.Index(s.parked, c); i >= 0 {
			s.parked = slices.Delete(s.parked, i, i+1)
		}
		s.mu.Unlock()
		return ctx.Err()
	}
}

// Run schedules until ctx ends, then admits every parked and future call.
// It must run on a goroutine inside the synctest bubble.
func (s *Seeded) Run(ctx context.Context) {
	defer s.stop()
	for {
		synctest.Wait()
		s.mu.Lock()
		if len(s.parked) == 0 {
			s.mu.Unlock()
			select {
			case <-s.arrive:
				continue
			case <-ctx.Done():
				return
			}
		}
		slices.SortFunc(s.parked, func(a, b *parkedCall) int {
			return cmp.Or(cmp.Compare(a.key, b.key), cmp.Compare(a.order, b.order))
		})
		i := s.rng.IntN(len(s.parked))
		c := s.parked[i]
		s.parked = slices.Delete(s.parked, i, i+1)
		s.trace = append(s.trace, c.key)
		s.mu.Unlock()
		close(c.release)
	}
}

func (s *Seeded) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
	for _, c := range s.parked {
		close(c.release)
	}
	s.parked = nil
}

// Trace returns the admitted calls in order.
func (s *Seeded) Trace() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.trace)
}
