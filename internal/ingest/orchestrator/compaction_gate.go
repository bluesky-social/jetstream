package orchestrator

import (
	"context"
	"sync"
)

// CompactionGate pauses steady-state compaction for a whole process, across
// writer sessions. A migration copying sealed segment files needs them to
// stay as they are, and needs the compaction watermark to describe what it
// copied (migration plan §6.5). A nil gate never pauses.
//
// Pausing stops new passes only; tombstones still accumulate, so the first
// pass after Resume, here or on the archive the migration built, removes
// everything the paused passes would have.
type CompactionGate struct {
	mu      sync.Mutex
	paused  bool
	running int
	// idle is closed when the last running pass exits while a Pause waits.
	idle chan struct{}
}

// NewCompactionGate returns an open gate.
func NewCompactionGate() *CompactionGate { return &CompactionGate{} }

// Pause stops new passes, then waits for a running pass to finish. A pass
// advances the watermark only once its rewrites are published, so after
// Pause returns every sealed file agrees with the watermark. If ctx ends
// first, the gate stays paused and Pause returns ctx.Err().
func (g *CompactionGate) Pause(ctx context.Context) error {
	g.mu.Lock()
	g.paused = true
	if g.running == 0 {
		g.mu.Unlock()
		return nil
	}
	if g.idle == nil {
		g.idle = make(chan struct{})
	}
	idle := g.idle
	g.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Resume lets passes run again.
func (g *CompactionGate) Resume() {
	g.mu.Lock()
	g.paused = false
	g.mu.Unlock()
}

// Paused reports whether passes are paused.
func (g *CompactionGate) Paused() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.paused
}

// enter starts a pass, or reports false while paused.
func (g *CompactionGate) enter() bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.paused {
		return false
	}
	g.running++
	return true
}

func (g *CompactionGate) exit() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.running--
	if g.running == 0 && g.idle != nil {
		close(g.idle)
		g.idle = nil
	}
}
