package lifecycle

import (
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
)

// Phase names a single jetstream-process lifecycle state.
type Phase string

const (
	// PhaseBootstrap means the backfill engine has not yet finished
	// initial repo download. Both the backfill engine and the
	// live_segments consumer run in this phase.
	PhaseBootstrap Phase = "bootstrap"

	// PhaseMerging means initial backfill has drained but the merge
	// step (DESIGN.md §4.2) has not yet completed. A process restart
	// in this phase resumes the cutover state machine at the merge
	// step; backfill and the bootstrap-phase live consumer are not
	// restarted.
	PhaseMerging Phase = "merging"

	// PhaseSteadyState means backfill is complete and the merge step
	// has folded live_segments into segments. Only the steady-state
	// live consumer runs here.
	PhaseSteadyState Phase = "steady_state"
)

// phaseKey is the pebble key holding the persisted phase value.
const phaseKey = "phase"

// ReadPhase returns the persisted phase. An empty value (no key
// stored) is reported as "" with nil error so callers can decide
// what to do on a fresh data dir. An unknown value crashes the read
// rather than silently mapping to a default — DESIGN.md and
// PRACTICES.md prefer crashing over data corruption.
func ReadPhase(s *store.Store) (Phase, error) {
	val, closer, err := s.Get([]byte(phaseKey))
	if errors.Is(err, pebble.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("lifecycle: read phase: %w", err)
	}
	defer func() { _ = closer.Close() }()

	p := Phase(string(val))
	if !p.valid() {
		return "", fmt.Errorf("lifecycle: unrecognized phase value %q in pebble", string(val))
	}
	return p, nil
}

// WritePhase persists p with pebble.Sync. Rejects unknown values so
// callers cannot accidentally write garbage that ReadPhase will
// later reject.
func WritePhase(s *store.Store, p Phase) error {
	if !p.valid() {
		return fmt.Errorf("lifecycle: refuse to write unrecognized phase %q", string(p))
	}
	if err := s.Set([]byte(phaseKey), []byte(p), store.SyncWrites); err != nil {
		return fmt.Errorf("lifecycle: write phase: %w", err)
	}
	return nil
}

func (p Phase) valid() bool {
	switch p {
	case PhaseBootstrap, PhaseMerging, PhaseSteadyState:
		return true
	default:
		return false
	}
}
