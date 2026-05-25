package lifecycle

import (
	"errors"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
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

const (
	phaseKey          = "phase"
	phaseEnteredAtKey = "phase/entered_at"
)

// ReadPhase returns the persisted phase. Empty on a fresh data dir.
// An unknown value crashes the read rather than silently mapping to a
// default.
func ReadPhase(s *store.Store) (Phase, error) {
	val, closer, err := s.Get([]byte(phaseKey))
	if errors.Is(err, store.ErrNotFound) {
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

// ReadPhaseEnteredAt returns the timestamp at which the current phase
// was entered. Zero time + nil error means the key isn't present (fresh
// data dir, or a process that pre-dates this field).
func ReadPhaseEnteredAt(s *store.Store) (time.Time, error) {
	val, closer, err := s.Get([]byte(phaseEnteredAtKey))
	if errors.Is(err, store.ErrNotFound) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("lifecycle: read phase/entered_at: %w", err)
	}
	defer func() { _ = closer.Close() }()

	t, err := time.Parse(time.RFC3339Nano, string(val))
	if err != nil {
		return time.Time{}, fmt.Errorf("lifecycle: decode phase/entered_at %q: %w", string(val), err)
	}
	return t.UTC(), nil
}

// WritePhase atomically persists p and enteredAt with pebble.Sync.
// Both keys land together via a single batch commit so a crash cannot
// leave a phase value paired with the wrong timestamp.
func WritePhase(s *store.Store, p Phase, enteredAt time.Time) error {
	if !p.valid() {
		return fmt.Errorf("lifecycle: refuse to write unrecognized phase %q", string(p))
	}
	b := s.NewBatch()
	defer func() { _ = b.Close() }()

	if err := b.Set([]byte(phaseKey), []byte(p), nil); err != nil {
		return fmt.Errorf("lifecycle: stage phase: %w", err)
	}
	tsBytes := []byte(enteredAt.UTC().Format(time.RFC3339Nano))
	if err := b.Set([]byte(phaseEnteredAtKey), tsBytes, nil); err != nil {
		return fmt.Errorf("lifecycle: stage phase/entered_at: %w", err)
	}
	if err := s.Commit(b, store.SyncWrites); err != nil {
		return fmt.Errorf("lifecycle: commit phase write: %w", err)
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
