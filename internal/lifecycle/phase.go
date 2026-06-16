package lifecycle

import (
	"errors"
	"fmt"
	"time"

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

const (
	phaseKey                     = "phase"
	phaseEnteredAtKey            = "phase/entered_at"
	backfillTimingStartedAtKey   = "backfill/timing/started_at"
	backfillTimingCompletedAtKey = "backfill/timing/completed_at"
)

// BackfillTiming records the wall-clock interval from entering bootstrap to
// the backfill engine draining and committing phase=merging.
type BackfillTiming struct {
	StartedAt   time.Time
	CompletedAt time.Time
}

func (t BackfillTiming) Duration() time.Duration {
	if t.StartedAt.IsZero() || t.CompletedAt.IsZero() {
		return 0
	}
	d := t.CompletedAt.Sub(t.StartedAt)
	if d < 0 {
		return 0
	}
	return d
}

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
	return readTime(s, phaseEnteredAtKey)
}

func readTime(s *store.Store, key string) (time.Time, error) {
	val, closer, err := s.Get([]byte(key))
	if errors.Is(err, store.ErrNotFound) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("lifecycle: read %s: %w", key, err)
	}
	defer func() { _ = closer.Close() }()

	t, err := time.Parse(time.RFC3339Nano, string(val))
	if err != nil {
		return time.Time{}, fmt.Errorf("lifecycle: decode %s %q: %w", key, string(val), err)
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

	if err := stagePhase(b, p, enteredAt); err != nil {
		return err
	}
	if err := s.Commit(b, store.SyncWrites); err != nil {
		return fmt.Errorf("lifecycle: commit phase write: %w", err)
	}
	return nil
}

func WritePhaseWithBackfillTiming(s *store.Store, p Phase, enteredAt time.Time, backfillStartedAt time.Time, backfillCompletedAt time.Time) error {
	if !p.valid() {
		return fmt.Errorf("lifecycle: refuse to write unrecognized phase %q", string(p))
	}
	b := s.NewBatch()
	defer func() { _ = b.Close() }()

	if err := stagePhase(b, p, enteredAt); err != nil {
		return err
	}
	if err := stageTime(b, backfillTimingStartedAtKey, backfillStartedAt); err != nil {
		return err
	}
	if err := stageTime(b, backfillTimingCompletedAtKey, backfillCompletedAt); err != nil {
		return err
	}
	if err := s.Commit(b, store.SyncWrites); err != nil {
		return fmt.Errorf("lifecycle: commit phase and backfill timing write: %w", err)
	}
	return nil
}

func ReadBackfillTiming(s *store.Store) (BackfillTiming, error) {
	startedAt, err := readTime(s, backfillTimingStartedAtKey)
	if err != nil {
		return BackfillTiming{}, err
	}
	completedAt, err := readTime(s, backfillTimingCompletedAtKey)
	if err != nil {
		return BackfillTiming{}, err
	}
	return BackfillTiming{StartedAt: startedAt, CompletedAt: completedAt}, nil
}

func WriteBackfillTiming(s *store.Store, startedAt time.Time, completedAt time.Time) error {
	b := s.NewBatch()
	defer func() { _ = b.Close() }()

	if err := stageTime(b, backfillTimingStartedAtKey, startedAt); err != nil {
		return err
	}
	if err := stageTime(b, backfillTimingCompletedAtKey, completedAt); err != nil {
		return err
	}
	if err := s.Commit(b, store.SyncWrites); err != nil {
		return fmt.Errorf("lifecycle: commit backfill timing: %w", err)
	}
	return nil
}

func stagePhase(b *pebble.Batch, p Phase, enteredAt time.Time) error {
	if err := b.Set([]byte(phaseKey), []byte(p), nil); err != nil {
		return fmt.Errorf("lifecycle: stage phase: %w", err)
	}
	if err := stageTime(b, phaseEnteredAtKey, enteredAt); err != nil {
		return err
	}
	return nil
}

func stageTime(b *pebble.Batch, key string, t time.Time) error {
	if err := b.Set([]byte(key), []byte(t.UTC().Format(time.RFC3339Nano)), nil); err != nil {
		return fmt.Errorf("lifecycle: stage %s: %w", key, err)
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

// IsSteadyState returns true iff the persisted phase is PhaseSteadyState.
// Any other state (empty, bootstrap, merging, corrupt) returns false. The
// /subscribe endpoint uses this as a fail-closed gate: a corrupt phase or
// missing key should surface as "service not ready" rather than as a
// crashed handler.
func IsSteadyState(s *store.Store) bool {
	p, err := ReadPhase(s)
	if err != nil {
		return false
	}
	return p == PhaseSteadyState
}
