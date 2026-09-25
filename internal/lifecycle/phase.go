package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream/internal/metastore"
)

// Phase names a single jetstream-process lifecycle state.
type Phase string

const (
	// PhaseBootstrap means the backfill engine has not yet finished
	// initial repo download. Both the backfill engine and the
	// live_segments consumer run in this phase.
	PhaseBootstrap Phase = "bootstrap"

	// PhaseMerging means initial backfill has drained but the merge
	// step (docs/README.md §4.2) has not yet completed. A process restart
	// in this phase resumes the cutover state machine at the merge
	// step; backfill and the bootstrap-phase live consumer are not
	// restarted.
	PhaseMerging Phase = "merging"

	// PhaseSteadyState means backfill is complete and the merge step
	// has folded live_segments into segments. Only the steady-state
	// live consumer runs here.
	PhaseSteadyState Phase = "steady_state"
)

// PhaseKey is the metadata key holding the persisted Phase. Disaggregated
// followers read it from the shared catalog's metadata_kv.
const PhaseKey = "phase"

const (
	phaseKey                     = PhaseKey
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
func ReadPhase(ctx context.Context, s metastore.Store) (Phase, error) {
	val, err := s.Get(ctx, []byte(phaseKey))
	if errors.Is(err, metastore.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("lifecycle: read phase: %w", err)
	}
	return ParsePhase(val)
}

// ParsePhase decodes a stored PhaseKey value. An unknown value is an error,
// never a default.
func ParsePhase(val []byte) (Phase, error) {
	p := Phase(string(val))
	if !p.valid() {
		return "", fmt.Errorf("lifecycle: unrecognized phase value %q in metadata store", string(val))
	}
	return p, nil
}

// ReadPhaseEnteredAt returns the timestamp at which the current phase
// was entered. Zero time + nil error means the key isn't present (fresh
// data dir, or a process that pre-dates this field).
func ReadPhaseEnteredAt(ctx context.Context, s metastore.Store) (time.Time, error) {
	return readTime(ctx, s, phaseEnteredAtKey)
}

func readTime(ctx context.Context, s metastore.Store, key string) (time.Time, error) {
	val, err := s.Get(ctx, []byte(key))
	if errors.Is(err, metastore.ErrNotFound) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("lifecycle: read %s: %w", key, err)
	}

	t, err := time.Parse(time.RFC3339Nano, string(val))
	if err != nil {
		return time.Time{}, fmt.Errorf("lifecycle: decode %s %q: %w", key, string(val), err)
	}
	return t.UTC(), nil
}

// WritePhase atomically and durably persists p and enteredAt. Both keys land
// together via a single batch commit so a crash cannot leave a phase value
// paired with the wrong timestamp.
func WritePhase(ctx context.Context, s metastore.Store, p Phase, enteredAt time.Time) error {
	if !p.valid() {
		return fmt.Errorf("lifecycle: refuse to write unrecognized phase %q", string(p))
	}
	b := s.NewBatch()
	stagePhase(b, p, enteredAt)
	if err := b.Commit(ctx); err != nil {
		return fmt.Errorf("lifecycle: commit phase write: %w", err)
	}
	return nil
}

func WritePhaseWithBackfillTiming(ctx context.Context, s metastore.Store, p Phase, enteredAt time.Time, backfillStartedAt time.Time, backfillCompletedAt time.Time) error {
	if !p.valid() {
		return fmt.Errorf("lifecycle: refuse to write unrecognized phase %q", string(p))
	}
	b := s.NewBatch()
	stagePhase(b, p, enteredAt)
	stageTime(b, backfillTimingStartedAtKey, backfillStartedAt)
	stageTime(b, backfillTimingCompletedAtKey, backfillCompletedAt)
	if err := b.Commit(ctx); err != nil {
		return fmt.Errorf("lifecycle: commit phase and backfill timing write: %w", err)
	}
	return nil
}

func ReadBackfillTiming(ctx context.Context, s metastore.Store) (BackfillTiming, error) {
	startedAt, err := readTime(ctx, s, backfillTimingStartedAtKey)
	if err != nil {
		return BackfillTiming{}, err
	}
	completedAt, err := readTime(ctx, s, backfillTimingCompletedAtKey)
	if err != nil {
		return BackfillTiming{}, err
	}
	return BackfillTiming{StartedAt: startedAt, CompletedAt: completedAt}, nil
}

func WriteBackfillTiming(ctx context.Context, s metastore.Store, startedAt time.Time, completedAt time.Time) error {
	b := s.NewBatch()
	stageTime(b, backfillTimingStartedAtKey, startedAt)
	stageTime(b, backfillTimingCompletedAtKey, completedAt)
	if err := b.Commit(ctx); err != nil {
		return fmt.Errorf("lifecycle: commit backfill timing: %w", err)
	}
	return nil
}

func stagePhase(b metastore.Batch, p Phase, enteredAt time.Time) {
	b.Set([]byte(phaseKey), []byte(p))
	stageTime(b, phaseEnteredAtKey, enteredAt)
}

func stageTime(b metastore.Batch, key string, t time.Time) {
	b.Set([]byte(key), []byte(t.UTC().Format(time.RFC3339Nano)))
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
func IsSteadyState(ctx context.Context, s metastore.Store) bool {
	p, err := ReadPhase(ctx, s)
	if err != nil {
		return false
	}
	return p == PhaseSteadyState
}
