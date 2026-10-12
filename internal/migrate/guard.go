package migrate

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream/internal/metastore"
)

// The migration's local state, in the source's Pebble. Nothing outside
// migration/ is ever written there by the migrator.
const (
	// HandoffGuardKey is the local half of the handoff's two-phase commit
	// (migration plan §6.7, §6.8). While it is set, local ingest must not
	// start: pending until PostgreSQL says whether the handoff committed,
	// and forever once it says it did.
	HandoffGuardKey = "migration/handoff"
	// CompactionPausedKey records when the migration paused local
	// compaction, so the pause survives restarts.
	CompactionPausedKey = "migration/compaction_paused"
)

// Guard is the handoff guard's value.
type Guard string

const (
	// GuardNone is no guard: local ingest may run.
	GuardNone Guard = ""
	// GuardPending means a handoff was about to commit. Only PostgreSQL
	// knows whether it did.
	GuardPending Guard = "pending"
	// GuardDone means the handoff committed: this archive's writer is now
	// a disaggregated pod, and local ingest never runs again.
	GuardDone Guard = "done"
)

// ReadGuard reads the handoff guard.
func ReadGuard(ctx context.Context, st metastore.Store) (Guard, error) {
	v, err := st.Get(ctx, []byte(HandoffGuardKey))
	if errors.Is(err, metastore.ErrNotFound) {
		return GuardNone, nil
	}
	if err != nil {
		return GuardNone, fmt.Errorf("migrate: read %s: %w", HandoffGuardKey, err)
	}
	switch g := Guard(v); g {
	case GuardPending, GuardDone:
		return g, nil
	default:
		return GuardNone, fmt.Errorf("migrate: %s holds %q", HandoffGuardKey, v)
	}
}

// writeGuard sets or, for GuardNone, clears the guard. Every Pebble write
// is synced, so the guard is durable when this returns.
func writeGuard(ctx context.Context, st metastore.Store, g Guard) error {
	var err error
	if g == GuardNone {
		err = st.Delete(ctx, []byte(HandoffGuardKey))
	} else {
		err = st.Set(ctx, []byte(HandoffGuardKey), []byte(g))
	}
	if err != nil {
		return fmt.Errorf("migrate: write %s: %w", HandoffGuardKey, err)
	}
	return nil
}

// ReadCompactionPause returns when the migration paused local compaction.
func ReadCompactionPause(ctx context.Context, st metastore.Store) (time.Time, bool, error) {
	v, err := st.Get(ctx, []byte(CompactionPausedKey))
	if errors.Is(err, metastore.ErrNotFound) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("migrate: read %s: %w", CompactionPausedKey, err)
	}
	if len(v) != 8 {
		return time.Time{}, false, fmt.Errorf("migrate: %s has length %d", CompactionPausedKey, len(v))
	}
	return time.UnixMicro(int64(binary.BigEndian.Uint64(v))), true, nil
}

func writeCompactionPause(ctx context.Context, st metastore.Store, at time.Time) error {
	if err := st.Set(ctx, []byte(CompactionPausedKey), binary.BigEndian.AppendUint64(nil, uint64(at.UnixMicro()))); err != nil {
		return fmt.Errorf("migrate: write %s: %w", CompactionPausedKey, err)
	}
	return nil
}

// ClearLocalState removes the guard and the compaction pause, as the
// emergency rollback does (migration plan §8).
func ClearLocalState(ctx context.Context, st metastore.Store) error {
	for _, k := range []string{HandoffGuardKey, CompactionPausedKey} {
		if err := st.Delete(ctx, []byte(k)); err != nil {
			return fmt.Errorf("migrate: clear %s: %w", k, err)
		}
	}
	return nil
}
