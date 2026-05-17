package backfill

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
)

// Phase tracks how far through the bootstrap pipeline (DESIGN.md
// §4.1) the data directory has progressed. Each phase is a step
// the server must finish before serving production traffic.
//
// Today we only persist PhaseSeed and PhaseComplete because the
// only phase implemented is the listRepos seed; the constants for
// the other phases are reserved here so future iterations can fill
// them in without churning on-disk values that an existing data
// dir might already carry.
type Phase string

const (
	// PhaseUnset is the implicit value of a fresh data directory.
	// Never written; surfaced from GetBootstrapState as the
	// "no row exists" signal so callers don't special-case
	// (state, ok, err).
	PhaseUnset Phase = ""

	// PhaseSeed means the listRepos enumeration is either running
	// or was interrupted. Re-running the seed step on an existing
	// PhaseSeed dir is the documented recovery path.
	PhaseSeed Phase = "seed"

	// PhaseDownload (reserved) will mean the per-repo getRepo phase
	// is in progress. Not yet emitted by Run().
	PhaseDownload Phase = "download"

	// PhaseMerge (reserved) will mean we're compacting live shadow
	// shards into the main segment store (DESIGN.md §4.2). Not yet
	// emitted by Run().
	PhaseMerge Phase = "merge"

	// PhaseComplete means bootstrap is finished and steady-state
	// ingest can take over. Once written, Run() is a no-op until
	// the data dir is wiped.
	PhaseComplete Phase = "complete"
)

// bootstrapStateKey is the on-disk pebble key holding the most
// recent BootstrapState. We deliberately keep it as a singleton key
// (rather than e.g. an append-only log) — the bootstrap state
// machine is tiny, the row is rewritten only at phase boundaries,
// and a single key is trivial to reason about across crashes.
var bootstrapStateKey = []byte("bootstrap/state")

// BootstrapState is the JSON-encoded value at bootstrap/state. It
// captures the current Phase plus enough timestamps to debug a
// stuck bootstrap from operations.
type BootstrapState struct {
	Phase Phase `json:"phase"`

	// StartedAt is the timestamp of the first time we entered any
	// non-Unset phase on this data directory. Stable across
	// retries.
	StartedAt time.Time `json:"started_at,omitzero"`

	// UpdatedAt is the timestamp of the most recent phase
	// transition. Useful for "how long has the seed been running?"
	// dashboards.
	UpdatedAt time.Time `json:"updated_at,omitzero"`

	// CompletedAt is set the first time we transition into
	// PhaseComplete. Distinct from UpdatedAt because future phases
	// (compaction, etc.) might rewrite the row but must not move
	// the bootstrap-completed marker.
	CompletedAt time.Time `json:"completed_at,omitzero"`
}

// IsComplete reports whether bootstrap has finished. Cheap helper
// for the most common branch in serve startup.
func (s BootstrapState) IsComplete() bool { return s.Phase == PhaseComplete }

// GetBootstrapState reads the persisted bootstrap state. A fresh
// data directory returns (BootstrapState{Phase: PhaseUnset}, nil)
// rather than an error — the absence of the row is the unset
// signal, and callers should treat it as "first time starting up".
func GetBootstrapState(s *store.Store) (BootstrapState, error) {
	val, closer, err := s.Get(bootstrapStateKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return BootstrapState{Phase: PhaseUnset}, nil
	}
	if err != nil {
		return BootstrapState{}, fmt.Errorf("backfill: get bootstrap/state: %w", err)
	}
	defer func() { _ = closer.Close() }()

	var st BootstrapState
	if err := json.Unmarshal(val, &st); err != nil {
		return BootstrapState{}, fmt.Errorf("backfill: decode bootstrap/state: %w", err)
	}
	return st, nil
}

// PutBootstrapState writes st with a synchronous fsync. We sync
// every write because the row is only touched at phase boundaries,
// crashes between phases must not lose the marker, and the cost is
// negligible (a few writes per bootstrap lifetime).
func PutBootstrapState(s *store.Store, st BootstrapState) error {
	buf, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("backfill: encode bootstrap/state: %w", err)
	}
	if err := s.Set(bootstrapStateKey, buf, pebble.Sync); err != nil {
		return fmt.Errorf("backfill: set bootstrap/state: %w", err)
	}
	return nil
}
