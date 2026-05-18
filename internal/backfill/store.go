// Package backfill: store.go implements the atmos backfill.Store
// interface against pebble. Keys live at repo/<did>; values are the
// JSON-encoded RepoStatus from status.go.
//
// All callbacks the engine fires (OnDiscover, OnUpdate, OnComplete,
// OnFail) write with pebble.Sync to satisfy atmos's durability
// contract: the engine treats a successful return as durable.
//
// atmos guarantees no two callbacks are in flight for the same DID
// simultaneously, so OnUpdate/OnComplete/OnFail use a non-transactional
// read-modify-write to preserve fields a future PR may have added to
// RepoStatus (e.g. RecordCount).
package backfill

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
)

// Store implements atmosbackfill.Store against the shared pebble
// metadata store. Construct via NewStore.
type Store struct {
	db      *store.Store
	metrics *Metrics
}

// Compile-time guarantee that Store satisfies the atmos contract.
var _ atmosbackfill.Store = (*Store)(nil)

// NewStore constructs a Store backed by the shared metadata pebble db.
// metrics may be nil; callbacks are no-ops in that case.
func NewStore(db *store.Store, metrics *Metrics) *Store {
	return &Store{db: db, metrics: metrics}
}

// Lookup reads repo/<did> and projects the on-disk RepoStatus into
// atmos's StoreEntry shape. A missing row returns StateUnknown — that's
// how atmos tells the engine to fire OnDiscover.
func (s *Store) Lookup(_ context.Context, did atmos.DID) (atmosbackfill.StoreEntry, error) {
	val, closer, err := s.db.Get(repoKey(did))
	if errors.Is(err, pebble.ErrNotFound) {
		return atmosbackfill.StoreEntry{State: atmosbackfill.StateUnknown}, nil
	}
	if err != nil {
		return atmosbackfill.StoreEntry{}, fmt.Errorf("backfill: lookup %s: %w", did, err)
	}
	defer func() { _ = closer.Close() }()

	rs, err := decodeRepoStatus(val)
	if err != nil {
		return atmosbackfill.StoreEntry{}, fmt.Errorf("backfill: lookup %s: %w", did, err)
	}

	var st atmosbackfill.State
	switch rs.Backfill.Status {
	case StatusNotStarted:
		st = atmosbackfill.StateDiscovered
	case StatusComplete:
		st = atmosbackfill.StateComplete
	case StatusFailed:
		st = atmosbackfill.StateFailed
	default:
		return atmosbackfill.StoreEntry{}, fmt.Errorf("backfill: lookup %s: unknown status %q", did, rs.Backfill.Status)
	}
	return atmosbackfill.StoreEntry{State: st, Active: rs.Active}, nil
}

// putRepoStatus writes the value durably. Used by all write paths.
func (s *Store) putRepoStatus(did atmos.DID, rs *RepoStatus) error {
	enc, err := encodeRepoStatus(rs)
	if err != nil {
		return err
	}
	if err := s.db.Set(repoKey(did), enc, pebble.Sync); err != nil {
		return fmt.Errorf("backfill: write repo/%s: %w", did, err)
	}
	return nil
}

// readRepoStatus is the RMW helper for OnUpdate/OnComplete/OnFail.
// It returns (nil, nil) when the row doesn't exist so callers can
// decide whether absence is an error in their context.
func (s *Store) readRepoStatus(did atmos.DID) (*RepoStatus, error) {
	val, closer, err := s.db.Get(repoKey(did))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("backfill: read repo/%s: %w", did, err)
	}
	defer func() { _ = closer.Close() }()
	return decodeRepoStatus(val)
}

// OnDiscover writes a fresh RepoStatus at status=not_started for a
// DID the engine has never seen. atmos guarantees this fires at most
// once per DID per Lookup-StateUnknown path.
func (s *Store) OnDiscover(_ context.Context, entry atmossync.ListReposEntry) error {
	rs := &RepoStatus{
		Backfill: RepoBackfillStatus{
			Status:    StatusNotStarted,
			StartedAt: timeNow(),
		},
		Active: entry.Active,
	}
	if err := s.putRepoStatus(entry.DID, rs); err != nil {
		return err
	}
	s.metrics.incDiscovered()
	return nil
}

// OnFail is added in a subsequent task.
// Compile-time assertion above will fail until it's done;
// stub it now so the package builds while we work.

// OnUpdate flips the Active flag on an existing row. The lifecycle
// Status is preserved — atmos fires OnUpdate only when the
// listRepos.Active value differs from what the Store last saw, and
// it never changes the Status as a side effect.
func (s *Store) OnUpdate(_ context.Context, entry atmossync.ListReposEntry) error {
	rs, err := s.readRepoStatus(entry.DID)
	if err != nil {
		return err
	}
	if rs == nil {
		return fmt.Errorf("backfill: on_update %s: missing row (atmos invariant violation)", entry.DID)
	}
	rs.Active = entry.Active
	if err := s.putRepoStatus(entry.DID, rs); err != nil {
		return err
	}
	s.metrics.incActiveFlips()
	return nil
}

// OnComplete records a successful repo download. The commit's rev is
// stored in both Backfill.Rev (the rev at end of initial download
// per DESIGN.md §3.5) and the top-level Rev (the latest known rev).
// They're equal here because initial backfill is the only thing
// updating Rev in this PR; steady-state ingest will diverge them.
//
// We RMW rather than write fresh so a future field on RepoStatus
// (RecordCount, TotalBytes) added between OnDiscover and OnComplete
// survives. atmos's no-concurrent-callback guarantee per-DID makes
// the RMW race-free.
func (s *Store) OnComplete(_ context.Context, did atmos.DID, commit *repo.Commit) error {
	rs, err := s.readRepoStatus(did)
	if err != nil {
		return err
	}
	if rs == nil {
		// Defensive: the engine only fires OnComplete after a Lookup
		// returned Discovered/Failed, so the row exists. If somehow
		// it doesn't, recreate it rather than failing the run — the
		// download already happened and we don't want to lose the
		// progress signal.
		rs = &RepoStatus{}
	}
	now := timeNow()
	rs.Backfill.Status = StatusComplete
	rs.Backfill.Rev = commit.Rev
	rs.Backfill.CompletedAt = now
	rs.Backfill.LastError = ""
	rs.Rev = commit.Rev
	rs.UpdatedAt = now
	if err := s.putRepoStatus(did, rs); err != nil {
		return err
	}
	s.metrics.incCompleted()
	return nil
}

func (s *Store) OnFail(_ context.Context, _ atmos.DID, _ error, _ int) error {
	panic("OnFail not yet implemented")
}

// timeNow is a package var so tests can pin wall-clock values.
// Production callers don't override this.
var timeNow = func() time.Time { return time.Now().UTC() }
