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
		return atmosbackfill.StoreEntry{}, err
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

// OnDiscover, OnUpdate, OnComplete, OnFail are added in subsequent
// tasks. Compile-time assertion above will fail until they're done;
// stub them now so the package builds while we work.
func (s *Store) OnDiscover(_ context.Context, _ atmossync.ListReposEntry) error {
	panic("OnDiscover not yet implemented")
}

func (s *Store) OnUpdate(_ context.Context, _ atmossync.ListReposEntry) error {
	panic("OnUpdate not yet implemented")
}

func (s *Store) OnComplete(_ context.Context, _ atmos.DID, _ *repo.Commit) error {
	panic("OnComplete not yet implemented")
}

func (s *Store) OnFail(_ context.Context, _ atmos.DID, _ error, _ int) error {
	panic("OnFail not yet implemented")
}

// timeNow is a package var so tests can pin wall-clock values.
// Production callers don't override this.
var timeNow = func() time.Time { return time.Now().UTC() }
