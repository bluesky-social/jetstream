package syncstate

import (
	"context"
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	atmossync "github.com/jcalabro/atmos/sync"
)

const (
	chainPrefix = "sync/chain/"
	hostPrefix  = "sync/host/"
)

// PebbleStateStore implements sync.StateStore against a *store.Store.
// Construction is cheap; one instance per process is enough — the
// underlying pebble db is concurrency-safe.
type PebbleStateStore struct {
	s *store.Store
}

// New returns a PebbleStateStore that stores chain and hosting state
// in the supplied pebble db under the keyspaces "sync/chain/<did>"
// and "sync/host/<did>".
func New(s *store.Store) *PebbleStateStore {
	return &PebbleStateStore{s: s}
}

func chainKey(did atmos.DID) []byte {
	return []byte(chainPrefix + string(did))
}

func hostKey(did atmos.DID) []byte {
	return []byte(hostPrefix + string(did))
}

func (p *PebbleStateStore) LoadChain(_ context.Context, did atmos.DID) (*atmossync.ChainState, error) {
	val, closer, err := p.s.Get(chainKey(did))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("syncstate: load chain %s: %w", did, err)
	}
	defer func() { _ = closer.Close() }()

	state, err := decodeChainState(val)
	if err != nil {
		return nil, fmt.Errorf("syncstate: load chain %s: %w", did, err)
	}
	return &state, nil
}

func (p *PebbleStateStore) SaveChain(_ context.Context, did atmos.DID, state atmossync.ChainState) error {
	buf, err := encodeChainState(state)
	if err != nil {
		return fmt.Errorf("syncstate: save chain %s: %w", did, err)
	}
	if err := p.s.Set(chainKey(did), buf, store.SyncWrites); err != nil {
		return fmt.Errorf("syncstate: save chain %s: %w", did, err)
	}
	return nil
}

func (p *PebbleStateStore) LoadHosting(_ context.Context, did atmos.DID) (*atmossync.HostingState, error) {
	val, closer, err := p.s.Get(hostKey(did))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("syncstate: load hosting %s: %w", did, err)
	}
	defer func() { _ = closer.Close() }()

	state, err := decodeHostingState(val)
	if err != nil {
		return nil, fmt.Errorf("syncstate: load hosting %s: %w", did, err)
	}
	return &state, nil
}

func (p *PebbleStateStore) SaveHosting(_ context.Context, did atmos.DID, state atmossync.HostingState) error {
	buf, err := encodeHostingState(state)
	if err != nil {
		return fmt.Errorf("syncstate: save hosting %s: %w", did, err)
	}
	if err := p.s.Set(hostKey(did), buf, store.SyncWrites); err != nil {
		return fmt.Errorf("syncstate: save hosting %s: %w", did, err)
	}
	return nil
}

// Delete atomically removes both chain and hosting state for did via
// a single pebble batch with Sync. Atomicity is required by the
// StateStore contract.
func (p *PebbleStateStore) Delete(_ context.Context, did atmos.DID) error {
	b := p.s.NewBatch()
	defer func() { _ = b.Close() }()

	if err := b.Delete(chainKey(did), nil); err != nil {
		return fmt.Errorf("syncstate: delete chain %s: %w", did, err)
	}
	if err := b.Delete(hostKey(did), nil); err != nil {
		return fmt.Errorf("syncstate: delete hosting %s: %w", did, err)
	}
	if err := b.Commit(store.SyncWrites); err != nil {
		return fmt.Errorf("syncstate: delete %s: %w", did, err)
	}
	return nil
}
