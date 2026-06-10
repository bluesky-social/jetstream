package syncstate

import (
	"context"
	"errors"
	"fmt"
	"sync"

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
	s  *store.Store
	mu sync.Mutex

	stagedChain   map[atmos.DID][]byte
	stagedHosting map[atmos.DID][]byte
}

// New returns a PebbleStateStore that stores chain and hosting state
// in the supplied pebble db under the keyspaces "sync/chain/<did>"
// and "sync/host/<did>".
func New(s *store.Store) *PebbleStateStore {
	return &PebbleStateStore{
		s:             s,
		stagedChain:   make(map[atmos.DID][]byte),
		stagedHosting: make(map[atmos.DID][]byte),
	}
}

func chainKey(did atmos.DID) []byte {
	return []byte(chainPrefix + string(did))
}

func hostKey(did atmos.DID) []byte {
	return []byte(hostPrefix + string(did))
}

func (p *PebbleStateStore) LoadChain(_ context.Context, did atmos.DID) (*atmossync.ChainState, error) {
	p.mu.Lock()
	if val, ok := p.stagedChain[did]; ok {
		cp := append([]byte(nil), val...)
		p.mu.Unlock()
		state, err := decodeChainState(cp)
		if err != nil {
			return nil, fmt.Errorf("syncstate: load staged chain %s: %w", did, err)
		}
		return &state, nil
	}
	p.mu.Unlock()

	val, closer, err := p.s.Get(chainKey(did))
	if errors.Is(err, store.ErrNotFound) {
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
	p.mu.Lock()
	p.stagedChain[did] = append([]byte(nil), buf...)
	p.mu.Unlock()
	return nil
}

func (p *PebbleStateStore) LoadHosting(_ context.Context, did atmos.DID) (*atmossync.HostingState, error) {
	p.mu.Lock()
	if val, ok := p.stagedHosting[did]; ok {
		cp := append([]byte(nil), val...)
		p.mu.Unlock()
		state, err := decodeHostingState(cp)
		if err != nil {
			return nil, fmt.Errorf("syncstate: load staged hosting %s: %w", did, err)
		}
		return &state, nil
	}
	p.mu.Unlock()

	val, closer, err := p.s.Get(hostKey(did))
	if errors.Is(err, store.ErrNotFound) {
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
	p.mu.Lock()
	p.stagedHosting[did] = append([]byte(nil), buf...)
	p.mu.Unlock()
	return nil
}

// StageFlush adds all staged verifier state writes to b. Staged entries are
// cleared only by CommitStaged after the caller's batch commit succeeds.
func (p *PebbleStateStore) StageFlush(b *pebble.Batch) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for did, val := range p.stagedChain {
		if err := b.Set(chainKey(did), val, nil); err != nil {
			return fmt.Errorf("syncstate: stage chain %s: %w", did, err)
		}
	}
	for did, val := range p.stagedHosting {
		if err := b.Set(hostKey(did), val, nil); err != nil {
			return fmt.Errorf("syncstate: stage hosting %s: %w", did, err)
		}
	}
	return nil
}

// CommitStaged clears staged entries after a batch containing StageFlush
// commits successfully.
func (p *PebbleStateStore) CommitStaged() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stagedChain = make(map[atmos.DID][]byte)
	p.stagedHosting = make(map[atmos.DID][]byte)
}

// Flush commits staged state by itself. Production uses StageFlush to batch
// with relay/cursor; tests and maintenance callers use this direct helper.
func (p *PebbleStateStore) Flush() error {
	b := p.s.NewBatch()
	defer func() { _ = b.Close() }()
	if err := p.StageFlush(b); err != nil {
		return err
	}
	if err := p.s.Commit(b, store.SyncWrites); err != nil {
		return fmt.Errorf("syncstate: flush: %w", err)
	}
	p.CommitStaged()
	return nil
}

// Delete atomically removes both chain and hosting state for did via
// a single pebble batch with Sync. Atomicity is required by the
// StateStore contract.
func (p *PebbleStateStore) Delete(_ context.Context, did atmos.DID) error {
	p.mu.Lock()
	delete(p.stagedChain, did)
	delete(p.stagedHosting, did)
	p.mu.Unlock()

	b := p.s.NewBatch()
	defer func() { _ = b.Close() }()

	if err := b.Delete(chainKey(did), nil); err != nil {
		return fmt.Errorf("syncstate: delete chain %s: %w", did, err)
	}
	if err := b.Delete(hostKey(did), nil); err != nil {
		return fmt.Errorf("syncstate: delete hosting %s: %w", did, err)
	}
	if err := p.s.Commit(b, store.SyncWrites); err != nil {
		return fmt.Errorf("syncstate: delete %s: %w", did, err)
	}
	return nil
}
