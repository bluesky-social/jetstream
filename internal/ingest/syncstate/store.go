package syncstate

import (
	"bytes"
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
//
// Durability is two-phase so verifier state can never run ahead of the
// archive (DESIGN.md invariant 1 / compaction spec §2.2):
//
//  1. SaveChain/SaveHosting land in PENDING maps. atmos calls them at
//     verification time, from its worker goroutines, before the event's
//     rows have been appended to a segment — let alone fsynced. Pending
//     entries are visible to LoadChain/LoadHosting (the verifier must
//     observe its own writes) but are never written to pebble; if the
//     process dies, they die with it, the relay redelivers from the
//     cursor, and re-verification regenerates them.
//
//  2. The live consumer PROMOTES an entry once every row of the upstream
//     event that produced it has been appended to the segment writer.
//     Promoted entries are flushed to pebble by StageFlush in the same
//     batch that advances relay/cursor, which the consumer commits only
//     after the segment fsync — so a durable chain/hosting entry always
//     has its event rows durable too. A crash mid-resync therefore
//     leaves chain state at the pre-resync rev: the #sync redelivers (or
//     the next commit chain-breaks) and a fresh resync re-archives the
//     full replacement set, instead of a durable KindSync tombstone
//     silently orphaning a partial one.
//
// Chain promotion is rev-keyed (TIDs sort lexicographically) and
// hosting promotion is seq-keyed (HostingState.Seq is the source
// #account event's upstream seq): a pending entry staged by a LATER
// pipelined event for the same DID stays pending until that event's
// own rows land, and a redelivered (verifier-replay-dropped) account
// row can never promote a newer event's state.
type PebbleStateStore struct {
	s  *store.Store
	mu sync.Mutex

	pendingChain   map[atmos.DID]pendingChainState
	pendingHosting map[atmos.DID]pendingHostingState

	promotedChain   map[atmos.DID][]byte
	promotedHosting map[atmos.DID][]byte

	// captured* record exactly which promoted values the most recent
	// StageFlush wrote into its batch. CommitStaged clears only those,
	// so a promotion that lands between StageFlush and CommitStaged is
	// never silently discarded (it flushes with the next batch).
	capturedChain   map[atmos.DID][]byte
	capturedHosting map[atmos.DID][]byte
}

type pendingChainState struct {
	buf []byte
	rev string
}

type pendingHostingState struct {
	buf []byte
	seq int64
}

// New returns a PebbleStateStore that stores chain and hosting state
// in the supplied pebble db under the keyspaces "sync/chain/<did>"
// and "sync/host/<did>".
func New(s *store.Store) *PebbleStateStore {
	return &PebbleStateStore{
		s:               s,
		pendingChain:    make(map[atmos.DID]pendingChainState),
		pendingHosting:  make(map[atmos.DID]pendingHostingState),
		promotedChain:   make(map[atmos.DID][]byte),
		promotedHosting: make(map[atmos.DID][]byte),
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
	var buf []byte
	if pending, ok := p.pendingChain[did]; ok {
		buf = append([]byte(nil), pending.buf...)
	} else if promoted, ok := p.promotedChain[did]; ok {
		buf = append([]byte(nil), promoted...)
	}
	p.mu.Unlock()
	if buf != nil {
		state, err := decodeChainState(buf)
		if err != nil {
			return nil, fmt.Errorf("syncstate: load staged chain %s: %w", did, err)
		}
		return &state, nil
	}

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
	p.pendingChain[did] = pendingChainState{buf: append([]byte(nil), buf...), rev: state.Rev}
	p.mu.Unlock()
	return nil
}

func (p *PebbleStateStore) LoadHosting(_ context.Context, did atmos.DID) (*atmossync.HostingState, error) {
	p.mu.Lock()
	var buf []byte
	if pending, ok := p.pendingHosting[did]; ok {
		buf = append([]byte(nil), pending.buf...)
	} else if promoted, ok := p.promotedHosting[did]; ok {
		buf = append([]byte(nil), promoted...)
	}
	p.mu.Unlock()
	if buf != nil {
		state, err := decodeHostingState(buf)
		if err != nil {
			return nil, fmt.Errorf("syncstate: load staged hosting %s: %w", did, err)
		}
		return &state, nil
	}

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
	p.pendingHosting[did] = pendingHostingState{buf: append([]byte(nil), buf...), seq: state.Seq}
	p.mu.Unlock()
	return nil
}

// PromoteChain marks the pending chain entry for did as flushable iff
// its rev is <= maxRev — i.e. it was produced by the upstream event
// whose rows the caller just finished appending (or an earlier one).
// A pending entry with a newer rev belongs to a later pipelined event
// whose rows have not landed yet; it stays pending.
func (p *PebbleStateStore) PromoteChain(did atmos.DID, maxRev string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pending, ok := p.pendingChain[did]
	if !ok || pending.rev > maxRev {
		return
	}
	p.promotedChain[did] = pending.buf
	delete(p.pendingChain, did)
}

// PromoteHosting marks the pending hosting entry for did as flushable
// iff its source #account event seq is <= maxSeq — i.e. the archived
// KindAccount row the caller just appended (UpstreamRelayCursor) is
// the event that produced it, or a later one. A redelivered account
// row (which the verifier replay-drops without re-staging) carries an
// older seq and can never promote a newer event's pending state.
func (p *PebbleStateStore) PromoteHosting(did atmos.DID, maxSeq int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pending, ok := p.pendingHosting[did]
	if !ok || pending.seq > maxSeq {
		return
	}
	p.promotedHosting[did] = pending.buf
	delete(p.pendingHosting, did)
}

// StageFlush adds all PROMOTED verifier state writes to b and records
// the staged values so CommitStaged can clear exactly them. Pending
// (not yet promoted) entries are never flushed — their event rows are
// not durable yet.
func (p *PebbleStateStore) StageFlush(b *pebble.Batch) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.capturedChain = make(map[atmos.DID][]byte, len(p.promotedChain))
	p.capturedHosting = make(map[atmos.DID][]byte, len(p.promotedHosting))
	for did, val := range p.promotedChain {
		if err := b.Set(chainKey(did), val, nil); err != nil {
			return fmt.Errorf("syncstate: stage chain %s: %w", did, err)
		}
		p.capturedChain[did] = val
	}
	for did, val := range p.promotedHosting {
		if err := b.Set(hostKey(did), val, nil); err != nil {
			return fmt.Errorf("syncstate: stage hosting %s: %w", did, err)
		}
		p.capturedHosting[did] = val
	}
	return nil
}

// CommitStaged clears the promoted entries captured by the most recent
// StageFlush after that batch commits successfully. Entries promoted
// (or re-saved) after the capture are left in place for the next flush
// — clearing the whole map here would silently discard a write that
// was never in the batch.
func (p *PebbleStateStore) CommitStaged() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for did, captured := range p.capturedChain {
		if cur, ok := p.promotedChain[did]; ok && bytes.Equal(cur, captured) {
			delete(p.promotedChain, did)
		}
	}
	for did, captured := range p.capturedHosting {
		if cur, ok := p.promotedHosting[did]; ok && bytes.Equal(cur, captured) {
			delete(p.promotedHosting, did)
		}
	}
	p.capturedChain = nil
	p.capturedHosting = nil
}

// Flush commits promoted state by itself, outside the consumer's
// cursor batch. Used on shutdown paths (after the consumer's writer
// has fsynced everything it appended) and by tests. Pending entries
// are deliberately NOT flushed: their events' rows may never have been
// appended, and committing them would let verifier state run ahead of
// the archive across a restart.
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
//
// Delete must not race the consumer's StageFlush/CommitStaged window:
// today nothing calls it (atmos documents it for operator tooling), but
// a future caller must run it on the consumer goroutine or while the
// consumer is stopped, or a captured promoted entry could be re-written
// by the in-flight cursor batch after this delete commits.
func (p *PebbleStateStore) Delete(_ context.Context, did atmos.DID) error {
	p.mu.Lock()
	delete(p.pendingChain, did)
	delete(p.pendingHosting, did)
	delete(p.promotedChain, did)
	delete(p.promotedHosting, did)
	delete(p.capturedChain, did)
	delete(p.capturedHosting, did)
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
