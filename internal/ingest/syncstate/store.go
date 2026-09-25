package syncstate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/jcalabro/atmos"
	atmossync "github.com/jcalabro/atmos/sync"
)

const (
	chainPrefix = "sync/chain/"
	hostPrefix  = "sync/host/"
	identPrefix = "sync/ident/"
	acctPrefix  = "sync/acct/"
)

// StateStore implements sync.StateStore over the metadata store. One instance
// can serve the process.
//
// SaveChain and SaveHosting first update pending maps. The verifier can read
// these writes, but they cannot reach the metadata store before the corresponding event
// rows are appended and fsynced.
//
// The live consumer promotes state after appending every row of its event.
// StageFlush adds promoted entries to the synced relay/cursor batch after
// segment fsync. A crash during resync therefore leaves the old verifier
// state, allowing redelivery or a chain break to archive the full replacement
// set.
//
// Chain promotion is keyed by rev (lexicographically ordered TIDs); hosting
// promotion uses the source account event's upstream seq. A later pipelined
// event stays pending until its own rows are appended, and a replayed account
// row cannot promote newer state.
type StateStore struct {
	s  metastore.Store
	mu sync.Mutex

	pendingChain   map[atmos.DID]pendingChainState
	pendingHosting map[atmos.DID]pendingHostingState

	promotedChain   map[atmos.DID][]byte
	promotedHosting map[atmos.DID][]byte

	// promotedIdent is the per-DID applied #identity seq ratchet
	// (#234). Unlike chain/hosting it is jetstream-owned, not verifier
	// state — atmos does not process #identity events, so there is no
	// pending phase: the consumer records the seq from the writer's
	// OnAppend hook (under the writer mutex, before the row's block can
	// flush), and StageFlush persists it with the cursor batch. Values
	// only ratchet upward.
	promotedIdent map[atmos.DID]int64

	// promotedAccount is the per-DID applied #account seq ratchet. It is
	// intentionally separate from verifier hosting state: hosting promotion
	// is gated by the pending event that produced it, but archival replay
	// dedupe only needs to know that this account row was appended.
	promotedAccount map[atmos.DID]int64

	// captured* record exactly which promoted values the most recent
	// StageFlush wrote into its batch. CommitStaged clears only those,
	// so a promotion that lands between StageFlush and CommitStaged is
	// never silently discarded (it flushes with the next batch).
	capturedChain   map[atmos.DID][]byte
	capturedHosting map[atmos.DID][]byte
	capturedIdent   map[atmos.DID]int64
	capturedAccount map[atmos.DID]int64
}

type pendingChainState struct {
	buf []byte
	rev string
}

type pendingHostingState struct {
	buf []byte
	seq int64
}

// New returns a StateStore that stores chain and hosting state in s under the
// keyspaces "sync/chain/<did>" and "sync/host/<did>".
func New(s metastore.Store) *StateStore {
	return &StateStore{
		s:               s,
		pendingChain:    make(map[atmos.DID]pendingChainState),
		pendingHosting:  make(map[atmos.DID]pendingHostingState),
		promotedChain:   make(map[atmos.DID][]byte),
		promotedHosting: make(map[atmos.DID][]byte),
		promotedIdent:   make(map[atmos.DID]int64),
		promotedAccount: make(map[atmos.DID]int64),
	}
}

func chainKey(did atmos.DID) []byte {
	return []byte(chainPrefix + string(did))
}

func hostKey(did atmos.DID) []byte {
	return []byte(hostPrefix + string(did))
}

func identKey(did atmos.DID) []byte {
	return []byte(identPrefix + string(did))
}

func acctKey(did atmos.DID) []byte {
	return []byte(acctPrefix + string(did))
}

func (p *StateStore) LoadChain(ctx context.Context, did atmos.DID) (*atmossync.ChainState, error) {
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

	val, err := p.s.Get(ctx, chainKey(did))
	if errors.Is(err, metastore.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("syncstate: load chain %s: %w", did, err)
	}

	state, err := decodeChainState(val)
	if err != nil {
		return nil, fmt.Errorf("syncstate: load chain %s: %w", did, err)
	}
	return &state, nil
}

func (p *StateStore) SaveChain(_ context.Context, did atmos.DID, state atmossync.ChainState) error {
	buf, err := encodeChainState(state)
	if err != nil {
		return fmt.Errorf("syncstate: save chain %s: %w", did, err)
	}
	p.mu.Lock()
	p.pendingChain[did] = pendingChainState{buf: append([]byte(nil), buf...), rev: state.Rev}
	p.mu.Unlock()
	return nil
}

func (p *StateStore) LoadHosting(ctx context.Context, did atmos.DID) (*atmossync.HostingState, error) {
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

	val, err := p.s.Get(ctx, hostKey(did))
	if errors.Is(err, metastore.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("syncstate: load hosting %s: %w", did, err)
	}

	state, err := decodeHostingState(val)
	if err != nil {
		return nil, fmt.Errorf("syncstate: load hosting %s: %w", did, err)
	}
	return &state, nil
}

// LoadAppliedHosting returns the hosting state for did EXCLUDING pending
// entries: promoted-but-unflushed state first, then the metadata store.
// Pending state is staged at verification time, possibly by a later
// pipelined event whose rows have not been appended yet, so it must not
// inform decisions about what has already been applied to the archive. The live consumer
// uses this view to detect relay-replayed #account events: an event whose
// seq is at or below this seq has already had its row appended (per-DID
// delivery is seq-ordered and promotion happens synchronously after
// append), so a second delivery is a relay duplicate, not new data.
func (p *StateStore) LoadAppliedHosting(ctx context.Context, did atmos.DID) (*atmossync.HostingState, error) {
	p.mu.Lock()
	var buf []byte
	if promoted, ok := p.promotedHosting[did]; ok {
		buf = append([]byte(nil), promoted...)
	}
	p.mu.Unlock()
	if buf == nil {
		val, err := p.s.Get(ctx, hostKey(did))
		if errors.Is(err, metastore.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("syncstate: load applied hosting %s: %w", did, err)
		}
		buf = val
	}
	state, err := decodeHostingState(buf)
	if err != nil {
		return nil, fmt.Errorf("syncstate: load applied hosting %s: %w", did, err)
	}
	return &state, nil
}

func (p *StateStore) SaveHosting(_ context.Context, did atmos.DID, state atmossync.HostingState) error {
	buf, err := encodeHostingState(state)
	if err != nil {
		return fmt.Errorf("syncstate: save hosting %s: %w", did, err)
	}
	p.mu.Lock()
	p.pendingHosting[did] = pendingHostingState{buf: append([]byte(nil), buf...), seq: state.Seq}
	p.mu.Unlock()
	return nil
}

// LoadAppliedIdentitySeq returns the highest #identity seq whose row
// has been appended for did (promoted-but-unflushed first, then the
// metadata store), or 0 when the DID has never had an identity row. The live
// consumer uses it to detect relay-replayed #identity events (#234) —
// the exact analogue of LoadAppliedHosting for a kind atmos does not
// verify, so jetstream owns the whole lifecycle.
func (p *StateStore) LoadAppliedIdentitySeq(ctx context.Context, did atmos.DID) (int64, error) {
	p.mu.Lock()
	seq, ok := p.promotedIdent[did]
	p.mu.Unlock()
	if ok {
		return seq, nil
	}

	val, err := p.s.Get(ctx, identKey(did))
	if errors.Is(err, metastore.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("syncstate: load applied identity seq %s: %w", did, err)
	}
	got, err := decodeIdentitySeq(val)
	if err != nil {
		return 0, fmt.Errorf("syncstate: load applied identity seq %s: %w", did, err)
	}
	return got, nil
}

// LoadAppliedAccountSeq returns the highest #account seq whose row has
// been appended for did (promoted-but-unflushed first, then the metadata
// store), or 0 when the DID has never had an account row. This is Jetstream's
// archive-owned replay ratchet; it deliberately does not consult verifier
// hosting state, whose pending/promotion lifecycle has a different contract.
func (p *StateStore) LoadAppliedAccountSeq(ctx context.Context, did atmos.DID) (int64, error) {
	p.mu.Lock()
	seq, ok := p.promotedAccount[did]
	p.mu.Unlock()
	if ok {
		return seq, nil
	}

	val, err := p.s.Get(ctx, acctKey(did))
	if errors.Is(err, metastore.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("syncstate: load applied account seq %s: %w", did, err)
	}
	got, err := decodeIdentitySeq(val)
	if err != nil {
		return 0, fmt.Errorf("syncstate: load applied account seq %s: %w", did, err)
	}
	return got, nil
}

// RecordIdentitySeq stages the applied #identity seq for did, to be
// flushed by the next StageFlush batch. Ratchet-only: a seq at or
// below the staged value is ignored, so out-of-order calls can never
// move the guard backwards. Callers must invoke it from the writer's
// OnAppend hook — after the row is buffered, before its block can
// flush. That ordering guarantees the ratchet is staged before ANY
// cursor batch that could cover the row commits (a full-block flush inside
// Append stages the durable batch synchronously), so a durable identity row
// always has a durable ratchet, and the flush batch commits after the segment
// fsync, so a durable ratchet value always has its row durable too.
func (p *StateStore) RecordIdentitySeq(did atmos.DID, seq int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cur, ok := p.promotedIdent[did]; ok && cur >= seq {
		return
	}
	p.promotedIdent[did] = seq
}

// RecordAccountSeq stages the applied #account seq for did, to be flushed
// by the next StageFlush batch. Ratchet-only, with the same append-before-
// flush ordering requirement as RecordIdentitySeq.
func (p *StateStore) RecordAccountSeq(did atmos.DID, seq int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cur, ok := p.promotedAccount[did]; ok && cur >= seq {
		return
	}
	p.promotedAccount[did] = seq
}

// PromoteChain marks the pending chain entry for did as flushable iff
// its rev is <= maxRev — i.e. it was produced by the upstream event
// whose rows the caller just finished appending (or an earlier one).
// A pending entry with a newer rev belongs to a later pipelined event
// whose rows have not landed yet; it stays pending.
func (p *StateStore) PromoteChain(did atmos.DID, maxRev string) {
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
func (p *StateStore) PromoteHosting(did atmos.DID, maxSeq int64) {
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
func (p *StateStore) StageFlush(b metastore.Batch) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.capturedChain = make(map[atmos.DID][]byte, len(p.promotedChain))
	p.capturedHosting = make(map[atmos.DID][]byte, len(p.promotedHosting))
	p.capturedIdent = make(map[atmos.DID]int64, len(p.promotedIdent))
	p.capturedAccount = make(map[atmos.DID]int64, len(p.promotedAccount))
	for did, val := range p.promotedChain {
		b.Set(chainKey(did), val)
		p.capturedChain[did] = val
	}
	for did, val := range p.promotedHosting {
		b.Set(hostKey(did), val)
		p.capturedHosting[did] = val
	}
	for did, seq := range p.promotedIdent {
		b.Set(identKey(did), encodeIdentitySeq(seq))
		p.capturedIdent[did] = seq
	}
	for did, seq := range p.promotedAccount {
		b.Set(acctKey(did), encodeIdentitySeq(seq))
		p.capturedAccount[did] = seq
	}
}

// CommitStaged clears the promoted entries captured by the most recent
// StageFlush after that batch commits successfully. Entries promoted
// (or re-saved) after the capture are left in place for the next flush
// — clearing the whole map here would silently discard a write that
// was never in the batch.
func (p *StateStore) CommitStaged() {
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
	for did, captured := range p.capturedIdent {
		if cur, ok := p.promotedIdent[did]; ok && cur == captured {
			delete(p.promotedIdent, did)
		}
	}
	for did, captured := range p.capturedAccount {
		if cur, ok := p.promotedAccount[did]; ok && cur == captured {
			delete(p.promotedAccount, did)
		}
	}
	p.capturedChain = nil
	p.capturedHosting = nil
	p.capturedIdent = nil
	p.capturedAccount = nil
}

// Delete atomically removes both chain and hosting state for did in one
// metadata batch. Atomicity is required by the sync.StateStore contract, which
// is also the only reason Delete exists.
//
// Delete must not race the consumer's StageFlush/CommitStaged window:
// today nothing calls it (atmos documents it for operator tooling), but
// a future caller must run it on the consumer goroutine or while the
// consumer is stopped, or a captured promoted entry could be re-written
// by the in-flight cursor batch after this delete commits.
func (p *StateStore) Delete(ctx context.Context, did atmos.DID) error {
	p.mu.Lock()
	delete(p.pendingChain, did)
	delete(p.pendingHosting, did)
	delete(p.promotedChain, did)
	delete(p.promotedHosting, did)
	delete(p.promotedIdent, did)
	delete(p.promotedAccount, did)
	delete(p.capturedChain, did)
	delete(p.capturedHosting, did)
	delete(p.capturedIdent, did)
	delete(p.capturedAccount, did)
	p.mu.Unlock()

	b := p.s.NewBatch()
	b.Delete(chainKey(did))
	b.Delete(hostKey(did))
	b.Delete(identKey(did))
	b.Delete(acctKey(did))
	if err := b.Commit(ctx); err != nil {
		return fmt.Errorf("syncstate: delete %s: %w", did, err)
	}
	return nil
}
