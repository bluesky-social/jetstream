package syncstate

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
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
// Each durable batch snapshots the promoted entries when it samples its relay
// cursor and stages them into the relay/cursor batch after segment fsync. A crash during resync therefore leaves the old verifier
// state, allowing redelivery or a chain break to archive the full replacement
// set.
//
// Chain promotion is keyed by rev (lexicographically ordered TIDs); hosting
// promotion uses the source account event's upstream seq. A later pipelined
// event stays pending until its own rows are appended, and a replayed account
// row cannot promote newer state.
//
// The verifier runs ahead of the appends, so a DID can have several verified
// events in flight. Each keeps its own pending entry: an event's promotion
// must make exactly its own state durable. With one entry per DID, a later
// event's save hid the earlier event's state, so a batch could commit the
// earlier event's rows and a cursor past it with the DID's state still older.
// After a crash, redelivery then archived the event twice, or the next commit
// chain-broke into a needless resync.
type StateStore struct {
	s  metastore.Store
	mu sync.Mutex

	pendingChain   map[atmos.DID][]pending[string]
	pendingHosting map[atmos.DID][]pending[int64]

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

	// unsnapped* are the promotions since the last Snapshot, a subset of
	// promoted*. A snapshot takes them rather than cloning promoted*: its
	// batch commits after every earlier snapshot's batch, which carried the
	// rest. Cloning all of promoted* made every pipelined hot batch restage
	// every uncommitted entry, so a commit backlog (bulk recovery) grew each
	// batch and slowed commits further (design §22.2).
	unsnappedChain   map[atmos.DID][]byte
	unsnappedHosting map[atmos.DID][]byte
	unsnappedIdent   map[atmos.DID]int64
	unsnappedAccount map[atmos.DID]int64

	// staged records exactly which promoted values each StageSnapshot wrote
	// into its batch, oldest first, for batches that have neither committed
	// nor failed. CommitStaged clears only the oldest's, so a promotion that
	// lands between StageSnapshot and CommitStaged is never silently
	// discarded (it flushes with the next batch). Several can be staged at
	// once: a group commit stages each of its batches before committing
	// them in one transaction.
	staged []*Snapshot
	// failed merges what AbortStaged took from staged; the next
	// StageSnapshot carries the entries that are still current.
	failed *Snapshot
}

// pending is one verified but unpromoted state: the chain state after a
// commit (key is its rev) or the hosting state after an account event (key is
// its upstream seq). A DID's entries are oldest first with increasing keys.
type pending[K cmp.Ordered] struct {
	buf []byte
	key K
}

// maxPendingPerDID bounds a DID's queue. The verifier cannot run far ahead of
// the appends, so it is reached only when events verify but never append
// (malformed after verification, say) and no later event promotes past them.
// Dropping the oldest then loses only that entry's promotion, which leaves
// durable state older than the archive: a chain break and resync, not a lost
// or duplicated event.
const maxPendingPerDID = 64

// savePending queues e as did's newest pending state. Entries at or above
// e's key are superseded: the verifier's view of the DID moved back.
func savePending[K cmp.Ordered](m map[atmos.DID][]pending[K], did atmos.DID, e pending[K]) {
	q := m[did]
	for len(q) > 0 && q[len(q)-1].key >= e.key {
		q = q[:len(q)-1]
	}
	if len(q) >= maxPendingPerDID {
		q = append(q[:0], q[len(q)-maxPendingPerDID+1:]...)
	}
	m[did] = append(q, e)
}

// latestPending returns did's newest pending state.
func latestPending[K cmp.Ordered](m map[atmos.DID][]pending[K], did atmos.DID) ([]byte, bool) {
	q := m[did]
	if len(q) == 0 {
		return nil, false
	}
	return q[len(q)-1].buf, true
}

// takePending removes and returns did's newest pending state whose key is at
// most max, with every older one, which it supersedes.
func takePending[K cmp.Ordered](m map[atmos.DID][]pending[K], did atmos.DID, max K) ([]byte, bool) {
	q := m[did]
	i := len(q) - 1
	for i >= 0 && q[i].key > max {
		i--
	}
	if i < 0 {
		return nil, false
	}
	buf := q[i].buf
	if i == len(q)-1 {
		delete(m, did)
	} else {
		m[did] = q[i+1:]
	}
	return buf, true
}

// New returns a StateStore that stores chain and hosting state in s under the
// keyspaces "sync/chain/<did>" and "sync/host/<did>".
func New(s metastore.Store) *StateStore {
	p := &StateStore{
		s:               s,
		pendingChain:    make(map[atmos.DID][]pending[string]),
		pendingHosting:  make(map[atmos.DID][]pending[int64]),
		promotedChain:   make(map[atmos.DID][]byte),
		promotedHosting: make(map[atmos.DID][]byte),
		promotedIdent:   make(map[atmos.DID]int64),
		promotedAccount: make(map[atmos.DID]int64),
	}
	p.resetUnsnappedLocked()
	return p
}

func (p *StateStore) resetUnsnappedLocked() {
	s := newSnapshot()
	p.unsnappedChain, p.unsnappedHosting = s.chain, s.hosting
	p.unsnappedIdent, p.unsnappedAccount = s.ident, s.account
}

func newSnapshot() *Snapshot {
	return &Snapshot{
		chain:   make(map[atmos.DID][]byte),
		hosting: make(map[atmos.DID][]byte),
		ident:   make(map[atmos.DID]int64),
		account: make(map[atmos.DID]int64),
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
	if pending, ok := latestPending(p.pendingChain, did); ok {
		buf = append([]byte(nil), pending...)
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
	savePending(p.pendingChain, did, pending[string]{buf: append([]byte(nil), buf...), key: state.Rev})
	p.mu.Unlock()
	return nil
}

func (p *StateStore) LoadHosting(ctx context.Context, did atmos.DID) (*atmossync.HostingState, error) {
	p.mu.Lock()
	var buf []byte
	if pending, ok := latestPending(p.pendingHosting, did); ok {
		buf = append([]byte(nil), pending...)
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
	savePending(p.pendingHosting, did, pending[int64]{buf: append([]byte(nil), buf...), key: state.Seq})
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
	p.unsnappedIdent[did] = seq
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
	p.unsnappedAccount[did] = seq
}

// PromoteChain marks the newest pending chain entry for did with rev <=
// maxRev as flushable — the one produced by the upstream event whose rows
// the caller just finished appending (or an earlier one) — and drops the
// older entries it supersedes. Entries with a newer rev belong to later
// pipelined events whose rows have not landed yet; they stay pending.
func (p *StateStore) PromoteChain(did atmos.DID, maxRev string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	buf, ok := takePending(p.pendingChain, did, maxRev)
	if !ok {
		return
	}
	p.promotedChain[did] = buf
	p.unsnappedChain[did] = buf
}

// PromoteHosting marks the newest pending hosting entry for did whose
// source #account event seq is <= maxSeq as flushable — i.e. the archived
// KindAccount row the caller just appended (UpstreamRelayCursor) is
// the event that produced it, or a later one. A redelivered account
// row (which the verifier replay-drops without re-staging) carries an
// older seq and can never promote a newer event's pending state.
func (p *StateStore) PromoteHosting(did atmos.DID, maxSeq int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	buf, ok := takePending(p.pendingHosting, did, maxSeq)
	if !ok {
		return
	}
	p.promotedHosting[did] = buf
	p.unsnappedHosting[did] = buf
}

// Snapshot is the state promoted between two instants, for a later
// StageSnapshot.
type Snapshot struct {
	chain, hosting map[atmos.DID][]byte
	ident, account map[atmos.DID]int64
}

func (s *Snapshot) forget(did atmos.DID) {
	delete(s.chain, did)
	delete(s.hosting, did)
	delete(s.ident, did)
	delete(s.account, did)
}

// Snapshot captures the state promoted since the previous Snapshot. A
// durable batch whose writes are prepared before they commit (async flush,
// pipelined hot batches) must snapshot when it samples its relay cursor,
// under the writer mutex: an entry promoted later can belong to an event
// whose rows are in a later batch, and persisting it with this one lets a
// crash between the two commits leave state newer than the archive, so the
// verifier drops the redelivered event as a rev replay.
//
// Staging only the delta relies on snapshots' batches committing in the
// order they were taken, which the writer guarantees: it commits in seq
// order, and a failed commit ends the writer and its session's StateStore.
// Were a later batch to commit after an earlier one failed, the failed
// batch's entries would be missing, which leaves durable state older than
// the archive: the verifier then sees a chain break and resyncs, rather than
// dropping an event.
func (p *StateStore) Snapshot() *Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	snap := &Snapshot{
		chain:   p.unsnappedChain,
		hosting: p.unsnappedHosting,
		ident:   p.unsnappedIdent,
		account: p.unsnappedAccount,
	}
	p.resetUnsnappedLocked()
	return snap
}

// StageFlush stages the state promoted now; see StageSnapshot.
func (p *StateStore) StageFlush(b metastore.Batch) {
	p.StageSnapshot(b, p.Snapshot())
}

// StageSnapshot adds snap's verifier state writes to b and records them so
// CommitStaged can clear exactly them. Pending (not yet promoted) entries
// are never flushed: their event rows are not durable yet.
//
// Each staged batch must end in CommitStaged or AbortStaged, in staging
// order.
func (p *StateStore) StageSnapshot(b metastore.Batch, snap *Snapshot) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Entries staged into a batch that failed to commit and still current
	// ride with this batch, as they would have in a full snapshot.
	if f := p.failed; f != nil {
		carry(snap.chain, f.chain, p.promotedChain, bytes.Equal)
		carry(snap.hosting, f.hosting, p.promotedHosting, bytes.Equal)
		carry(snap.ident, f.ident, p.promotedIdent, func(a, b int64) bool { return a == b })
		carry(snap.account, f.account, p.promotedAccount, func(a, b int64) bool { return a == b })
		p.failed = nil
	}
	p.staged = append(p.staged, snap)
	for did, val := range snap.chain {
		b.Set(chainKey(did), val)
	}
	for did, val := range snap.hosting {
		b.Set(hostKey(did), val)
	}
	for did, seq := range snap.ident {
		b.Set(identKey(did), encodeIdentitySeq(seq))
	}
	for did, seq := range snap.account {
		b.Set(acctKey(did), encodeIdentitySeq(seq))
	}
}

// carry adds to dst each failed capture that is still the promoted value
// and that dst does not supersede.
func carry[V any](dst, failed, promoted map[atmos.DID]V, equal func(a, b V) bool) {
	for did, v := range failed {
		if _, ok := dst[did]; ok {
			continue
		}
		if cur, ok := promoted[did]; ok && equal(cur, v) {
			dst[did] = v
		}
	}
}

// CommitStaged clears the promoted entries the oldest staged batch
// captured, after that batch commits successfully. Entries promoted
// (or re-saved) after the capture are left in place for the next flush
// — clearing the whole map here would silently discard a write that
// was never in the batch.
func (p *StateStore) CommitStaged() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.staged) == 0 {
		return
	}
	s := p.staged[0]
	p.staged[0] = nil
	p.staged = p.staged[1:]
	for did, captured := range s.chain {
		if cur, ok := p.promotedChain[did]; ok && bytes.Equal(cur, captured) {
			delete(p.promotedChain, did)
		}
	}
	for did, captured := range s.hosting {
		if cur, ok := p.promotedHosting[did]; ok && bytes.Equal(cur, captured) {
			delete(p.promotedHosting, did)
		}
	}
	for did, captured := range s.ident {
		if cur, ok := p.promotedIdent[did]; ok && cur == captured {
			delete(p.promotedIdent, did)
		}
	}
	for did, captured := range s.account {
		if cur, ok := p.promotedAccount[did]; ok && cur == captured {
			delete(p.promotedAccount, did)
		}
	}
}

// AbortStaged records that every staged batch not yet committed failed.
// Writers commit in order and nothing after a failure, so a failed batch
// takes every later staged batch with it. The next StageSnapshot restages
// what they held that is still current.
func (p *StateStore) AbortStaged() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.staged) == 0 {
		return
	}
	if p.failed == nil {
		p.failed = newSnapshot()
	}
	// Oldest first, so a later capture of the same DID wins.
	for _, s := range p.staged {
		maps.Copy(p.failed.chain, s.chain)
		maps.Copy(p.failed.hosting, s.hosting)
		maps.Copy(p.failed.ident, s.ident)
		maps.Copy(p.failed.account, s.account)
	}
	p.staged = nil
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
	delete(p.unsnappedChain, did)
	delete(p.unsnappedHosting, did)
	delete(p.unsnappedIdent, did)
	delete(p.unsnappedAccount, did)
	for _, s := range p.staged {
		s.forget(did)
	}
	if p.failed != nil {
		p.failed.forget(did)
	}
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
