package world

// These test-only generators create malformed input for the ingest oracle
// (#204); RunTraffic does not call them. Invalid paths enter through raw
// mst.Tree.Insert so they pass verifier consistency checks and reach the
// ingest validators. Invalid revs are signed into the commit to match the
// envelope, with time taken from the logical clock.
//
// AdversarialLedger records expected drops, whole-event cursor gaps, and
// required drop-counter increments. This lets the oracle distinguish injected
// invalid input from unexpected loss.
//
// Invalid UTF-8 cannot appear in a live op.Path because CBOR text decoding
// rejects it. CAR MST keys use byte strings, so
// InjectAdversarialRecordForBackfill can test this case through getRepo.

import (
	"context"
	"fmt"
	"sync"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/gt"
)

// AdversarialSource labels which ingest path a recorded lie targets.
type AdversarialSource string

const (
	AdversarialSourceLive     AdversarialSource = "live"
	AdversarialSourceBackfill AdversarialSource = "backfill"
)

// AdversarialLayer labels which layer of the consuming stack is
// expected to reject the lie. Gate-owned lies land on jetstream's
// shared drop counter with a specific reason; verifier-owned lies are
// rejected or repaired by atmos's Sync-1.1 verifier before the gate.
type AdversarialLayer string

const (
	AdversarialLayerGate     AdversarialLayer = "gate"
	AdversarialLayerVerifier AdversarialLayer = "verifier"
)

// AdversarialEntry is one recorded lie. Reason carries the expected
// drop-reason label for gate-owned lies (matching jetstream's
// ingest.DropReason values: "invalid_rev", "invalid_collection",
// "invalid_rkey", "field_too_long") and a descriptive tag for
// verifier-owned ones. WholeEvent marks lies that drop the entire
// event (every row of the seq) rather than a single op.
type AdversarialEntry struct {
	Source     AdversarialSource
	Layer      AdversarialLayer
	Reason     string
	Seq        int64 // firehose seq of the lying frame; 0 for backfill-only lies
	DID        string
	Collection string
	Rkey       string
	WholeEvent bool
}

// AdversarialLedger accumulates every lie the world told, in emission
// order, plus a key index so honest traffic can refuse to touch lie
// records (see pickUntouchedRecord).
type AdversarialLedger struct {
	mu      sync.Mutex
	entries []AdversarialEntry
	keys    map[string]struct{}
}

func (l *AdversarialLedger) record(e AdversarialEntry) {
	l.recordWithKey(e, "")
}

// recordWithKey records e and indexes rawKey (the verbatim MST key)
// for ContainsKey. rawKey is passed separately because
// repo.SplitMSTKey is lossy: a no-slash key like "nosslash" splits to
// ("nosslash", ""), which would re-join as "nosslash/" and never match
// the real MST key again.
func (l *AdversarialLedger) recordWithKey(e AdversarialEntry, rawKey string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, e)
	if rawKey == "" && (e.Collection != "" || e.Rkey != "") {
		rawKey = e.Collection + "/" + e.Rkey
	}
	if rawKey != "" {
		if l.keys == nil {
			l.keys = make(map[string]struct{})
		}
		l.keys[rawKey] = struct{}{}
	}
}

// ContainsKey reports whether any recorded lie carries the MST key
// (collection/rkey form). Honest traffic generators consult this so
// they never mutate a lie record: a spec-valid-but-unrepresentable
// key (e.g. a 300-byte rkey) passes every spec check, but an honest
// single-op commit touching it would be gate-dropped whole and its
// cursor would never be archived — starving the oracle's gap-free
// cursor accounting on an event the ledger never promised to drop.
func (l *AdversarialLedger) ContainsKey(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.keys[key]
	return ok
}

// Entries returns a copy of all recorded lies in emission order.
func (l *AdversarialLedger) Entries() []AdversarialEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]AdversarialEntry, len(l.entries))
	copy(out, l.entries)
	return out
}

// AdversarialLedger exposes the world's lie ledger for oracle
// reconciliation.
func (w *World) AdversarialLedger() *AdversarialLedger { return &w.adversarial }

// GenerateAdversarialOpForTest emits one #commit frame carrying TWO
// create ops: a benign sibling on a fresh honest path, and a lie whose
// raw MST key is the caller-supplied badKey (full "collection/rkey"
// form, NOT validated). The lie is inserted with mst.Tree.Insert —
// bypassing repo.Create's spec validation — so the signed MST, the CAR
// diff, and the wire op all agree and the commit verifies cleanly.
//
// The sibling is the survivors-contract probe: the oracle asserts it
// archives even though the lie in the same commit drops. Returns the
// sibling's GeneratedChainOp (the row the oracle should find durable).
//
// reason must be the drop-reason label the ingest gate is expected to
// emit for badKey ("invalid_collection", "invalid_rkey", or
// "field_too_long" for spec-valid-but-unrepresentable keys).
func (w *World) GenerateAdversarialOpForTest(ctx context.Context, idx int, badKey, reason string) (GeneratedChainOp, error) {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()

	if err := ctx.Err(); err != nil {
		return GeneratedChainOp{}, err
	}
	author, rp, store, prevState, err := w.loadRepoForTargetedCommit(idx)
	if err != nil {
		return GeneratedChainOp{}, err
	}

	// Benign sibling via the honest, validating path.
	sibColl := chooseCreateCollection(w.rng)
	sibRkey := newRkey(w.rng)
	sibOp, sibPayload, err := w.applyTargetedOp(rp, idx, "create", sibColl, sibRkey)
	if err != nil {
		return GeneratedChainOp{}, err
	}

	// The lie: raw insert, no validation. Reuse the sibling's record
	// block so the CAR carries the CID the wire op claims.
	sibCID, _, err := rp.Get(sibColl, sibRkey)
	if err != nil {
		return GeneratedChainOp{}, fmt.Errorf("simulator: reload sibling for adversarial op: %w", err)
	}
	if err := rp.Tree.Insert(badKey, sibCID); err != nil {
		return GeneratedChainOp{}, fmt.Errorf("simulator: adversarial insert %q: %w", badKey, err)
	}
	badOp := comatproto.SyncSubscribeRepos_RepoOp{
		Action: "create",
		Path:   badKey,
		CID:    gt.Some(lextypes.LexCIDLink{Link: sibCID.String()}),
	}

	frame, newState, err := w.commitAndBroadcast(author, rp, store, prevState, []comatproto.SyncSubscribeRepos_RepoOp{sibOp, badOp}, nil)
	if err != nil {
		return GeneratedChainOp{}, err
	}
	_ = frame

	badColl, badRkey := repo.SplitMSTKey(badKey)
	w.adversarial.recordWithKey(AdversarialEntry{
		Source:     AdversarialSourceLive,
		Layer:      AdversarialLayerGate,
		Reason:     reason,
		Seq:        w.seq.Load(),
		DID:        string(author.DID),
		Collection: badColl,
		Rkey:       badRkey,
	}, badKey)
	return GeneratedChainOp{
		Action:     "create",
		Collection: sibColl,
		Rkey:       sibRkey,
		Rev:        newState.Rev,
		Payload:    sibPayload,
	}, nil
}

// InjectAdversarialRecordForBackfill commits a lie into account idx's
// repo WITHOUT publishing any firehose frame (the silent-mutation
// precedent). The adversarial key rides the persisted MST, so
// jetstream's backfill getRepo download walks straight into it and the
// backfill half of the #197 gate must drop it while archiving the
// account's honest records. This is also the ONLY route for
// invalid-UTF-8 rkeys (wire-blocked on the live path; MST node keys
// are CBOR byte strings and carry arbitrary bytes).
//
// Must be called BEFORE jetstream bootstraps (or before the account's
// repo is fetched) for the lie to be visible to backfill.
func (w *World) InjectAdversarialRecordForBackfill(ctx context.Context, idx int, badKey, reason string) error {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	author, rp, _, _, err := w.loadRepoForTargetedCommit(idx)
	if err != nil {
		return err
	}

	// A real record block for the lie to reference.
	target, err := w.pickTargetAccount(idx)
	if err != nil {
		return err
	}
	rec := generateRecord(w.rng, collPost, string(target))
	data, err := cbor.Marshal(rec)
	if err != nil {
		return fmt.Errorf("simulator: marshal adversarial record: %w", err)
	}
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	if err := rp.Store.PutBlock(cid, data); err != nil {
		return fmt.Errorf("simulator: store adversarial block: %w", err)
	}
	if err := rp.Tree.Insert(badKey, cid); err != nil {
		return fmt.Errorf("simulator: adversarial backfill insert %q: %w", badKey, err)
	}
	if _, err := w.commitAndPersist(author, rp); err != nil {
		return err
	}

	badColl, badRkey := repo.SplitMSTKey(badKey)
	w.adversarial.recordWithKey(AdversarialEntry{
		Source:     AdversarialSourceBackfill,
		Layer:      AdversarialLayerGate,
		Reason:     reason,
		DID:        string(author.DID),
		Collection: badColl,
		Rkey:       badRkey,
	}, badKey)
	return nil
}

// GenerateAdversarialSyncForTest silently mutates a repo, then emits sync
// with an invalid envelope rev. The mutation makes the data CID differ from
// verifier state. A rev at or below verifier state is replay-dropped;
// matching data triggers an envelope/commit mismatch. Divergent data with a
// higher rev instead resyncs and reaches convertSync, which drops the event
// as invalid_rev.
//
// badRev must be invalid as a TID and sort above all TIDs, such as
// "not-a-tid". The resync advances verifier state, so a later valid sync at
// the same rev cannot recover the silently created record. The ledger
// excludes that record and event seq from expected output and cursor
// coverage.
func (w *World) GenerateAdversarialSyncForTest(ctx context.Context, idx int, badRev string) ([]byte, error) {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := atmos.ParseTID(badRev); err == nil {
		return nil, fmt.Errorf("simulator: adversarial sync rev %q is a valid TID; use GenerateSyncForTest for honest syncs", badRev)
	}
	state, err := w.loadState(idx)
	if err != nil {
		return nil, err
	}
	if badRev <= state.Rev {
		return nil, fmt.Errorf("simulator: adversarial sync rev %q sorts at or below head rev %q; the verifier's replay check would silently drop it before the gate", badRev, state.Rev)
	}

	// Targeted silent create so the ledger knows the exact record the
	// dropped sync would have materialized.
	author, rp, _, _, err := w.loadRepoForTargetedCommit(idx)
	if err != nil {
		return nil, err
	}
	coll := chooseCreateCollection(w.rng)
	rkey := newRkey(w.rng)
	if _, _, err := w.applyTargetedOp(rp, idx, "create", coll, rkey); err != nil {
		return nil, err
	}
	if _, err := w.commitAndPersist(author, rp); err != nil {
		return nil, err
	}

	// The honest path stamps Time by parsing the rev; the lie is
	// unparseable, so stamp from the logical clock instead.
	clock, err := w.loadLogicalClock()
	if err != nil {
		return nil, err
	}
	if clock == 0 {
		clock = logicalClockBaseMicros
	}
	frame, seq, did, err := w.emitSyncWithRev(ctx, idx, badRev, formatLogicalClockTime(clock))
	if err != nil {
		return nil, err
	}
	// Whole-event entry: exempts the seq from cursor-gap accounting
	// and drops the KindSync + replacement rows from the expected log.
	w.adversarial.record(AdversarialEntry{
		Source:     AdversarialSourceLive,
		Layer:      AdversarialLayerGate,
		Reason:     "invalid_rev",
		Seq:        seq,
		DID:        did,
		WholeEvent: true,
	})
	// Dropped-op entry: excludes the permanently-unarchivable silent
	// record from final-state ground truth.
	w.adversarial.record(AdversarialEntry{
		Source:     AdversarialSourceLive,
		Layer:      AdversarialLayerGate,
		Reason:     "invalid_rev",
		Seq:        seq,
		DID:        did,
		Collection: coll,
		Rkey:       rkey,
	})
	return frame, nil
}

// commitAndBroadcastWithRev is commitAndBroadcast with a signed-in
// caller-supplied rev and a Time stamped from the logical clock (the
// honest path derives Time by parsing the rev, which a lie fails).
// Adversarial-only; caller must hold mutationMu.
func (w *World) commitAndBroadcastWithRev(author account, rp *repo.Repo, store *diffStore, prevState repoState, wireOps []comatproto.SyncSubscribeRepos_RepoOp, rev string) ([]byte, error) {
	newState, err := w.commitAndPersistWithRev(author, rp, rev)
	if err != nil {
		return nil, err
	}
	carBuf, err := packageCARDiff(store, newState.CommitCID, nil)
	if err != nil {
		return nil, err
	}
	clock, err := w.loadLogicalClock()
	if err != nil {
		return nil, err
	}
	if clock == 0 {
		clock = logicalClockBaseMicros
	}
	frame, _, err := w.broadcastCommitFrame(author, newState, prevState, wireOps, carBuf, formatLogicalClockTime(clock))
	return frame, err
}

// GenerateVerifierRejectedCommitForTest signs an invalid rev into an
// otherwise valid commit. Supported reasons are non_tid_rev (InvalidRevError)
// and future_rev (FutureRevError, more than five minutes ahead of the
// consumer). Callers supply the future TID to match the test clock. The
// verifier rejects these before the ingest gate, so tests check verifier
// classification, archive absence, and cursor advancement rather than gate
// counters.
//
// The world head advances to the invalid rev. Follow with a valid commit on
// the same account before final-state comparison: it restores a fetchable
// head and causes a chain break against Jetstream's earlier state, triggering
// resync. The injected record remains in the world MST and can then be
// repaired. Layer=verifier ledger entries exempt cursor gaps; final-state
// exclusion must account for completed repair.
func (w *World) GenerateVerifierRejectedCommitForTest(ctx context.Context, idx int, badRev, reason string) ([]byte, error) {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	author, rp, store, prevState, err := w.loadRepoForTargetedCommit(idx)
	if err != nil {
		return nil, err
	}

	coll := chooseCreateCollection(w.rng)
	rkey := newRkey(w.rng)
	op, _, err := w.applyTargetedOp(rp, idx, "create", coll, rkey)
	if err != nil {
		return nil, err
	}

	frame, err := w.commitAndBroadcastWithRev(author, rp, store, prevState, []comatproto.SyncSubscribeRepos_RepoOp{op}, badRev)
	if err != nil {
		return nil, err
	}
	w.adversarial.record(AdversarialEntry{
		Source:     AdversarialSourceLive,
		Layer:      AdversarialLayerVerifier,
		Reason:     reason,
		Seq:        w.seq.Load(),
		DID:        string(author.DID),
		Collection: coll,
		Rkey:       rkey,
		WholeEvent: true,
	})
	return frame, nil
}
