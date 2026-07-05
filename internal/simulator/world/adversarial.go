package world

// Adversarial traffic generators for the ingest-validation oracle tier
// (issue #204). Everything here is test-targeted — RunTraffic never
// calls into this file, so default simulator traffic stays polite (see
// specs/oracle.md "Adding Simulator Behavior"). Each generator commits
// a REAL, signed, verifier-consistent lie:
//
//   - op-path lies bypass repo.Create's validation via raw
//     mst.Tree.Insert (the MST layer accepts any byte key), so atmos's
//     Sync-1.1 verifier — which checks op paths only for MST
//     consistency, never spec validity — passes them through to
//     jetstream's #197 ingest gate;
//   - rev lies are signed into the inner commit (the verifier requires
//     envelope.Rev == signed commit.Rev), with the envelope Time
//     stamped from the logical clock because the honest path derives
//     Time by parsing the rev.
//
// Every lie is recorded in the world's AdversarialLedger at generation
// time. The oracle uses the ledger to (a) exclude intentionally-dropped
// rows from expected output, (b) exempt whole-event-dropped seqs from
// cursor-gap checks, and (c) assert per-(source, reason) drop-counter
// deltas — the anti-vacuity proof that each lie actually fired.
//
// One wire-reachability limit, spike-verified 2026-07-04: invalid UTF-8
// cannot ride a live #commit op.Path (the wire envelope encodes Path as
// a CBOR text string, and atmos's decoder rejects invalid UTF-8), but
// CAN sit in a getRepo CAR's MST node (KeySuffix is a CBOR byte
// string). Invalid-UTF-8 lies are therefore backfill-only:
// InjectAdversarialRecordForBackfill commits them silently so they are
// served by getRepo without ever appearing on the firehose.

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
// order. Guarded by World.mutationMu (same lock as all generation).
type AdversarialLedger struct {
	mu      sync.Mutex
	entries []AdversarialEntry
}

func (l *AdversarialLedger) record(e AdversarialEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, e)
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

	frame, newState, err := w.commitAndBroadcast(author, rp, store, prevState, []comatproto.SyncSubscribeRepos_RepoOp{sibOp, badOp})
	if err != nil {
		return GeneratedChainOp{}, err
	}
	_ = frame

	badColl, badRkey := repo.SplitMSTKey(badKey)
	w.adversarial.record(AdversarialEntry{
		Source:     AdversarialSourceLive,
		Layer:      AdversarialLayerGate,
		Reason:     reason,
		Seq:        w.seq.Load(),
		DID:        string(author.DID),
		Collection: badColl,
		Rkey:       badRkey,
	})
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
	w.adversarial.record(AdversarialEntry{
		Source:     AdversarialSourceBackfill,
		Layer:      AdversarialLayerGate,
		Reason:     reason,
		DID:        string(author.DID),
		Collection: badColl,
		Rkey:       badRkey,
	})
	return nil
}

// GenerateAdversarialSyncForTest emits a #sync frame for account idx's
// current head whose ENVELOPE rev is the caller-supplied lie (empty or
// garbage — anything ParseTID rejects). atmos's verifier has no
// ParseTID gate on #sync envelopes and its future-rev check skips
// unparseable revs, so the frame reaches jetstream's convertSync where
// validateRev drops the WHOLE event ({live, invalid_rev}).
//
// The CAR body still carries the honest signed head commit; only the
// envelope lies. Records the lie as WholeEvent so the oracle exempts
// the seq from cursor-gap accounting.
func (w *World) GenerateAdversarialSyncForTest(ctx context.Context, idx int, badRev string) ([]byte, error) {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := atmos.ParseTID(badRev); err == nil {
		return nil, fmt.Errorf("simulator: adversarial sync rev %q is a valid TID; use GenerateSyncForTest for honest syncs", badRev)
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
	w.adversarial.record(AdversarialEntry{
		Source:     AdversarialSourceLive,
		Layer:      AdversarialLayerGate,
		Reason:     "invalid_rev",
		Seq:        seq,
		DID:        did,
		WholeEvent: true,
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
	carBuf, err := packageCARDiff(store, newState.CommitCID)
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

// GenerateVerifierRejectedCommitForTest emits a #commit frame whose rev
// is signed-in but invalid at the VERIFIER layer: reason selects the
// lie shape. These frames never reach the ingest gate — atmos rejects
// them pre-conversion — so the oracle asserts verifier-failure
// classification + no archive + cursor advance instead of a gate
// counter. Supported reasons:
//
//   - "non_tid_rev": rev fails ParseTID (VerifyCommit InvalidRevError)
//   - "future_rev": rev is a valid TID > 5m ahead of the consumer's
//     clock (checkFutureRev FutureRevError). The caller supplies the
//     TID via rev since only the test knows the consumer's fake clock.
//
// The commit is otherwise honest: a real create op, real signed MST.
// The world's persisted head DOES advance to the lying rev, which has
// two consequences callers must manage:
//
//  1. While the head rev is invalid, a getRepo fetch of this account
//     fails at atmos's repo loader (non-empty invalid rev) or produces
//     gate-dropped rows (empty rev), so a verifier-triggered resync
//     cannot repair the DID yet.
//  2. The next HONEST commit on the account restores a valid head; its
//     PrevData points at the lie's MST root, which jetstream never
//     accepted, so the verifier chain-breaks and repairs via resync
//     from the now-honest head. Self-healing, and the repair itself is
//     useful coverage.
//
// Oracle scenarios should therefore follow this call with at least one
// honest commit on the same account before final-state comparison.
// The lie's record stays in the world MST (ground truth); the ledger
// entry lets the oracle exclude it until the follow-up honest commit's
// resync materializes it. Because the record IS eventually repaired,
// the entry is recorded with Layer=verifier for cursor-gap exemption
// only — final-state exclusion must check whether repair happened.
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
