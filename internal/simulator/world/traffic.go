package world

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/gt"
)

// RunTraffic blocks generating + broadcasting events until ctx is
// cancelled. One event per loop iteration; inter-arrival drawn from
// the exponential distribution. Returns nil on graceful cancel.
func (w *World) RunTraffic(ctx context.Context, logger *slog.Logger) error {
	logger = logger.With(slog.String("component", "simulator/traffic"))
	mean := 1.0 / (w.cfg.CommitsPerSec * w.cfg.RateMultiplier)
	logger.InfoContext(ctx, "starting", "mean_delay_sec", mean)

	for {
		delay := w.nextTrafficDelay(mean)
		t := time.NewTimer(time.Duration(delay * float64(time.Second)))
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
		if _, err := w.generateOne(ctx); err != nil {
			logger.ErrorContext(ctx, "generate failed", "err", err)
			return err
		}
	}
}

func (w *World) nextTrafficDelay(mean float64) float64 {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()
	return exponentialDelay(w.rng, mean)
}

// buildTrafficMixTables precomputes the weighted-draw tables from a
// TrafficMix. Zero-weight kinds are omitted entirely rather than kept
// at weight 0: weightedChoice's final-option fallback could otherwise
// return a disabled kind, and a future swarm tier (#233) relies on
// omission being genuine absence.
func buildTrafficMixTables(m TrafficMix) (kindMix, actionMix []weighted[string]) {
	add := func(dst []weighted[string], name string, wt float64) []weighted[string] {
		if wt > 0 {
			dst = append(dst, weighted[string]{value: name, weight: wt})
		}
		return dst
	}
	actionMix = add(actionMix, "create", m.Create)
	actionMix = add(actionMix, "update", m.Update)
	actionMix = add(actionMix, "delete", m.Delete)
	kindMix = append(kindMix, actionMix...)
	kindMix = add(kindMix, "identity", m.Identity)
	return kindMix, actionMix
}

// generateOne is one tick of the live traffic pump: draw a frame kind
// from the configured mix, pick an account (Zipfian), and emit either
// a #commit (apply N ops — mostly 1 — of the drawn action, sign +
// persist, build a CAR diff with only the new blocks) or an #identity
// frame. Returns the wire frame so tests can inspect it.
func (w *World) generateOne(ctx context.Context) ([]byte, error) {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()
	return w.generateOneLocked(ctx)
}

func (w *World) generateOneLocked(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	kind := weightedChoice(w.rng, w.kindMix)

	// Choose an active author. Deleted accounts keep their historical repo
	// state for backfill/compaction tests, but must not emit new commits —
	// and their #identity churn is not modeled either.
	authorIdx, err := w.pickActiveAuthor()
	if err != nil {
		return nil, err
	}
	if kind == "identity" {
		return w.generateIdentity(ctx, authorIdx)
	}
	return w.generateOneForAccount(ctx, authorIdx, kind)
}

func (w *World) generateOneForAccount(ctx context.Context, authorIdx int, action string) ([]byte, error) {
	// Honor cancellation before doing work, consistent with every sibling
	// generate helper (generateOneLocked, the targeted/sync/silent paths). The
	// check consumes no RNG, so it does not perturb the deterministic draw stream.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if authorIdx < 0 || authorIdx >= w.cfg.Accounts {
		return nil, fmt.Errorf("simulator: author index %d out of range", authorIdx)
	}
	deleted, err := w.isAccountDeleted(authorIdx)
	if err != nil {
		return nil, err
	}
	if deleted {
		return nil, fmt.Errorf("simulator: author account %d is deleted", authorIdx)
	}
	author, err := w.loadAccount(authorIdx)
	if err != nil {
		return nil, err
	}
	prevState, err := w.loadState(authorIdx)
	if err != nil {
		return nil, err
	}

	// Build a *repo.Repo whose BlockStore is a diffStore wrapping a
	// pebbleStore: reads come from disk, writes are captured in the
	// diff for CAR packaging.
	store := &diffStore{base: &pebbleStore{db: w.db, idx: authorIdx}}
	tree := mst.NewTree(store)
	if prevState.DataCID.Defined() {
		tree = mst.LoadTree(store, prevState.DataCID)
	}
	rp := &repo.Repo{
		DID:   author.DID,
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  tree,
	}

	// Apply N ops of the chosen action. v1 keeps actions homogeneous
	// per commit — mixing actions per commit doesn't add useful test
	// surface for our distributions.
	//
	// touched is the set of (collection/rkey) paths already mutated by
	// this commit; applyOp skips them so we never emit two ops on the
	// same path. atmos's verifier rejects duplicate paths in a single
	// commit (DuplicatePathError) — real PDSes collapse intra-commit
	// duplicates before publishing. A small repo + multi-op commit
	// (~30%, via geometric distribution) makes collisions on
	// update/delete likely without this guard.
	nOps := geometricAtLeastOne(w.rng, 0.7)
	wireOps := make([]comatproto.SyncSubscribeRepos_RepoOp, 0, nOps)
	touched := make(map[string]struct{}, nOps)

	for range nOps {
		op, err := w.applyOp(rp, author.Index, action, touched)
		if err != nil {
			return nil, err
		}
		wireOps = append(wireOps, op)
		touched[op.Path] = struct{}{}
	}

	frame, _, err := w.commitAndBroadcast(author, rp, store, prevState, wireOps, nil)
	return frame, err
}

// commitAndBroadcast is the shared tail of the live commit pump: persist
// the post-op state, package a CAR diff over exactly the blocks this
// commit touched, allocate a seq, encode + persist + publish the #commit
// frame. Callers must have applied their ops to rp (a *repo.Repo whose
// Store is the supplied diffStore) and assembled the matching wireOps.
//
// omitBlocks, when non-nil, names block CIDs to exclude from the CAR
// diff — the wire frame then carries ops referencing blocks the CAR
// does not contain, the partial-CAR shape a non-canonical PDS emits.
// The world's own persisted repo state is NOT affected; only the
// broadcast frame is partial. Callers must only omit record LEAF
// blocks: omitting the commit block or an MST node would fail the
// verifier's inversion and model a different (malformed-CAR) fault.
//
// Returns the wire frame and the post-commit state. The returned state is
// the authoritative source for the rev/CID this frame carries — callers
// MUST use it rather than re-reading via loadState, which would race a
// concurrent commit on the same account and report a rev that disagrees
// with the broadcast frame.
func (w *World) commitAndBroadcast(author account, rp *repo.Repo, store *diffStore, prevState repoState, wireOps []comatproto.SyncSubscribeRepos_RepoOp, omitBlocks map[cbor.CID]struct{}) ([]byte, repoState, error) {
	// Persist the new state. commitAndPersist signs + flushes blocks
	// + updates the MST index in one batch.
	newState, err := w.commitAndPersist(author, rp)
	if err != nil {
		return nil, repoState{}, err
	}

	carBuf, err := packageCARDiff(store, newState.CommitCID, omitBlocks)
	if err != nil {
		return nil, repoState{}, err
	}
	revTID, err := atmos.ParseTID(newState.Rev)
	if err != nil {
		return nil, repoState{}, fmt.Errorf("simulator: parse generated rev: %w", err)
	}

	frame, _, err := w.broadcastCommitFrame(author, newState, prevState, wireOps, carBuf,
		revTID.Time().UTC().Format("2006-01-02T15:04:05.000Z"))
	if err != nil {
		return nil, repoState{}, err
	}
	return frame, newState, nil
}

// packageCARDiff builds a CAR containing every block the diffStore
// touched: writes (new MST nodes + new record blocks + the commit
// block) AND reads (existing MST nodes traversed during op
// application). atmos's verifier inverts each op against the
// post-state MST loaded from this CAR; reading a path back to an
// unchanged neighbor requires that neighbor be present in the diff.
// omitBlocks (nil for the honest paths) names block CIDs to exclude —
// see commitAndBroadcast for the partial-CAR contract.
func packageCARDiff(store *diffStore, commitCID cbor.CID, omitBlocks map[cbor.CID]struct{}) (carBytesWriter, error) {
	var carBuf carBytesWriter
	commitData, err := store.GetBlock(commitCID)
	if err != nil {
		return carBuf, err
	}
	carBlocks := make([]car.Block, 0, len(store.writes)+len(store.reads)+1)
	carBlocks = append(carBlocks, car.Block{CID: commitCID, Data: commitData})
	for _, cid := range sortedCIDs(store.writes) {
		if cid == commitCID {
			continue
		}
		if _, omit := omitBlocks[cid]; omit {
			continue
		}
		carBlocks = append(carBlocks, car.Block{CID: cid, Data: store.writes[cid]})
	}
	for _, cid := range sortedCIDs(store.reads) {
		if _, written := store.writes[cid]; written {
			continue
		}
		if _, omit := omitBlocks[cid]; omit {
			continue
		}
		carBlocks = append(carBlocks, car.Block{CID: cid, Data: store.reads[cid]})
	}
	if err := car.WriteAll(&carBuf, []cbor.CID{commitCID}, carBlocks); err != nil {
		return carBuf, fmt.Errorf("simulator: write CAR diff: %w", err)
	}
	return carBuf, nil
}

// broadcastCommitFrame allocates a seq, assembles + encodes the
// #commit envelope, persists it to firehose history, and publishes it.
// newState supplies the rev/CID the frame claims; prevState supplies
// Since/PrevData. Returns the frame and its seq.
func (w *World) broadcastCommitFrame(author account, newState, prevState repoState, wireOps []comatproto.SyncSubscribeRepos_RepoOp, carBuf carBytesWriter, timeStr string) ([]byte, int64, error) {
	seq := w.seq.Add(1)
	envelope := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   string(author.DID),
		Rev:    newState.Rev,
		Seq:    seq,
		Time:   timeStr,
		Commit: lextypes.LexCIDLink{Link: newState.CommitCID.String()},
		Blocks: carBuf.bytes(),
		Ops:    wireOps,
	}
	if prevState.DataCID.Defined() {
		envelope.PrevData = gt.Some(lextypes.LexCIDLink{Link: prevState.DataCID.String()})
		envelope.Since = gt.Some(prevState.Rev)
	}

	frame, err := encodeCommitFrame(envelope)
	if err != nil {
		return nil, 0, err
	}

	if err := w.persistFirehoseFrame(seq, frame); err != nil {
		return nil, 0, err
	}
	w.fanout.Publish(frame)
	return frame, seq, nil
}

// GeneratedChainOp describes one op injected via GenerateRecordOpForTest,
// carrying enough detail for a test to derive the durable event-log row
// it should produce: the action, its (collection, rkey), the rev the
// commit assigned, and the record's CBOR block (nil for delete). Payload
// equals the record block jetstream records on disk for create/update.
type GeneratedChainOp struct {
	Action     string
	Collection string
	Rkey       string
	Rev        string
	Payload    []byte
}

// GenerateRecordOpForTest applies a single create/update/delete on
// account idx against the caller-specified (collection, rkey), commits,
// and broadcasts the resulting #commit frame on the live firehose. It is
// the targeted analogue of GenerateOneForTest: where ordinary traffic
// picks random paths, this lets a test drive an exact chain on a known
// key — in particular a delete followed by a recreate reusing the SAME
// rkey, which random traffic (fresh TID rkeys) never produces. Record
// payloads are still drawn from the world RNG, so payload bytes vary by
// seed. Returns the wire frame and the op descriptor (assigned rev +
// record block).
func (w *World) GenerateRecordOpForTest(ctx context.Context, idx int, action, coll, rkey string) ([]byte, GeneratedChainOp, error) {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, GeneratedChainOp{}, err
	}
	author, rp, store, prevState, err := w.loadRepoForTargetedCommit(idx)
	if err != nil {
		return nil, GeneratedChainOp{}, err
	}

	op, payload, err := w.applyTargetedOp(rp, idx, action, coll, rkey)
	if err != nil {
		return nil, GeneratedChainOp{}, err
	}

	// commitAndBroadcast returns the authoritative post-commit state. We use
	// its rev directly rather than re-reading via loadState: the helper is
	// documented for use while live firehose traffic runs, and a concurrent
	// commit on this account between the broadcast and a re-read would make
	// the descriptor's rev disagree with the rev carried in the frame we just
	// published — silently corrupting any oracle row derived from it.
	frame, newState, err := w.commitAndBroadcast(author, rp, store, prevState, []comatproto.SyncSubscribeRepos_RepoOp{op}, nil)
	if err != nil {
		return nil, GeneratedChainOp{}, err
	}
	return frame, GeneratedChainOp{
		Action:     action,
		Collection: coll,
		Rkey:       rkey,
		Rev:        newState.Rev,
		Payload:    payload,
	}, nil
}

// TargetedOpSpec describes one op in a GenerateMultiOpCommitForTest
// commit. StripBlock excludes the op's record leaf block from the
// broadcast CAR diff — the wire op still references the block's CID,
// so the frame carries the partial-CAR shape a non-canonical PDS
// emits (spec-permitted; the record is unarchivable from the frame
// alone). Only valid on create/update: deletes carry no block.
type TargetedOpSpec struct {
	Action     string
	Collection string
	Rkey       string
	StripBlock bool
}

// GenerateMultiOpCommitForTest applies several targeted ops on account
// idx in ONE commit, optionally stripping chosen record leaf blocks
// from the CAR diff (see TargetedOpSpec.StripBlock). Only record leaf
// blocks are ever stripped — the commit block and every MST node stay
// in the CAR, so the frame still verifies (atmos's inversion needs the
// tree, not the leaves) and the fault is precisely "ops whose record
// block is absent", not a malformed CAR. The world's own persisted
// repo state includes every op; only the wire frame is partial.
//
// Specs must name distinct (collection, rkey) paths: atmos's verifier
// rejects duplicate paths in a single commit, and applyTargetedOp's
// create-on-existing guard would trip anyway for repeated creates.
func (w *World) GenerateMultiOpCommitForTest(ctx context.Context, idx int, specs []TargetedOpSpec) ([]byte, []GeneratedChainOp, error) {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if len(specs) == 0 {
		return nil, nil, fmt.Errorf("simulator: multi-op commit needs at least one op")
	}
	if idx < 0 || idx >= w.cfg.Accounts {
		return nil, nil, fmt.Errorf("simulator: chain account index %d out of range", idx)
	}
	deleted, err := w.isAccountDeleted(idx)
	if err != nil {
		return nil, nil, err
	}
	if deleted {
		return nil, nil, fmt.Errorf("simulator: chain account %d is deleted", idx)
	}
	author, err := w.loadAccount(idx)
	if err != nil {
		return nil, nil, err
	}
	prevState, err := w.loadState(idx)
	if err != nil {
		return nil, nil, err
	}

	store := &diffStore{base: &pebbleStore{db: w.db, idx: idx}}
	tree := mst.NewTree(store)
	if prevState.DataCID.Defined() {
		tree = mst.LoadTree(store, prevState.DataCID)
	}
	rp := &repo.Repo{
		DID:   author.DID,
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  tree,
	}

	wireOps := make([]comatproto.SyncSubscribeRepos_RepoOp, 0, len(specs))
	payloads := make([][]byte, 0, len(specs))
	seenPaths := make(map[string]struct{}, len(specs))
	var omitBlocks map[cbor.CID]struct{}
	for _, spec := range specs {
		path := spec.Collection + "/" + spec.Rkey
		if _, dup := seenPaths[path]; dup {
			return nil, nil, fmt.Errorf("simulator: multi-op commit duplicates path %s", path)
		}
		seenPaths[path] = struct{}{}

		op, payload, err := w.applyTargetedOp(rp, idx, spec.Action, spec.Collection, spec.Rkey)
		if err != nil {
			return nil, nil, err
		}
		if spec.StripBlock {
			if spec.Action == "delete" {
				return nil, nil, fmt.Errorf("simulator: cannot strip block from delete op %s", path)
			}
			cid, err := cbor.ParseCIDString(op.CID.Val().Link)
			if err != nil {
				return nil, nil, fmt.Errorf("simulator: parse stripped op CID for %s: %w", path, err)
			}
			if omitBlocks == nil {
				omitBlocks = make(map[cbor.CID]struct{})
			}
			omitBlocks[cid] = struct{}{}
		}
		wireOps = append(wireOps, op)
		payloads = append(payloads, payload)
	}

	frame, newState, err := w.commitAndBroadcast(author, rp, store, prevState, wireOps, omitBlocks)
	if err != nil {
		return nil, nil, err
	}
	out := make([]GeneratedChainOp, 0, len(specs))
	for i, spec := range specs {
		out = append(out, GeneratedChainOp{
			Action:     spec.Action,
			Collection: spec.Collection,
			Rkey:       spec.Rkey,
			Rev:        newState.Rev,
			Payload:    payloads[i],
		})
	}
	return frame, out, nil
}

// applyTargetedOp performs one op on rp against the exact (coll, rkey)
// and returns the wire RepoOp plus the record's CBOR block (nil for
// delete). Unlike applyOp it never falls back to a different action or
// path: a create on an existing key, or an update/delete on a missing
// key, surfaces as a loud error rather than silently mutating something
// else — the test's chain expectations depend on the exact op landing.
func (w *World) applyTargetedOp(rp *repo.Repo, authorIdx int, action, coll, rkey string) (comatproto.SyncSubscribeRepos_RepoOp, []byte, error) {
	switch action {
	case "create":
		// repo.Create is an upsert (Tree.Insert), so guard the
		// create-on-existing case explicitly: a chain that expects a
		// fresh create must not silently overwrite a live record.
		if _, _, err := rp.Get(coll, rkey); err == nil {
			return comatproto.SyncSubscribeRepos_RepoOp{}, nil, fmt.Errorf("simulator: targeted create on existing %s/%s", coll, rkey)
		}
		target, err := w.pickTargetAccount(authorIdx)
		if err != nil {
			return comatproto.SyncSubscribeRepos_RepoOp{}, nil, err
		}
		rec := generateRecord(w.rng, coll, string(target))
		if err := rp.Create(coll, rkey, rec); err != nil {
			return comatproto.SyncSubscribeRepos_RepoOp{}, nil, fmt.Errorf("simulator: targeted create %s/%s: %w", coll, rkey, err)
		}
		cid, block, err := rp.Get(coll, rkey)
		if err != nil {
			return comatproto.SyncSubscribeRepos_RepoOp{}, nil, fmt.Errorf("simulator: lookup targeted create %s/%s: %w", coll, rkey, err)
		}
		return comatproto.SyncSubscribeRepos_RepoOp{
			Action: "create",
			Path:   coll + "/" + rkey,
			CID:    gt.Some(lextypes.LexCIDLink{Link: cid.String()}),
		}, append([]byte(nil), block...), nil

	case "update":
		prevCID, _, err := rp.Get(coll, rkey)
		if err != nil {
			return comatproto.SyncSubscribeRepos_RepoOp{}, nil, fmt.Errorf("simulator: targeted update missing %s/%s: %w", coll, rkey, err)
		}
		target, err := w.pickTargetAccount(authorIdx)
		if err != nil {
			return comatproto.SyncSubscribeRepos_RepoOp{}, nil, err
		}
		rec := generateRecord(w.rng, coll, string(target))
		if err := rp.Update(coll, rkey, rec); err != nil {
			return comatproto.SyncSubscribeRepos_RepoOp{}, nil, fmt.Errorf("simulator: targeted update %s/%s: %w", coll, rkey, err)
		}
		cid, block, err := rp.Get(coll, rkey)
		if err != nil {
			return comatproto.SyncSubscribeRepos_RepoOp{}, nil, fmt.Errorf("simulator: lookup targeted update %s/%s: %w", coll, rkey, err)
		}
		return comatproto.SyncSubscribeRepos_RepoOp{
			Action: "update",
			Path:   coll + "/" + rkey,
			CID:    gt.Some(lextypes.LexCIDLink{Link: cid.String()}),
			Prev:   gt.Some(lextypes.LexCIDLink{Link: prevCID.String()}),
		}, append([]byte(nil), block...), nil

	case "delete":
		prevCID, _, err := rp.Get(coll, rkey)
		if err != nil {
			return comatproto.SyncSubscribeRepos_RepoOp{}, nil, fmt.Errorf("simulator: targeted delete missing %s/%s: %w", coll, rkey, err)
		}
		if err := rp.Delete(coll, rkey); err != nil {
			return comatproto.SyncSubscribeRepos_RepoOp{}, nil, fmt.Errorf("simulator: targeted delete %s/%s: %w", coll, rkey, err)
		}
		return comatproto.SyncSubscribeRepos_RepoOp{
			Action: "delete",
			Path:   coll + "/" + rkey,
			Prev:   gt.Some(lextypes.LexCIDLink{Link: prevCID.String()}),
		}, nil, nil

	default:
		return comatproto.SyncSubscribeRepos_RepoOp{}, nil, fmt.Errorf("simulator: unknown targeted action %q", action)
	}
}

// loadRepoForTargetedCommit is the shared prelude of every targeted
// (test-driven) commit generator: bounds/deleted checks, then a
// *repo.Repo over a diffStore so the commit's touched blocks can be
// packaged into a CAR diff. Caller must hold mutationMu.
func (w *World) loadRepoForTargetedCommit(idx int) (account, *repo.Repo, *diffStore, repoState, error) {
	if idx < 0 || idx >= w.cfg.Accounts {
		return account{}, nil, nil, repoState{}, fmt.Errorf("simulator: account index %d out of range", idx)
	}
	deleted, err := w.isAccountDeleted(idx)
	if err != nil {
		return account{}, nil, nil, repoState{}, err
	}
	if deleted {
		return account{}, nil, nil, repoState{}, fmt.Errorf("simulator: account %d is deleted", idx)
	}
	author, err := w.loadAccount(idx)
	if err != nil {
		return account{}, nil, nil, repoState{}, err
	}
	prevState, err := w.loadState(idx)
	if err != nil {
		return account{}, nil, nil, repoState{}, err
	}
	store := &diffStore{base: &pebbleStore{db: w.db, idx: idx}}
	tree := mst.NewTree(store)
	if prevState.DataCID.Defined() {
		tree = mst.LoadTree(store, prevState.DataCID)
	}
	rp := &repo.Repo{
		DID:   author.DID,
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  tree,
	}
	return author, rp, store, prevState, nil
}

// GenerateSyncForTest emits a real subscribeRepos #sync frame for the current
// head of account idx. It does not mutate the repo; it packages the current
// commit block in the #sync CAR body, persists the frame to firehose history,
// and publishes it to live subscribers.
func (w *World) GenerateSyncForTest(ctx context.Context, idx int) ([]byte, error) {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return w.emitSyncForAccount(ctx, idx)
}

// GenerateSilentMutationThenSyncForTest mutates account idx, intentionally
// skips publishing the corresponding #commit frame, then emits a #sync for the
// new repo head. Oracle tests use this to force a true local/upstream
// divergence: Jetstream must recover the authoritative state via getRepo.
func (w *World) GenerateSilentMutationThenSyncForTest(ctx context.Context, idx int) ([]byte, error) {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := w.silentCreateForAccount(idx); err != nil {
		return nil, err
	}
	return w.emitSyncForAccount(ctx, idx)
}

// GenerateSilentMutationThenCommitForTest mutates account idx, skips that
// commit frame, then emits the next commit for the same DID. The emitted
// commit's prevData points at a state Jetstream never saw, forcing the verifier
// chain-break path and its async resync repair.
func (w *World) GenerateSilentMutationThenCommitForTest(ctx context.Context, idx int) ([]byte, error) {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := w.silentCreateForAccount(idx); err != nil {
		return nil, err
	}
	// Draw from the commit-action mix, never the full kind mix: this
	// helper's contract is "the next frame for this DID is a #commit
	// whose prevData chain-breaks" — an #identity draw here would
	// silently defuse the divergence the caller is setting up.
	if len(w.actionMix) == 0 {
		return nil, errors.New("simulator: silent-mutation trigger commit needs a commit action in the TrafficMix")
	}
	return w.generateOneForAccount(ctx, idx, weightedChoice(w.rng, w.actionMix))
}

func (w *World) silentCreateForAccount(idx int) error {
	if idx < 0 || idx >= w.cfg.Accounts {
		return fmt.Errorf("simulator: silent account index %d out of range", idx)
	}
	deleted, err := w.isAccountDeleted(idx)
	if err != nil {
		return err
	}
	if deleted {
		return fmt.Errorf("simulator: silent account %d is deleted", idx)
	}
	author, err := w.loadAccount(idx)
	if err != nil {
		return err
	}
	rp, err := w.loadRepo(author)
	if err != nil {
		return err
	}
	coll := chooseCreateCollection(w.rng)
	rkey := newRkey(w.rng)
	target, err := w.pickTargetAccount(idx)
	if err != nil {
		return err
	}
	if err := rp.Create(coll, rkey, generateRecord(w.rng, coll, string(target))); err != nil {
		return fmt.Errorf("simulator: silent create %s/%s: %w", coll, rkey, err)
	}
	if _, err := w.commitAndPersist(author, rp); err != nil {
		return err
	}
	return nil
}

func (w *World) emitSyncForAccount(ctx context.Context, idx int) ([]byte, error) {
	state, err := w.loadState(idx)
	if err != nil {
		return nil, err
	}
	revTID, err := atmos.ParseTID(state.Rev)
	if err != nil {
		return nil, fmt.Errorf("simulator: parse sync rev: %w", err)
	}
	frame, _, _, err := w.emitSyncWithRev(ctx, idx, state.Rev,
		revTID.Time().UTC().Format("2006-01-02T15:04:05.000Z"))
	return frame, err
}

// emitSyncWithRev emits a #sync frame for account idx's current head
// whose envelope carries the caller-supplied rev and Time. The honest
// path (emitSyncForAccount) passes the persisted head rev; the
// adversarial path passes a lie. The CAR body always carries the real
// head commit block. Returns the frame, its seq, and the author DID.
func (w *World) emitSyncWithRev(ctx context.Context, idx int, rev, timeStr string) ([]byte, int64, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, "", err
	}
	if idx < 0 || idx >= w.cfg.Accounts {
		return nil, 0, "", fmt.Errorf("simulator: sync account index %d out of range", idx)
	}
	deleted, err := w.isAccountDeleted(idx)
	if err != nil {
		return nil, 0, "", err
	}
	if deleted {
		return nil, 0, "", fmt.Errorf("simulator: sync account %d is deleted", idx)
	}
	author, err := w.loadAccount(idx)
	if err != nil {
		return nil, 0, "", err
	}
	state, err := w.loadState(idx)
	if err != nil {
		return nil, 0, "", err
	}
	if !state.CommitCID.Defined() {
		return nil, 0, "", fmt.Errorf("simulator: sync account %d has no commit", idx)
	}
	commitData, err := (&pebbleStore{db: w.db, idx: idx}).GetBlock(state.CommitCID)
	if err != nil {
		return nil, 0, "", fmt.Errorf("simulator: load sync commit block: %w", err)
	}
	var carBuf carBytesWriter
	if err := car.WriteAll(&carBuf, []cbor.CID{state.CommitCID}, []car.Block{{
		CID:  state.CommitCID,
		Data: commitData,
	}}); err != nil {
		return nil, 0, "", fmt.Errorf("simulator: write sync CAR: %w", err)
	}

	seq := w.seq.Add(1)
	envelope := &comatproto.SyncSubscribeRepos_Sync{
		DID:    string(author.DID),
		Rev:    rev,
		Seq:    seq,
		Time:   timeStr,
		Blocks: carBuf.bytes(),
	}
	frame, err := encodeSyncFrame(envelope)
	if err != nil {
		return nil, 0, "", err
	}
	if err := w.persistFirehoseFrame(seq, frame); err != nil {
		return nil, 0, "", err
	}
	w.fanout.Publish(frame)
	return frame, seq, string(author.DID), nil
}

// applyOp performs a single create/update/delete on rp and returns
// the corresponding wire RepoOp. touched is the set of paths already
// mutated within the current commit; update/delete skip them and fall
// back to create when no eligible record remains. The fall-back also
// covers the repo-empty case (initial bootstrap pre-populates
// InitialRecords per account, so this is rare in steady state but
// defensive for tests using small initial counts).
func (w *World) applyOp(rp *repo.Repo, authorIdx int, action string, touched map[string]struct{}) (comatproto.SyncSubscribeRepos_RepoOp, error) {
	switch action {
	case "create":
		coll := chooseCreateCollection(w.rng)
		rkey := newRkey(w.rng)
		target, err := w.pickTargetAccount(authorIdx)
		if err != nil {
			return comatproto.SyncSubscribeRepos_RepoOp{}, err
		}
		rec := generateRecord(w.rng, coll, string(target))
		if err := rp.Create(coll, rkey, rec); err != nil {
			return comatproto.SyncSubscribeRepos_RepoOp{}, fmt.Errorf("simulator: create %s/%s: %w", coll, rkey, err)
		}
		cid, _, err := rp.Get(coll, rkey)
		if err != nil {
			return comatproto.SyncSubscribeRepos_RepoOp{}, fmt.Errorf("simulator: lookup new record: %w", err)
		}
		return comatproto.SyncSubscribeRepos_RepoOp{
			Action: "create",
			Path:   coll + "/" + rkey,
			CID:    gt.Some(lextypes.LexCIDLink{Link: cid.String()}),
		}, nil

	case "update":
		coll, rkey, ok := w.pickUntouchedRecord(rp, touched)
		if !ok {
			return w.applyOp(rp, authorIdx, "create", touched)
		}
		prevCID, _, _ := rp.Get(coll, rkey)
		target, err := w.pickTargetAccount(authorIdx)
		if err != nil {
			return comatproto.SyncSubscribeRepos_RepoOp{}, err
		}
		rec := generateRecord(w.rng, coll, string(target))
		if err := rp.Update(coll, rkey, rec); err != nil {
			return comatproto.SyncSubscribeRepos_RepoOp{}, fmt.Errorf("simulator: update %s/%s: %w", coll, rkey, err)
		}
		cid, _, _ := rp.Get(coll, rkey)
		return comatproto.SyncSubscribeRepos_RepoOp{
			Action: "update",
			Path:   coll + "/" + rkey,
			CID:    gt.Some(lextypes.LexCIDLink{Link: cid.String()}),
			Prev:   gt.Some(lextypes.LexCIDLink{Link: prevCID.String()}),
		}, nil

	case "delete":
		coll, rkey, ok := w.pickUntouchedRecord(rp, touched)
		if !ok {
			return w.applyOp(rp, authorIdx, "create", touched)
		}
		prevCID, _, _ := rp.Get(coll, rkey)
		if err := rp.Delete(coll, rkey); err != nil {
			return comatproto.SyncSubscribeRepos_RepoOp{}, fmt.Errorf("simulator: delete %s/%s: %w", coll, rkey, err)
		}
		return comatproto.SyncSubscribeRepos_RepoOp{
			Action: "delete",
			Path:   coll + "/" + rkey,
			Prev:   gt.Some(lextypes.LexCIDLink{Link: prevCID.String()}),
		}, nil

	default:
		return comatproto.SyncSubscribeRepos_RepoOp{}, fmt.Errorf("simulator: unknown action %q", action)
	}
}

// pickTargetAccount returns a target DID for a like/follow/repost,
// uniformly chosen from accounts other than the author when more than
// one exists. Rejection-sampling away the author's own index is
// deterministic — it depends only on the RNG sequence. A load error,
// by contrast, is fatal rather than retried: retrying would (a) spin
// forever if a load consistently failed, masking real db corruption as
// a hang instead of crashing loudly, and (b) make the number of RNG
// draws depend on which accounts happen to load, desyncing the
// deterministic draw stream. Accounts are derived deterministically and
// persisted during bootstrap, so a load failure is a genuine fault.
func (w *World) pickTargetAccount(authorIdx int) (atmos.DID, error) {
	if w.cfg.Accounts <= 1 {
		deleted, err := w.isAccountDeleted(authorIdx)
		if err != nil {
			return "", err
		}
		if deleted {
			return "", errors.New("simulator: no active target accounts")
		}
		a, err := w.loadAccount(authorIdx)
		if err != nil {
			return "", fmt.Errorf("simulator: load only target account %d: %w", authorIdx, err)
		}
		return a.DID, nil
	}
	for attempts := 0; attempts < max(1, w.cfg.Accounts*4); attempts++ {
		idx := w.rng.IntN(w.cfg.Accounts)
		if idx == authorIdx {
			continue
		}
		deleted, err := w.isAccountDeleted(idx)
		if err != nil {
			return "", err
		}
		if deleted {
			continue
		}
		a, err := w.loadAccount(idx)
		if err != nil {
			return "", fmt.Errorf("simulator: load target account %d: %w", idx, err)
		}
		return a.DID, nil
	}
	for idx := range w.cfg.Accounts {
		if idx == authorIdx {
			continue
		}
		deleted, err := w.isAccountDeleted(idx)
		if err != nil {
			return "", err
		}
		if deleted {
			continue
		}
		a, err := w.loadAccount(idx)
		if err != nil {
			return "", fmt.Errorf("simulator: load target account %d: %w", idx, err)
		}
		return a.DID, nil
	}
	return "", errors.New("simulator: no active target accounts")
}

func (w *World) pickActiveAuthor() (int, error) {
	for attempts := 0; attempts < max(1, w.cfg.Accounts*4); attempts++ {
		idx := zipfian(w.rng, 1.07, w.cfg.Accounts)
		deleted, err := w.isAccountDeleted(idx)
		if err != nil {
			return 0, err
		}
		if !deleted {
			return idx, nil
		}
	}
	for idx := range w.cfg.Accounts {
		deleted, err := w.isAccountDeleted(idx)
		if err != nil {
			return 0, err
		}
		if !deleted {
			return idx, nil
		}
	}
	return 0, errors.New("simulator: no active author accounts")
}

// pickUntouchedRecord chooses one (collection, rkey) at random from
// the account's current MST, excluding any path already in `touched`
// and any adversarial lie key (adversarial.go). The lie exclusion is
// load-bearing two ways: a spec-INVALID pick would fail
// repo.Update/Delete's validation loudly, and a spec-valid-but-
// unrepresentable pick (300-byte rkey) would ride an honest commit
// that the gate then drops — an unledgered drop that starves the
// oracle's gap-free cursor accounting. No honest PDS actor mutates
// records that could never have been honestly created. ok=false when
// the repo is empty or every record was already touched by an earlier
// op in the same commit; callers fall back to create in that case.
func (w *World) pickUntouchedRecord(rp *repo.Repo, touched map[string]struct{}) (collection, rkey string, ok bool) {
	type entry struct{ coll, rkey string }
	var entries []entry
	_ = rp.Tree.Walk(func(key string, _ cbor.CID) error {
		if _, dup := touched[key]; dup {
			return nil
		}
		if w.adversarial.ContainsKey(key) {
			return nil
		}
		c, k := repo.SplitMSTKey(key)
		entries = append(entries, entry{c, k})
		return nil
	})
	if len(entries) == 0 {
		return "", "", false
	}
	pick := entries[w.rng.IntN(len(entries))]
	return pick.coll, pick.rkey, true
}

// diffStore wraps a base BlockStore (the persisted-blocks pebbleStore)
// and captures every block touched by this commit — both PutBlock'd
// (newly written this commit) and GetBlock'd (read during op or
// inversion-proof traversal). The combined set is the proof set for
// the CAR diff: it carries every node atmos's verifier needs to (a)
// walk the post-state MST and (b) invert ops back to the prev-state
// MST root.
//
// Capturing only writes was insufficient: deletes/updates touch
// existing nodes the verifier later needs to traverse during
// `tree.Insert(prevCID)`, but those nodes are unchanged so the writes
// map alone omitted them, which surfaced as
// `mst: loading node ...: block not found` inversion failures.
type diffStore struct {
	base   mst.BlockStore
	writes map[cbor.CID][]byte
	reads  map[cbor.CID][]byte
}

func (s *diffStore) GetBlock(cid cbor.CID) ([]byte, error) {
	if data, ok := s.writes[cid]; ok {
		return data, nil
	}
	if data, ok := s.reads[cid]; ok {
		return data, nil
	}
	data, err := s.base.GetBlock(cid)
	if err != nil {
		return nil, err
	}
	if s.reads == nil {
		s.reads = make(map[cbor.CID][]byte)
	}
	s.reads[cid] = append([]byte(nil), data...)
	return data, nil
}

func (s *diffStore) PutBlock(cid cbor.CID, data []byte) error {
	if s.writes == nil {
		s.writes = make(map[cbor.CID][]byte)
	}
	s.writes[cid] = append([]byte(nil), data...)
	return s.base.PutBlock(cid, data)
}

func sortedCIDs(blocks map[cbor.CID][]byte) []cbor.CID {
	cids := make([]cbor.CID, 0, len(blocks))
	for cid := range blocks {
		cids = append(cids, cid)
	}
	sort.Slice(cids, func(i, j int) bool {
		return cids[i].String() < cids[j].String()
	})
	return cids
}

// carBytesWriter is a tiny io.Writer over a growable byte slice, so
// we don't pull in bytes.Buffer just for the CAR diff.
type carBytesWriter struct{ buf []byte }

func (w *carBytesWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}
func (w *carBytesWriter) bytes() []byte { return w.buf }
