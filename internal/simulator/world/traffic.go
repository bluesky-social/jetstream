package world

import (
	"context"
	"fmt"
	"log/slog"
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
		delay := exponentialDelay(w.rng, mean)
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

// actionMix is the design-doc weighted action distribution.
var actionMix = []weighted[string]{
	{value: "create", weight: 75},
	{value: "update", weight: 15},
	{value: "delete", weight: 10},
}

// generateOne is one tick of the live commit pump: pick an account
// (Zipfian), apply N ops (mostly 1) of a chosen action, sign + persist,
// build a CAR diff with only the new blocks, and broadcast the frame.
// Returns the wire frame so tests can inspect it.
func (w *World) generateOne(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Choose author.
	authorIdx := zipfian(w.rng, 1.07, w.cfg.Accounts)
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
	action := weightedChoice(w.rng, actionMix)
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

	// Persist the new state. commitAndPersist signs + flushes blocks
	// + updates the MST index in one batch.
	newState, err := w.commitAndPersist(author, rp)
	if err != nil {
		return nil, err
	}

	// Build a CAR diff containing every block our diffStore touched:
	// writes (new MST nodes + new record blocks + the commit block)
	// AND reads (existing MST nodes traversed during op application).
	// atmos's verifier inverts each op against the post-state MST
	// loaded from this CAR; reading a path back to an unchanged
	// neighbor requires that neighbor be present in the diff.
	commitData, err := store.GetBlock(newState.CommitCID)
	if err != nil {
		return nil, err
	}
	carBlocks := make([]car.Block, 0, len(store.writes)+len(store.reads)+1)
	carBlocks = append(carBlocks, car.Block{CID: newState.CommitCID, Data: commitData})
	for cid, data := range store.writes {
		if cid == newState.CommitCID {
			continue
		}
		carBlocks = append(carBlocks, car.Block{CID: cid, Data: data})
	}
	for cid, data := range store.reads {
		if _, written := store.writes[cid]; written {
			continue
		}
		carBlocks = append(carBlocks, car.Block{CID: cid, Data: data})
	}
	var carBuf carBytesWriter
	if err := car.WriteAll(&carBuf, []cbor.CID{newState.CommitCID}, carBlocks); err != nil {
		return nil, fmt.Errorf("simulator: write CAR diff: %w", err)
	}

	// Allocate the seq and assemble the envelope.
	seq := w.seq.Add(1)
	envelope := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   string(author.DID),
		Rev:    newState.Rev,
		Seq:    seq,
		Time:   time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
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
		return nil, err
	}

	if err := w.persistFirehoseFrame(seq, frame); err != nil {
		return nil, err
	}
	if err := w.saveSeq(seq); err != nil {
		return nil, err
	}
	w.fanout.Publish(frame)
	return frame, nil
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
		target := w.pickAnotherAccount(authorIdx).DID
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
		rec := generateRecord(w.rng, coll, string(w.pickAnotherAccount(authorIdx).DID))
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

func (w *World) pickAnotherAccount(notIdx int) account {
	for {
		idx := w.rng.IntN(w.cfg.Accounts)
		if idx == notIdx {
			continue
		}
		a, err := w.loadAccount(idx)
		if err == nil {
			return a
		}
	}
}

// pickUntouchedRecord chooses one (collection, rkey) at random from
// the account's current MST, excluding any path already in `touched`.
// ok=false when the repo is empty or every record was already touched
// by an earlier op in the same commit; callers fall back to create in
// that case.
func (w *World) pickUntouchedRecord(rp *repo.Repo, touched map[string]struct{}) (collection, rkey string, ok bool) {
	type entry struct{ coll, rkey string }
	var entries []entry
	_ = rp.Tree.Walk(func(key string, _ cbor.CID) error {
		if _, dup := touched[key]; dup {
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

// carBytesWriter is a tiny io.Writer over a growable byte slice, so
// we don't pull in bytes.Buffer just for the CAR diff.
type carBytesWriter struct{ buf []byte }

func (w *carBytesWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}
func (w *carBytesWriter) bytes() []byte { return w.buf }
