package world

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
)

// Bootstrap generates and persists per-account initial records.
// Idempotent: state rows already at the target shape are not
// rewritten, so re-running on a partially-populated db is safe.
//
// Uses a dedicated PCG seeded from cfg.Seed for the *content* of
// initial records. The runtime RNG owned by *World drives only live
// traffic; mixing the two would make resume-from-disk
// content-dependent on whether a previous run had bootstrapped fully.
func (w *World) Bootstrap(ctx context.Context, logger *slog.Logger) error {
	logger = logger.With(slog.String("component", "simulator/bootstrap"))
	// Pre-derive every account so account picks for like/follow/repost
	// targets can come from the full roster.
	accounts := make([]account, w.cfg.Accounts)
	for i := range w.cfg.Accounts {
		a, err := deriveAccount(w.cfg.Seed, i)
		if err != nil {
			return fmt.Errorf("simulator: derive account %d: %w", i, err)
		}
		accounts[i] = a
	}
	counts := initialRecordCounts(w.cfg)

	const logEvery = 1000
	for i, a := range accounts {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Skip accounts already bootstrapped to a durable commit. We key
		// on a defined commit rather than RecordCount because record-key
		// TID collisions (newRkey draws random TIDs, and repo.Create
		// upserts on an equal MST key) can leave the persisted count below
		// the sampled target. Keying on RecordCount would then wrongly
		// treat a finished account as incomplete and re-bootstrap it from
		// an empty repo, advancing its rev/commit and breaking the
		// documented idempotency contract. commitAndPersist runs only
		// after the full create loop, so a defined commit means this
		// account completed.
		state, err := w.loadState(i)
		if err != nil {
			return err
		}
		if state.CommitCID.Defined() {
			continue
		}

		b := w.db.NewBatch()
		if err := w.saveAccount(b, a); err != nil {
			_ = b.Close()
			return err
		}
		if err := b.Commit(nil); err != nil {
			return fmt.Errorf("simulator: save account %d: %w", i, err)
		}

		rp, err := newEmptyRepo(a)
		if err != nil {
			return err
		}
		r := bootstrapRecordRand(w.cfg.Seed, i)
		for range counts[i] {
			coll := chooseCreateCollection(r)
			target := accounts[r.IntN(len(accounts))].DID
			rkey := newRkey(r)
			rec := generateRecord(r, coll, string(target))
			if err := rp.Create(coll, rkey, rec); err != nil {
				return fmt.Errorf("simulator: bootstrap create %s/%s on %d: %w", coll, rkey, i, err)
			}
		}
		if _, err := w.commitAndPersist(a, rp); err != nil {
			return err
		}

		if (i+1)%logEvery == 0 {
			logger.InfoContext(ctx, "bootstrapped accounts", "n", i+1, "of", w.cfg.Accounts)
		}
	}
	logger.InfoContext(ctx, "bootstrap complete", "accounts", len(accounts))
	return nil
}

func bootstrapRecordRand(seed uint64, accountIdx int) *rand.Rand {
	// Per-account streams keep bootstrap resume deterministic. A
	// completed account may be skipped on restart, and that must not
	// change the random content generated for later accounts.
	return rand.New(rand.NewPCG(seed^0xb007, uint64(accountIdx)^0x1a17))
}
