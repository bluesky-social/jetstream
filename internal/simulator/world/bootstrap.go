package world

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
)

// Bootstrap generates and persists Accounts × InitialRecords records.
// Idempotent: state rows already at the target shape are not
// rewritten, so re-running on a partially-populated db is safe.
//
// Uses a dedicated PCG seeded from cfg.Seed for the *content* of
// initial records. The runtime RNG owned by *World drives only live
// traffic; mixing the two would make resume-from-disk
// content-dependent on whether a previous run had bootstrapped fully.
func (w *World) Bootstrap(ctx context.Context, logger *slog.Logger) error {
	logger = logger.With(slog.String("component", "simulator/bootstrap"))
	// Stream 0xb007 ("boot") namespaces the bootstrap RNG away from
	// the live-traffic stream so the two never share state.
	r := rand.New(rand.NewPCG(w.cfg.Seed, 0xb007))

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

	const logEvery = 1000
	for i, a := range accounts {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Skip if already populated to the target record count.
		state, err := w.loadState(i)
		if err != nil {
			return err
		}
		if state.RecordCount >= w.cfg.InitialRecords {
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
		for range w.cfg.InitialRecords {
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
