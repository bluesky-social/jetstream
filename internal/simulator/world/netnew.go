package world

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

// AddHiddenAccountForTest creates a real simulator account outside the
// listRepos roster. getRepo/PLC lookup can find it by DID, and it can emit
// signed live traffic, but AccountCount/ListReposPage still expose only the
// original cfg.Accounts accounts. This models a DID first seen on the steady
// firehose after the bootstrap listRepos sweep omitted it.
func (w *World) AddHiddenAccountForTest(ctx context.Context, initialRecords int) (int, Account, error) {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()

	if err := ctx.Err(); err != nil {
		return 0, Account{}, err
	}
	if initialRecords < 0 {
		return 0, Account{}, fmt.Errorf("world: hidden account initialRecords must be >= 0 (got %d)", initialRecords)
	}

	idx, err := w.nextHiddenAccountIndex()
	if err != nil {
		return 0, Account{}, err
	}
	a, err := deriveAccount(w.cfg.Seed^0x207_207, idx)
	if err != nil {
		return 0, Account{}, fmt.Errorf("world: derive hidden account %d: %w", idx, err)
	}

	b := w.db.NewBatch()
	if err := w.saveAccount(b, a); err != nil {
		_ = b.Close()
		return 0, Account{}, err
	}
	if err := b.Commit(nil); err != nil {
		return 0, Account{}, fmt.Errorf("world: save hidden account %d: %w", idx, err)
	}

	rp, err := newEmptyRepo(a)
	if err != nil {
		return 0, Account{}, err
	}
	r := bootstrapRecordRand(w.cfg.Seed^0x207_207, idx)
	for range initialRecords {
		rkey := newRkey(r)
		rec := generateRecord(r, collPost, string(a.DID))
		if err := rp.Create(collPost, rkey, rec); err != nil {
			return 0, Account{}, fmt.Errorf("world: hidden bootstrap create %s/%s on %d: %w", collPost, rkey, idx, err)
		}
	}
	if _, err := w.commitAndPersist(a, rp); err != nil {
		return 0, Account{}, err
	}

	acct, err := w.LoadAccount(idx)
	if err != nil {
		return 0, Account{}, err
	}
	return idx, acct, nil
}

func (w *World) nextHiddenAccountIndex() (int, error) {
	for idx := w.cfg.Accounts; idx < w.cfg.Accounts+1024; idx++ {
		_, closer, err := w.db.Get(keyAccountDID(idx))
		if err == nil {
			_ = closer.Close()
			continue
		}
		if errors.Is(err, pebble.ErrNotFound) {
			return idx, nil
		}
		return 0, fmt.Errorf("world: probe hidden account %d: %w", idx, err)
	}
	return 0, fmt.Errorf("world: exhausted hidden account index search from %d", w.cfg.Accounts)
}
