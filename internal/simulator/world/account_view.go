package world

import (
	"context"
	"fmt"
	"strconv"

	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/repo"
)

// Account is the exported view of a simulator account, for HTTP
// handlers and tests living outside this package. Internal code
// (everything else in package world) uses the unexported `account`
// directly.
type Account struct {
	Index  int
	DID    atmos.DID
	pubKey *crypto.K256PublicKey
}

// LoadAccount returns the account at the given index.
func (w *World) LoadAccount(idx int) (Account, error) {
	a, err := w.loadAccount(idx)
	if err != nil {
		return Account{}, err
	}
	pubKey, ok := a.priv.PublicKey().(*crypto.K256PublicKey)
	if !ok {
		return Account{}, fmt.Errorf("world: account %d public key is not K256", idx)
	}
	return Account{
		Index:  a.Index,
		DID:    a.DID,
		pubKey: pubKey,
	}, nil
}

// FindAccountByDID returns (account, true) if a matching account
// exists; (Account{}, false, nil) otherwise. Linear scan over the
// account/<idx>/did rows; acceptable at 10k accounts because the
// simulator caches identity resolutions through atmos's directory
// cache anyway.
func (w *World) FindAccountByDID(did atmos.DID) (Account, bool, error) {
	iter, err := w.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("sim/account/"),
		UpperBound: []byte("sim/account/\xff"),
	})
	if err != nil {
		return Account{}, false, fmt.Errorf("world: did lookup iter: %w", err)
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		// Match keys ending in "/did".
		const suffix = "/did"
		if len(key) < len(suffix) || string(key[len(key)-len(suffix):]) != suffix {
			continue
		}
		if string(iter.Value()) != string(did) {
			continue
		}
		// Parse the index out of the key: "sim/account/<idx>/did".
		rest := key[len("sim/account/") : len(key)-len(suffix)]
		idx, err := strconv.Atoi(string(rest))
		if err != nil {
			return Account{}, false, fmt.Errorf("world: bad account key %q: %w", key, err)
		}
		a, err := w.LoadAccount(idx)
		if err != nil {
			return Account{}, false, err
		}
		return a, true, nil
	}
	if err := iter.Error(); err != nil {
		return Account{}, false, fmt.Errorf("world: did lookup iter: %w", err)
	}
	return Account{}, false, nil
}

// HandleSuffix is the cosmetic handle disambiguator: just the index.
func (a Account) HandleSuffix() string { return strconv.Itoa(a.Index) }

// PubkeyMultibase returns the z-prefixed base58 multibase encoding of
// the account's atproto signing key.
func (a Account) PubkeyMultibase() string { return a.pubKey.Multibase() }

// SubscribeFanout adds a new subscriber to the live broadcast.
func (w *World) SubscribeFanout() *fanout.Subscriber {
	return w.fanout.Subscribe()
}

// LoadRepo returns a fully-loaded *repo.Repo plus the signing key
// needed to call ExportCAR. Reads MST/record blocks lazily from
// pebble; safe to call concurrently because the underlying
// pebbleStore only reads.
func (w *World) LoadRepo(idx int) (*repo.Repo, *crypto.K256PrivateKey, error) {
	a, err := w.loadAccount(idx)
	if err != nil {
		return nil, nil, err
	}
	rp, err := w.loadRepo(a)
	if err != nil {
		return nil, nil, err
	}
	return rp, a.priv, nil
}

// AccountCount returns the total accounts in the world.
func (w *World) AccountCount() int { return w.cfg.Accounts }

// ListReposEntry is one row of a listRepos response.
type ListReposEntry struct {
	DID    atmos.DID
	Rev    string
	Head   string // commit CID string
	Active bool
}

// ListReposPage returns up to limit entries starting at index `start`.
// nextStart is start + len(entries); when nextStart == AccountCount(),
// the caller has paged through everything.
func (w *World) ListReposPage(start, limit int) (entries []ListReposEntry, nextStart int, err error) {
	if start < 0 {
		start = 0
	}
	if limit > 1000 {
		limit = 1000
	}
	if limit <= 0 {
		limit = 50
	}
	end := min(start+limit, w.cfg.Accounts)
	out := make([]ListReposEntry, 0, end-start)
	for i := start; i < end; i++ {
		a, err := w.LoadAccount(i)
		if err != nil {
			return nil, 0, err
		}
		state, err := w.loadState(i)
		if err != nil {
			return nil, 0, err
		}
		deleted, err := w.isAccountDeleted(i)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, ListReposEntry{
			DID:    a.DID,
			Rev:    state.Rev,
			Head:   state.CommitCID.String(),
			Active: !deleted,
		})
	}
	return out, end, nil
}

// EncodeOutdatedCursorInfo returns a wire-format #info frame
// signalling OutdatedCursor. The relay handler sends this before
// falling back to live streaming when a consumer's cursor is older
// than the retained history.
func EncodeOutdatedCursorInfo() []byte {
	return encodeInfoFrame("OutdatedCursor", "cursor older than retained history")
}

// GenerateOneForTest exposes generateOne for the http_test package.
// Production callers use RunTraffic; only tests need to drive
// individual events synchronously.
func (w *World) GenerateOneForTest(ctx context.Context) ([]byte, error) {
	return w.generateOne(ctx)
}

func (w *World) GenerateAccountDeleteForTest(ctx context.Context, idx int) ([]byte, error) {
	return w.generateAccountDelete(ctx, idx)
}

func (w *World) IsAccountDeleted(idx int) (bool, error) {
	return w.isAccountDeleted(idx)
}
