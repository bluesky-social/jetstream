package world

import (
	"context"
	"fmt"
	"sort"
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

// AccountIndicesForTest returns every account index persisted in the world,
// including hidden test accounts that AccountCount/ListReposPage intentionally
// omit.
func (w *World) AccountIndicesForTest() ([]int, error) {
	iter, err := w.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("sim/account/"),
		UpperBound: []byte("sim/account/\xff"),
	})
	if err != nil {
		return nil, fmt.Errorf("world: account index iter: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var out []int
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		const suffix = "/did"
		if len(key) < len(suffix) || string(key[len(key)-len(suffix):]) != suffix {
			continue
		}
		rest := key[len("sim/account/") : len(key)-len(suffix)]
		idx, err := strconv.Atoi(string(rest))
		if err != nil {
			return nil, fmt.Errorf("world: bad account key %q: %w", key, err)
		}
		out = append(out, idx)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("world: account index iter: %w", err)
	}
	sort.Ints(out)
	return out, nil
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

// PDSHostCount returns the number of virtual PDSes in this world.
func (w *World) PDSHostCount() int { return w.cfg.PDSHosts }

// PDSIndexForAccount deterministically assigns an account to a virtual PDS.
// Host zero receives roughly 60% of accounts (the "big mushroom"); the tail
// is spread uniformly across the remaining hosts. The first host-count
// accounts pin one account to each host so small oracle worlds still exercise
// the full topology.
func (w *World) PDSIndexForAccount(accountIdx int) int {
	if w.cfg.PDSHosts == 1 {
		return 0
	}
	if accountIdx >= 0 && accountIdx < w.cfg.PDSHosts {
		return accountIdx
	}
	x := uint64(accountIdx) + w.cfg.Seed + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x ^= x >> 31
	if x%100 < 60 {
		return 0
	}
	return 1 + int((x/100)%uint64(w.cfg.PDSHosts-1))
}

// VirtualPDSHostname is the stable hostname used by the in-process simulator.
func VirtualPDSHostname(index int) string { return fmt.Sprintf("pds%d.sim.invalid", index) }

// RelayKnowsAccount models the recreated relay's incomplete roster. Every
// third account is absent, guaranteeing a relay gap while preserving a large
// realistic subset for legacy relay-listRepos adversity tests.
func (w *World) RelayKnowsAccount(accountIdx int) bool { return accountIdx%3 != 0 }

// PDSAccountCount returns the authoritative direct-listRepos count for a host.
func (w *World) PDSAccountCount(pdsIndex int) int {
	count := 0
	for i := range w.cfg.Accounts {
		if w.PDSIndexForAccount(i) == pdsIndex {
			count++
		}
	}
	return count
}

// RelayAccountFloor returns the incomplete count advertised by listHosts.
func (w *World) RelayAccountFloor(pdsIndex int) int {
	count := 0
	for i := range w.cfg.Accounts {
		if w.PDSIndexForAccount(i) == pdsIndex && w.RelayKnowsAccount(i) {
			count++
		}
	}
	return count
}

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
	indices := make([]int, w.cfg.Accounts)
	for i := range indices {
		indices[i] = i
	}
	return w.listReposPageFromIndices(indices, start, limit)
}

// ListReposPageForPDS pages one host's authoritative roster. start and the
// returned nextStart are ordinals in that host's own cursor space.
func (w *World) ListReposPageForPDS(pdsIndex, start, limit int) ([]ListReposEntry, int, error) {
	if pdsIndex < 0 || pdsIndex >= w.cfg.PDSHosts {
		return nil, 0, fmt.Errorf("world: PDS index %d out of range", pdsIndex)
	}
	indices := make([]int, 0, w.PDSAccountCount(pdsIndex))
	for i := range w.cfg.Accounts {
		if w.PDSIndexForAccount(i) == pdsIndex {
			indices = append(indices, i)
		}
	}
	return w.listReposPageFromIndices(indices, start, limit)
}

// RelayListReposPage pages only the relay-known subset.
func (w *World) RelayListReposPage(start, limit int) ([]ListReposEntry, int, error) {
	indices := make([]int, 0, w.cfg.Accounts*2/3)
	for i := range w.cfg.Accounts {
		if w.RelayKnowsAccount(i) {
			indices = append(indices, i)
		}
	}
	return w.listReposPageFromIndices(indices, start, limit)
}

func (w *World) listReposPageFromIndices(indices []int, start, limit int) (entries []ListReposEntry, nextStart int, err error) {
	if start < 0 {
		start = 0
	}
	if limit > 1000 {
		limit = 1000
	}
	if limit <= 0 {
		limit = 50
	}
	start = min(start, len(indices))
	end := min(start+limit, len(indices))
	out := make([]ListReposEntry, 0, end-start)
	for _, accountIdx := range indices[start:end] {
		a, err := w.LoadAccount(accountIdx)
		if err != nil {
			return nil, 0, err
		}
		state, err := w.loadState(accountIdx)
		if err != nil {
			return nil, 0, err
		}
		deleted, err := w.isAccountDeleted(accountIdx)
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
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()

	return w.generateAccountDelete(ctx, idx)
}

// GenerateAccountReactivateForTest clears a deleted account's flag and
// emits an Active:true #account frame, re-enabling commits. Oracle tests
// use it for the DID-level no-permanent-tombstone path.
func (w *World) GenerateAccountReactivateForTest(ctx context.Context, idx int) ([]byte, error) {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()

	return w.generateAccountReactivate(ctx, idx)
}

// GenerateAccountStatusForTest emits a #account frame with the caller-supplied
// active/status pair without mutating the world's repo or deleted flag. Oracle
// tests use this to pin non-deleted hosting statuses end-to-end: only
// Active:false,status:"deleted" is a tombstone.
func (w *World) GenerateAccountStatusForTest(ctx context.Context, idx int, active bool, status string) ([]byte, error) {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()

	return w.generateAccountStatus(ctx, idx, active, status)
}

// GenerateIdentityForTest emits one polite #identity frame for account
// idx: handle-absent (the dominant production shape) or, with
// handleChange, a handle-change payload backed by the account's
// persisted change counter. Oracle tests use it to pin deterministic
// identity coverage independent of the random traffic mix.
func (w *World) GenerateIdentityForTest(ctx context.Context, idx int, handleChange bool) ([]byte, error) {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()

	if idx < 0 || idx >= w.cfg.Accounts {
		return nil, fmt.Errorf("simulator: identity account index %d out of range", idx)
	}
	if handleChange {
		return w.generateIdentityHandleChange(ctx, idx)
	}
	return w.generateIdentityAbsent(ctx, idx)
}

// GenerateMalformedIdentityForTest emits an #identity frame whose DID
// (MalformedIdentityDID) fails atproto DID syntax, modeling the
// unverified-upstream reality that #identity bodies are not
// signature-checked by relays. Injection-only adversarial input — the
// random traffic mix never produces it.
func (w *World) GenerateMalformedIdentityForTest(ctx context.Context) ([]byte, error) {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()

	return w.generateMalformedIdentity(ctx)
}

func (w *World) IsAccountDeleted(idx int) (bool, error) {
	return w.isAccountDeleted(idx)
}

// SetRepoUnavailableForTest makes getRepo for account idx return a terminal
// unavailable XRPC error. Status must be "takendown", "suspended", or
// "deactivated".
func (w *World) SetRepoUnavailableForTest(idx int, status string) error {
	w.mutationMu.Lock()
	defer w.mutationMu.Unlock()

	return w.setRepoUnavailableStatus(idx, status)
}

// RepoUnavailableStatus returns the terminal getRepo-unavailable status for
// account idx, if one has been configured.
func (w *World) RepoUnavailableStatus(idx int) (string, bool, error) {
	return w.repoUnavailableStatus(idx)
}
