package backfill

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
)

// repoKeyPrefix is the namespace for per-DID RepoStatus rows in the
// metadata store (DESIGN.md §3.5). Keep it as a byte slice so we can
// use it directly with pebble's []byte API and avoid the per-call
// []byte(...) conversion on hot enumeration paths.
var repoKeyPrefix = []byte("repo/")

// repoKey assembles the on-disk key for a per-DID row. Returning a
// freshly-allocated slice keeps callers safe to retain the result
// across pebble calls (pebble batch APIs make a copy internally, but
// the cost of one allocation per write is negligible compared to
// the JSON-encode that follows).
func repoKey(did atmos.DID) []byte {
	k := make([]byte, 0, len(repoKeyPrefix)+len(did))
	k = append(k, repoKeyPrefix...)
	k = append(k, did...)
	return k
}

// repoKeyUpperBound returns the exclusive upper bound for the repo/
// keyspace. We bump the last byte of the prefix to get the first
// key strictly greater than any possible repo/<did> key.
func repoKeyUpperBound() []byte {
	end := make([]byte, len(repoKeyPrefix))
	copy(end, repoKeyPrefix)
	end[len(end)-1]++
	return end
}

// GetRepoStatus reads the RepoStatus for did. Returns (zero, false,
// nil) if no row exists yet — this is the expected path for a DID
// that has not been seeded yet, and not an error.
func GetRepoStatus(s *store.Store, did atmos.DID) (RepoStatus, bool, error) {
	val, closer, err := s.Get(repoKey(did))
	if errors.Is(err, pebble.ErrNotFound) {
		return RepoStatus{}, false, nil
	}
	if err != nil {
		return RepoStatus{}, false, fmt.Errorf("backfill: get repo/%s: %w", did, err)
	}
	defer func() { _ = closer.Close() }()

	var rs RepoStatus
	if err := json.Unmarshal(val, &rs); err != nil {
		return RepoStatus{}, false, fmt.Errorf("backfill: decode repo/%s: %w", did, err)
	}
	return rs, true, nil
}

// PutRepoStatus persists rs at repo/<did> with a synchronous fsync.
// Bulk callers (the seed step) should batch via SeedRepos rather
// than looping over PutRepoStatus.
func PutRepoStatus(s *store.Store, did atmos.DID, rs RepoStatus) error {
	buf, err := json.Marshal(rs)
	if err != nil {
		return fmt.Errorf("backfill: encode repo/%s: %w", did, err)
	}
	if err := s.Set(repoKey(did), buf, pebble.Sync); err != nil {
		return fmt.Errorf("backfill: set repo/%s: %w", did, err)
	}
	return nil
}

// HasRepo reports whether a row exists at repo/<did>. Equivalent to
// GetRepoStatus but skips the JSON-decode, which is the inner-loop
// shape of the seed step's "have we seen this DID before?" check.
func HasRepo(s *store.Store, did atmos.DID) (bool, error) {
	_, closer, err := s.Get(repoKey(did))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("backfill: probe repo/%s: %w", did, err)
	}
	_ = closer.Close()
	return true, nil
}

// CountRepos counts the number of repo/<did> rows in the store.
// Intended for tests and for the bootstrap CLI's final progress
// summary; the implementation is a single forward iterator scan, so
// don't call this on the hot path of ingest.
func CountRepos(s *store.Store) (int64, error) {
	it, err := s.NewIter(&pebble.IterOptions{
		LowerBound: repoKeyPrefix,
		UpperBound: repoKeyUpperBound(),
	})
	if err != nil {
		return 0, fmt.Errorf("backfill: open iterator: %w", err)
	}
	defer func() { _ = it.Close() }()

	var n int64
	for valid := it.First(); valid; valid = it.Next() {
		n++
	}
	if err := it.Error(); err != nil {
		return 0, fmt.Errorf("backfill: iterate repo/: %w", err)
	}
	return n, nil
}
