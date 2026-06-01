package backfill

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
)

// Counts is the per-status row count produced by CountStatuses.
type Counts struct {
	Total      uint64 `json:"total"`
	Discovered uint64 `json:"discovered"`
	Complete   uint64 `json:"complete"`
	Failed     uint64 `json:"failed"`
}

const countsKey = "backfill/counts"

// LoadCounts reads a precomputed aggregate count. Missing counts are
// expected for data dirs that have not been repaired or migrated to
// include this optional operator-facing summary.
func LoadCounts(s *store.Store) (Counts, bool, error) {
	val, closer, err := s.Get([]byte(countsKey))
	if errors.Is(err, store.ErrNotFound) {
		return Counts{}, false, nil
	}
	if err != nil {
		return Counts{}, false, fmt.Errorf("backfill: load counts: %w", err)
	}
	defer func() { _ = closer.Close() }()

	c, err := decodeCounts(val)
	if err != nil {
		return Counts{}, false, err
	}
	return c, true, nil
}

func decodeCounts(val []byte) (Counts, error) {
	var c Counts
	if err := json.Unmarshal(val, &c); err != nil {
		return Counts{}, fmt.Errorf("backfill: decode counts: %w", err)
	}
	return c, nil
}

func encodeCounts(c Counts) ([]byte, error) {
	enc, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("backfill: encode counts: %w", err)
	}
	return enc, nil
}

// SaveCounts writes aggregate counts. It is exported for repair/migration
// tools and tests; normal backfill state transitions maintain it via Store.
func SaveCounts(s *store.Store, c Counts) error {
	enc, err := encodeCounts(c)
	if err != nil {
		return err
	}
	if err := s.Set([]byte(countsKey), enc, store.SyncWrites); err != nil {
		return fmt.Errorf("backfill: write counts: %w", err)
	}
	return nil
}

// CountStatuses range-scans the repo/ keyspace and tallies rows by
// Backfill.Status. Total is the sum of the three buckets plus any
// rows whose status doesn't decode to a recognized value (those are
// counted under Total but not under any bucket; surfacing the
// mismatch via Total != sum is intentional).
//
// At full network scale this scans tens of millions of keys; cost
// scales linearly with row count. Use behind a TTL cache.
func CountStatuses(s *store.Store) (Counts, error) {
	var c Counts

	prefix := []byte(repoKeyPrefix)
	upper := store.PrefixUpperBound(prefix)

	it, err := s.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return Counts{}, fmt.Errorf("backfill: open iter: %w", err)
	}
	defer func() { _ = it.Close() }()

	for it.First(); it.Valid(); it.Next() {
		c.Total++
		val, err := it.ValueAndErr()
		if err != nil {
			return Counts{}, fmt.Errorf("backfill: read value: %w", err)
		}
		rs, err := decodeRepoStatus(val)
		if err != nil {
			// Don't fail the whole count for one bad row — the row is
			// counted in Total but not under any bucket. Total != sum
			// is the operator's signal that something is corrupt.
			continue
		}
		switch rs.Backfill.Status {
		case StatusNotStarted:
			c.Discovered++
		case StatusComplete:
			c.Complete++
		case StatusFailed:
			c.Failed++
		}
	}
	if err := it.Error(); err != nil {
		return Counts{}, fmt.Errorf("backfill: iter error: %w", err)
	}
	return c, nil
}
