// package live: cursor.go persists the upstream relay firehose
// cursor in pebble so a process restart resumes from the last
// durably-flushed block. DESIGN.md §3.1.1: persisted cursor must be
// less than or equal to the latest durable event in the segment file.
//
// The on-disk encoding is [1B version][8B LE uint64], delegated to
// the store package's GetVersionedUint64LE / SetVersionedUint64LE
// helpers so every cursor-shaped key in pebble shares one layout.
// atmos exposes the cursor as int64; we cast at the boundary and
// document the implicit non-negativity constraint (atmos relays
// only emit positive seq values).
package live

import (
	"fmt"
	"math"

	"github.com/bluesky-social/jetstream-v2/internal/store"
)

const (
	// cursorV1 is the only currently-supported cursor format version.
	// A strict-equal check on read means a forward-incompatible writer
	// surfaces as an explicit error rather than a silent
	// misinterpretation of the payload bytes.
	cursorV1 byte = 0x01
)

// LoadUpstreamCursor reads the persisted relay cursor for key.
// A missing key returns 0 with nil error so a fresh data dir
// starts the firehose at "live" (atmos's "no cursor" semantics).
//
// Returns an error if the stored bytes have the high bit set:
// reading those as int64 would yield a negative number, which atmos's
// dial silently treats as "no cursor → live tail". A corrupted
// cursor must surface as an error so the operator notices, not a
// silent re-tail of the firehose that drops every historical event
// between the corrupt seq and now (AGENTS.md: crashing > silent
// data loss).
func LoadUpstreamCursor(s *store.Store, key string) (int64, error) {
	v, ok, err := s.GetVersionedUint64LE(key, cursorV1)
	if err != nil {
		return 0, fmt.Errorf("livestream: %s: %w", key, err)
	}
	if !ok {
		return 0, nil
	}
	if v > math.MaxInt64 {
		return 0, fmt.Errorf("livestream: %s: decodes to negative cursor (raw=0x%016x)", key, v)
	}
	return int64(v), nil
}

// SaveUpstreamCursor durably persists v under key with pebble.Sync.
// Used inside ingest.Writer's OnAfterFlush so the cursor advance
// is ordered after the per-block fsync.
//
// Rejects negative values so the on-disk invariant "stored cursor >= 0"
// holds by construction at every write site, and LoadUpstreamCursor
// can surface storage corruption (rather than caller bugs) as the
// only path to a negative read.
func SaveUpstreamCursor(s *store.Store, key string, v int64) error {
	if v < 0 {
		return fmt.Errorf("livestream: refuse to save negative cursor %d to %s", v, key)
	}
	if err := s.SetVersionedUint64LE(key, cursorV1, uint64(v)); err != nil {
		return fmt.Errorf("livestream: save %s: %w", key, err)
	}
	return nil
}
