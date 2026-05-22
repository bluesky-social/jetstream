// Package livestream: cursor.go persists the upstream relay firehose
// cursor in pebble so a process restart resumes from the last
// durably-flushed block. DESIGN.md §3.1.1: persisted cursor must be
// less than or equal to the latest durable event in the segment file.
//
// The encoding is little-endian uint64 bytes — the same shape used
// by ingest.Writer for seq/next, so operators inspecting pebble see
// a consistent layout. atmos exposes the cursor as int64; we cast
// at the boundary and document the implicit non-negativity
// constraint (atmos relays only emit positive seq values).
package livestream

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
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
// between the corrupt seq and now (PRACTICES.md: crashing > silent
// data loss).
func LoadUpstreamCursor(s *store.Store, key string) (int64, error) {
	val, closer, err := s.Get([]byte(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("livestream: load %s: %w", key, err)
	}
	defer func() { _ = closer.Close() }()

	if len(val) != 8 {
		return 0, fmt.Errorf("livestream: %s has wrong length %d (want 8)", key, len(val))
	}
	raw := binary.LittleEndian.Uint64(val)
	cur := int64(raw)
	if cur < 0 {
		return 0, fmt.Errorf("livestream: %s decodes to negative cursor (raw=0x%016x)", key, raw)
	}
	return cur, nil
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
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	if err := s.Set([]byte(key), buf[:], store.SyncWrites); err != nil {
		return fmt.Errorf("livestream: save %s: %w", key, err)
	}
	return nil
}
