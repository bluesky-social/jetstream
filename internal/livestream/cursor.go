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
	return int64(binary.LittleEndian.Uint64(val)), nil
}

// SaveUpstreamCursor durably persists v under key with pebble.Sync.
// Used inside ingest.Writer's OnAfterFlush so the cursor advance
// is ordered after the per-block fsync.
func SaveUpstreamCursor(s *store.Store, key string, v int64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	if err := s.Set([]byte(key), buf[:], store.SyncWrites); err != nil {
		return fmt.Errorf("livestream: save %s: %w", key, err)
	}
	return nil
}
