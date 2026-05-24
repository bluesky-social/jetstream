// package live: cursor.go persists the upstream relay firehose
// cursor in pebble so a process restart resumes from the last
// durably-flushed block. DESIGN.md §3.1.1: persisted cursor must be
// less than or equal to the latest durable event in the segment file.
//
// The on-disk encoding is [1B version][8B LE uint64]. The version
// byte gives us a future evolution path (rename a relay, attach a
// generation counter, ...) that a bare uint64 doesn't. The uint64
// payload is little-endian to match ingest.Writer's seq/next layout
// so operators inspecting pebble see a consistent shape. atmos
// exposes the cursor as int64; we cast at the boundary and document
// the implicit non-negativity constraint (atmos relays only emit
// positive seq values).
package live

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream-v2/internal/store"
)

const (
	// cursorV1 is the only currently-supported cursor format. A
	// strict-equal check on read means a forward-incompatible writer
	// surfaces as an explicit error rather than a silent
	// misinterpretation of the payload bytes.
	cursorV1 = 0x01

	// cursorV1Len is the exact byte length of a v1 cursor value:
	// 1 version byte + 8 little-endian uint64 bytes.
	cursorV1Len = 1 + 8
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
	if errors.Is(err, store.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("livestream: load %s: %w", key, err)
	}
	defer func() { _ = closer.Close() }()

	cur, err := decodeUpstreamCursor(val)
	if err != nil {
		return 0, fmt.Errorf("livestream: %s: %w", key, err)
	}
	return cur, nil
}

// decodeUpstreamCursor parses the on-disk cursor payload. Pure
// function so it can be fuzzed without a pebble dependency.
func decodeUpstreamCursor(val []byte) (int64, error) {
	if len(val) != cursorV1Len {
		return 0, fmt.Errorf("wrong length %d (want %d)", len(val), cursorV1Len)
	}
	if val[0] != cursorV1 {
		return 0, fmt.Errorf("unknown version 0x%02x (want 0x%02x)", val[0], cursorV1)
	}
	raw := binary.LittleEndian.Uint64(val[1:])
	cur := int64(raw)
	if cur < 0 {
		return 0, fmt.Errorf("decodes to negative cursor (raw=0x%016x)", raw)
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
	var buf [cursorV1Len]byte
	buf[0] = cursorV1
	binary.LittleEndian.PutUint64(buf[1:], uint64(v))
	if err := s.Set([]byte(key), buf[:], store.SyncWrites); err != nil {
		return fmt.Errorf("livestream: save %s: %w", key, err)
	}
	return nil
}
