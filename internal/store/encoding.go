package store

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// GetUint64LE reads key as an 8-byte little-endian uint64. Returns
// (0, false, nil) when the key is absent. Returns an error if the
// stored value is not exactly 8 bytes.
//
// Centralized so every consumer that stores a uint64 counter under a
// pebble key — seq/next, live_segments/seq/next, future maintained
// counters — uses the same encoding.
func (s *Store) GetUint64LE(key string) (uint64, bool, error) {
	val, closer, err := s.Get([]byte(key))
	if errors.Is(err, ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("store: get %s: %w", key, err)
	}
	defer func() { _ = closer.Close() }()

	if len(val) != 8 {
		return 0, false, fmt.Errorf("store: %s has wrong length %d (want 8)", key, len(val))
	}
	return binary.LittleEndian.Uint64(val), true, nil
}

// PrefixUpperBound returns the lexicographically-next byte slice after
// prefix, suitable as pebble.IterOptions.UpperBound for a range scan
// of "all keys with prefix". For an ASCII prefix this is the prefix
// with its last byte incremented (e.g. "repo/" → "repo0"). Returns
// nil for an all-0xFF prefix; in that case callers should leave
// UpperBound unset and rely on LowerBound alone.
//
// Pure function. The returned slice is a fresh allocation; callers
// may modify it.
func PrefixUpperBound(prefix []byte) []byte {
	out := make([]byte, len(prefix))
	copy(out, prefix)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}
