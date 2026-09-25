package metastore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
)

// GetUint64LE reads key as an 8-byte little-endian uint64. Returns
// (0, false, nil) when the key is absent. Returns an error if the stored
// value is not exactly 8 bytes.
//
// Centralized so every consumer that stores a uint64 counter (seq/next,
// live_segments/seq/next, maintained counters) uses the same encoding.
func GetUint64LE(ctx context.Context, s Store, key string) (uint64, bool, error) {
	val, err := s.Get(ctx, []byte(key))
	if errors.Is(err, ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("metastore: get %s: %w", key, err)
	}
	if len(val) != 8 {
		return 0, false, fmt.Errorf("metastore: %s has wrong length %d (want 8)", key, len(val))
	}
	return binary.LittleEndian.Uint64(val), true, nil
}

// PrefixUpperBound returns the lexicographically-next byte slice after
// prefix, suitable as the upper bound of a NewIter scan over "all keys with
// prefix". For an ASCII prefix this is the prefix with its last byte
// incremented (e.g. "repo/" → "repo0"). Returns nil for an all-0xFF prefix,
// which NewIter treats as unbounded.
//
// The returned slice is a fresh allocation; callers may modify it.
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

// versionedUint64LELen is the stored size of a versioned uint64 LE payload:
// 1 byte for the format version + 8 bytes for the LE uint64. Matches the live
// cursor convention so every cursor-shaped key has the same layout.
const versionedUint64LELen = 1 + 8

// GetVersionedUint64LE reads key as a [1B version][8B LE uint64] payload and
// returns (value, true, nil) on a hit. A missing key returns (0, false, nil).
// Errors when the stored bytes are the wrong length or carry a version byte
// that differs from wantVersion.
//
// Centralized so cursor-shaped keys (live's relay/cursor, merge's
// merge/next_source_idx) share one encoding and one set of error checks.
func GetVersionedUint64LE(ctx context.Context, s Store, key string, wantVersion byte) (uint64, bool, error) {
	val, err := s.Get(ctx, []byte(key))
	if errors.Is(err, ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("metastore: get %s: %w", key, err)
	}
	if len(val) != versionedUint64LELen {
		return 0, false, fmt.Errorf("metastore: %s: wrong length %d (want %d)",
			key, len(val), versionedUint64LELen)
	}
	if val[0] != wantVersion {
		return 0, false, fmt.Errorf("metastore: %s: unknown version 0x%02x (want 0x%02x)",
			key, val[0], wantVersion)
	}
	return binary.LittleEndian.Uint64(val[1:]), true, nil
}

// SetVersionedUint64LE durably writes key as a [1B version][8B LE uint64]
// payload. Callers staging into a Batch should use EncodeVersionedUint64LE
// with Batch.Set instead.
func SetVersionedUint64LE(ctx context.Context, s Store, key string, version byte, v uint64) error {
	if err := s.Set(ctx, []byte(key), EncodeVersionedUint64LE(version, v)); err != nil {
		return fmt.Errorf("metastore: set %s: %w", key, err)
	}
	return nil
}

// EncodeVersionedUint64LE returns a fresh [1B version][8B LE uint64] payload.
func EncodeVersionedUint64LE(version byte, v uint64) []byte {
	buf := make([]byte, versionedUint64LELen)
	buf[0] = version
	binary.LittleEndian.PutUint64(buf[1:], v)
	return buf
}
