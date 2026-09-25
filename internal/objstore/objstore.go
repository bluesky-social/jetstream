package objstore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

var (
	// ErrNotFound means the Blob has no object under the key. Real AWS S3
	// answers 403 rather than 404 for a missing key when the credentials lack
	// ListBucket, so an S3 Blob may report either as ErrNotFound. Callers
	// must therefore treat ErrNotFound as "maybe missing" and decide from
	// catalog state, never as proof that the object was deleted.
	ErrNotFound = errors.New("objstore: object not found")

	// ErrInvalidRange means a ranged read asked for bytes the object cannot
	// supply: a negative offset, a non-positive length, or an offset at or
	// past the end of the object (S3's 416).
	ErrInvalidRange = errors.New("objstore: invalid range")

	// ErrCorrupt means bytes read back from a Blob do not match the length or
	// SHA-256 recorded in the catalog.
	ErrCorrupt = errors.New("objstore: object corrupt")

	// ErrGone means a Store has no available object under the ID, even after
	// refreshing its catalog view: compaction or GC replaced the reference
	// the caller resolved. The caller refreshes its own view and retries
	// with the new reference (design §7.5 step 2). If its reference still
	// names the object, that is corruption at the caller's level.
	ErrGone = errors.New("objstore: object no longer available")
)

// Blob is raw, unverified key/value transport for immutable objects.
//
// Implementations must be safe for concurrent use and must not retain or
// mutate caller-owned slices: PutKey copies data if it keeps it, and the
// slices returned by GetKey and GetKeyRange belong to the caller.
type Blob interface {
	// PutKey stores data under key. Keys are never reused, so the behavior
	// of a second PutKey to the same key is unspecified.
	PutKey(ctx context.Context, key string, data []byte) error

	// GetKey returns the whole object, or ErrNotFound.
	GetKey(ctx context.Context, key string) ([]byte, error)

	// GetKeyRange returns bytes [off, off+n) of the object, truncated at the
	// end of the object as S3 does. See RangeLen for the exact semantics.
	// It returns ErrNotFound for a missing key and ErrInvalidRange for a
	// range RangeLen rejects.
	GetKeyRange(ctx context.Context, key string, off, n int64) ([]byte, error)

	// DeleteKey removes the object. Deleting a missing key succeeds, so GC
	// can retry a delete whose first attempt had an unknown result.
	DeleteKey(ctx context.Context, key string) error
}

// Store reads objects by catalog object_id with the design §7.5 checks.
// protocol.Reader implements it. Consumers (the cold reader, getSegment)
// depend on this interface so their tests can substitute a fake.
//
// Writes are not part of Store. An upload needs the leader's catalog session,
// because the uploading row is a fenced write (§7.3), so uploads go through
// protocol.Uploader with a *catalog.Session.
//
// Both methods return ErrGone when the object is no longer available, and a
// catalog.CorruptionError with source "read" when an available object's
// bytes are missing or wrong. Returned slices may be shared with a cache and
// must not be modified.
type Store interface {
	// Get returns the whole object after verifying its length and SHA-256.
	Get(ctx context.Context, objectID uint64) ([]byte, error)

	// GetRange returns a byte range of the object, verifying only the length.
	// Payload integrity comes from the zstd frame checksum inside each block.
	// The range follows RangeLen against the catalog length; a range it
	// rejects returns ErrInvalidRange.
	GetRange(ctx context.Context, objectID uint64, off, n int64) ([]byte, error)
}

// Key returns the Blob key for an object (design §7.1):
// <archive_id>/objects/<uuid>, with both UUIDs in canonical form. The S3 Blob
// puts JETSTREAM_S3_PREFIX in front, so these keys never start with a slash.
func Key(archiveID, key [16]byte) string {
	return FormatUUID(archiveID) + "/objects/" + FormatUUID(key)
}

// NewUUID returns a random version 4 UUID. Every upload attempt gets a fresh
// one, so no key is ever written twice.
func NewUUID() [16]byte {
	var u [16]byte
	_, _ = rand.Read(u[:]) // crypto/rand.Read never returns an error
	u[6] = u[6]&0x0f | 0x40
	u[8] = u[8]&0x3f | 0x80
	return u
}

// FormatUUID formats u as xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx.
func FormatUUID(u [16]byte) string {
	var b [36]byte
	hex.Encode(b[0:8], u[0:4])
	b[8] = '-'
	hex.Encode(b[9:13], u[4:6])
	b[13] = '-'
	hex.Encode(b[14:18], u[6:8])
	b[18] = '-'
	hex.Encode(b[19:23], u[8:10])
	b[23] = '-'
	hex.Encode(b[24:], u[10:])
	return string(b[:])
}

// RangeLen returns how many bytes a ranged read of [off, off+n) returns
// from an object of size bytes. It mirrors S3: a range running past the end
// is truncated, and a range starting at or past the end is ErrInvalidRange.
// Blob implementations use it to agree on semantics, and the Store uses it
// to check the length of a range read.
func RangeLen(size, off, n int64) (int64, error) {
	if off < 0 || n <= 0 || off >= size {
		return 0, fmt.Errorf("%w: off=%d n=%d size=%d", ErrInvalidRange, off, n, size)
	}
	return min(n, size-off), nil
}

// Verify checks bytes read from a Blob against the length and SHA-256 the
// catalog recorded at upload. A mismatch returns an error wrapping
// ErrCorrupt. The length is checked first so a truncated read is reported as
// such without hashing.
func Verify(data []byte, wantLen int64, wantSHA256 [sha256.Size]byte) error {
	if int64(len(data)) != wantLen {
		return fmt.Errorf("%w: length %d, want %d", ErrCorrupt, len(data), wantLen)
	}
	if got := sha256.Sum256(data); got != wantSHA256 {
		return fmt.Errorf("%w: sha256 %x, want %x", ErrCorrupt, got, wantSHA256)
	}
	return nil
}
