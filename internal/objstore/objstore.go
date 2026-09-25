package objstore

import (
	"context"
	"crypto/sha256"
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

// Store is the object protocol (design §7.3 and §7.5) over a Blob and the
// catalog. Objects are addressed by catalog object_id, not by key.
//
// The implementation lands with the catalog transaction layer (plan S2.5).
type Store interface {
	// Put uploads data (or dedups against an existing available object with
	// the same SHA-256) and returns its object_id.
	Put(ctx context.Context, data []byte) (objectID uint64, err error)

	// Get returns the whole object after verifying its length and SHA-256.
	Get(ctx context.Context, objectID uint64) ([]byte, error)

	// GetRange returns a byte range of the object, verifying only the length.
	// Payload integrity comes from the zstd frame checksum inside each block.
	GetRange(ctx context.Context, objectID uint64, off, n int64) ([]byte, error)
}

// RangeLen returns how many bytes a ranged read of [off, off+n) returns
// from an object of size bytes. It mirrors S3: a range running past the end
// is truncated, and a range starting at or past the end is ErrInvalidRange.
// Blob implementations use it to agree on semantics; Store uses it to check
// the length of a range read.
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
