// Package memblob is an in-memory objstore.Blob for tests and the layer 3
// storage fake. It follows S3 semantics where the contract leaves room
// (ranged reads, missing-key deletes) and carries a deterministic fault
// seam for the failures S3 can produce: errors, wrong bytes, and a PUT that
// is acknowledged but never stored.
package memblob

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/bluesky-social/jetstream/internal/objstore"
)

// Blob is an in-memory objstore.Blob. The zero value is not usable; call New.
type Blob struct {
	faults FaultInjector

	mu      sync.Mutex
	objects map[string][]byte
}

var _ objstore.Blob = (*Blob)(nil)

// Option configures a Blob.
type Option func(*Blob)

// WithFaultInjector installs a test-only fault seam. Passing nil is a no-op,
// so a test can thread an optional injector unconditionally.
func WithFaultInjector(f FaultInjector) Option {
	return func(b *Blob) {
		if f != nil {
			b.faults = f
		}
	}
}

// New returns an empty Blob.
func New(opts ...Option) *Blob {
	b := &Blob{objects: make(map[string][]byte)}
	for _, o := range opts {
		o(b)
	}
	return b
}

// PutKey implements objstore.Blob.
func (b *Blob) PutKey(ctx context.Context, key string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	kind, ferr := b.before(OpPut, key)
	switch kind {
	case FaultError:
		return ferr
	case FaultDropPut:
		return nil
	}
	stored := slices.Clone(data)
	if stored == nil {
		// Keep a present-but-empty object distinct from a missing one.
		stored = []byte{}
	}
	if kind == FaultWrongBytes {
		stored = corrupt(stored)
	}
	b.mu.Lock()
	b.objects[key] = stored
	b.mu.Unlock()
	if kind == FaultErrorAfter {
		return ferr
	}
	return nil
}

// GetKey implements objstore.Blob.
func (b *Blob) GetKey(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	kind, ferr := b.before(OpGet, key)
	if kind == FaultError {
		return nil, ferr
	}
	b.mu.Lock()
	data, ok := b.objects[key]
	b.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("memblob: get %q: %w", key, objstore.ErrNotFound)
	}
	// Stored slices are never mutated after PutKey, so cloning outside the
	// lock is safe.
	out := slices.Clone(data)
	if kind == FaultWrongBytes {
		out = corrupt(out)
	}
	return out, nil
}

// GetKeyRange implements objstore.Blob.
func (b *Blob) GetKeyRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	kind, ferr := b.before(OpGetRange, key)
	if kind == FaultError {
		return nil, ferr
	}
	b.mu.Lock()
	data, ok := b.objects[key]
	b.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("memblob: get range %q: %w", key, objstore.ErrNotFound)
	}
	got, err := objstore.RangeLen(int64(len(data)), off, n)
	if err != nil {
		return nil, fmt.Errorf("memblob: get range %q: %w", key, err)
	}
	out := slices.Clone(data[off : off+got])
	if kind == FaultWrongBytes {
		out = corrupt(out)
	}
	return out, nil
}

// DeleteKey implements objstore.Blob.
func (b *Blob) DeleteKey(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	kind, ferr := b.before(OpDelete, key)
	if kind == FaultError {
		return ferr
	}
	b.mu.Lock()
	delete(b.objects, key)
	b.mu.Unlock()
	if kind == FaultErrorAfter {
		return ferr
	}
	return nil
}

// Keys returns every stored key in sorted order, so a test can check for
// leaked or prematurely deleted objects.
func (b *Blob) Keys() []string {
	b.mu.Lock()
	keys := make([]string, 0, len(b.objects))
	for k := range b.objects {
		keys = append(keys, k)
	}
	b.mu.Unlock()
	slices.Sort(keys)
	return keys
}

// before consults the injector (if any) and rejects a fault kind that has
// no meaning for op. A misconfigured fault fails the op loudly rather than
// being silently ignored, so a test cannot pass without its fault firing.
func (b *Blob) before(op Op, key string) (FaultKind, error) {
	if b.faults == nil {
		return FaultNone, nil
	}
	kind, err := b.faults.BeforeBlobOp(op, key)
	if kind == FaultNone {
		return FaultNone, nil
	}
	if !kind.appliesTo(op) {
		return FaultError, fmt.Errorf("memblob: fault %q does not apply to op %q", kind, op)
	}
	if err == nil && (kind == FaultError || kind == FaultErrorAfter) {
		err = fmt.Errorf("memblob: injected %s fault on %s %q", kind, op, key)
	}
	return kind, err
}

// corrupt returns data with one bit flipped, or a single stray byte when data
// is empty, so the result always differs from the original.
func corrupt(data []byte) []byte {
	if len(data) == 0 {
		return []byte{0}
	}
	data[len(data)/2] ^= 0x01
	return data
}
