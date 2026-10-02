package protocol

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/objstore/objcache"
)

// RowSource looks up object rows for the Reader. Production readers pass
// the catalog mirror (§11.1), so the hot path makes no PostgreSQL round
// trip; refresh asks it to catch up with the catalog before answering.
type RowSource interface {
	// Object returns the row for id, or found=false if there is none.
	Object(ctx context.Context, id uint64, refresh bool) (row catalog.ObjectRow, found bool, err error)
}

// DBRows is a RowSource that reads each row in its own read transaction. It
// is always current, so refresh changes nothing.
type DBRows struct {
	DB catalog.DB
}

// Object implements RowSource.
func (d DBRows) Object(ctx context.Context, id uint64, _ bool) (catalog.ObjectRow, bool, error) {
	tx, err := d.DB.BeginRead(ctx)
	if err != nil {
		return catalog.ObjectRow{}, false, err
	}
	rows, err := tx.Objects(ctx, []uint64{id})
	if cerr := tx.Close(ctx); err == nil {
		err = cerr
	}
	if err != nil {
		return catalog.ObjectRow{}, false, err
	}
	for _, r := range rows {
		if r.ID == id {
			return r, true, nil
		}
	}
	return catalog.ObjectRow{}, false, nil
}

// ReaderConfig configures a Reader.
type ReaderConfig struct {
	Rows      RowSource
	Blob      objstore.Blob
	ArchiveID [16]byte
	// Cache may be nil.
	Cache          *objcache.Cache
	Metrics        *Metrics
	CatalogMetrics *catalog.Metrics
}

// Reader runs the §7.5 read protocol and implements objstore.Store. It is
// safe for concurrent use.
//
// The corruption rule is decided per object row: an available row promises
// its bytes exist, because GC deletes bytes only after claiming the row out
// of available (§13). So bytes that stay missing or wrong across a refresh
// while the row is still available are corruption. A row that is no longer
// available is objstore.ErrGone, and the caller, which knows the reference,
// re-resolves it.
type Reader struct {
	cfg ReaderConfig
}

var _ objstore.Store = (*Reader)(nil)

// NewReader validates cfg and returns a Reader.
func NewReader(cfg ReaderConfig) (*Reader, error) {
	if cfg.Rows == nil || cfg.Blob == nil {
		return nil, errors.New("protocol: reader needs a RowSource and a Blob")
	}
	return &Reader{cfg: cfg}, nil
}

// Get implements objstore.Store.
func (r *Reader) Get(ctx context.Context, id uint64) ([]byte, error) {
	ctx, span := tracer.Start(ctx, "objstore.Get", trace.WithAttributes(attribute.Int64("objstore.object_id", int64(id))))
	defer span.End()
	data, err := r.read(ctx, id, func(row catalog.ObjectRow) ([]byte, bool, error) {
		if data, ok := r.cached(row); ok {
			return data, true, nil
		}
		data, err := r.cfg.Blob.GetKey(ctx, r.key(row))
		if err == nil {
			err = objstore.Verify(data, row.Length, row.SHA256)
		}
		if err == nil {
			r.cfg.Cache.Add(row.SHA256, data)
		}
		return data, false, err
	})
	return data, record(span, err)
}

// GetRange implements objstore.Store.
func (r *Reader) GetRange(ctx context.Context, id uint64, off, n int64) ([]byte, error) {
	ctx, span := tracer.Start(ctx, "objstore.GetRange", trace.WithAttributes(
		attribute.Int64("objstore.object_id", int64(id)),
		attribute.Int64("objstore.offset", off),
		attribute.Int64("objstore.length", n)))
	defer span.End()
	data, err := r.read(ctx, id, func(row catalog.ObjectRow) ([]byte, bool, error) {
		want, err := objstore.RangeLen(row.Length, off, n)
		if err != nil {
			return nil, false, errCallerRange{err}
		}
		if data, ok := r.cached(row); ok {
			return data[off : off+want], true, nil
		}
		data, err := r.cfg.Blob.GetKeyRange(ctx, r.key(row), off, n)
		switch {
		case errors.Is(err, objstore.ErrInvalidRange):
			// The catalog length says the range is valid, so the stored
			// object is shorter than recorded.
			return nil, false, fmt.Errorf("%w: store rejected range [%d,+%d) of a %d-byte object: %w",
				objstore.ErrCorrupt, off, n, row.Length, err)
		case err != nil:
			return nil, false, err
		case int64(len(data)) != want:
			return nil, false, fmt.Errorf("%w: range [%d,+%d) returned %d bytes, want %d",
				objstore.ErrCorrupt, off, n, len(data), want)
		}
		return data, false, nil
	})
	return data, record(span, err)
}

// errCallerRange is a range the catalog length rejects: the caller's
// mistake, returned as is.
type errCallerRange struct{ err error }

func (e errCallerRange) Error() string { return e.err.Error() }
func (e errCallerRange) Unwrap() error { return e.err }

// read runs the §7.5 flow around fetch, which reads and checks one row's
// bytes and reports whether it served them from the cache.
func (r *Reader) read(ctx context.Context, id uint64, fetch func(catalog.ObjectRow) ([]byte, bool, error)) ([]byte, error) {
	row, err := r.row(ctx, id, false)
	if err != nil {
		return nil, err
	}
	data, cached, err := fetch(row)
	trace.SpanFromContext(ctx).SetAttributes(attribute.Bool("objstore.cache_hit", cached))
	if !missingOrBad(err) {
		return data, unwrapCallerRange(err)
	}

	// §7.5 step 1: refresh and look again. A mirror that lagged a GC or
	// compaction publish may have handed out a row that no longer stands.
	r.cfg.Metrics.verifyFailed(pathRead)
	fresh, err := r.row(ctx, id, true)
	if err != nil {
		return nil, err
	}
	if fresh.Key != row.Key || fresh.SHA256 != row.SHA256 || fresh.Length != row.Length {
		return nil, r.corrupt(catalog.Corruptf(catalog.SourceObject,
			"object %d changed identity: key %s sha256 %x length %d, then key %s sha256 %x length %d",
			id, objstore.FormatUUID(row.Key), row.SHA256, row.Length,
			objstore.FormatUUID(fresh.Key), fresh.SHA256, fresh.Length))
	}
	data, _, err = fetch(fresh)
	if !missingOrBad(err) {
		return data, unwrapCallerRange(err)
	}
	r.cfg.Metrics.verifyFailed(pathRead)

	// Confirm the row is still available right before calling it
	// corruption: GC may have claimed it between the refresh and the
	// second read.
	if _, err := r.row(ctx, id, true); err != nil {
		return nil, err
	}
	return nil, r.corrupt(catalog.Corruptf(catalog.SourceRead, "object %d key %s: %w",
		id, r.key(fresh), err))
}

// row returns id's row if it is available, refreshing once if it is not.
func (r *Reader) row(ctx context.Context, id uint64, refresh bool) (catalog.ObjectRow, error) {
	row, found, err := r.cfg.Rows.Object(ctx, id, refresh)
	if err != nil {
		return catalog.ObjectRow{}, fmt.Errorf("protocol: look up object %d: %w", id, err)
	}
	if found && row.State == catalog.ObjectAvailable {
		return row, nil
	}
	if !refresh {
		return r.row(ctx, id, true)
	}
	state := "no row"
	if found {
		state = string(row.State)
	}
	return catalog.ObjectRow{}, fmt.Errorf("%w: object %d: %s", objstore.ErrGone, id, state)
}

// cached returns the cached bytes for row. The cache holds verified bytes
// by SHA-256, so a hit whose length disagrees with the row means the row is
// wrong; it goes to the store, which fails the check and reports it.
func (r *Reader) cached(row catalog.ObjectRow) ([]byte, bool) {
	data, ok := r.cfg.Cache.Get(row.SHA256)
	return data, ok && int64(len(data)) == row.Length
}

func (r *Reader) key(row catalog.ObjectRow) string {
	return objstore.Key(r.cfg.ArchiveID, row.Key)
}

func (r *Reader) corrupt(err error) error {
	r.cfg.CatalogMetrics.ObserveCorruption(err)
	return err
}

// missingOrBad reports a read that §7.5 answers with refresh and retry.
// Other errors (transient store failures that outlasted the Blob's retries,
// cancellation) are returned as is: they say nothing about the object.
func missingOrBad(err error) bool {
	var cr errCallerRange
	if errors.As(err, &cr) {
		return false
	}
	return errors.Is(err, objstore.ErrNotFound) || errors.Is(err, objstore.ErrCorrupt)
}

func unwrapCallerRange(err error) error {
	var cr errCallerRange
	if errors.As(err, &cr) {
		return cr.err
	}
	return err
}

func record(span trace.Span, err error) error {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "read failed")
	}
	return err
}
