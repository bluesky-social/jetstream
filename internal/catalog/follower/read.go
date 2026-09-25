package follower

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
)

var (
	// ErrLoading is Ready's error until the first steady-state mirror and
	// its footers are loaded.
	ErrLoading = errors.New("catalog: mirror loading")
	// ErrStale is Ready's error while the mirror is older than MaxViewAge.
	ErrStale = errors.New("catalog: mirror stale")
)

// Object implements protocol.RowSource. A plain lookup answers from the
// current mirror, falling back to the catalog for objects it does not hold
// (a ref from an older view). refresh reads the catalog directly rather than
// through a tick: object state is not revision-tracked, so only a direct
// read can see a GC claim, and a reader inside a tick must not wait on it.
func (f *Follower) Object(ctx context.Context, id uint64, refresh bool) (catalog.ObjectRow, bool, error) {
	if !refresh {
		if m := f.cur.Load(); m != nil {
			if r, ok := m.objects.get(id); ok {
				return r, true, nil
			}
		}
	}
	return protocol.DBRows{DB: f.cfg.DB}.Object(ctx, id, true)
}

// Snapshot implements catalog.Catalog. Before the first tick it is an empty
// view.
func (f *Follower) Snapshot() catalog.CatalogView {
	if m := f.cur.Load(); m != nil {
		return m.view
	}
	v, _ := catalog.NewView(0, nil, nil, nil)
	return v
}

// Log returns the pod's readable log, or nil before the first steady-state
// mirror.
func (f *Follower) Log() *ingest.ReadableLog {
	if l := f.flog.Load(); l != nil {
		return l.Log()
	}
	return nil
}

// Ready implements lifecycle.Readiness (design §11.6): a mirror refreshed
// within MaxViewAge, in steady state, with the manifest loaded.
func (f *Follower) Ready(context.Context) error {
	m := f.cur.Load()
	if m == nil {
		return ErrLoading
	}
	if age := f.cfg.Now().Sub(m.refreshed); age > f.cfg.MaxViewAge {
		return fmt.Errorf("%w: last refresh %s ago", ErrStale, age.Round(time.Millisecond))
	}
	if m.phase != lifecycle.PhaseSteadyState {
		return lifecycle.ErrBootstrapInProgress
	}
	if f.cfg.Manifest != nil && !f.footersLoaded.Load() {
		return ErrLoading
	}
	return nil
}

// NextCompactionAt returns the compaction deadline the mirror holds
// (metadata_kv compaction/deadline), for archive Cache-Control.
func (f *Follower) NextCompactionAt() (time.Time, bool) {
	if m := f.cur.Load(); m != nil && m.deadlineOK {
		return m.deadline, true
	}
	return time.Time{}, false
}

// Fetch implements catalog.Fetcher over the object store. A sealed ref
// whose generation the current mirror no longer holds, or an object the
// catalog no longer holds, is ErrStaleRef: the reader takes a fresh view.
func (f *Follower) Fetch(ctx context.Context, ref catalog.BlockRef) ([]byte, error) {
	switch loc := ref.Loc.(type) {
	case catalog.InlineBlock:
		return loc.Frame, nil
	case catalog.ObjectBlock:
		if ref.Generation != 0 {
			m := f.cur.Load()
			if m == nil {
				return nil, catalog.ErrStaleRef
			}
			if v, ok := m.sealed[ref.Namespace].Get(ref.Segment); !ok || v.Generation != ref.Generation {
				return nil, fmt.Errorf("%w: %s segment %d generation %d", catalog.ErrStaleRef, ref.Namespace, ref.Segment, ref.Generation)
			}
		}
		frame, err := f.rd.Get(ctx, loc.ObjectID)
		if errors.Is(err, objstore.ErrGone) {
			return nil, fmt.Errorf("%w: %w", catalog.ErrStaleRef, err)
		}
		return frame, err
	default:
		return nil, fmt.Errorf("follower: unsupported locator %T", ref.Loc)
	}
}

// inlineKey is the decoded-block cache key of an inline hot batch.
type inlineKey struct {
	firstSeq uint64
	sum      [sha256.Size]byte
}

// BlockCacheKey returns the decoded-block cache key for ref (design §11.4):
// a sealed block's object SHA-256, or an inline batch's first seq and frame
// SHA-256. Active blocks and pointer hot batches are not cached.
func (f *Follower) BlockCacheKey(ref catalog.BlockRef) (any, bool) {
	switch loc := ref.Loc.(type) {
	case catalog.InlineBlock:
		return inlineKey{firstSeq: ref.MinSeq, sum: sha256.Sum256(loc.Frame)}, true
	case catalog.ObjectBlock:
		if ref.Generation == 0 {
			return nil, false
		}
		m := f.cur.Load()
		if m == nil {
			return nil, false
		}
		if r, ok := m.objects.get(loc.ObjectID); ok {
			return r.SHA256, true
		}
	}
	return nil, false
}
