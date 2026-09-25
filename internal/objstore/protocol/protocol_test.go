package protocol_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
	"github.com/bluesky-social/jetstream/internal/objstore/objcache"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"github.com/bluesky-social/jetstream/internal/objstore/s3/s3test"
	"github.com/bluesky-social/jetstream/internal/storagefake"
)

var archiveID = [16]byte{0xa1, 0xb2, 0xc3}

const (
	gcDelay   = 6 * time.Hour
	orphanAge = time.Hour
)

// objectPrefix matches every object key, for memblob faults.
var objectPrefix = objstore.FormatUUID(archiveID) + "/objects/"

type harness struct {
	db       *storagefake.DB
	blob     objstore.Blob
	session  *catalog.Session
	up       *protocol.Uploader
	rd       *protocol.Reader
	metrics  *protocol.Metrics
	catalogM *catalog.Metrics
	cache    *objcache.Cache
}

type options struct {
	blob        objstore.Blob
	wrapDB      func(catalog.DB) catalog.DB // wraps the session's DB
	rows        protocol.RowSource
	concurrency int
	cacheBytes  int64
}

func newHarness(t *testing.T, o options) *harness {
	t.Helper()
	db := storagefake.New(storagefake.Config{ArchiveID: archiveID})
	lease := db.NewLease()
	require.NoError(t, lease.Acquire(t.Context(), time.Hour))
	reg := prometheus.NewRegistry()
	h := &harness{
		db:       db,
		blob:     o.blob,
		metrics:  protocol.NewMetrics(reg),
		catalogM: catalog.NewMetrics(reg),
	}
	if h.blob == nil {
		h.blob = memblob.New()
	}
	var sdb catalog.DB = db
	if o.wrapDB != nil {
		sdb = o.wrapDB(db)
	}
	h.session = catalog.NewSession(catalog.SessionConfig{DB: sdb, Epoch: lease.Epoch(), Metrics: h.catalogM})
	_, err := h.session.InitNamespace(t.Context(), catalog.Main, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if !t.Failed() {
			require.NoError(t, db.Violation(), "catalog invariants")
		}
	})
	h.up, err = protocol.NewUploader(protocol.UploaderConfig{
		Blob: h.blob, ArchiveID: archiveID, GCDelay: gcDelay, OrphanAge: orphanAge,
		Concurrency: o.concurrency, Metrics: h.metrics,
	})
	require.NoError(t, err)
	if o.cacheBytes > 0 {
		h.cache = objcache.New(objcache.Config{MaxBytes: o.cacheBytes})
	}
	rows := o.rows
	if rows == nil {
		rows = protocol.DBRows{DB: db}
	}
	h.rd, err = protocol.NewReader(protocol.ReaderConfig{
		Rows: rows, Blob: h.blob, ArchiveID: archiveID, Cache: h.cache,
		Metrics: h.metrics, CatalogMetrics: h.catalogM,
	})
	require.NoError(t, err)
	return h
}

// keys lists the stored keys of a harness on a plain memblob.
func (h *harness) keys() []string {
	mb, _ := h.blob.(*memblob.Blob)
	return mb.Keys()
}

func (h *harness) verifyFailures(path string) float64 {
	return testutil.ToFloat64(h.metrics.VerifyFailures.WithLabelValues(path))
}

func (h *harness) corruption(source string) float64 {
	return testutil.ToFloat64(h.catalogM.Corruption.WithLabelValues(source))
}

func (h *harness) object(t *testing.T, id uint64) catalog.ObjectRow {
	t.Helper()
	row, found, err := protocol.DBRows{DB: h.db}.Object(t.Context(), id, false)
	require.NoError(t, err)
	require.True(t, found, "object %d", id)
	return row
}

// put uploads data and makes it available.
func (h *harness) put(t *testing.T, data []byte) uint64 {
	t.Helper()
	ref, err := h.up.Put(t.Context(), h.session, data)
	require.NoError(t, err)
	require.False(t, ref.Pending)
	return ref.ID
}

func payload(i int) []byte {
	return []byte(strings.Repeat(fmt.Sprintf("object %d;", i), 50))
}

func TestUploadBatchAndRead(t *testing.T) {
	t.Parallel()
	h := newHarness(t, options{})
	ctx := t.Context()
	objs := [][]byte{payload(0), payload(1), payload(0)}
	refs, err := h.up.Upload(ctx, h.session, objs)
	require.NoError(t, err)
	require.Len(t, refs, 3)
	require.Equal(t, refs[0], refs[2], "identical inputs share one object")
	require.NotEqual(t, refs[0].ID, refs[1].ID)
	for i, r := range refs {
		require.True(t, r.Pending)
		require.Equal(t, sha256.Sum256(objs[i]), r.SHA256)
		row := h.object(t, r.ID)
		require.Equal(t, catalog.ObjectUploading, row.State)
		require.EqualValues(t, len(objs[i]), row.Length)
	}
	require.Len(t, h.keys(), 2)

	// Uploading objects are not readable yet.
	_, err = h.rd.Get(ctx, refs[0].ID)
	require.ErrorIs(t, err, objstore.ErrGone)

	for _, r := range refs[:2] {
		id, err := h.session.MarkAvailable(ctx, r)
		require.NoError(t, err)
		require.Equal(t, r.ID, id)
	}
	for i, r := range refs {
		require.Contains(t, h.keys(), objstore.Key(archiveID, h.object(t, r.ID).Key))
		got, err := h.rd.Get(ctx, r.ID)
		require.NoError(t, err)
		require.Equal(t, objs[i], got)
	}
	require.Zero(t, h.verifyFailures("read"))
}

func TestDedup(t *testing.T) {
	t.Parallel()
	h := newHarness(t, options{})
	id := h.put(t, payload(0))
	refs, err := h.up.Upload(t.Context(), h.session, [][]byte{payload(0), payload(1)})
	require.NoError(t, err)
	require.Equal(t, catalog.ObjectRef{ID: id, SHA256: sha256.Sum256(payload(0))}, refs[0], "deduplicated, ready to reference")
	require.True(t, refs[1].Pending)
	require.Len(t, h.keys(), 2, "no second PUT of the deduplicated bytes")

	// A still-uploading copy is not a dedup target.
	again, err := h.up.Upload(t.Context(), h.session, [][]byte{payload(1)})
	require.NoError(t, err)
	require.True(t, again[0].Pending)
	require.NotEqual(t, refs[1].ID, again[0].ID)
}

// ageDB records the maxUnrefAge BeginUploads passes to the dedup lookup.
type ageDB struct {
	catalog.DB
	ages chan time.Duration
}

func (d ageDB) Begin(ctx context.Context, kind catalog.TxKind) (catalog.Tx, error) {
	tx, err := d.DB.Begin(ctx, kind)
	return ageTx{tx, d.ages}, err
}

type ageTx struct {
	catalog.Tx
	ages chan time.Duration
}

func (t ageTx) FindAvailableObject(ctx context.Context, sha [32]byte, maxUnrefAge time.Duration) (catalog.ObjectRow, bool, error) {
	select {
	case t.ages <- maxUnrefAge:
	default:
	}
	return t.Tx.FindAvailableObject(ctx, sha, maxUnrefAge)
}

func TestDedupAgeIsHalfGCDelay(t *testing.T) {
	t.Parallel()
	ages := make(chan time.Duration, 1)
	h := newHarness(t, options{wrapDB: func(db catalog.DB) catalog.DB { return ageDB{db, ages} }})
	_, err := h.up.Upload(t.Context(), h.session, [][]byte{payload(0)})
	require.NoError(t, err)
	require.Equal(t, gcDelay/2, <-ages)
}

// TestUploadRace is §7.3 step 6's unique-index race: two uploads of the same
// bytes both commit uploading rows, and whichever becomes available first
// wins. The loser's caller gets the winner's ID, both through Put and
// through a pending ref folded into the referencing transaction.
func TestUploadRace(t *testing.T) {
	t.Parallel()
	h := newHarness(t, options{})
	ctx := t.Context()
	data := payload(7)
	a, err := h.up.Upload(ctx, h.session, [][]byte{data})
	require.NoError(t, err)
	b, err := h.up.Upload(ctx, h.session, [][]byte{data})
	require.NoError(t, err)
	require.NotEqual(t, a[0].ID, b[0].ID)

	// c's upload starts while both are still uploading, and Put makes it
	// available first.
	c, err := h.up.Put(ctx, h.session, data)
	require.NoError(t, err)
	require.NotEqual(t, a[0].ID, c.ID)
	require.NotEqual(t, b[0].ID, c.ID)

	winner, err := h.session.MarkAvailable(ctx, a[0])
	require.NoError(t, err)
	require.Equal(t, c.ID, winner)
	require.Equal(t, catalog.ObjectUploading, h.object(t, a[0].ID).State, "loser left for GC")

	commit, err := h.session.CommitHotBatch(ctx, catalog.HotBatch{FirstSeq: 1, LastSeq: 1, Object: b[0]})
	require.NoError(t, err)
	require.Equal(t, c.ID, commit.ObjectID, "the folded step 6 references the winner")
	require.Equal(t, catalog.ObjectUploading, h.object(t, b[0].ID).State)

	got, err := h.rd.Get(ctx, c.ID)
	require.NoError(t, err)
	require.Equal(t, data, got)
	require.NoError(t, h.session.Err())
}

// gateBlob holds the first PutKey for a while, standing in for a pod that
// pauses between committing its uploading rows and issuing a PUT.
type gateBlob struct {
	objstore.Blob
	pause time.Duration
	puts  atomic.Int64
}

func (g *gateBlob) PutKey(ctx context.Context, key string, data []byte) error {
	if g.puts.Add(1) == 1 {
		time.Sleep(g.pause)
	}
	return g.Blob.PutKey(ctx, key, data)
}

func TestSkipPutAfterHalfOrphanAge(t *testing.T) {
	t.Parallel()
	for _, pause := range []time.Duration{orphanAge/2 - time.Second, orphanAge/2 + time.Second} {
		t.Run(pause.String(), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				mb := memblob.New()
				h := newHarness(t, options{blob: &gateBlob{Blob: mb, pause: pause}, concurrency: 1})
				_, err := h.up.Upload(t.Context(), h.session, [][]byte{payload(0), payload(1)})
				if pause < orphanAge/2 {
					require.NoError(t, err)
					require.Len(t, mb.Keys(), 2)
					return
				}
				require.ErrorIs(t, err, catalog.ErrSessionEnded)
				require.ErrorContains(t, err, "orphan_age/2")
				require.ErrorIs(t, h.session.Err(), catalog.ErrSessionEnded)
				_, corrupt := catalog.IsCorruption(err)
				require.False(t, corrupt)
				require.Len(t, mb.Keys(), 1, "the late object is never PUT")
			})
		})
	}
}

// faultFunc adapts a function to memblob.FaultInjector.
type faultFunc func(op memblob.Op, key string) (memblob.FaultKind, error)

func (f faultFunc) BeforeBlobOp(op memblob.Op, key string) (memblob.FaultKind, error) {
	return f(op, key)
}

func always(op memblob.Op, kind memblob.FaultKind) faultFunc {
	return func(o memblob.Op, _ string) (memblob.FaultKind, error) {
		if o == op {
			return kind, nil
		}
		return memblob.FaultNone, nil
	}
}

// switchable faults every matching op while on.
type switchable struct {
	on    atomic.Bool
	fault memblob.FaultInjector
}

func (s *switchable) BeforeBlobOp(op memblob.Op, key string) (memblob.FaultKind, error) {
	if !s.on.Load() {
		return memblob.FaultNone, nil
	}
	return s.fault.BeforeBlobOp(op, key)
}

func TestReadBackFailureRetriesWithNewKey(t *testing.T) {
	t.Parallel()
	for _, kind := range []memblob.FaultKind{memblob.FaultWrongBytes, memblob.FaultDropPut} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			mb := memblob.New(memblob.WithFaultInjector(&memblob.KeyPrefixFault{
				Prefix: objectPrefix, Op: memblob.OpPut, Ordinal: 1, Kind: kind,
			}))
			h := newHarness(t, options{blob: mb})
			ref, err := h.up.Put(t.Context(), h.session, payload(0))
			require.NoError(t, err)
			require.EqualValues(t, 1, h.verifyFailures("upload"))
			require.Equal(t, catalog.ObjectAvailable, h.object(t, ref.ID).State)
			first := h.object(t, ref.ID-1)
			require.Equal(t, catalog.ObjectUploading, first.State, "the failed attempt's row is left for GC")
			require.NotEqual(t, first.Key, h.object(t, ref.ID).Key)
			got, err := h.rd.Get(t.Context(), ref.ID)
			require.NoError(t, err)
			require.Equal(t, payload(0), got)
		})
	}
}

func TestPersistentReadBackFailureEndsSession(t *testing.T) {
	t.Parallel()
	mb := memblob.New(memblob.WithFaultInjector(always(memblob.OpPut, memblob.FaultWrongBytes)))
	h := newHarness(t, options{blob: mb})
	_, err := h.up.Upload(t.Context(), h.session, [][]byte{payload(0), payload(1)})
	require.ErrorIs(t, err, catalog.ErrSessionEnded)
	require.ErrorIs(t, err, objstore.ErrCorrupt)
	_, corrupt := catalog.IsCorruption(err)
	require.False(t, corrupt, "a store that mangles fresh uploads ends the session; nothing durable is corrupt")
	require.EqualValues(t, 6, h.verifyFailures("upload"))
}

func TestUploadErrorEndsSession(t *testing.T) {
	t.Parallel()
	for _, op := range []memblob.Op{memblob.OpPut, memblob.OpGet} {
		t.Run(string(op), func(t *testing.T) {
			t.Parallel()
			mb := memblob.New(memblob.WithFaultInjector(&memblob.KeyPrefixFault{Prefix: objectPrefix, Op: op, Ordinal: 1}))
			h := newHarness(t, options{blob: mb})
			_, err := h.up.Put(t.Context(), h.session, payload(0))
			require.ErrorIs(t, err, catalog.ErrSessionEnded)
			require.ErrorIs(t, h.session.Err(), catalog.ErrSessionEnded)
			_, err = h.up.Put(t.Context(), h.session, payload(1))
			require.ErrorIs(t, err, catalog.ErrSessionEnded, "an ended session uploads nothing")
			require.Zero(t, h.verifyFailures("upload"))
		})
	}
}

func TestReadRecoversAfterRefresh(t *testing.T) {
	t.Parallel()
	for _, kind := range []memblob.FaultKind{memblob.FaultWrongBytes, memblob.FaultError} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			f := &switchable{}
			mb := memblob.New(memblob.WithFaultInjector(f))
			h := newHarness(t, options{blob: mb})
			id := h.put(t, payload(0))

			// One bad read, then good ones.
			var err error
			if kind == memblob.FaultError {
				err = objstore.ErrNotFound
			}
			f.fault = &memblob.KeyPrefixFault{Prefix: objectPrefix, Op: memblob.OpGet, Ordinal: 1, Kind: kind, Err: err}
			f.on.Store(true)
			got, err := h.rd.Get(t.Context(), id)
			require.NoError(t, err)
			require.Equal(t, payload(0), got)
			require.EqualValues(t, 1, h.verifyFailures("read"))
			require.Zero(t, h.corruption(catalog.SourceRead))
		})
	}
}

func TestReadCorruption(t *testing.T) {
	t.Parallel()
	cases := map[string]func(h *harness, f *switchable, key string){
		"wrong bytes": func(_ *harness, f *switchable, _ string) {
			f.fault = always(memblob.OpGet, memblob.FaultWrongBytes)
			f.on.Store(true)
		},
		"missing": func(h *harness, _ *switchable, key string) {
			require.NoError(t, h.blob.DeleteKey(context.Background(), key))
		},
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := &switchable{}
			mb := memblob.New(memblob.WithFaultInjector(f))
			h := newHarness(t, options{blob: mb})
			id := h.put(t, payload(0))
			breakIt(h, f, objstore.Key(archiveID, h.object(t, id).Key))

			_, err := h.rd.Get(t.Context(), id)
			src, ok := catalog.IsCorruption(err)
			require.True(t, ok, "%v", err)
			require.Equal(t, catalog.SourceRead, src)
			require.EqualValues(t, 1, h.corruption(catalog.SourceRead))
			require.EqualValues(t, 2, h.verifyFailures("read"))
		})
	}
}

// fakeRows serves object rows from a script, to stage catalog changes that
// land between the first read and the refresh.
type fakeRows struct {
	mu    sync.Mutex
	calls int
	rows  func(call int, refresh bool) (catalog.ObjectRow, bool)
}

func (f *fakeRows) Object(_ context.Context, _ uint64, refresh bool) (catalog.ObjectRow, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	row, ok := f.rows(f.calls, refresh)
	return row, ok, nil
}

func TestReadRowGoneAfterFailure(t *testing.T) {
	t.Parallel()
	mb := memblob.New()
	data := payload(0)
	key := [16]byte{9}
	require.NoError(t, mb.PutKey(t.Context(), objstore.Key(archiveID, key), data))
	row := catalog.ObjectRow{ID: 5, Key: key, SHA256: sha256.Sum256(data), Length: int64(len(data)), State: catalog.ObjectAvailable}

	cases := map[string]func(call int, refresh bool) (catalog.ObjectRow, bool){
		// A stale mirror named a key GC has since deleted.
		"row deleted": func(call int, refresh bool) (catalog.ObjectRow, bool) {
			r := row
			r.Key = [16]byte{0xde, 0xad}
			if refresh {
				return r, false
			}
			return r, true
		},
		// GC claimed the row between the refresh and the second read.
		"claimed during retry": func(call int, refresh bool) (catalog.ObjectRow, bool) {
			r := row
			r.Key = [16]byte{0xde, 0xad}
			if call >= 3 {
				r.State = catalog.ObjectDeleting
			}
			return r, true
		},
		"never available": func(int, bool) (catalog.ObjectRow, bool) {
			r := row
			r.State = catalog.ObjectUploading
			return r, true
		},
	}
	for name, rows := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, options{blob: mb, rows: &fakeRows{rows: rows}})
			_, err := h.rd.Get(t.Context(), 5)
			require.ErrorIs(t, err, objstore.ErrGone)
			_, corrupt := catalog.IsCorruption(err)
			require.False(t, corrupt)
			require.Zero(t, h.corruption(catalog.SourceRead))
		})
	}

	t.Run("identity changed", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, options{blob: mb, rows: &fakeRows{rows: func(call int, refresh bool) (catalog.ObjectRow, bool) {
			r := row
			if !refresh {
				r.SHA256[0] ^= 1
			}
			return r, true
		}}})
		_, err := h.rd.Get(t.Context(), 5)
		src, ok := catalog.IsCorruption(err)
		require.True(t, ok)
		require.Equal(t, catalog.SourceObject, src)
		require.EqualValues(t, 1, h.corruption(catalog.SourceObject))
	})
}

func TestReadTransientErrorIsNotCorruption(t *testing.T) {
	t.Parallel()
	f := &switchable{}
	h := newHarness(t, options{blob: memblob.New(memblob.WithFaultInjector(f))})
	id := h.put(t, payload(0))
	f.fault = always(memblob.OpGet, memblob.FaultError)
	f.on.Store(true)
	_, err := h.rd.Get(t.Context(), id)
	require.Error(t, err)
	require.NotErrorIs(t, err, objstore.ErrGone)
	_, corrupt := catalog.IsCorruption(err)
	require.False(t, corrupt)
	require.Zero(t, h.verifyFailures("read"))
}

func TestGetRange(t *testing.T) {
	t.Parallel()
	f := &switchable{}
	h := newHarness(t, options{blob: memblob.New(memblob.WithFaultInjector(f))})
	ctx := t.Context()
	data := payload(3)
	size := int64(len(data))
	id := h.put(t, data)

	got, err := h.rd.GetRange(ctx, id, 10, 20)
	require.NoError(t, err)
	require.Equal(t, data[10:30], got)
	got, err = h.rd.GetRange(ctx, id, size-5, 100)
	require.NoError(t, err)
	require.Equal(t, data[size-5:], got, "truncated at the end like S3")

	for _, r := range [][2]int64{{size, 1}, {-1, 5}, {0, 0}} {
		_, err = h.rd.GetRange(ctx, id, r[0], r[1])
		require.ErrorIs(t, err, objstore.ErrInvalidRange)
	}
	require.Zero(t, h.verifyFailures("read"), "a bad caller range is not a verify failure")

	// Ranges verify only the length: the zstd frame checksum covers bytes.
	f.fault = always(memblob.OpGetRange, memblob.FaultWrongBytes)
	f.on.Store(true)
	got, err = h.rd.GetRange(ctx, id, 0, 10)
	require.NoError(t, err)
	require.NotEqual(t, data[:10], got)
	f.on.Store(false)

	_, err = h.rd.GetRange(ctx, 999, 0, 1)
	require.ErrorIs(t, err, objstore.ErrGone)
}

// shortBlob returns ranges shorter than asked for, or a 416 as if the stored
// object were shorter than the catalog says.
type shortBlob struct {
	objstore.Blob
	bad     atomic.Int64 // bad reads left
	invalid bool
}

func (s *shortBlob) GetKeyRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	data, err := s.Blob.GetKeyRange(ctx, key, off, n)
	if err != nil || s.bad.Add(-1) < 0 {
		return data, err
	}
	if s.invalid {
		return nil, objstore.ErrInvalidRange
	}
	return data[:len(data)-1], nil
}

func TestGetRangeLengthFailure(t *testing.T) {
	t.Parallel()
	for _, invalid := range []bool{false, true} {
		for _, persistent := range []bool{false, true} {
			t.Run(fmt.Sprintf("invalid=%v/persistent=%v", invalid, persistent), func(t *testing.T) {
				t.Parallel()
				sb := &shortBlob{Blob: memblob.New(), invalid: invalid}
				h := newHarness(t, options{blob: sb})
				data := payload(4)
				id := h.put(t, data)
				if persistent {
					sb.bad.Store(1 << 50)
				} else {
					sb.bad.Store(1)
				}
				got, err := h.rd.GetRange(t.Context(), id, 5, 10)
				if !persistent {
					require.NoError(t, err)
					require.Equal(t, data[5:15], got)
					require.EqualValues(t, 1, h.verifyFailures("read"))
					return
				}
				src, ok := catalog.IsCorruption(err)
				require.True(t, ok, "%v", err)
				require.Equal(t, catalog.SourceRead, src)
				require.ErrorIs(t, err, objstore.ErrCorrupt)
			})
		}
	}
}

// countBlob counts reads that reach the store.
type countBlob struct {
	objstore.Blob
	gets atomic.Int64
}

func (c *countBlob) GetKey(ctx context.Context, key string) ([]byte, error) {
	c.gets.Add(1)
	return c.Blob.GetKey(ctx, key)
}

func (c *countBlob) GetKeyRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	c.gets.Add(1)
	return c.Blob.GetKeyRange(ctx, key, off, n)
}

func TestCache(t *testing.T) {
	t.Parallel()
	cb := &countBlob{Blob: memblob.New()}
	h := newHarness(t, options{blob: cb, cacheBytes: 1 << 20})
	ctx := t.Context()
	data := payload(5)
	id := h.put(t, data)
	base := cb.gets.Load() // the upload's read-back

	for range 3 {
		got, err := h.rd.Get(ctx, id)
		require.NoError(t, err)
		require.Equal(t, data, got)
	}
	got, err := h.rd.GetRange(ctx, id, 3, 4)
	require.NoError(t, err)
	require.Equal(t, data[3:7], got)
	require.EqualValues(t, 1, cb.gets.Load()-base, "one store read, the rest from cache")
	require.EqualValues(t, len(data), h.cache.Size())

	// A corrupt read is never cached.
	f := &switchable{fault: always(memblob.OpGet, memblob.FaultWrongBytes)}
	f.on.Store(true)
	h2 := newHarness(t, options{blob: memblob.New(memblob.WithFaultInjector(f)), cacheBytes: 1 << 20})
	f.on.Store(false)
	id2 := h2.put(t, data)
	f.on.Store(true)
	_, err = h2.rd.Get(ctx, id2)
	require.Error(t, err)
	require.Zero(t, h2.cache.Size())
}

func TestNewValidates(t *testing.T) {
	t.Parallel()
	_, err := protocol.NewUploader(protocol.UploaderConfig{Blob: memblob.New(), GCDelay: time.Hour})
	require.Error(t, err)
	_, err = protocol.NewUploader(protocol.UploaderConfig{OrphanAge: time.Hour, GCDelay: time.Hour})
	require.Error(t, err)
	_, err = protocol.NewReader(protocol.ReaderConfig{Blob: memblob.New()})
	require.Error(t, err)
}

// TestRealStore runs the protocol against the object store named by
// JETSTREAM_TEST_S3_*.
func TestRealStore(t *testing.T) {
	t.Parallel()
	blob := s3test.New(t, s3test.Env(t))
	h := newHarness(t, options{blob: blob})
	ctx := t.Context()
	objs := [][]byte{payload(0), payload(1), payload(0)}
	refs, err := h.up.Upload(ctx, h.session, objs)
	require.NoError(t, err)
	for i, r := range refs[:2] {
		id, err := h.session.MarkAvailable(ctx, r)
		require.NoError(t, err)
		got, err := h.rd.Get(ctx, id)
		require.NoError(t, err)
		require.Equal(t, objs[i], got)
		got, err = h.rd.GetRange(ctx, id, 7, 9)
		require.NoError(t, err)
		require.Equal(t, objs[i][7:16], got)
	}
	// Missing bytes behind an available row: 404 or 403 alike end as
	// corruption, never as a silent miss.
	require.NoError(t, blob.DeleteKey(ctx, objstore.Key(archiveID, h.object(t, refs[1].ID).Key)))
	_, err = h.rd.Get(ctx, refs[1].ID)
	src, ok := catalog.IsCorruption(err)
	require.True(t, ok, "%v", err)
	require.Equal(t, catalog.SourceRead, src)
	require.True(t, errors.Is(err, objstore.ErrNotFound))
}
