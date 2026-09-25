package pg_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/metastore"
	metapg "github.com/bluesky-social/jetstream/internal/metastore/pg"
	"github.com/bluesky-social/jetstream/internal/metastore/storetest"
	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgtest"
	"github.com/stretchr/testify/require"
)

// leaderStore returns a writable store whose commits run in a fenced
// session, as a leader pod's does.
func leaderStore(t *testing.T, db *pgstore.Store, pageSize int) (*metapg.Store, *catalog.Session) {
	t.Helper()
	l := db.NewLease()
	require.NoError(t, l.Acquire(t.Context(), time.Hour))
	s := catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: l.Epoch()})
	return metapg.New(metapg.Config{
		DB: db,
		Commit: func(ctx context.Context, ops []metastore.Op) error {
			_, err := s.CommitMeta(ctx, ops)
			return err
		},
		PageSize: pageSize,
	}), s
}

func TestContract(t *testing.T) {
	t.Parallel()
	pgtest.URL(t)
	storetest.Run(t, func(t *testing.T) metastore.Store {
		db, _ := pgtest.Open(t, nil)
		// A tiny page makes every multi-key iteration cross pages.
		s, _ := leaderStore(t, db, 2)
		return s
	})
}

func TestIterPages(t *testing.T) {
	t.Parallel()
	pgtest.URL(t)
	db, _ := pgtest.Open(t, nil)
	ctx := t.Context()
	s, _ := leaderStore(t, db, 7)

	b := s.NewBatch()
	var want []storetest.KV
	for i := range 50 {
		k := fmt.Appendf(nil, "k/%03d", i)
		b.Set(k, []byte{byte(i)})
		want = append(want, storetest.KV{Key: k, Value: []byte{byte(i)}})
	}
	// Keys that differ only past a prefix, and a trailing zero byte, land
	// on page boundaries too.
	for _, k := range []string{"k/010\x00", "k/010\x00\x00", "k/013\xff"} {
		b.Set([]byte(k), []byte("x"))
	}
	require.NoError(t, b.Commit(ctx))

	got := storetest.Scan(t, s, []byte("k/"), []byte("k0"))
	require.Len(t, got, 53)
	for i := 1; i < len(got); i++ {
		require.Less(t, string(got[i-1].Key), string(got[i].Key))
	}
	for _, kv := range want {
		require.Contains(t, got, kv)
	}

	// Exactly one page, then an empty one.
	got = storetest.Scan(t, s, []byte("k/000"), []byte("k/007"))
	require.Len(t, got, 7)
	require.Empty(t, storetest.Scan(t, s, []byte("k0"), nil))
}

func TestReadOnly(t *testing.T) {
	t.Parallel()
	pgtest.URL(t)
	db, u := pgtest.Open(t, nil)
	ctx := t.Context()
	w, _ := leaderStore(t, db, 0)
	require.NoError(t, w.Set(ctx, []byte("k"), []byte("v")))

	// A reader pod connects as the read-only role and has no commit path.
	rdb := pgtest.OpenURL(t, pgtest.ReaderURL(t, u), nil)
	ro := metapg.New(metapg.Config{DB: rdb})
	v, err := ro.Get(ctx, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, "v", string(v))
	require.Equal(t, []storetest.KV{{Key: []byte("k"), Value: []byte("v")}}, storetest.Scan(t, ro, nil, nil))
	require.ErrorIs(t, ro.Set(ctx, []byte("k"), []byte("x")), metastore.ErrReadOnly)
	require.ErrorIs(t, ro.Delete(ctx, []byte("k")), metastore.ErrReadOnly)
	b := ro.NewBatch()
	b.DeleteRange([]byte("a"), []byte("z"))
	require.ErrorIs(t, b.Commit(ctx), metastore.ErrReadOnly)
	require.ErrorIs(t, ro.NewBatch().Commit(ctx), metastore.ErrReadOnly, "a reader pod never commits, even nothing")
}

// A pod fenced out by a new leader cannot write metadata, and its session
// stays ended.
func TestFencedOut(t *testing.T) {
	t.Parallel()
	pgtest.URL(t)
	db, _ := pgtest.Open(t, nil)
	ctx := t.Context()
	old, _ := leaderStore(t, db, 0)
	require.NoError(t, old.Set(ctx, []byte("k"), []byte("old")))

	// Take over: expire the old lease without waiting for it.
	_, err := db.Pool().Exec(ctx, `UPDATE archive SET lease_expires_at = now()`)
	require.NoError(t, err)
	cur, _ := leaderStore(t, db, 0)
	require.NoError(t, cur.Set(ctx, []byte("k"), []byte("new")))

	require.ErrorIs(t, old.Set(ctx, []byte("k"), []byte("stale")), catalog.ErrFenced)
	require.ErrorIs(t, old.Delete(ctx, []byte("k")), catalog.ErrSessionEnded)
	v, err := cur.Get(ctx, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, "new", string(v))
}

// A backfill-sized batch is a handful of set-based statements, and the
// default page splits the scan.
func TestLargeBatch(t *testing.T) {
	t.Parallel()
	pgtest.URL(t)
	db, _ := pgtest.Open(t, nil)
	ctx := t.Context()
	s, _ := leaderStore(t, db, 0)

	const n = 25_000
	b := s.NewBatch()
	for i := range n {
		b.Set(fmt.Appendf(nil, "r/%06d", i), fmt.Appendf(nil, "v%d", i))
	}
	b.DeleteRange([]byte("r/010000"), []byte("r/011000"))
	for i := range 500 {
		b.Delete(fmt.Appendf(nil, "r/%06d", 2*i))
	}
	require.NoError(t, b.Commit(ctx))

	it, err := s.NewIter(ctx, nil, nil)
	require.NoError(t, err)
	count := 0
	for it.Next() {
		count++
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	require.Equal(t, n-1000-500, count)
}
