package pgstore_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/catalogtest"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/leader/lockertest"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgtest"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// The migration is the design's §8 schema, byte for byte, so the two can
// never drift.
func TestMigrationMatchesDesign(t *testing.T) {
	t.Parallel()
	doc, err := os.ReadFile("../../specs/notes/2026-09-25-disaggregated-storage-v2-design.md")
	require.NoError(t, err)
	_, sec, ok := strings.Cut(string(doc), "\n## 8. PostgreSQL schema\n")
	require.True(t, ok)
	_, sec, ok = strings.Cut(sec, "```sql\n")
	require.True(t, ok)
	want, _, ok := strings.Cut(sec, "```")
	require.True(t, ok)

	mig, err := os.ReadFile("migrations/0001_init.sql")
	require.NoError(t, err)
	var body []string
	for line := range strings.Lines(string(mig)) {
		if len(body) == 0 && (strings.HasPrefix(line, "--") || line == "\n") {
			continue
		}
		body = append(body, line)
	}
	require.Equal(t, want, strings.Join(body, ""))
}

func TestVersionsMatchStoragefake(t *testing.T) {
	t.Parallel()
	require.Equal(t, storagefake.SchemaVersion, pgstore.SchemaVersion)
	require.Equal(t, storagefake.FormatVersion, catalog.FormatVersion)
}

func TestParseConfigHidesURL(t *testing.T) {
	t.Parallel()
	for _, u := range []string{
		"postgres://user:s3cretpw@host:notaport/db",
		"postgres://user:s3cretpw@host/db?sslmode=bogus",
		"host=h password=s3cretpw port=x",
	} {
		_, err := pgstore.ParseConfig(u)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "s3cretpw")
	}
	cfg, err := pgstore.ParseConfig("postgres://u:p@h/db")
	require.NoError(t, err)
	require.Equal(t, "10000ms", cfg.ConnConfig.RuntimeParams["statement_timeout"])
	require.Equal(t, "5000ms", cfg.ConnConfig.RuntimeParams["lock_timeout"])
	require.Equal(t, "30000ms", cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"])
}

func TestMetaBatch(t *testing.T) {
	t.Parallel()
	set := func(k, v string) metastore.Op {
		return metastore.Op{Kind: metastore.OpSet, Key: []byte(k), Value: []byte(v)}
	}
	del := func(k string) metastore.Op { return metastore.Op{Kind: metastore.OpDelete, Key: []byte(k)} }
	b := pgstore.MetaBatch([]metastore.Op{
		set("a", "1"), set("b", "2"), set("a", "3"),
		del("a"), del("c"),
		{Kind: metastore.OpDeleteRange, Key: []byte("x"), End: []byte("y")},
		{Kind: metastore.OpDeleteRange, Key: []byte("z")},
		set("n", ""), {Kind: metastore.OpSet, Key: []byte("m")},
	})
	var kinds []string
	for _, q := range b.QueuedQueries {
		kinds = append(kinds, strings.Fields(q.SQL)[0])
	}
	require.Equal(t, []string{"INSERT", "DELETE", "DELETE", "DELETE", "INSERT"}, kinds)
	q := b.QueuedQueries
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, q[0].Arguments[0])
	require.Equal(t, [][]byte{[]byte("3"), []byte("2")}, q[0].Arguments[1], "last write per key wins")
	require.Equal(t, [][]byte{[]byte("a"), []byte("c")}, q[1].Arguments[0])
	require.Len(t, q[3].Arguments, 1, "an unbounded range has no end")
	require.Equal(t, [][]byte{{}, {}}, q[4].Arguments[1], "values are never NULL")
	require.Zero(t, pgstore.MetaBatch(nil).Len())
}

func TestContract(t *testing.T) {
	t.Parallel()
	pgtest.URL(t)
	catalogtest.Run(t, func(t *testing.T) catalogtest.Backend {
		s, _ := pgtest.Open(t, nil)
		return catalogtest.Backend{
			DB:        s,
			NewLocker: func() leader.Locker { return s.NewLease() },
			Listener:  s,
		}
	})
}

// The lease runs on PostgreSQL's clock, so the suite sleeps. The lease is
// long enough that a slow statement cannot eat the margins (a fifth of it).
func TestLockerContract(t *testing.T) {
	t.Parallel()
	pgtest.URL(t)
	lockertest.Run(t, func(t *testing.T) lockertest.Backend {
		s, _ := pgtest.Open(t, nil)
		return lockertest.Backend{
			NewLocker: func() leader.Locker { return s.NewLease() },
			Exclusive: true,
			Advance:   time.Sleep,
			Lease:     500 * time.Millisecond,
			Fence:     lockertest.CatalogFence(s),
		}
	})
}

func TestInitialize(t *testing.T) {
	t.Parallel()
	u := pgtest.NewDatabase(t)
	s := pgtest.OpenURL(t, u, nil)
	_, err := s.CheckVersions(t.Context())
	require.ErrorIs(t, err, pgstore.ErrNotInitialized)

	id := [16]byte{1, 2, 3}
	require.NoError(t, s.Initialize(t.Context(), id))
	require.ErrorIs(t, s.Initialize(t.Context(), [16]byte{9}), pgstore.ErrInitialized)
	a, err := s.CheckVersions(t.Context())
	require.NoError(t, err)
	require.Equal(t, id, a.ArchiveID)
	require.Equal(t, pgstore.SchemaVersion, a.SchemaVersion)
	require.Equal(t, catalog.FormatVersion, a.FormatVersion)
	require.Zero(t, a.WriterEpoch)
	require.Zero(t, a.CatalogRevision)

	_, err = s.Pool().Exec(t.Context(), "UPDATE archive SET schema_version = 2")
	require.NoError(t, err)
	_, err = s.CheckVersions(t.Context())
	require.ErrorIs(t, err, pgstore.ErrVersionMismatch)
	_, err = s.Pool().Exec(t.Context(), "UPDATE archive SET schema_version = $1, format_version = 7", pgstore.SchemaVersion)
	require.NoError(t, err)
	_, err = s.CheckVersions(t.Context())
	require.ErrorIs(t, err, pgstore.ErrVersionMismatch)
}

// A failed Initialize leaves the database empty: migrations and the archive
// row are one transaction.
func TestInitializeAtomic(t *testing.T) {
	t.Parallel()
	u := pgtest.NewDatabase(t)
	s := pgtest.OpenURL(t, u, nil)
	_, err := s.Pool().Exec(t.Context(), "CREATE TABLE hot_batches (x int)")
	require.NoError(t, err)
	require.Error(t, s.Initialize(t.Context(), [16]byte{1}))
	_, err = s.CheckVersions(t.Context())
	require.ErrorIs(t, err, pgstore.ErrNotInitialized)
}

func setMeta(t *testing.T, db catalog.DB, epoch uint64, k, v string) catalog.Tx {
	t.Helper()
	tx, err := db.Begin(t.Context(), catalog.TxMetadata)
	require.NoError(t, err)
	_, ok, err := tx.FenceBump(t.Context(), epoch)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, tx.ApplyMeta(t.Context(), []metastore.Op{{Kind: metastore.OpSet, Key: []byte(k), Value: []byte(v)}}))
	return tx
}

func getMeta(t *testing.T, db catalog.DB, k string) (string, bool) {
	t.Helper()
	r, err := db.BeginRead(t.Context())
	require.NoError(t, err)
	defer func() { _ = r.Close(t.Context()) }()
	got, err := r.MetaGet(t.Context(), [][]byte{[]byte(k)})
	require.NoError(t, err)
	v, ok := got[k]
	return string(v), ok
}

// "Commit applied, result unknown": the error ends the session even though
// the data is durable, and the next session sees it.
func TestCommitResultUnknown(t *testing.T) {
	t.Parallel()
	direct, u := pgtest.Open(t, nil)
	proxy, pu := pgtest.NewProxy(t, u)
	reg := prometheus.NewRegistry()
	m := pgstore.NewMetrics(reg)
	s := pgtest.OpenURL(t, pu, m)

	tx := setMeta(t, s, 0, "k", "v")
	proxy.DropCommitResponse()
	err := tx.Commit(t.Context())
	require.ErrorIs(t, err, catalog.ErrSessionEnded)
	require.ErrorIs(t, err, leader.ErrRestartSession)
	require.NoError(t, tx.Rollback(t.Context()))
	v, ok := getMeta(t, direct, "k")
	require.True(t, ok, "the commit applied")
	require.Equal(t, "v", v)
	require.InDelta(t, 1, testutil.ToFloat64(m.TxnErrors.WithLabelValues(string(catalog.TxMetadata))), 0)

	// The pool replaces the dead connection.
	require.NoError(t, setMeta(t, s, 0, "k", "w").Commit(t.Context()))
	v, _ = getMeta(t, direct, "k")
	require.Equal(t, "w", v)
}

// A connection killed mid-transaction rolls back, and the server releases
// the archive row lock at once, so the next leader transaction proceeds.
func TestConnKilledMidTransaction(t *testing.T) {
	t.Parallel()
	direct, u := pgtest.Open(t, nil)
	proxy, pu := pgtest.NewProxy(t, u)
	s := pgtest.OpenURL(t, pu, nil)

	tx := setMeta(t, s, 0, "k", "v")
	proxy.KillAll()
	_, _, err := tx.MetaGetForUpdate(t.Context(), []byte("k"))
	require.ErrorIs(t, err, catalog.ErrSessionEnded)
	require.Error(t, tx.Commit(t.Context()))
	require.NoError(t, tx.Rollback(t.Context()))

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	tx2, err := direct.Begin(ctx, catalog.TxMetadata)
	require.NoError(t, err)
	_, ok, err := tx2.FenceBump(ctx, 0)
	require.NoError(t, err, "the killed transaction's lock is gone")
	require.True(t, ok)
	require.NoError(t, tx2.Commit(ctx))
	_, found := getMeta(t, direct, "k")
	require.False(t, found)
}

// A fence waiting on the row lock gives up when its context ends, and the
// abandoned transaction's connection is discarded.
func TestFenceWaitHonorsContext(t *testing.T) {
	t.Parallel()
	s, _ := pgtest.Open(t, nil)
	tx := setMeta(t, s, 0, "k", "v")
	defer func() { _ = tx.Rollback(context.Background()) }()
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	tx2, err := s.Begin(ctx, catalog.TxMetadata)
	require.NoError(t, err)
	_, _, err = tx2.FenceBump(ctx, 0)
	require.ErrorIs(t, err, catalog.ErrSessionEnded)
	require.NoError(t, tx2.Rollback(context.Background()))
}

func TestSessionSettings(t *testing.T) {
	t.Parallel()
	s, _ := pgtest.Open(t, nil)
	for param, want := range map[string]string{
		"statement_timeout":                   "10s",
		"lock_timeout":                        "5s",
		"idle_in_transaction_session_timeout": "30s",
		"application_name":                    "jetstream",
	} {
		var got string
		require.NoError(t, s.Pool().QueryRow(t.Context(), "SHOW "+param).Scan(&got))
		require.Equal(t, want, got, param)
	}
}

// The listener survives its connection dying.
func TestListenReconnects(t *testing.T) {
	t.Parallel()
	direct, u := pgtest.Open(t, nil)
	proxy, pu := pgtest.NewProxy(t, u)
	reg := prometheus.NewRegistry()
	m := pgstore.NewMetrics(reg)
	s := pgtest.OpenURL(t, pu, m)

	ctx, cancel := context.WithCancel(t.Context())
	ch, err := s.Listen(ctx)
	require.NoError(t, err)
	proxy.KillAll()
	require.Eventually(t, func() bool { return testutil.ToFloat64(m.ListenReconnects) == 1 }, 5*time.Second, 10*time.Millisecond)

	notify := func(rev uint64) {
		tx, err := direct.Begin(t.Context(), catalog.TxMetadata)
		require.NoError(t, err)
		require.NoError(t, tx.Notify(t.Context(), rev))
		require.NoError(t, tx.Commit(t.Context()))
	}
	notify(42)
	select {
	case got := <-ch:
		require.Equal(t, uint64(42), got)
	case <-time.After(5 * time.Second):
		t.Fatal("no notification after reconnect")
	}
	cancel()
	for range ch {
	}
}

func TestTxnMetrics(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := pgstore.NewMetrics(reg)
	_, u := pgtest.Open(t, nil)
	s := pgtest.OpenURL(t, u, m)
	require.NoError(t, setMeta(t, s, 0, "k", "v").Commit(t.Context()))
	getMeta(t, s, "k")
	tx := setMeta(t, s, 0, "k", "v")
	require.Error(t, tx.InsertHotBatch(t.Context(), catalog.HotBatchRow{FirstSeq: 1, LastSeq: 1, EventCount: 1}))
	require.NoError(t, tx.Rollback(t.Context()))

	require.Equal(t, 2, testutil.CollectAndCount(m.TxnDuration))
	require.InDelta(t, 1, testutil.ToFloat64(m.TxnErrors.WithLabelValues(string(catalog.TxMetadata))), 0)
	require.InDelta(t, 0, testutil.ToFloat64(m.TxnErrors.WithLabelValues("read")), 0)
}
