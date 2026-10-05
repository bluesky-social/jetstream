package pgstore_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
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
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

func TestMetaBatchSplitsLongRuns(t *testing.T) {
	t.Parallel()
	const per = 10_000 // metaBatchStatementRows
	key := func(i int) []byte { return fmt.Appendf(nil, "k%06d", i) }
	rowCounts := func(b *pgx.Batch) (kinds []string, rows []int) {
		for _, q := range b.QueuedQueries {
			keys, ok := q.Arguments[0].([][]byte)
			require.True(t, ok, "keys are the first argument")
			kinds = append(kinds, strings.Fields(q.SQL)[0])
			rows = append(rows, len(keys))
		}
		return kinds, rows
	}

	for _, n := range []int{per - 1, per, per + 1, 2*per + 1} {
		var sets, dels []metastore.Op
		for i := range n {
			sets = append(sets, metastore.Op{Kind: metastore.OpSet, Key: key(i), Value: []byte("v")})
			dels = append(dels, metastore.Op{Kind: metastore.OpDelete, Key: key(i)})
		}
		want := []int{}
		for left := n; left > 0; left -= per {
			want = append(want, min(left, per))
		}
		_, rows := rowCounts(pgstore.MetaBatch(sets))
		require.Equal(t, want, rows, "sets, n=%d", n)
		_, rows = rowCounts(pgstore.MetaBatch(dels))
		require.Equal(t, want, rows, "deletes, n=%d", n)
	}

	// A key rewritten after the split point lands in the later statement,
	// so the last write still applies last.
	var ops []metastore.Op
	for i := range per {
		ops = append(ops, metastore.Op{Kind: metastore.OpSet, Key: key(i), Value: []byte("old")})
	}
	ops = append(ops,
		metastore.Op{Kind: metastore.OpSet, Key: key(per), Value: []byte("v")},
		metastore.Op{Kind: metastore.OpSet, Key: key(0), Value: []byte("new")},
		metastore.Op{Kind: metastore.OpDelete, Key: key(1)},
	)
	b := pgstore.MetaBatch(ops)
	kinds, rows := rowCounts(b)
	require.Equal(t, []string{"INSERT", "INSERT", "DELETE"}, kinds)
	require.Equal(t, []int{per, 2, 1}, rows)
	require.Equal(t, [][]byte{key(per), key(0)}, b.QueuedQueries[1].Arguments[0])
	require.Equal(t, [][]byte{[]byte("v"), []byte("new")}, b.QueuedQueries[1].Arguments[1])
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

// TestContractSplitReads runs the contract with every read split into
// statements of one or two IDs: the rows must come back as one statement
// returns them, in ID order with duplicates folded.
func TestContractSplitReads(t *testing.T) {
	t.Parallel()
	pgtest.URL(t)
	for _, n := range []int{1, 2} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			t.Parallel()
			catalogtest.Run(t, func(t *testing.T) catalogtest.Backend {
				s, _ := pgtest.Open(t, nil)
				pgstore.SetReadLimits(s, n, n)
				return catalogtest.Backend{
					DB:        s,
					NewLocker: func() leader.Locker { return s.NewLease() },
					Listener:  s,
				}
			})
		})
	}
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
	err := tx.Read(t.Context(), &catalog.MetaRead{Key: []byte("k")})
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
	require.NoError(t, tx.InsertHotBatch(t.Context(), catalog.HotBatchRow{FirstSeq: 1, LastSeq: 1, EventCount: 1}), "queued")
	require.Error(t, tx.Commit(t.Context()))
	require.NoError(t, tx.Rollback(t.Context()))

	require.Equal(t, 2, testutil.CollectAndCount(m.TxnDuration))
	require.InDelta(t, 1, testutil.ToFloat64(m.TxnErrors.WithLabelValues(string(catalog.TxMetadata))), 0)
	require.InDelta(t, 0, testutil.ToFloat64(m.TxnErrors.WithLabelValues("read")), 0)
}

// The pool keeps a warm floor of connections and exports its statistics,
// so a saturated pool shows on /metrics instead of only in a goroutine dump.
func TestPoolTuningAndMetrics(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := pgstore.NewMetrics(reg)
	// Nothing is exported before a pool is opened.
	require.Zero(t, testutil.CollectAndCount(reg, "jetstream_pg_pool_conns"))

	_, u := pgtest.Open(t, nil)
	s := pgtest.OpenURL(t, u, m)
	cfg := s.Pool().Config()
	require.Equal(t, int32(8), cfg.MaxConns)
	require.Equal(t, int32(1), cfg.MinIdleConns)
	require.Equal(t, int32(1), cfg.MinConns)
	require.Positive(t, cfg.MaxConnLifetimeJitter)

	getMeta(t, s, "k")
	require.Equal(t, 4, testutil.CollectAndCount(reg, "jetstream_pg_pool_conns"))
	gauge, err := reg.Gather()
	require.NoError(t, err)
	var maxConns, acquires float64
	for _, mf := range gauge {
		for _, metric := range mf.GetMetric() {
			for _, l := range metric.GetLabel() {
				switch {
				case mf.GetName() == "jetstream_pg_pool_conns" && l.GetValue() == "max":
					maxConns = metric.GetGauge().GetValue()
				case mf.GetName() == "jetstream_pg_pool_acquires_total" && l.GetValue() != "canceled":
					acquires += metric.GetCounter().GetValue()
				}
			}
		}
	}
	require.InDelta(t, 8, maxConns, 0)
	require.Positive(t, acquires)

	// The default pool is sized for a leader's concurrent readers.
	require.Equal(t, 32, pgstore.DefaultMaxConns)
}

// A follower connects as the read-only role (design §24). It can LISTEN,
// check versions, and load the whole catalog in one read snapshot, and it
// cannot write anything: reader pods need no write privilege.
func TestReaderRole(t *testing.T) {
	t.Parallel()
	leaderDB, u := pgtest.Open(t, nil)
	reader := pgtest.OpenURL(t, pgtest.ReaderURL(t, u), nil)
	ctx := t.Context()

	notes, err := reader.Listen(ctx)
	require.NoError(t, err)

	lock := leaderDB.NewLease()
	require.NoError(t, lock.Acquire(ctx, time.Minute))
	s := catalog.NewSession(catalog.SessionConfig{DB: leaderDB, Epoch: lock.Epoch()})
	_, err = s.InitNamespace(ctx, catalog.Main, nil)
	require.NoError(t, err)
	c, err := s.CommitHotBatch(ctx, catalog.HotBatch{
		FirstSeq: 1, LastSeq: 2, Frame: []byte("frame"),
		Meta: []metastore.Op{{Kind: metastore.OpSet, Key: []byte(catalog.RelayCursorKey), Value: []byte("7")}},
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		select {
		case rev := <-notes:
			return rev == c.Revision
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond, "the reader hears the commit")

	a, err := reader.CheckVersions(ctx)
	require.NoError(t, err)
	require.Equal(t, c.Revision, a.CatalogRevision)
	r, err := reader.BeginRead(ctx)
	require.NoError(t, err)
	snap, err := catalog.LoadSnapshot(ctx, r)
	require.NoError(t, err)
	require.NoError(t, catalog.CheckInvariants(snap, catalog.InvariantOptions{}))
	require.Equal(t, "7", string(snap.Meta[catalog.RelayCursorKey]))
	hot, err := r.HotBatches(ctx, 0)
	require.NoError(t, err)
	require.Len(t, hot, 1)
	require.Equal(t, []byte("frame"), hot[0].Frame)
	require.NoError(t, r.Close(ctx))

	// No write path works: not the leader transaction, not the lease, and
	// not raw SQL against any table.
	tx, err := reader.Begin(ctx, catalog.TxMetadata)
	if err == nil {
		_, _, err = tx.FenceBump(ctx, lock.Epoch())
		require.NoError(t, tx.Rollback(ctx))
	}
	require.Error(t, err, "the fence needs UPDATE on archive")
	require.Error(t, reader.NewLease().Acquire(ctx, time.Minute))
	for _, stmt := range []string{
		`UPDATE archive SET catalog_revision = catalog_revision + 1`,
		`INSERT INTO metadata_kv (key, value) VALUES ('\x00', '\x00')`,
		`DELETE FROM hot_batches`,
		`DELETE FROM objects`,
		`TRUNCATE segments`,
	} {
		_, err := reader.Pool().Exec(ctx, stmt)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr, stmt)
		require.Equal(t, "42501", pgErr.Code, "insufficient_privilege: %s", stmt)
	}
}

// The leader transaction pipelines its statements (tx.go): BEGIN and the
// script's reads ride with the fence, and writes ride with the next
// statement or COMMIT. These are the round trips the scripts make once the
// connection has the statements prepared. The archive row lock is held for
// all but the first, so each extra one is time every other leader
// transaction waits.
func TestPipelinedRoundTrips(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	_, u := pgtest.Open(t, nil)
	proxy, pu := pgtest.NewProxy(t, u)
	// One connection, so the warm-up prepares every statement on the
	// connection the measured transactions use.
	s, err := pgstore.Open(ctx, pgstore.Config{URL: pu, MaxConns: 1})
	require.NoError(t, err)
	t.Cleanup(s.Close)
	l := s.NewLease()
	require.NoError(t, l.Acquire(ctx, time.Minute))
	sess := catalog.NewSession(catalog.SessionConfig{DB: s, Epoch: l.Epoch()})
	_, err = sess.InitNamespace(ctx, catalog.Main, nil)
	require.NoError(t, err)

	roundTrips := func(f func()) int64 {
		before := proxy.RoundTrips()
		f()
		return proxy.RoundTrips() - before
	}
	n := 0
	request := func() catalog.UploadRequest {
		n++
		data := fmt.Appendf(nil, "payload %d", n)
		req := catalog.UploadRequest{SHA256: sha256.Sum256(data), Length: int64(len(data))}
		_, err := rand.Read(req.Key[:])
		require.NoError(t, err)
		return req
	}
	var (
		pending   catalog.ObjectRef
		available [32]byte // the bytes of an object a block references
	)
	beginUploads := func() {
		req := request()
		slots, _, err := sess.BeginUploads(ctx, []catalog.UploadRequest{req}, time.Hour)
		require.NoError(t, err)
		require.False(t, slots[0].Dedup)
		pending = catalog.ObjectRef{ID: slots[0].ObjectID, SHA256: req.SHA256, Pending: true}
	}
	dedup := func() {
		req := request()
		req.SHA256 = available
		slots, _, err := sess.BeginUploads(ctx, []catalog.UploadRequest{req}, time.Hour)
		require.NoError(t, err)
		require.True(t, slots[0].Dedup)
	}
	next := uint64(1)
	commitBlock := func() {
		_, err := sess.CommitBlock(ctx, catalog.Block{
			Namespace: catalog.Main,
			Info:      segment.BlockInfo{MinSeq: next, MaxSeq: next + 9, EventCount: 10, CompressedSize: 10, UncompressedSize: 100},
			Object:    pending,
			Meta:      []metastore.Op{{Kind: metastore.OpSet, Key: []byte("block"), Value: fmt.Appendf(nil, "block at %d", next)}},
		})
		require.NoError(t, err)
		available = pending.SHA256
		next += 10
	}
	commitHot := func() {
		_, err := sess.CommitHotBatches(ctx, []catalog.HotBatch{
			{FirstSeq: next, LastSeq: next + 4, Frame: []byte("inline")},
			{FirstSeq: next + 5, LastSeq: next + 9, Object: pending},
		})
		require.NoError(t, err)
		next += 10
	}
	fold := func() {
		_, err := sess.Fold(ctx, catalog.Block{
			Namespace: catalog.Main,
			Info:      segment.BlockInfo{MinSeq: next - 10, MaxSeq: next - 1, EventCount: 10, CompressedSize: 10, UncompressedSize: 100},
			Object:    pending,
		})
		require.NoError(t, err)
	}
	markAvailable := func() {
		_, err := sess.MarkAvailable(ctx, pending)
		require.NoError(t, err)
	}
	commitMeta := func() {
		_, err := sess.CommitMeta(ctx, []metastore.Op{
			{Kind: metastore.OpSet, Key: []byte("a"), Value: []byte("1")},
			{Kind: metastore.OpDelete, Key: []byte("b")},
		})
		require.NoError(t, err)
	}
	initNamespace := func() {
		_, err := sess.InitNamespace(ctx, catalog.Main, nil)
		require.NoError(t, err)
	}

	// Warm up: prepare every statement the measured transactions send.
	beginUploads()
	commitBlock()
	dedup()
	beginUploads()
	commitHot()
	beginUploads()
	fold()
	beginUploads()
	markAvailable()
	commitMeta()
	initNamespace()

	for _, tc := range []struct {
		name  string
		setup func()
		run   func()
		want  int64
	}{
		// BEGIN+fence+lookup, then insert+NOTIFY+COMMIT.
		{"BeginUploads", nil, beginUploads, 2},
		// BEGIN+fence+lookup, then NOTIFY+COMMIT.
		{"BeginUploads dedup", nil, dedup, 2},
		// BEGIN+fence+seq+segment+last block+object rows, then
		// object updates+insert+meta+NOTIFY+COMMIT.
		{"CommitBlock", beginUploads, commitBlock, 2},
		// BEGIN+fence+seq+object rows, then the rest.
		{"CommitHotBatches", beginUploads, commitHot, 2},
		// BEGIN+fence+reads, the hot batch delete, then the rest.
		{"Fold", beginUploads, fold, 3},
		{"MarkAvailable", beginUploads, markAvailable, 2},
		// BEGIN+fence, then the meta statements+NOTIFY+COMMIT.
		{"CommitMeta", nil, commitMeta, 2},
		{"InitNamespace", nil, initNamespace, 2},
	} {
		if tc.setup != nil {
			tc.setup()
		}
		require.Equal(t, tc.want, roundTrips(tc.run), tc.name)
	}

	got, ok := getMeta(t, s, "block")
	require.True(t, ok)
	require.Equal(t, "block at 21", got, "the pipelined commits applied")
}

// A queued write's failure surfaces from the next round trip, named for the
// statement that failed rather than the one that carried it.
func TestQueuedFailureNamesItsStatement(t *testing.T) {
	t.Parallel()
	s, _ := pgtest.Open(t, nil)
	tx := setMeta(t, s, 0, "k", "v")
	require.NoError(t, tx.InsertHotBatch(t.Context(), catalog.HotBatchRow{FirstSeq: 1, LastSeq: 1, EventCount: 1}))
	err := tx.Read(t.Context(), &catalog.MetaRead{Key: []byte("k")})
	require.ErrorIs(t, err, catalog.ErrSessionEnded)
	require.ErrorContains(t, err, "metadata/insert_hot_batch (sent with meta_get_for_update)")
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr, "the server's error is still reachable")
	require.Error(t, tx.Commit(t.Context()))
	require.NoError(t, tx.Rollback(t.Context()))
	_, found := getMeta(t, s, "k")
	require.False(t, found)
}
