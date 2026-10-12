package migrate

import (
	"context"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestClassify(t *testing.T) {
	t.Parallel()
	for key, want := range map[string]keyClass{
		"repo/did:plc:a":                  classCopy,
		"handle/alice.test":               classCopy,
		"pdshost/did:plc:a":               classCopy,
		"host/pds.test":                   classCopy,
		"backfill/timing/x":               classCopy,
		"backfill/counts":                 classCopy,
		"sync/chain/did:plc:a":            classCopy,
		"sync/host/pds.test":              classCopy,
		"sync/ident/did:plc:a":            classCopy,
		"sync/acct/did:plc:a":             classCopy,
		"compaction/seq":                  classCopy,
		"phase/entered_at":                classCopy,
		"relay/list_repos_cursor":         classCopy,
		"bootstrap/last_listrepos_cursor": classCopy,
		lifecycle.PhaseKey:                classPhase,
		catalog.RelayCursorKey:            classHandoff,
		"sync/identity/did:plc:a":         classDrop,
		"seq/gap/00000001":                classDrop,
		catalog.MainSeqKey:                classDrop,
		"seq/max_reserved":                classDrop,
		"merge/x":                         classDrop,
		"live_segments/x":                 classDrop,
		"import/x":                        classDrop,
		HandoffGuardKey:                   classDrop,
		CompactionPausedKey:               classDrop,
		"bogus/x":                         classUnknown,
		"sync/other":                      classUnknown,
		"phase2":                          classUnknown,
		"":                                classUnknown,
	} {
		require.Equalf(t, want, classify([]byte(key)), "classify(%q)", key)
	}
	require.True(t, classCopy.kept(false))
	require.False(t, classPhase.kept(false))
	require.True(t, classPhase.kept(true))
	require.False(t, classHandoff.kept(true))
	require.False(t, classDrop.kept(true))
}

func TestDirtySet(t *testing.T) {
	t.Parallel()
	d := newDirtySet(3, nil)
	d.observe([][]byte{[]byte("repo/a"), []byte(catalog.RelayCursorKey), []byte("sync/identity/a"), []byte("repo/a")}, nil)
	keys, ranges, overflow := d.take()
	require.Equal(t, [][]byte{[]byte("repo/a")}, keys, "drop and handoff keys are not kept")
	require.Empty(t, ranges)
	require.False(t, overflow)

	d.observe([][]byte{[]byte("repo/a"), []byte("repo/b")}, [][2][]byte{{[]byte("repo/"), []byte("repo0")}})
	require.Equal(t, 3, d.len())
	d.observe([][]byte{[]byte("repo/c")}, nil)
	require.Zero(t, d.len(), "past max the set is dropped")
	d.observe([][]byte{[]byte("repo/d")}, nil)
	require.Zero(t, d.len(), "an overflowed set takes nothing more")
	keys, _, overflow = d.take()
	require.Empty(t, keys)
	require.True(t, overflow)
	_, _, overflow = d.take()
	require.False(t, overflow, "take resets the overflow")
}

// metaRig is a source Pebble store observed by a dirty set, and a catalog
// initialized for a migration.
type metaRig struct {
	t      *testing.T
	ctx    context.Context
	db     *storagefake.DB
	sess   *catalog.Session
	local  *pebblestore.Store
	remote metastore.Store
	ms     *metaSync
}

func newMetaRig(t *testing.T, batch, maxDirty int) *metaRig {
	t.Helper()
	ctx := t.Context()
	db := storagefake.New(storagefake.Config{})
	lease := db.NewLease()
	require.NoError(t, lease.Acquire(ctx, time.Hour))
	sess := catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: lease.Epoch()})
	_, err := sess.InitMigration(ctx)
	require.NoError(t, err)
	local, err := pebblestore.Open("/data", nil, pebblestore.WithFS(vfs.NewMem()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	dirty := newDirtySet(maxDirty, nil)
	local.SetCommitObserver(dirty.observe)
	return &metaRig{
		t: t, ctx: ctx, db: db, sess: sess, local: local, remote: db.MetaStore(nil),
		ms: &metaSync{local: local, remote: db.MetaStore(nil), dirty: dirty, batch: batch},
	}
}

func (r *metaRig) set(kv map[string]string) {
	r.t.Helper()
	b := r.local.NewBatch()
	for k, v := range kv {
		b.Set([]byte(k), []byte(v))
	}
	require.NoError(r.t, b.Commit(r.ctx))
}

// remoteKeys returns the catalog's metadata outside the keys the catalog
// owns.
func (r *metaRig) remoteKeys() map[string]string {
	r.t.Helper()
	it, err := r.remote.NewIter(r.ctx, nil, nil)
	require.NoError(r.t, err)
	defer func() { _ = it.Close() }()
	out := map[string]string{}
	for it.Next() {
		switch k := string(it.Key()); k {
		case catalog.MainSeqKey, catalog.MigrationStateKey:
		default:
			out[k] = string(it.Value())
		}
	}
	require.NoError(r.t, it.Err())
	return out
}

func (r *metaRig) requireInSync(tailing bool) {
	r.t.Helper()
	diffs, err := r.ms.resync(r.ctx, r.sess, tailing, true)
	require.NoError(r.t, err)
	require.Zero(r.t, diffs, "the catalog matches the source")
}

func TestMetaSync_ResyncAndFlush(t *testing.T) {
	t.Parallel()
	r := newMetaRig(t, 2, 1000)
	r.set(map[string]string{
		"repo/a": "1", "repo/b": "2", "handle/x": "a", "sync/chain/a": "c",
		lifecycle.PhaseKey: string(lifecycle.PhaseSteadyState), catalog.RelayCursorKey: "99",
		"sync/identity/a": "cached", "seq/max_reserved": "z", catalog.MainSeqKey: "local",
	})
	// A stale key from an earlier attempt, and catalog-owned keys the
	// resync must leave alone.
	_, err := r.sess.ImportMeta(r.ctx, []metastore.Op{{Kind: metastore.OpSet, Key: []byte("repo/stale"), Value: []byte("x")}})
	require.NoError(t, err)

	diffs, err := r.ms.resync(r.ctx, r.sess, false, false)
	require.NoError(t, err)
	require.Equal(t, 5, diffs, "4 copies and 1 delete")
	require.Equal(t, map[string]string{"repo/a": "1", "repo/b": "2", "handle/x": "a", "sync/chain/a": "c"}, r.remoteKeys(),
		"seeding copies neither the phase nor the relay cursor")
	snap, err := r.db.Snapshot()
	require.NoError(t, err)
	require.Equal(t, []byte(catalog.MigrationSeeding), snap.Meta[catalog.MigrationStateKey])
	require.Zero(t, r.ms.dirty.len(), "a resync empties the dirty set")
	r.requireInSync(false)

	// Changes after the resync reach the catalog through a flush.
	b := r.local.NewBatch()
	b.Set([]byte("repo/a"), []byte("1b"))
	b.Delete([]byte("handle/x"))
	b.Set([]byte("repo/c"), []byte("3"))
	b.Set([]byte(catalog.RelayCursorKey), []byte("100"))
	require.NoError(t, b.Commit(r.ctx))
	require.Equal(t, 3, r.ms.dirty.len())
	n, err := r.ms.flush(r.ctx, r.sess, false)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, map[string]string{"repo/a": "1b", "repo/b": "2", "repo/c": "3", "sync/chain/a": "c"}, r.remoteKeys())
	r.requireInSync(false)

	// Tailing copies the phase; a range delete re-copies its range.
	require.NoError(t, r.local.Set(r.ctx, []byte(lifecycle.PhaseKey), []byte(lifecycle.PhaseSteadyState)))
	b = r.local.NewBatch()
	b.DeleteRange([]byte("repo/"), []byte("repo0"))
	b.Set([]byte("repo/d"), []byte("4"))
	require.NoError(t, b.Commit(r.ctx))
	_, err = r.ms.flush(r.ctx, r.sess, true)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"repo/d": "4", "sync/chain/a": "c", lifecycle.PhaseKey: string(lifecycle.PhaseSteadyState)}, r.remoteKeys())
	r.requireInSync(true)
}

func TestMetaSync_ResyncRepairsDrift(t *testing.T) {
	t.Parallel()
	r := newMetaRig(t, 3, 1000)
	want := map[string]string{}
	for i := range 20 {
		k := "repo/" + string(rune('a'+i))
		want[k] = k
	}
	r.set(want)
	_, err := r.ms.resync(r.ctx, r.sess, false, false)
	require.NoError(t, err)
	require.Equal(t, want, r.remoteKeys())

	// Writes the dirty set never saw: the observer was off.
	r.local.SetCommitObserver(nil)
	r.set(map[string]string{"repo/a": "changed", "repo/zz": "new"})
	require.NoError(t, r.local.Delete(r.ctx, []byte("repo/b")))
	_, err = r.sess.ImportMeta(r.ctx, []metastore.Op{{Kind: metastore.OpSet, Key: []byte("repo/c"), Value: []byte("drifted")}})
	require.NoError(t, err)
	diffs, err := r.ms.resync(r.ctx, r.sess, false, true)
	require.NoError(t, err)
	require.Equal(t, 4, diffs)
	diffs, err = r.ms.resync(r.ctx, r.sess, false, false)
	require.NoError(t, err)
	require.Equal(t, 4, diffs)
	want["repo/a"], want["repo/zz"] = "changed", "new"
	delete(want, "repo/b")
	require.Equal(t, want, r.remoteKeys())
	last, n := r.ms.lastResyncAt()
	require.False(t, last.IsZero())
	require.Equal(t, 4, n)
}

func TestMetaSync_Overflow(t *testing.T) {
	t.Parallel()
	r := newMetaRig(t, 100, 3)
	_, err := r.ms.resync(r.ctx, r.sess, false, false)
	require.NoError(t, err)
	r.set(map[string]string{"repo/a": "1", "repo/b": "2", "repo/c": "3", "repo/d": "4"})
	_, err = r.ms.flush(r.ctx, r.sess, false)
	require.ErrorIs(t, err, errResyncNeeded)
	_, err = r.ms.resync(r.ctx, r.sess, false, false)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"repo/a": "1", "repo/b": "2", "repo/c": "3", "repo/d": "4"}, r.remoteKeys())
}

func TestMetaSync_RefusesUnclassifiedKeys(t *testing.T) {
	t.Parallel()
	r := newMetaRig(t, 100, 1000)
	r.set(map[string]string{"repo/a": "1", "newfeature/x": "1"})
	_, err := r.ms.resync(r.ctx, r.sess, false, false)
	require.ErrorIs(t, err, errUnclassified)
	_, err = r.ms.flush(r.ctx, r.sess, false)
	require.NoError(t, err, "the resync emptied the dirty set")

	r.set(map[string]string{"other/y": "1"})
	_, err = r.ms.flush(r.ctx, r.sess, false)
	require.ErrorIs(t, err, errUnclassified)
}

func TestGuardAndPause(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, err := pebblestore.Open("/data", nil, pebblestore.WithFS(vfs.NewMem()))
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()

	g, err := ReadGuard(ctx, st)
	require.NoError(t, err)
	require.Equal(t, GuardNone, g)
	for _, want := range []Guard{GuardPending, GuardDone, GuardNone} {
		require.NoError(t, writeGuard(ctx, st, want))
		g, err = ReadGuard(ctx, st)
		require.NoError(t, err)
		require.Equal(t, want, g)
	}
	require.NoError(t, st.Set(ctx, []byte(HandoffGuardKey), []byte("maybe")))
	_, err = ReadGuard(ctx, st)
	require.Error(t, err, "an unknown guard value is an error, never no guard")

	_, ok, err := ReadCompactionPause(ctx, st)
	require.NoError(t, err)
	require.False(t, ok)
	at := time.UnixMicro(time.Now().UnixMicro())
	require.NoError(t, writeCompactionPause(ctx, st, at))
	got, ok, err := ReadCompactionPause(ctx, st)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, at.Equal(got))

	require.NoError(t, ClearLocalState(ctx, st))
	g, err = ReadGuard(ctx, st)
	require.NoError(t, err)
	require.Equal(t, GuardNone, g)
	_, ok, err = ReadCompactionPause(ctx, st)
	require.NoError(t, err)
	require.False(t, ok)
}
