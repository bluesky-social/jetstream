package backfill

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/bluesky-social/jetstream/internal/metastore/storetest"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

func setOp(k, v string) metastore.Op {
	return metastore.Op{Kind: metastore.OpSet, Key: []byte(k), Value: []byte(v)}
}

func TestCommitPipe_ReadsStagedRowsAndCommitsInStagingOrder(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	base := memstore.New()
	require.NoError(t, base.Set(ctx, []byte("a"), []byte("committed")))
	var p commitPipe
	r := &pipeReader{Store: base, p: &p}

	t1, err := p.stage(&pipeReader{}, []metastore.Op{setOp("a", "one"), {Kind: metastore.OpDelete, Key: []byte("b")}})
	require.NoError(t, err)
	t2, err := p.stage(&pipeReader{}, []metastore.Op{setOp("a", "two"), setOp("c", "")})
	require.NoError(t, err)

	got, err := r.GetMany(ctx, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")})
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("two"), nil, {}, nil}, got, "the newest staged rows, over the store")
	_, err = r.Get(ctx, []byte("b"))
	require.ErrorIs(t, err, metastore.ErrNotFound, "a staged delete reads as absent")

	var order []string
	var mu sync.Mutex
	record := func(name string) func() error {
		return func() error {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, name)
			return nil
		}
	}
	second := make(chan error, 1)
	go func() { second <- t2.commit(record("t2")) }()
	select {
	case err := <-second:
		t.Fatalf("t2 committed before t1 finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, t1.commit(record("t1")))
	got, err = r.GetMany(ctx, [][]byte{[]byte("a")})
	require.NoError(t, err)
	require.Equal(t, []byte("two"), got[0], "t1 finishing leaves t2's newer row staged")
	require.NoError(t, <-second)
	require.Equal(t, []string{"t1", "t2"}, order)

	got, err = r.GetMany(ctx, [][]byte{[]byte("a")})
	require.NoError(t, err)
	require.Equal(t, []byte("committed"), got[0], "finished rows leave the pipe; the store answers")
	require.Empty(t, p.staged)
	require.Nil(t, p.tail)
}

func TestCommitPipe_FailedWriteFailsOnlyWritesStagedOnIt(t *testing.T) {
	t.Parallel()
	var p commitPipe
	t1, err := p.stage(&pipeReader{}, []metastore.Op{setOp("a", "1")})
	require.NoError(t, err)
	t2, err := p.stage(&pipeReader{}, []metastore.Op{setOp("b", "2")})
	require.NoError(t, err)
	t3, err := p.stage(&pipeReader{}, []metastore.Op{setOp("c", "3")})
	require.NoError(t, err)

	boom := errors.New("boom")
	ran := map[string]bool{}
	require.ErrorIs(t, t1.commit(func() error { ran["t1"] = true; return boom }), boom)
	err = t2.commit(func() error { ran["t2"] = true; return nil })
	require.ErrorIs(t, err, boom, "t2 was staged on t1's rows")
	require.ErrorIs(t, t3.commit(func() error { ran["t3"] = true; return nil }), boom, "and t3 on t2's")
	require.Equal(t, map[string]bool{"t1": true}, ran, "no write staged on a failure commits")
	require.Empty(t, p.staged, "failed rows are never read again")

	t4, err := p.stage(&pipeReader{}, []metastore.Op{setOp("a", "4")})
	require.NoError(t, err)
	require.NoError(t, t4.commit(func() error { return nil }), "a write staged after the failures settled is clean")
}

// Writes finish outside countsMu, so a staging can read a write's rows and
// see that write fail and leave the pipe before taking its own ticket. The
// new ticket would not wait behind the failure, so stage must refuse it; a
// write read after it finished cleanly is fine, since the store has it.
func TestCommitPipe_RefusesWriteThatReadAFailedOne(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	var p commitPipe
	base := memstore.New()

	failed, err := p.stage(&pipeReader{}, []metastore.Op{setOp("counts", "tainted")})
	require.NoError(t, err)
	r := &pipeReader{Store: base, p: &p}
	got, err := r.GetMany(ctx, [][]byte{[]byte("counts")})
	require.NoError(t, err)
	require.Equal(t, []byte("tainted"), got[0])
	boom := errors.New("boom")
	failed.finish(boom)
	require.Nil(t, p.tail, "the failed write was the tail, so a new ticket would wait for nothing")
	_, err = p.stage(r, []metastore.Op{setOp("counts", "tainted+1")})
	require.ErrorIs(t, err, boom)
	_, ok := errors.AsType[*stagedOnFailureError](err)
	require.True(t, ok, "the caller can stage again from the store")
	require.Empty(t, p.staged)

	clean, err := p.stage(&pipeReader{}, []metastore.Op{setOp("counts", "1")})
	require.NoError(t, err)
	r = &pipeReader{Store: base, p: &p}
	_, err = r.GetMany(ctx, [][]byte{[]byte("counts")})
	require.NoError(t, err)
	clean.finish(nil)
	next, err := p.stage(r, []metastore.Op{setOp("counts", "2")})
	require.NoError(t, err, "a write read after it committed is in the store")
	next.finish(nil)
}

func TestCommitPipe_RejectsRangeDelete(t *testing.T) {
	t.Parallel()
	var p commitPipe
	_, err := p.stage(&pipeReader{}, []metastore.Op{setOp("a", "1"), {Kind: metastore.OpDeleteRange, Key: []byte("a"), End: []byte("b")}})
	require.ErrorIs(t, err, errPipeDeleteRange)
	require.Empty(t, p.staged)
	require.Nil(t, p.tail)
}

// A write waiting its turn blocks on a channel, so testing/synctest counts
// it as durably blocked and the layer 3 oracle's seeded scheduler can park
// the commit ahead of it (see chanMutex).
func TestCommitPipe_WaiterIsDurablyBlocked(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var p commitPipe
		t1, err := p.stage(&pipeReader{}, []metastore.Op{setOp("a", "1")})
		require.NoError(t, err)
		t2, err := p.stage(&pipeReader{}, []metastore.Op{setOp("a", "2")})
		require.NoError(t, err)
		var committed atomic.Bool
		go func() {
			_ = t2.commit(func() error { committed.Store(true); return nil })
		}()
		synctest.Wait()
		require.False(t, committed.Load())
		t1.finish(nil)
		synctest.Wait()
		require.True(t, committed.Load())
	})
}

// gatedStore holds or fails the next batch commit on request, standing in
// for a slow or failing catalog transaction.
type gatedStore struct {
	metastore.Store
	mu      sync.Mutex
	hold    chan struct{}
	entered chan struct{}
	fail    error
}

// holdNext makes the next commit signal entered and wait for release; it
// then fails with fail, or applies when fail is nil.
func (g *gatedStore) holdNext(fail error) (entered <-chan struct{}, release func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.hold, g.entered, g.fail = make(chan struct{}), make(chan struct{}), fail
	hold := g.hold
	return g.entered, func() { close(hold) }
}

func (g *gatedStore) NewBatch() metastore.Batch {
	return metastore.NewOpBatch(func(ctx context.Context, ops []metastore.Op) error {
		g.mu.Lock()
		hold, entered, fail := g.hold, g.entered, g.fail
		g.hold, g.entered, g.fail = nil, nil, nil
		g.mu.Unlock()
		if hold != nil {
			close(entered)
			<-hold
			if fail != nil {
				return fail
			}
		}
		return applyOps(ctx, g.Store, ops)
	})
}

// requireStaged waits until key is staged in st's pipe, i.e. a writer staged
// it without waiting for the commit ahead of it.
func requireStaged(t *testing.T, st *Store, key []byte) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, _, ok := st.pipe.lookup(key)
		return ok
	}, 5*time.Second, time.Millisecond, "%s never staged", key)
}

func requireBlocked(t *testing.T, ch <-chan error, what string) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("%s returned before the commit ahead of it finished: %v", what, err)
	case <-time.After(20 * time.Millisecond):
	}
}

// requireAggregatesMatchRows recounts backfill/counts and every host
// aggregate from the repo and roster rows. A lost update, or a write that
// committed on top of one that failed, leaves an aggregate off.
func requireAggregatesMatchRows(t *testing.T, db metastore.Store) {
	t.Helper()
	want, err := CountStatuses(db)
	require.NoError(t, err)
	for _, kv := range storetest.Scan(t, db, []byte(pdsHostKeyPrefix), metastore.PrefixUpperBound([]byte(pdsHostKeyPrefix))) {
		host, err := decodePDSHost(kv.Value)
		require.NoError(t, err)
		switch atmosbackfill.HostState(host.State) {
		case atmosbackfill.HostStateDrained:
			want.HostsDrained++
		case atmosbackfill.HostStateExhausted:
			want.HostsExhausted++
		}
	}
	got, ok, err := LoadCounts(db)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, got, "backfill/counts")

	expect := map[string]*HostStatus{}
	for _, kv := range storetest.Scan(t, db, []byte(repoKeyPrefix), metastore.PrefixUpperBound([]byte(repoKeyPrefix))) {
		rs, err := decodeRepoStatus(kv.Value)
		require.NoError(t, err)
		if rs.Host == "" {
			continue
		}
		host, _, err := normalizeHostStatusKey(rs.Host)
		require.NoError(t, err)
		hs := expect[host]
		if hs == nil {
			hs = newHostStatus(host)
			expect[host] = hs
		}
		hs.Total++
		if rs.Active {
			hs.Active++
		}
		incrementStatus(hs, rs.Backfill.Status)
	}
	type tally struct{ Total, Active, NotStarted, Pending, Complete, Failed, Unavailable uint64 }
	of := func(hs *HostStatus) tally {
		if hs == nil {
			return tally{}
		}
		return tally{hs.Total, hs.Active, hs.NotStarted, hs.Pending, hs.Complete, hs.Failed, hs.Unavailable}
	}
	for _, kv := range storetest.Scan(t, db, []byte(hostKeyPrefix), metastore.PrefixUpperBound([]byte(hostKeyPrefix))) {
		hs, err := decodeHostStatus(kv.Value)
		require.NoError(t, err)
		require.Equal(t, of(expect[hs.Host]), of(hs), "host aggregate %s", hs.Host)
		delete(expect, hs.Host)
	}
	for host, hs := range expect {
		require.Zero(t, hs.Total, "host %s has repo rows but no aggregate", host)
	}
}

func discoverEntries(dids ...atmos.DID) []atmossync.ListReposEntry {
	out := make([]atmossync.ListReposEntry, len(dids))
	for i, did := range dids {
		out[i] = atmossync.ListReposEntry{DID: did, Active: true}
	}
	return out
}

// commitHook runs one durable-batch hook call and its commit the way the
// segment writers do.
func commitHook(t *testing.T, db metastore.Store, cb *completionBatcher) error {
	t.Helper()
	b := db.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(context.Background(), b, 1<<40, false, nil)
	if err != nil {
		return err
	}
	commitErr := b.Commit(context.Background())
	if commitErr == nil && afterCommit != nil {
		afterCommit()
	}
	if afterDone != nil {
		afterDone(commitErr)
	}
	return commitErr
}

// TestStore_StagingProceedsDuringACommit is the point of the pipe: while one
// write's commit is in flight, other read-modify-writes and the segment
// writer's completion staging take countsMu, stage on top of it, and only
// their commits wait.
func TestStore_StagingProceedsDuringACommit(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := &gatedStore{Store: memstore.New()}
	st := newSeededStore(t, db, nil)
	adapter := st.AtmosStore()
	const host = "pds.example.test"
	a, b, c := atmos.DID("did:plc:pipea"), atmos.DID("did:plc:pipeb"), atmos.DID("did:plc:pipec")
	require.NoError(t, adapter.OnDiscover(ctx, host, discoverEntries(a, c)))
	cb := NewCompletionBatcher(st, nil)
	st.SetCompletionBatcher(cb)

	entered, release := db.holdNext(nil)
	discovered := make(chan error, 1)
	go func() { discovered <- adapter.OnDiscover(ctx, host, discoverEntries(b)) }()
	<-entered

	failed := make(chan error, 1)
	go func() { failed <- st.OnFail(ctx, a, host, errors.New("boom"), 1) }()
	requireStaged(t, st, repoKey(a))
	requireBlocked(t, failed, "OnFail")

	cb.RecordWatermark(c, 7, true)
	require.NoError(t, cb.QueueComplete(ctx, c, host, &repo.Commit{DID: string(c), Rev: "rev"}))
	hooked := make(chan error, 1)
	go func() { hooked <- commitHook(t, db, cb) }()
	requireStaged(t, st, repoKey(c))
	requireBlocked(t, hooked, "the durable-batch hook")

	release()
	require.NoError(t, <-discovered)
	require.NoError(t, <-failed)
	require.NoError(t, <-hooked)
	requireAggregatesMatchRows(t, db)
	counts, _, err := LoadCounts(db)
	require.NoError(t, err)
	require.Equal(t, Counts{Total: 3, Discovered: 1, Failed: 1, Complete: 1}, counts)
	require.Empty(t, st.pipe.staged)
}

// A failed commit fails the writes staged on top of it, which never commit,
// and nothing else: the next write reads the store and succeeds.
func TestStore_FailedCommitDoesNotLeakIntoLaterWrites(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := &gatedStore{Store: memstore.New()}
	st := newSeededStore(t, db, nil)
	adapter := st.AtmosStore()
	const host = "pds.example.test"
	a, b := atmos.DID("did:plc:leaka"), atmos.DID("did:plc:leakb")
	require.NoError(t, adapter.OnDiscover(ctx, host, discoverEntries(a)))

	boom := errors.New("injected commit failure")
	entered, release := db.holdNext(boom)
	discovered := make(chan error, 1)
	go func() { discovered <- adapter.OnDiscover(ctx, host, discoverEntries(b)) }()
	<-entered
	failed := make(chan error, 1)
	go func() { failed <- st.OnFail(ctx, a, host, errors.New("boom"), 1) }()
	requireStaged(t, st, repoKey(a))
	release()

	require.ErrorIs(t, <-discovered, boom)
	err := <-failed
	require.ErrorIs(t, err, boom, "OnFail was staged on the failed discovery")
	require.ErrorContains(t, err, "staged ahead of this one failed")
	requireLookupState(t, st, b, atmosbackfill.StateUnknown)
	rs, err := st.readRepoStatus(a)
	require.NoError(t, err)
	require.Equal(t, StatusNotStarted, rs.Backfill.Status, "the tainted OnFail did not commit")
	requireAggregatesMatchRows(t, db)

	require.NoError(t, st.OnFail(ctx, a, host, errors.New("boom"), 1))
	require.NoError(t, adapter.OnDiscover(ctx, host, discoverEntries(b)))
	requireAggregatesMatchRows(t, db)
	require.Empty(t, st.pipe.staged)
}

// The durable-batch hook stages again when a write staged ahead of it fails,
// rather than fail the segment writer over another write's commit.
func TestStore_HookRestagesAfterFailedPredecessor(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := &gatedStore{Store: memstore.New()}
	st := newSeededStore(t, db, nil)
	adapter := st.AtmosStore()
	const host = "pds.example.test"
	a, b := atmos.DID("did:plc:restagea"), atmos.DID("did:plc:restageb")
	require.NoError(t, adapter.OnDiscover(ctx, host, discoverEntries(a)))
	cb := NewCompletionBatcher(st, nil)
	st.SetCompletionBatcher(cb)
	cb.RecordWatermark(a, 3, true)
	require.NoError(t, cb.QueueComplete(ctx, a, host, &repo.Commit{DID: string(a), Rev: "rev"}))

	boom := errors.New("injected commit failure")
	entered, release := db.holdNext(boom)
	discovered := make(chan error, 1)
	go func() { discovered <- adapter.OnDiscover(ctx, host, discoverEntries(b)) }()
	<-entered
	hooked := make(chan error, 1)
	go func() { hooked <- commitHook(t, db, cb) }()
	requireStaged(t, st, repoKey(a))
	release()

	require.ErrorIs(t, <-discovered, boom)
	require.NoError(t, <-hooked, "the hook staged again and committed")
	requireLookupState(t, st, a, atmosbackfill.StateComplete)
	requireLookupState(t, st, b, atmosbackfill.StateUnknown)
	// Had the hook committed what it first staged, the counts would carry
	// the failed discovery of b.
	requireAggregatesMatchRows(t, db)
}

func TestStore_HookRefusesReentry(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := memstore.New()
	st := newSeededStore(t, db, nil)
	a, b := atmos.DID("did:plc:reentera"), atmos.DID("did:plc:reenterb")
	require.NoError(t, st.AtmosStore().OnDiscover(ctx, "pds.example.test", discoverEntries(a, b)))
	cb := NewCompletionBatcher(st, nil)
	st.SetCompletionBatcher(cb)
	cb.RecordWatermark(a, 3, true)
	require.NoError(t, cb.QueueComplete(ctx, a, "pds.example.test", &repo.Commit{DID: string(a), Rev: "rev"}))

	first := db.NewBatch()
	_, afterDone, err := cb.StageDurable(ctx, first, 1<<40, false, nil)
	require.NoError(t, err)
	cb.RecordWatermark(b, 4, true)
	require.NoError(t, cb.QueueComplete(ctx, b, "pds.example.test", &repo.Commit{DID: string(b), Rev: "rev"}))
	_, _, err = cb.StageDurable(ctx, db.NewBatch(), 1<<40, false, nil)
	require.ErrorIs(t, err, errHookReentered, "a second batch before the first finished would wait on itself")

	afterDone(first.Commit(ctx))
	require.NoError(t, commitHook(t, db, cb), "the hook works again once the batch finished")
	requireAggregatesMatchRows(t, db)
}

// randomFaults fails a seeded fraction of batch commits.
type randomFaults struct {
	mu  sync.Mutex
	rng *rand.Rand
	p   float64
	off atomic.Bool
}

var errRandomFault = errors.New("injected random commit failure")

func (f *randomFaults) BeforeWrite(op metastore.WriteOp, _ [][]byte) error {
	if f.off.Load() || op != metastore.WriteOpBatchCommit {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rng.Float64() < f.p {
		return errRandomFault
	}
	return nil
}

// jitterStore delays each commit by a seeded random amount, so concurrent
// writers interleave differently per seed.
type jitterStore struct {
	metastore.Store
	mu  sync.Mutex
	rng *rand.Rand
}

func (j *jitterStore) NewBatch() metastore.Batch {
	inner := j.Store.NewBatch()
	return metastore.NewOpBatch(func(ctx context.Context, ops []metastore.Op) error {
		j.mu.Lock()
		d := time.Duration(j.rng.IntN(300)) * time.Microsecond
		j.mu.Unlock()
		time.Sleep(d)
		for _, op := range ops {
			switch op.Kind {
			case metastore.OpSet:
				inner.Set(op.Key, op.Value)
			case metastore.OpDelete:
				inner.Delete(op.Key)
			case metastore.OpDeleteRange:
				inner.DeleteRange(op.Key, op.End)
			}
		}
		return inner.Commit(ctx)
	})
}

// TestStore_CommitPipeSwarm runs concurrent discovery pages, Active flips,
// failures, host state changes, and segment-writer completion commits
// against one Store, with seeded commit delays and failures. Each worker owns
// its DIDs, as atmos's per-DID locks guarantee in production, but every write
// shares the counts row and host aggregates. Once quiet, every aggregate must
// match a recount of the rows: a lost update, or a write that committed on
// rows that never landed, would leave one off.
func TestStore_CommitPipeSwarm(t *testing.T) {
	t.Parallel()
	for seed := range uint64(12) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			runCommitPipeSwarm(t, seed)
		})
	}
}

func runCommitPipeSwarm(t *testing.T, seed uint64) {
	ctx := t.Context()
	faults := &randomFaults{rng: rand.New(rand.NewPCG(seed, 0xfa17)), p: 0.1}
	db := &jitterStore{Store: metastore.WithFaults(memstore.New(), faults), rng: rand.New(rand.NewPCG(seed, 0x7177))}
	st := newSeededStore(t, db, nil)
	cb := NewCompletionBatcher(st, nil)
	st.SetCompletionBatcher(cb)
	adapter := st.AtmosStore()
	hosts := []string{"alpha.example.test", "beta.example.test", "gamma.example.test:8443", "delta.example.test"}

	const workers, didsPerWorker, opsPerWorker = 6, 24, 60
	var seq atomic.Uint64
	var queued atomic.Int64
	var wg sync.WaitGroup
	for w := range workers {
		rng := rand.New(rand.NewPCG(seed, uint64(w)))
		wg.Go(func() {
			type didState struct {
				did        atmos.DID
				discovered bool
				active     bool
				done       bool // completion queued; atmos sends nothing more
			}
			dids := make([]*didState, didsPerWorker)
			for i := range dids {
				dids[i] = &didState{did: atmos.DID(fmt.Sprintf("did:plc:swarm%d-%02d", w, i))}
			}
			pick := func(ok func(*didState) bool) *didState {
				for range 2 * didsPerWorker {
					if d := dids[rng.IntN(len(dids))]; ok(d) {
						return d
					}
				}
				return nil
			}
			for range opsPerWorker {
				host := hosts[rng.IntN(len(hosts))]
				switch rng.IntN(6) {
				case 0, 1:
					var page []atmossync.ListReposEntry
					var picked []*didState
					inPage := map[atmos.DID]bool{}
					for range 1 + rng.IntN(6) {
						d := pick(func(d *didState) bool { return !d.discovered && !inPage[d.did] })
						if d == nil {
							break
						}
						inPage[d.did] = true
						picked = append(picked, d)
						page = append(page, atmossync.ListReposEntry{DID: d.did, Active: true})
					}
					if len(page) == 0 {
						continue
					}
					if adapter.OnDiscover(ctx, host, page) == nil {
						for _, d := range picked {
							d.discovered, d.active = true, true
						}
					}
				case 2:
					d := pick(func(d *didState) bool { return d.discovered && !d.done })
					if d == nil {
						continue
					}
					if adapter.OnUpdate(ctx, host, []atmossync.ListReposEntry{{DID: d.did, Active: !d.active}}) == nil {
						d.active = !d.active
					}
				case 3:
					if d := pick(func(d *didState) bool { return d.discovered && !d.done }); d != nil {
						_ = st.OnFail(ctx, d.did, host, errors.New("download failed"), 1+rng.IntN(3))
					}
				case 4:
					d := pick(func(d *didState) bool { return d.discovered && !d.done })
					if d == nil {
						continue
					}
					cb.RecordWatermark(d.did, seq.Add(1), true)
					if cb.QueueComplete(ctx, d.did, host, &repo.Commit{DID: string(d.did), Rev: "rev"}) == nil {
						d.done = true
						queued.Add(1)
					}
				case 5:
					if rng.IntN(2) == 0 {
						_ = adapter.OnHost(ctx, []atmosbackfill.HostInfo{{Hostname: host, RelayStatus: "active", RelayAccounts: int64(rng.IntN(100))}})
					} else {
						_ = st.updateHostStateDirect(ctx, host, func(h *PDSHost) atmosbackfill.HostState {
							if rng.IntN(2) == 0 {
								return atmosbackfill.HostStateExhausted
							}
							return atmosbackfill.HostStateRunning
						})
					}
				}
			}
		})
	}

	// The segment writer: one hook call and commit at a time, failures
	// included, until the workers stop.
	stop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := commitHook(t, db, cb); err != nil && !errors.Is(err, errRandomFault) {
				t.Errorf("hook commit: %v", err)
				return
			}
			time.Sleep(50 * time.Microsecond)
		}
	}()
	wg.Wait()
	close(stop)
	<-writerDone

	faults.off.Store(true)
	for cb.hasPendingDurability() {
		require.NoError(t, commitHook(t, db, cb))
	}
	require.Empty(t, st.pipe.staged)
	require.Nil(t, st.pipe.tail)
	requireAggregatesMatchRows(t, db)
	counts, _, err := LoadCounts(db)
	require.NoError(t, err)
	require.Equal(t, uint64(queued.Load()), counts.Complete, "every queued completion committed")
}
