package oracle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream"
	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/jetstreamd/jetstreamdtest"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/internal/migrate"
	"github.com/bluesky-social/jetstream/internal/seqspace"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// migrationRig runs the migration end to end: a local-mode archive on the
// simulator, the migrator inside it writing to storagefake and memblob,
// and disaggregated pods on the same backend.
type migrationRig struct {
	t     *testing.T
	ctx   context.Context
	cfg   Config
	scale int
	w     *world.World
	relay string
	fs    vfs.FS
	// dir is a real directory: the local archive endpoints open sealed
	// segments with os.Open.
	dir  string
	fake *jetstreamdtest.Backend
	// gate sees every frame any runtime archives, so a wait covers the
	// whole run whichever process ingested each frame.
	gate atomic.Pointer[cutoverDeliveryGate]
	// maxSeq is the highest seq any runtime archived. A pod reports an
	// event once appended, before its catalog commit lands.
	maxSeq atomic.Uint64
	// archivedBy names the runtime that archived each seq, for failures.
	archivedMu sync.Mutex
	archivedBy map[uint64]string

	local *rigRuntime
	pods  []*rigRuntime
}

type rigRuntime struct {
	name   string
	rt     *jetstreamd.Runtime
	cancel context.CancelFunc
	done   chan error
}

func newMigrationRig(t *testing.T, seedIdx int) *migrationRig {
	t.Helper()
	cfg := Config{
		Mode:                "migration",
		Seed:                restartSeed(seedIdx),
		Accounts:            4,
		MinInitialRecords:   1,
		MaxInitialRecords:   4,
		LiveEventsBootstrap: 4,
		LiveEventsSteady:    4,
	}
	scale := 1
	if os.Getenv(envOracleMode) == "stress" {
		cfg.Accounts, cfg.MaxInitialRecords, scale = 24, 8, 5
	}
	w := newRestartWorld(t, cfg)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	srv := newRestartServer(t, w, nil)
	t.Cleanup(srv.Close)
	r := &migrationRig{t: t, ctx: t.Context(), cfg: cfg, scale: scale, w: w, relay: srv.URL, fs: vfs.Default, dir: t.TempDir(), archivedBy: map[uint64]string{}, fake: jetstreamdtest.New(storagefake.Config{})}
	r.gate.Store(newCutoverDeliveryGate(srv.URL, 30*time.Second))
	t.Cleanup(func() {
		for _, p := range r.pods {
			r.stop(p)
		}
		if r.local != nil {
			r.stop(r.local)
		}
	})
	return r
}

// generate emits n relay events, times the stress scale.
func (r *migrationRig) generate(n int) { generateN(r.t, r.w, n*r.scale) }

func (r *migrationRig) observer(name string) func(*segment.Event) {
	return func(ev *segment.Event) {
		r.archivedMu.Lock()
		r.archivedBy[ev.Seq] = name
		r.archivedMu.Unlock()
		r.observe(ev)
	}
}

func (r *migrationRig) observe(ev *segment.Event) {
	for {
		cur := r.maxSeq.Load()
		if ev.Seq <= cur || r.maxSeq.CompareAndSwap(cur, ev.Seq) {
			break
		}
	}
	r.gate.Load().observe(ev)
}

// archivedTip returns the last seq archived once waitArchived has
// returned. The gate counts a frame delivered at its first row, so a
// multi-op commit's later rows can still be landing: the tip is read once
// it holds still.
func (r *migrationRig) archivedTip() uint64 {
	r.t.Helper()
	tip, stillFor := r.maxSeq.Load(), 0
	require.Eventually(r.t, func() bool {
		if cur := r.maxSeq.Load(); cur != tip {
			tip, stillFor = cur, 0
			return false
		}
		stillFor++
		return stillFor >= 5
	}, 10*time.Second, 10*time.Millisecond)
	return tip
}

// committedTip waits until the catalog holds every seq any runtime
// archived, and returns the last.
func (r *migrationRig) committedTip() uint64 {
	r.t.Helper()
	tip := r.archivedTip()
	require.Eventually(r.t, func() bool {
		next, err := catalog.DecodeSeq(catalog.MainSeqKey, r.catalogSnapshot().Meta[catalog.MainSeqKey], true)
		require.NoError(r.t, err)
		return next > tip
	}, 10*time.Second, 5*time.Millisecond, "the catalog holds seq %d", tip)
	return tip
}

// waitArchived waits until every frame the relay has emitted is archived.
func (r *migrationRig) waitArchived() {
	r.t.Helper()
	require.NoError(r.t, r.gate.Load().waitDelivered(r.ctx))
}

func (r *migrationRig) baseOptions(name string) jetstreamd.Options {
	return jetstreamd.Options{
		PublicAddr:                     "127.0.0.1:0",
		RelayURL:                       r.relay,
		PLCURL:                         r.relay,
		OTelServiceName:                "jetstream-oracle-" + name,
		LogLevel:                       "warn",
		LogFormat:                      "text",
		LogOutput:                      testWriter{t: r.t},
		ShutdownTimeout:                5 * time.Second,
		ClientDrainTimeout:             time.Second,
		CursorLookback:                 36 * time.Hour,
		PlanMaxDIDs:                    xrpcapi.DefaultPlanMaxDIDs,
		PlanMaxCollections:             xrpcapi.DefaultPlanMaxCollections,
		PlanMaxEntries:                 xrpcapi.DefaultPlanMaxEntries,
		PlanWholeSegmentThreshold:      xrpcapi.DefaultPlanWholeSegmentThreshold,
		SubscribeReadLogRetentionBytes: 16 << 20,
		SubscribeBlockCacheBytes:       16 << 20,
		SubscribeReadBatch:             1024,
		SubscribeSlowWindow:            time.Second,
		SubscribeSlowMinRate:           1,
		CursorBlockIndexCacheSize:      32,
		CompactionInterval:             time.Hour,
		// Small blocks and segments give the migration many of each.
		SteadyMaxEventsPerBlock: 3,
		SteadyMaxSegmentBytes:   2048,
		SessionRestartDelay:     10 * time.Millisecond,
		OnSteadyStateEvent:      r.observer(name),
	}
}

// startLocal starts the local-mode process; with migrate set it runs the
// migrator.
func (r *migrationRig) startLocal(migrateOn bool, crash crashpoint.Injector) {
	r.t.Helper()
	opts := r.baseOptions("local")
	opts.DataDir = r.dir
	opts.StorageFS = r.fs
	opts.OnBootstrapLiveEvent = r.observer("local")
	opts.BarrierBeforeCutover = func(ctx context.Context) error { return r.gate.Load().waitDelivered(ctx) }
	opts.CrashInjector = crash
	if migrateOn {
		opts.Storage = jetstreamd.DefaultStorageConfig()
		opts.Storage.Leader.AcquireInterval = 10 * time.Millisecond
		opts.StorageBackend = r.fake.Backend
		mc := jetstreamd.DefaultMigrationConfig()
		mc.Enabled = true
		mc.TailPollInterval = 10 * time.Millisecond
		mc.MetaFlushInterval = 20 * time.Millisecond
		mc.ControlInterval = 10 * time.Millisecond
		mc.DrainSpread = 50 * time.Millisecond
		mc.HandoffTimeout = 10 * time.Second
		mc.HandoffFullVerify = true
		mc.ReadBytesPerSec = 0
		mc.TailMaxBlocksPerTxn = 2
		opts.Migration = mc
	}
	r.local = r.start("local", opts)
}

// startPod starts a disaggregated pod on the migration's backend.
func (r *migrationRig) startPod(name string) *rigRuntime {
	r.t.Helper()
	opts := r.baseOptions(name)
	opts.Storage = jetstreamd.DefaultStorageConfig()
	opts.Storage.Mode = jetstreamd.StorageDisaggregated
	opts.Storage.Leader.AcquireInterval = 10 * time.Millisecond
	opts.Storage.Hot.BatchMaxAge = 5 * time.Millisecond
	opts.Storage.CatalogPollInterval = 10 * time.Millisecond
	opts.StorageBackend = r.fake.Backend
	opts.MemoryLimit = 8 << 30
	opts.Migration.StandbyBackoff = 20 * time.Millisecond
	p := r.start(name, opts)
	r.pods = append(r.pods, p)
	return p
}

func (r *migrationRig) start(name string, opts jetstreamd.Options) *rigRuntime {
	r.t.Helper()
	ctx, cancel := context.WithCancel(r.ctx)
	rt, err := jetstreamd.Build(ctx, opts)
	require.NoErrorf(r.t, err, "build %s", name)
	p := &rigRuntime{name: name, rt: rt, cancel: cancel, done: make(chan error, 1)}
	if inj, ok := opts.CrashInjector.(*killInjector); ok {
		inj.kill.Store(&cancel)
	}
	go func() { p.done <- rt.Run(ctx) }()
	require.Eventually(r.t, func() bool { return rt.PublicAddr() != "" }, 10*time.Second, 5*time.Millisecond, "%s listening", name)
	return p
}

func (r *migrationRig) stop(p *rigRuntime) {
	r.t.Helper()
	if p == nil || p.cancel == nil {
		return
	}
	p.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	closeErr := p.rt.Close(ctx)
	runErr := <-p.done
	p.cancel = nil
	require.NoErrorf(r.t, errors.Join(runErr, closeErr), "%s stopped", p.name)
}

func (r *migrationRig) url(p *rigRuntime) string { return "http://" + p.rt.PublicAddr() }

// abandonLease leaves the local archive as an unclean stop does: the seq
// lease reserved past seq/next, so the next session registers a vacancy.
func (r *migrationRig) abandonLease(width uint64) {
	r.t.Helper()
	st, err := pebblestore.Open(r.dir, nil, pebblestore.WithFS(r.fs))
	require.NoError(r.t, err)
	defer func() { require.NoError(r.t, st.Close()) }()
	v, err := st.Get(r.ctx, []byte(catalog.MainSeqKey))
	require.NoError(r.t, err)
	next, err := catalog.DecodeSeq(catalog.MainSeqKey, v, true)
	require.NoError(r.t, err)
	require.NoError(r.t, st.Set(r.ctx, []byte(ingest.SeqReservedKey), catalog.EncodeSeq(next+width)))
}

func (r *migrationRig) localNext() uint64 {
	r.t.Helper()
	st, err := pebblestore.Open(r.dir, nil, pebblestore.WithFS(r.fs))
	require.NoError(r.t, err)
	defer func() { require.NoError(r.t, st.Close()) }()
	v, err := st.Get(r.ctx, []byte(catalog.MainSeqKey))
	require.NoError(r.t, err)
	next, err := catalog.DecodeSeq(catalog.MainSeqKey, v, true)
	require.NoError(r.t, err)
	return next
}

func (r *migrationRig) localGuard() migrate.Guard {
	r.t.Helper()
	st, err := pebblestore.Open(r.dir, nil, pebblestore.WithFS(r.fs))
	require.NoError(r.t, err)
	defer func() { require.NoError(r.t, st.Close()) }()
	g, err := migrate.ReadGuard(r.ctx, st)
	require.NoError(r.t, err)
	return g
}

func (r *migrationRig) status() migrate.Status {
	var st migrate.Status
	if doc := r.fake.Control.LastStatus(); doc != nil {
		require.NoError(r.t, json.Unmarshal(doc, &st))
	}
	return st
}

// waitTailing waits until the migrator tails with no lag, past seq min.
func (r *migrationRig) waitTailing(min uint64) migrate.Status {
	r.t.Helper()
	var st migrate.Status
	require.Eventuallyf(r.t, func() bool {
		st = r.status()
		return st.State == string(catalog.MigrationTailing) && st.Error == "" && st.LagSeqs == 0 &&
			st.RemoteNextSeq >= min && !st.LastResync.IsZero()
	}, 30*time.Second, 10*time.Millisecond, "migrator tailing past %d", min)
	return st
}

func (r *migrationRig) catalogSnapshot() *catalog.Snapshot {
	r.t.Helper()
	snap, err := r.fake.DB.Snapshot()
	require.NoError(r.t, err)
	return snap
}

// download reads host's whole archive and live tail through the Go client,
// up to and including seq last.
func (r *migrationRig) download(host string, last uint64) []ObservedEvent {
	r.t.Helper()
	client, err := jetstream.Subscribe(host, jetstream.WithAfterSeq(0), jetstream.WithBatchSize(64))
	require.NoError(r.t, err)
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
	defer cancel()
	var out []ObservedEvent
	for batch, err := range client.Events(ctx) {
		require.NoErrorf(r.t, err, "download from %s", host)
		for _, ev := range batch.Events() {
			oe, err := observedEventFromClientErr(ev)
			require.NoError(r.t, err)
			out = append(out, oe)
		}
		if len(out) > 0 && out[len(out)-1].Seq >= last {
			return out
		}
	}
	r.t.Fatalf("download from %s ended before seq %d", host, last)
	return nil
}

// splitClient is a client whose archive requests reach one server and
// whose live tail reaches another: what a client sees when the routes flip
// to the pods between its plan and its cutover to live.
type splitClient struct {
	cancel context.CancelFunc
	done   chan error
	// plans and lives count the requests each side served.
	plans, lives atomic.Int64
	mu           sync.Mutex
	events       []ObservedEvent
}

// startSplitClient subscribes from seq 0, with archive requests sent to
// archive and everything else to live.
func (r *migrationRig) startSplitClient(archive, live string) *splitClient {
	r.t.Helper()
	proxyTo := func(host string) *httputil.ReverseProxy {
		u, err := url.Parse(host)
		require.NoError(r.t, err)
		return httputil.NewSingleHostReverseProxy(u)
	}
	toArchive, toLive := proxyTo(archive), proxyTo(live)
	c := &splitClient{done: make(chan error, 1)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case strings.HasSuffix(req.URL.Path, ".planSnapshot"):
			c.plans.Add(1)
			toArchive.ServeHTTP(w, req)
		case strings.HasSuffix(req.URL.Path, ".getSegment"), strings.HasSuffix(req.URL.Path, ".getBlock"):
			toArchive.ServeHTTP(w, req)
		default:
			if strings.HasSuffix(req.URL.Path, ".subscribeEvents") {
				c.lives.Add(1)
			}
			toLive.ServeHTTP(w, req)
		}
	}))
	r.t.Cleanup(srv.Close)
	client, err := jetstream.Subscribe(srv.URL, jetstream.WithAfterSeq(0), jetstream.WithBatchSize(64))
	require.NoError(r.t, err)
	ctx, cancel := context.WithCancel(r.ctx)
	c.cancel = cancel
	go func() {
		defer func() { _ = client.Close() }()
		for batch, err := range client.Events(ctx) {
			if err != nil {
				c.done <- err
				return
			}
			for _, ev := range batch.Events() {
				oe, err := observedEventFromClientErr(ev)
				if err != nil {
					c.done <- err
					return
				}
				c.mu.Lock()
				c.events = append(c.events, oe)
				c.mu.Unlock()
			}
		}
		c.done <- ctx.Err()
	}()
	r.t.Cleanup(func() { cancel(); <-c.done })
	return c
}

// waitSplitClient returns everything c received once it has received seq
// last.
func (r *migrationRig) waitSplitClient(c *splitClient, last uint64) []ObservedEvent {
	r.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		c.mu.Lock()
		got := slices.Clone(c.events)
		c.mu.Unlock()
		if len(got) > 0 && got[len(got)-1].Seq >= last {
			require.NotZero(r.t, c.plans.Load(), "the split client planned on the archive side")
			require.NotZero(r.t, c.lives.Load(), "the split client went live on the live side")
			return got
		}
		select {
		case err := <-c.done:
			c.done <- err
			r.t.Fatalf("the split client ended at %d events, before seq %d: %v", len(got), last, err)
		default:
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("the split client reached %d events, not seq %d", len(got), last)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// getSegment downloads one sealed segment's bytes.
func getSegment(t *testing.T, host string, idx uint64) []byte {
	t.Helper()
	resp, err := httpGet(t.Context(), fmt.Sprintf("%s/xrpc/network.bsky.jetstream.getSegment?name=%s", host, ingest.SegmentFilename(idx)))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "segment %d from %s: %s", idx, host, body)
	return body
}

// requireModel checks a client's download of the archive against the
// model, and that no commit archived at or past seam repeats one archived
// before it: the handoff and a reclaim each resume ingest from a copied
// relay cursor, and a stale cursor or replay guard shows up as exactly that.
//
// It does not assert per-DID rev order (CheckInvariants): a plain local
// archive under stress can archive a commit older than the backfill or
// resync before it, on the commit before this migration existed too, so
// the order check would test that and not the migration.
func (r *migrationRig) requireModel(events []ObservedEvent, seam uint64, what string) {
	r.t.Helper()
	require.NoErrorf(r.t, CheckStructuralInvariants(events), "%s", what)
	want, err := GroundTruthFromWorld(r.w)
	require.NoError(r.t, err)
	got, err := Reconstruct(EventsSortedBySeq(events))
	require.NoError(r.t, err)
	if err := Compare(want, got); err != nil {
		r.logHistory(events, err)
		require.NoErrorf(r.t, err, "%s", what)
	}
	type commitKey struct {
		did, collection, rkey, rev string
		kind                       segment.Kind
	}
	before := map[commitKey]uint64{}
	for _, ev := range events {
		if ev.Rev == "" {
			continue
		}
		k := commitKey{ev.DID, ev.Collection, ev.Rkey, ev.Rev, ev.Kind}
		if ev.Seq < seam {
			before[k] = ev.Seq
		} else if prev, ok := before[k]; ok {
			err := fmt.Errorf("oracle: %s %s/%s rev %s archived at seq %d before the seam %d and again at %d", ev.DID, ev.Collection, ev.Rkey, ev.Rev, prev, seam, ev.Seq)
			r.logHistory(events, err)
			require.NoErrorf(r.t, err, "%s", what)
		}
	}
}

// handoffSeq is the catalog's migration/handoff_seq.
func (r *migrationRig) handoffSeq() uint64 {
	r.t.Helper()
	v, err := r.fake.DB.MetaStore(nil).Get(r.ctx, []byte(catalog.MigrationHandoffSeqKey))
	require.NoError(r.t, err, "the handoff committed")
	seq, err := catalog.DecodeSeq(catalog.MigrationHandoffSeqKey, v, true)
	require.NoError(r.t, err)
	return seq
}

// TestMigration_LocalToDisaggregated migrates a live local archive with
// vacancies, across a restart of the source, while a shadow pod serves the
// replica; hands off; and lets a pod carry on ingest. Every client view of
// the archive matches the model, seqs and vacancies carry over exactly, and
// every sealed segment is byte-identical in both places.
func TestMigration_LocalToDisaggregated(t *testing.T) {
	t.Parallel()
	r := newMigrationRig(t, 500)
	ctx := r.ctx

	// A local archive in steady state, stopped uncleanly once: a vacancy.
	r.startLocal(false, nil)
	r.waitSteady(r.local)
	r.generate(30)
	r.waitArchived()
	r.stop(r.local)
	r.abandonLease(50)

	// The migration seeds, then tails.
	require.NoError(t, r.fake.InitMigration(ctx))
	r.startLocal(true, nil)
	r.generate(30)
	r.waitArchived()
	first := r.waitTailing(1)
	require.NotZero(t, first.RemoteActiveSegment, "the seed imported sealed segments")

	// The source restarts uncleanly mid-migration: another vacancy, which
	// the migrator ships with the first block after it.
	r.stop(r.local)
	r.abandonLease(20)
	r.startLocal(true, nil)
	r.generate(30)
	r.waitArchived()
	st := r.waitTailing(first.RemoteNextSeq + 1)
	snap := r.catalogSnapshot()
	gaps, err := catalog.DecodeVacancies(snap.Meta[catalog.VacanciesKey], true)
	require.NoError(t, err)
	require.Equal(t, 2, gaps.Count(), "both vacancies are in the catalog: %v", gaps.Ranges())

	// A shadow pod serves the replica read-only, and never leads it.
	pod := r.startPod("pod-a")
	local := r.download(r.url(r.local), st.RemoteNextSeq-1)
	replica := r.download(r.url(pod), st.RemoteNextSeq-1)
	require.Equal(t, seqsOf(local[:len(replica)]), seqsOf(replica), "the replica serves the local archive's seqs")
	before := r.fake.DB.Archive().WriterEpoch

	// Byte equality of every sealed segment, through both servers.
	sealed := sealedMain(snap)
	require.NotEmpty(t, sealed)
	for _, idx := range sealed {
		want, err := vfsReadFile(r.fs, filepath.Join(r.dir, "segments", ingest.SegmentFilename(idx)))
		require.NoError(t, err)
		require.Truef(t, bytes.Equal(want, getSegment(t, r.url(r.local), idx)), "local segment %d", idx)
		require.Truef(t, bytes.Equal(want, getSegment(t, r.url(pod), idx)), "replica segment %d", idx)
	}

	// A client plans on the source and goes live on the pod, and keeps its
	// live tail across the handoff.
	split := r.startSplitClient(r.url(r.local), r.url(pod))

	// Handoff.
	result, err := r.fake.Backend.RequestMigration(ctx, migrate.ActionHandoff, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "done", result)
	snap = r.catalogSnapshot()
	require.Equal(t, []byte(catalog.MigrationDone), snap.Meta[catalog.MigrationStateKey])

	// The source is drained: it refuses subscribers and still serves its
	// archive.
	r.waitSubscribeRefused(r.url(r.local))
	require.NotEmpty(t, getSegment(t, r.url(r.local), sealed[0]))

	// A pod takes the lease and carries on from the copied relay cursor.
	r.generate(30)
	r.waitArchived()
	tip := r.committedTip()
	// The done transition is fenced, so the migrator held the lease to the
	// end; a pod that had led during the migration would show as more than
	// one acquisition here.
	require.Equal(t, before+1, r.catalogSnapshot().Archive.WriterEpoch, "the pod acquired once, after done")
	all := r.download(r.url(pod), tip)
	r.requireModel(all, r.handoffSeq(), "the migrated archive and the pod's ingest after it")
	r.requireModel(r.waitSplitClient(split, tip), r.handoffSeq(), "a client that planned on the source and went live on the pod")
	require.NoError(t, catalog.CheckInvariants(r.catalogSnapshot(), catalog.InvariantOptions{MaxEventsPerBlock: 1 << 16}))

	// A restart of the source stays drained and never ingests.
	r.stop(r.local)
	require.Equal(t, migrate.GuardDone, r.localGuard())
	localNext := r.localNext()
	r.startLocal(true, nil)
	r.generate(5)
	r.waitArchived()
	r.waitSubscribeRefused(r.url(r.local))
	r.stop(r.local)
	require.Equal(t, migrate.GuardDone, r.localGuard())
	require.Equal(t, localNext, r.localNext(), "the drained source ingested nothing")
}

// logHistory logs, for each DID err names, its events and who archived
// them.
func (r *migrationRig) logHistory(events []ObservedEvent, err error) {
	r.archivedMu.Lock()
	defer r.archivedMu.Unlock()
	for _, ev := range events {
		if strings.Contains(err.Error(), ev.DID) {
			r.t.Logf("history: seq=%d by=%q kind=%d %s/%s rev=%s", ev.Seq, r.archivedBy[ev.Seq], ev.Kind, ev.Collection, ev.Rkey, ev.Rev)
		}
	}
}

func httpGet(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

func seqsOf(evs []ObservedEvent) []uint64 {
	out := make([]uint64, len(evs))
	for i, ev := range evs {
		out[i] = ev.Seq
	}
	return out
}

func sealedMain(snap *catalog.Snapshot) []uint64 {
	var out []uint64
	for _, s := range snap.Segments {
		if s.Namespace == catalog.Main && s.State == catalog.Sealed {
			out = append(out, s.Index)
		}
	}
	slices.Sort(out)
	return out
}

func vfsReadFile(fs vfs.FS, path string) ([]byte, error) {
	f, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return io.ReadAll(f)
}

// killInjector kills the local process at one crash point: the runtime's
// context ends, as a SIGKILL ends every goroutine, and the seam returns.
// Every Pebble write is synced, so what a kill leaves on disk is what was
// committed.
type killInjector struct {
	target crashpoint.Point
	kill   atomic.Pointer[context.CancelFunc]
	fired  atomic.Bool
}

func (i *killInjector) SimulateCrash(_ context.Context, p crashpoint.Point) error {
	if p != i.target || !i.fired.CompareAndSwap(false, true) {
		return nil
	}
	(*i.kill.Load())()
	return fmt.Errorf("migration test: killed at %s", p)
}

// waitSteady waits until p serves its archive, which it does from
// steady_state on: a migration only starts from there. Traffic generated
// after it reaches the steady consumer, so the migration tests do not
// depend on the bootstrap cutover.
func (r *migrationRig) waitSteady(p *rigRuntime) {
	r.t.Helper()
	require.Eventually(r.t, func() bool {
		// Until then /subscribe answers 503; after, a plain GET is refused
		// for not being a websocket upgrade.
		resp, err := httpGet(r.ctx, r.url(p)+"/subscribe")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode != http.StatusServiceUnavailable
	}, 60*time.Second, 10*time.Millisecond, "%s reaches steady_state", p.name)
}

// waitSubscribeRefused waits until host's /subscribe answers 503.
func (r *migrationRig) waitSubscribeRefused(host string) {
	r.t.Helper()
	require.Eventually(r.t, func() bool {
		resp, err := httpGet(r.ctx, host+"/subscribe")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusServiceUnavailable
	}, 10*time.Second, 10*time.Millisecond)
}

// podCarriesOn starts a pod, generates traffic, and checks the whole
// archive through the pod against the model.
func (r *migrationRig) podCarriesOn(what string) {
	r.t.Helper()
	pod := r.startPod("pod-a")
	r.generate(20)
	r.waitArchived()
	r.requireModel(r.download(r.url(pod), r.committedTip()), r.handoffSeq(), what)
	require.NoError(r.t, catalog.CheckInvariants(r.catalogSnapshot(), catalog.InvariantOptions{MaxEventsPerBlock: 1 << 16}))
}

func (r *migrationRig) migrationState() catalog.MigrationState {
	r.t.Helper()
	st, err := catalog.ParseMigrationState(r.catalogSnapshot().Meta[catalog.MigrationStateKey])
	require.NoError(r.t, err)
	return st
}

// TestMigration_CrashSeams kills the source at each migration crash point
// and restarts it on the same disk. Before the done commit, the restart
// reverts and a later handoff succeeds; after it, the source comes back
// drained and never ingests again. Either way the pods' archive matches
// the model.
func TestMigration_CrashSeams(t *testing.T) {
	t.Parallel()
	handoff := map[crashpoint.Point]bool{
		crashpoint.AfterMigrationHandingOff:    true,
		crashpoint.AfterMigrationIngestStopped: true,
		crashpoint.AfterMigrationFinalShip:     true,
		crashpoint.AfterMigrationGuardPending:  true,
		crashpoint.AfterMigrationDone:          true,
	}
	points := []crashpoint.Point{
		crashpoint.AfterMigrationSegmentUpload,
		crashpoint.AfterMigrationSegmentImport,
		crashpoint.AfterMigrationActiveUpload,
		crashpoint.AfterMigrationHandingOff,
		crashpoint.AfterMigrationIngestStopped,
		crashpoint.AfterMigrationFinalShip,
		crashpoint.AfterMigrationGuardPending,
		crashpoint.AfterMigrationDone,
	}
	for i, point := range points {
		t.Run(string(point), func(t *testing.T) {
			t.Parallel()
			r := newMigrationRig(t, 600+i)
			ctx := r.ctx
			r.startLocal(false, nil)
			r.waitSteady(r.local)
			r.generate(30)
			r.waitArchived()
			r.stop(r.local)
			r.abandonLease(10)
			require.NoError(t, r.fake.InitMigration(ctx))

			inj := &killInjector{target: point}
			r.startLocal(true, inj)
			r.generate(20)
			var req int64
			if handoff[point] {
				r.waitArchived()
				r.waitTailing(1)
				req = r.fake.Control.Request(migrate.ActionHandoff)
			}
			require.Eventuallyf(t, inj.fired.Load, 30*time.Second, 5*time.Millisecond, "crash point reached; request %d: %s; status %+v",
				req, func() string { res, _, _ := r.fake.Control.Result(req); return res }(), r.status())
			r.stop(r.local)

			guard, state := r.localGuard(), r.migrationState()
			switch point {
			case crashpoint.AfterMigrationDone:
				require.Equal(t, migrate.GuardPending, guard)
				require.Equal(t, catalog.MigrationDone, state)
			case crashpoint.AfterMigrationGuardPending:
				require.Equal(t, migrate.GuardPending, guard)
				require.Equal(t, catalog.MigrationHandingOff, state)
			default:
				require.Equal(t, migrate.GuardNone, guard)
				if handoff[point] {
					require.Equal(t, catalog.MigrationHandingOff, state)
				}
			}

			if point == crashpoint.AfterMigrationDone {
				// The handoff committed: the restarted source settles its
				// guard, serves reads only, and never ingests again.
				localNext := r.localNext()
				r.startLocal(true, nil)
				r.waitSubscribeRefused(r.url(r.local))
				r.podCarriesOn("a pod after a kill at the done commit")
				r.stop(r.local)
				require.Equal(t, migrate.GuardDone, r.localGuard())
				require.Equal(t, localNext, r.localNext(), "the source never ingested after the handoff")
				return
			}

			// The restart reverts or resumes, and the migration completes.
			r.startLocal(true, nil)
			r.generate(20)
			r.waitArchived()
			r.waitTailing(1)
			result, err := r.fake.Backend.RequestMigration(ctx, migrate.ActionHandoff, time.Minute)
			require.NoError(t, err)
			require.Equal(t, "done", result)
			require.Equal(t, migrate.GuardDone, func() migrate.Guard {
				r.stop(r.local)
				return r.localGuard()
			}())
			r.podCarriesOn("a pod after a kill at " + string(point))
		})
	}
}

// TestMigration_Reclaim hands off, lets a pod ingest, and then rolls back:
// with the pods stopped, reclaim reverts the catalog and moves the local
// seq lease past every seq the catalog assigned. The source resumes from
// its own relay cursor, so its archive matches the model again, with no
// seq reused, and no pod leads the reverted catalog.
func TestMigration_Reclaim(t *testing.T) {
	t.Parallel()
	r := newMigrationRig(t, 700)
	ctx := r.ctx
	r.startLocal(false, nil)
	r.waitSteady(r.local)
	r.generate(30)
	r.waitArchived()
	r.stop(r.local)
	require.NoError(t, r.fake.InitMigration(ctx))
	r.startLocal(true, nil)
	r.generate(20)
	r.waitArchived()
	r.waitTailing(1)
	result, err := r.fake.Backend.RequestMigration(ctx, migrate.ActionHandoff, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "done", result)
	r.stop(r.local)
	r.local = nil

	pod := r.startPod("pod-a")
	r.generate(20)
	r.waitArchived()
	r.committedTip()

	// Reclaim refuses while a pod holds the lease.
	_, err = r.fake.Backend.ReclaimLocal(ctx, time.Minute, r.dir, r.fs, 0)
	require.ErrorContains(t, err, "take the writer lease")
	r.stop(pod)
	r.pods = nil
	_, err = r.fake.Backend.ReclaimLocal(ctx, time.Minute, r.dir, r.fs, seqspace.CursorSeqMaxThreshold)
	require.ErrorContains(t, err, "seq ceiling")
	require.Equal(t, catalog.MigrationDone, r.migrationState(), "a bad margin changes nothing")
	_, err = r.fake.Backend.ReclaimLocal(ctx, time.Minute, r.dir, r.fs, seqspace.CursorSeqMaxThreshold-10)
	require.ErrorContains(t, err, "seq ceiling")
	res, err := r.fake.Backend.ReclaimLocal(ctx, time.Minute, r.dir, r.fs, 1000)
	require.NoError(t, err)
	require.Equal(t, catalog.MigrationReverted, r.migrationState())
	require.Greater(t, res.ResumeSeq, res.CatalogNext)
	again, err := r.fake.Backend.ReclaimLocal(ctx, time.Minute, r.dir, r.fs, 1000)
	require.NoError(t, err, "reclaim is idempotent")
	require.Equal(t, res.ResumeSeq, again.ResumeSeq, "a second reclaim moves nothing")
	require.Equal(t, migrate.GuardNone, r.localGuard())

	// The local archive carries on; a pod stands by on the reverted catalog.
	epoch := r.fake.DB.Archive().WriterEpoch
	r.startLocal(false, nil)
	pod = r.startPod("pod-b")
	r.generate(20)
	r.waitArchived()
	// Every seq the source assigns now is past the pods', so the highest
	// archived seq is the source's.
	local := r.download(r.url(r.local), r.archivedTip())
	r.requireModel(local, res.ResumeSeq, "the reclaimed local archive")
	for _, ev := range local {
		require.Falsef(t, ev.Seq >= res.LocalNext && ev.Seq < res.ResumeSeq, "seq %d is in the reclaim vacancy", ev.Seq)
	}
	require.Greater(t, local[len(local)-1].Seq, res.CatalogNext, "the source assigns past the catalog")
	require.Equal(t, epoch, r.fake.DB.Archive().WriterEpoch, "no pod led the reverted catalog")
	require.Equal(t, catalog.MigrationReverted, r.migrationState())
	_ = pod
}
