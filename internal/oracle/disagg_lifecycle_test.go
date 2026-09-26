package oracle

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/follower"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/segment"
)

// The layer 3 oracle's lifecycle prelude (plan S3.5). The pods start from
// a catalog `storage init` just created and run bootstrap, merge, and
// steady state on it, the leader killed in each phase:
//
//   - Bootstrap: a kill after a repo completes, at each direct-mode block
//     seam (cut, upload, commit reported), at a seal, a block commit that
//     lands but reports failure, and lease loss. Backfill may list only
//     its first PDS until these have fired, so it cannot finish under them.
//   - Merging: a kill between closing bootstrap_live and sealing it, and at
//     every merge crashpoint in order.
//   - Steady state: the waves in disagg_oracle_test.go.
//
// Like the local lifecycle harness (harness_test.go), the oracle checks the
// catalog at two boundaries against the uncompacted model (D5). At cutover,
// main and bootstrap_live together must reconstruct the world, less the
// repos a kill deferred to merge's pending pass. After merge, main alone
// must reconstruct all of it, with bootstrap_live and its keys gone. Main
// as merge left it is then the model's prefix for the steady-state checks.

const (
	// disaggBootMaxFrames bounds the upstream frames a bootstrap fault may
	// take to fire.
	disaggBootMaxFrames = 64
	// disaggBootPolls is how many 10ms polls follow each frame.
	disaggBootPolls = 20
	// disaggRepoCompletePolls is how many follow a repo-complete fault's
	// first frame. A completion row commits with main's next block, and
	// direct mode cuts main's blocks only when full or when backfill's
	// periodic durability drain runs, every 30s.
	disaggRepoCompletePolls = 4000

	disaggMergeFaultPrefix = "merge/"
	disaggBootFaultPrefix  = "bootstrap/"
)

// initCatalog runs `jetstream storage init` on the empty fake.
func (h *disaggHarness) initCatalog() {
	b := &jetstreamd.StorageBackend{
		DB:       h.db,
		Listener: h.db,
		Blob:     h.blob,
		Archive:  func(context.Context) (catalog.ArchiveRow, error) { return h.db.Archive(), nil },
		// The fake starts with its archive row.
		CreateArchive: func(context.Context, [16]byte) error { return nil },
		NewLease:      func() leader.Locker { return h.db.NewLease() },
		MetaStore:     h.db.MetaStore,
	}
	_, err := b.Init(h.ctx, time.Minute)
	require.NoError(h.t, err, "storage init")
}

// runLifecycle starts the pods and takes the catalog from bootstrap to
// steady state, firing the plan's bootstrap and merge faults. It leaves
// h.expected holding main as merge left it.
func (h *disaggHarness) runLifecycle() {
	t := h.t
	plan := h.plan

	// The bootstrap-live consumer replays the firehose from seq 1; backfill
	// supersedes most of these.
	h.stage = "bootstrap"
	for range plan.pre {
		h.emit()
	}
	fired, mid := h.arm(plan.bootFaults[0])
	for range plan.readers {
		h.startPod(true)
	}
	for range plan.leaders {
		h.startPod(false)
	}
	// Each fault hits a session that started after the one the last fault
	// was armed in; the first is armed before any session.
	armedIn := 1
	for i, f := range plan.bootFaults {
		if i > 0 {
			h.awaitSessions(armedIn, string(f))
			armedIn = len(h.sessionList())
			fired, mid = h.arm(f)
		}
		h.bootFault(f, fired, mid)
	}
	// A fault may fire before backfill reaches the gate.
	deadline := time.Now().Add(disaggConvergeTimeout)
	for !h.gate.holding() {
		h.reap()
		h.checkPods()
		if time.Now().After(deadline) {
			h.failf("backfill did not reach the listRepos gate after %s", disaggConvergeTimeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if phase := h.readPhase(); phase != lifecycle.PhaseBootstrap {
		h.failf("the phase is %q after the bootstrap faults; backfill finished under them", phase)
	}

	// Cutover. These events are newer than every repo backfill
	// downloaded, so all of them survive merge.
	h.gate.release()
	pod := h.await("the cutover barrier", h.cutoverReached)
	for range plan.cutover {
		h.survivors = append(h.survivors, h.emit()...)
	}
	h.awaitArchived("cutover")
	t.Logf("cutover: %s holds the barrier; %d rows generated since backfill", pod, len(h.survivors))

	h.stage = "merging"
	merge := plan.mergeFaults
	if merge[0] == dfCrashCloseBeforeSeal {
		fired, _ := h.arm(merge[0])
		close(h.cutoverGo)
		// No replacement until the check: the catalog stays as the killed
		// leader left it until the lease runs out.
		h.awaitFired(merge[0], fired, false)
		h.checkBootstrap()
		merge = merge[1:]
	} else {
		close(h.cutoverGo)
		h.await("the after-bootstrap barrier", h.bootBarrier)
		h.checkBootstrap()
	}
	for _, f := range merge {
		fired, _ := h.arm(f)
		h.resumeBoot()
		h.awaitFired(f, fired, true)
	}
	h.resumeBoot()

	h.awaitMerged()
	h.checkMerged()
	h.stage = "steady"
}

// emit generates one upstream frame outside the model, and returns the
// keys of the rows it should archive.
func (h *disaggHarness) emit() []disaggKey {
	t := h.t
	frame, err := h.w.GenerateOneForTest(h.ctx)
	require.NoError(t, err)
	evt, err := decodeOracleFirehoseFrame(frame)
	require.NoError(t, err)
	rows, err := expectedSegmentEventsFromFirehoseEvent(h.w, evt)
	require.NoError(t, err)
	keys := make([]disaggKey, len(rows))
	for i, row := range rows {
		keys[i] = disaggKeyOf(observedFromSegment(row))
		keys[i].Seq = 0
	}
	h.reap()
	time.Sleep(time.Duration(1+h.rng.IntN(8)) * time.Millisecond)
	return keys
}

// bootFault drives upstream frames until f fires.
func (h *disaggHarness) bootFault(f disaggFault, fired func() bool, mid func()) {
	for n := 0; !fired(); n++ {
		if n == disaggBootMaxFrames {
			h.failf("bootstrap fault %s did not fire after %d frames", f, n)
		}
		h.emit()
		if n == 0 && mid != nil {
			mid()
		}
		polls := disaggBootPolls
		if f == dfCrashRepoComplete && n == 0 {
			polls = disaggRepoCompletePolls
		}
		for range polls {
			if fired() {
				break
			}
			h.reap()
			h.checkPods()
			time.Sleep(10 * time.Millisecond)
		}
	}
	h.fired = append(h.fired, disaggBootFaultPrefix+string(f))
	h.t.Logf("bootstrap fault %s fired; sessions %v", f, h.sessionList())
}

// awaitFired waits for f with no traffic. With reap false a killed leader
// is not replaced.
func (h *disaggHarness) awaitFired(f disaggFault, fired func() bool, reap bool) {
	deadline := time.Now().Add(disaggConvergeTimeout)
	for !fired() {
		if reap {
			h.reap()
		}
		h.checkPods()
		if time.Now().After(deadline) {
			h.failf("merge fault %s did not fire after %s", f, disaggConvergeTimeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.fired = append(h.fired, disaggMergeFaultPrefix+string(f))
	h.t.Logf("merge fault %s fired; sessions %v", f, h.sessionList())
	if f == dfCrashMergeFlush {
		h.mergeReplay = true
	}
}

// awaitSessions waits until more than n leader sessions have started.
func (h *disaggHarness) awaitSessions(n int, what string) {
	deadline := time.Now().Add(disaggConvergeTimeout)
	for len(h.sessionList()) <= n {
		h.reap()
		h.checkPods()
		if time.Now().After(deadline) {
			h.failf("before %s: no leader session started after %d", what, n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// await receives the pod that reached a barrier.
func (h *disaggHarness) await(what string, reached <-chan string) string {
	deadline := time.Now().Add(disaggConvergeTimeout)
	for {
		select {
		case pod := <-reached:
			return pod
		default:
		}
		h.reap()
		h.checkPods()
		if time.Now().After(deadline) {
			h.failf("no leader reached %s after %s", what, disaggConvergeTimeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// resumeBoot releases the after-bootstrap barrier, once.
func (h *disaggHarness) resumeBoot() {
	h.resumeOnce.Do(func() { close(h.bootResume) })
}

// disaggBarrier is a leader's phase barrier: the first to reach it reports
// in and waits for resume. Once resume is closed it no longer stops.
func disaggBarrier(reached chan<- string, resume <-chan struct{}, pod string) jetstreamd.PhaseBarrier {
	return func(ctx context.Context) error {
		select {
		case <-resume:
			return nil
		case reached <- pod:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-resume:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (h *disaggHarness) readPhase() lifecycle.Phase {
	phase, err := lifecycle.ReadPhase(h.ctx, h.db.MetaStore(nil))
	require.NoError(h.t, err)
	return phase
}

// awaitArchived waits until main then bootstrap_live reconstructs the
// world and holds every row generated after backfill. The relay cursor
// cannot serve: it is sampled when a block is cut, so it trails the last
// frame until another block follows.
func (h *disaggHarness) awaitArchived(what string) {
	deadline := time.Now().Add(disaggConvergeTimeout)
	for {
		why := h.archivedPending()
		if why == "" {
			return
		}
		h.reap()
		h.checkPods()
		if time.Now().After(deadline) {
			h.failf("%s: not archived after %s: %s", what, disaggConvergeTimeout, why)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// archivedPending says why the catalog does not yet hold every upstream
// frame, or "".
func (h *disaggHarness) archivedPending() string {
	got := h.readCatalog()
	if k, ok := h.missingSurvivor(got[catalog.BootstrapLive]); ok {
		return "bootstrap_live lacks " + k
	}
	ground, model, err := h.cutoverModels(got)
	if err != nil {
		return err.Error()
	}
	if err := Compare(ground, model); err != nil {
		return err.Error()
	}
	return ""
}

// cutoverModels returns the world and what main then bootstrap_live
// reconstruct, both without the repos a bootstrap kill deferred.
// Backfill defers a repo it discovered before a kill and had not finished
// to merge's pending pass (#262), so at cutover the catalog holds only the
// firehose's rows for it. The after-merge check covers those repos.
func (h *disaggHarness) cutoverModels(got map[catalog.Namespace][]segment.Event) (ground, model *Model, err error) {
	ground, err = GroundTruthFromWorld(h.w)
	require.NoError(h.t, err)
	model, err = Reconstruct(disaggObserved(slices.Concat(got[catalog.Main], got[catalog.BootstrapLive])))
	if err != nil {
		return nil, nil, err
	}
	for did := range h.deferredRepos() {
		delete(ground.Accounts, did)
		delete(model.Accounts, did)
	}
	return ground, model, nil
}

// deferredRepos is the set of repos backfill left pending.
func (h *disaggHarness) deferredRepos() map[string]bool {
	it, err := h.db.MetaStore(nil).NewIter(h.ctx, []byte("repo/"), []byte("repo0"))
	require.NoError(h.t, err)
	defer func() { _ = it.Close() }()
	out := map[string]bool{}
	for it.Next() {
		rs, err := backfill.DecodeRepoStatus(it.Value())
		require.NoErrorf(h.t, err, "decode %s", it.Key())
		if rs.Backfill.Status == backfill.StatusPending {
			out[strings.TrimPrefix(string(it.Key()), "repo/")] = true
		}
	}
	require.NoError(h.t, it.Err())
	return out
}

// catalogMark summarizes what a leader writes: any change between two
// marks means the catalog moved.
func (h *disaggHarness) catalogMark() string {
	snap, err := h.db.Snapshot()
	require.NoError(h.t, err)
	var b strings.Builder
	fmt.Fprintf(&b, "epoch=%d segments=%d generations=%d blocks=%d active=%d hot=%d objects=%d",
		snap.Archive.WriterEpoch, len(snap.Segments), len(snap.Generations), len(snap.GenerationBlocks),
		len(snap.ActiveBlocks), len(snap.HotBatches), len(snap.Objects))
	keys := make([]string, 0, len(snap.Meta))
	for k := range snap.Meta {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, " %s=%s", k, hex.EncodeToString(snap.Meta[k]))
	}
	return b.String()
}

// readCatalog reads main and bootstrap_live from seq 1 through a fresh
// follower, the read path every pod serves from.
func (h *disaggHarness) readCatalog() map[catalog.Namespace][]segment.Event {
	t := h.t
	ctx := h.ctx
	f, err := follower.New(follower.Config{
		DB:              h.db.Client("oracle-check"),
		Blob:            h.blob,
		ArchiveID:       h.db.Archive().ArchiveID,
		Logger:          slog.New(slog.DiscardHandler),
		Metrics:         follower.NewMetrics(prometheus.NewRegistry()),
		ReadConcurrency: 4,
	})
	require.NoError(t, err)
	require.NoError(t, f.Refresh(ctx))
	view := f.Snapshot()
	out := map[catalog.Namespace][]segment.Event{}
	for _, ns := range []catalog.Namespace{catalog.Main, catalog.BootstrapLive} {
		for ref := range view.RefsFrom(ns, 1) {
			evs, err := catalog.DecodeRef(ctx, f, ref)
			require.NoErrorf(t, err, "read %s segment %d block %d", ns, ref.Segment, ref.Block)
			for _, ev := range evs {
				ev.Payload = bytes.Clone(ev.Payload)
				out[ns] = append(out[ns], ev)
			}
		}
	}
	return out
}

func disaggObserved(evs []segment.Event) []ObservedEvent {
	out := make([]ObservedEvent, len(evs))
	for i, ev := range evs {
		out[i] = observedFromSegment(ev)
	}
	return out
}

// requireDense fails unless evs' seqs run 1, 2, ... with no gap.
func (h *disaggHarness) requireDense(evs []segment.Event, what string) {
	for i, ev := range evs {
		if ev.Seq != uint64(i+1) {
			h.failf("%s: row %d has seq %d; seqs must be dense from 1", what, i, ev.Seq)
		}
	}
}

// requireSurvivors fails unless every row generated after backfill is in
// evs: merge's rev filter keeps rows newer than every download.
func (h *disaggHarness) requireSurvivors(evs []segment.Event, what string) {
	if k, ok := h.missingSurvivor(evs); ok {
		h.failf("%s: a row generated after backfill is missing: %s", what, k)
	}
}

// missingSurvivor names a row generated after backfill that evs lacks.
func (h *disaggHarness) missingSurvivor(evs []segment.Event) (string, bool) {
	have := make(map[disaggKey]bool, len(evs))
	for _, ev := range evs {
		k := disaggKeyOf(observedFromSegment(ev))
		k.Seq = 0
		have[k] = true
	}
	for _, k := range h.survivors {
		if !have[k] {
			return disaggKeyAt([]disaggKey{k}, 0), true
		}
	}
	return "", false
}

// checkBootstrap is the cutover check (harness_test.go's
// assertBootstrapOracleMatches): each namespace is dense and clean, and
// main followed by bootstrap_live reconstructs the world. Bootstrap_live
// replays the firehose from seq 1, so replaying it over backfill's
// snapshots leaves each record as its last event did. The caller has
// stopped the writer: the leader waits at a barrier or is dead.
func (h *disaggHarness) checkBootstrap() {
	t := h.t
	before := h.catalogMark()
	got := h.readCatalog()
	if after := h.catalogMark(); after != before {
		h.failf("the catalog moved during the bootstrap check:\n  before %s\n  after  %s", before, after)
	}
	if phase := h.readPhase(); phase != lifecycle.PhaseMerging {
		h.failf("the phase at cutover is %q", phase)
	}
	main, live := got[catalog.Main], got[catalog.BootstrapLive]
	require.NotEmpty(t, main, "backfill archived nothing")
	require.NotEmpty(t, live, "bootstrap-live archived nothing")
	for ns, evs := range got {
		h.requireDense(evs, "bootstrap "+string(ns))
		require.NoErrorf(t, CheckInvariants(disaggObserved(evs)), "bootstrap %s", ns)
	}
	h.requireSurvivors(live, "bootstrap_live at cutover")
	ground, model, err := h.cutoverModels(got)
	require.NoError(t, err)
	require.NoError(t, Compare(ground, model), "main then bootstrap_live at cutover")
	t.Logf("bootstrap check: main %d rows, bootstrap_live %d rows, %d repos deferred",
		len(main), len(live), len(h.deferredRepos()))
}

// awaitMerged waits for merge's final transaction to have committed and
// every merge fault's successor to have run it.
func (h *disaggHarness) awaitMerged() {
	deadline := time.Now().Add(disaggConvergeTimeout)
	for {
		h.reap()
		h.checkPods()
		why := h.mergePending()
		if why == "" {
			return
		}
		if time.Now().After(deadline) {
			h.failf("merge did not finish after %s: %s", disaggConvergeTimeout, why)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// mergePending says why the catalog is not as merge's final transaction
// leaves it, or "".
func (h *disaggHarness) mergePending() string {
	snap, err := h.db.Snapshot()
	require.NoError(h.t, err)
	if phase := h.readPhase(); phase != lifecycle.PhaseSteadyState {
		return fmt.Sprintf("phase %q", phase)
	}
	for _, row := range snap.Segments {
		if row.Namespace == catalog.BootstrapLive {
			return fmt.Sprintf("bootstrap_live segment %d survived merge", row.Index)
		}
	}
	if _, ok := snap.Meta[catalog.BootstrapLiveSeqKey]; ok {
		return catalog.BootstrapLiveSeqKey + " survived merge"
	}
	_, found, err := metastore.GetUint64LE(h.ctx, h.db.MetaStore(nil), "merge/next_source_idx")
	require.NoError(h.t, err)
	if found {
		return "the merge cursor survived merge"
	}
	return ""
}

// checkMerged is the after-merge check: main alone is dense, clean unless a
// kill replayed a merge source, holds every row generated after backfill,
// and reconstructs the world. It becomes the model's prefix.
func (h *disaggHarness) checkMerged() {
	t := h.t
	got := h.readCatalog()
	main := got[catalog.Main]
	require.Empty(t, got[catalog.BootstrapLive], "bootstrap_live rows after merge")
	h.requireDense(main, "main after merge")
	require.Equal(t, uint64(len(main))+1, h.mainNext(), "main's seq/next after merge")
	obs := disaggObserved(main)
	require.NoError(t, CheckStructuralInvariants(obs), "main after merge")
	if !h.mergeReplay {
		// A kill after a source's rows are flushed and before its cursor
		// commits re-drains that source at new seqs, which regresses revs
		// (CheckInvariants).
		require.NoError(t, CheckInvariants(obs), "main after merge")
	}
	h.requireSurvivors(main, "main after merge")
	ground, err := GroundTruthFromWorld(h.w)
	require.NoError(t, err)
	model, err := Reconstruct(obs)
	require.NoError(t, err)
	require.NoError(t, Compare(ground, model), "main after merge")
	h.expected = main
	h.mergedRows = len(main)
	t.Logf("merge check: main %d rows (replayed source: %t)", len(main), h.mergeReplay)
}

// disaggListGate lets backfill list the first PDS it asks and holds every
// listRepos request to any other until release, so no bootstrap session can
// finish enumerating while the faults fire. The oracle's backfill lists one
// host at a time and dispatches a host's downloads once it has listed it
// all, so the first host's repos download and complete under the faults.
// Each later session skips the drained first host and asks a held one. A
// held request ends when its caller gives up, as a killed leader's does.
type disaggListGate struct {
	next http.Handler

	mu    sync.Mutex
	first string

	held     atomic.Int64
	released chan struct{}
	once     sync.Once
}

func newDisaggListGate(next http.Handler) *disaggListGate {
	return &disaggListGate{next: next, released: make(chan struct{})}
}

func (d *disaggListGate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/xrpc/com.atproto.sync.listRepos" && d.holds(r.Host) {
		d.held.Add(1)
		select {
		case <-d.released:
		case <-r.Context().Done():
			return
		}
	}
	d.next.ServeHTTP(w, r)
}

// holds reports whether a listRepos request to host waits for release.
// The relay's own listRepos is never held.
func (d *disaggListGate) holds(host string) bool {
	if host == strings.TrimPrefix(disaggSimURL, "http://") {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.first == "" {
		d.first = host
	}
	return host != d.first
}

// holding reports whether a listRepos request has reached the gate.
func (d *disaggListGate) holding() bool { return d.held.Load() > 0 }

func (d *disaggListGate) release() { d.once.Do(func() { close(d.released) }) }
