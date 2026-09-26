package oracle

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/streaming"
	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream"
	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/bluesky-social/jetstream/segment"
)

// The layer 3 oracle (plans S2.18 and S3.5, design "Layer 3: deterministic
// disaggregated oracle"). Real jetstreamd pods share one storagefake catalog
// and one memblob store inside a synctest bubble. Reader pods never take the
// lease and serve v1 and v2 subscribers; leader-capable pods contend for it
// and are killed at seeded faults. Every storage and object store call is a
// yield point of the seeded scheduler (D4).
//
// The pods start from a catalog `storage init` just created and run the
// whole lifecycle (disagg_lifecycle_test.go): bootstrap and merge, with the
// leader killed in each, then steady-state waves of faults.
//
// What it proves: across leader kills at every direct-mode and hot-path
// seam and every merge crashpoint, lease loss, a fenced stale leader, S3 PUT
// failures and wrong bytes, lost NOTIFYs, and a slow follower, the catalog
// at cutover and after merge reconstructs the world; every reader delivers
// the model's events on both wire protocols with dense seqs and each DID's
// order kept, readers agree with each other exactly, every live pod serves
// the same archive to the real client, and the catalog invariants hold
// after every commit.
//
// What it does not prove: PostgreSQL and S3 semantics. storagefake models
// them (the design lists its deliberate differences); layer 4
// (`just test-storage`) checks the real ones. Nor is the interleaving
// replayable: the scheduler orders parked calls by (actor, arrival), and
// arrival within one pod depends on Go's scheduler. The determinism test
// therefore checks the seeded fault schedule and the outcome, not the
// scheduler trace (the D4 fallback, specs/oracle.md).
//
// The bubble must be the only one in its process (synctestBubbleUsed), so
// TestDisagg_Oracle re-runs this binary once per seed.

const (
	envDisaggChild  = "JETSTREAM_ORACLE_DISAGG_CHILD"
	envDisaggSeed   = "JETSTREAM_ORACLE_DISAGG_SEED"
	envDisaggMode   = "JETSTREAM_ORACLE_DISAGG_MODE"
	envDisaggSeeds  = "JETSTREAM_ORACLE_DISAGG_SEEDS"
	envDisaggResult = "JETSTREAM_ORACLE_DISAGG_RESULT"

	disaggModeShort = "short"
	disaggModeFull  = "full"

	// disaggChildTimeout is a deadlock guard on the wall clock; a healthy
	// full run takes a few seconds, more under -race.
	disaggChildTimeout = 10 * time.Minute
	// disaggConvergeTimeout bounds each wait on the fake clock. The slowest
	// recovery is a killed leader's lease running out (3s) plus a slow read.
	disaggConvergeTimeout = 2 * time.Minute
	// disaggServeTimeout bounds, on the fake clock, how long pods may take
	// to serve a catalog that holds every model row and has stopped
	// changing. A committed batch is due within a catalog poll; the bound
	// allows a slow read (2s) and retries, and sits well under
	// BlockMaxAge, whose fold would otherwise reveal a batch the follower
	// failed to serve.
	disaggServeTimeout = 10 * time.Second
	// disaggMaxExtraChunks bounds the extra events a wave generates while
	// its fault has not fired yet.
	disaggMaxExtraChunks = 8
	disaggExtraChunk     = 8

	disaggSimURL = "http://sim.invalid"
)

// TestDisagg_Oracle runs the layer 3 oracle, one child process per seed.
// -short runs one small seed; otherwise JETSTREAM_ORACLE_DISAGG_SEEDS
// (default 1,2,3) run the full fault mix. JETSTREAM_ORACLE_DISAGG_MODE
// overrides the mode.
//
// nolint:paralleltest // the per-seed subtests are parallel; each is its own process.
func TestDisagg_Oracle(t *testing.T) {
	if os.Getenv(envDisaggChild) == "1" {
		t.Skip("a disaggregated oracle child does not re-run the parent")
	}
	mode, seeds := disaggModeFull, "1,2,3"
	if testing.Short() {
		mode, seeds = disaggModeShort, "1"
	}
	if v := os.Getenv(envDisaggMode); v != "" {
		mode = v
	}
	if v := os.Getenv(envDisaggSeeds); v != "" {
		seeds = v
	}
	for seed := range strings.SplitSeq(seeds, ",") {
		seed = strings.TrimSpace(seed)
		t.Run("seed="+seed, func(t *testing.T) {
			t.Parallel()
			res := runDisaggChild(t, seed, mode)
			t.Logf("seed %s (%s): readers=%d leaders=%d fired=%v events=%d sessions=%d scheduler turns=%d",
				seed, mode, res.Readers, res.Leaders, res.Fired, res.Events, res.Sessions, res.Turns)
		})
	}
}

// TestDisagg_Determinism runs one seed twice and requires the same fault
// schedule and the same faults fired, in order. The delivered stream and
// the scheduler trace are logged, not compared (D4 fallback; see
// disaggResult.deterministic).
//
// nolint:paralleltest // spawns two child processes.
func TestDisagg_Determinism(t *testing.T) {
	if os.Getenv(envDisaggChild) == "1" {
		t.Skip("a disaggregated oracle child does not re-run the parent")
	}
	if testing.Short() {
		t.Skip("spawns two full disaggregated oracle runs")
	}
	const seed = "11"
	first := runDisaggChild(t, seed, disaggModeFull)
	second := runDisaggChild(t, seed, disaggModeFull)
	require.Equal(t, first.deterministic(), second.deterministic(),
		"the same seed must give the same fault schedule and fired faults")
	t.Logf("seed %s: streams %s and %s (%d and %d rows), scheduler traces %s (%d turns) and %s (%d turns); the interleaving is not claimed replayable",
		seed, first.Stream, second.Stream, first.MainNext-1, second.MainNext-1, first.Trace, first.Turns, second.Trace, second.Turns)
}

// TestDisagg_OracleChild is one seed's run. It runs only when re-executed by
// TestDisagg_Oracle or TestDisagg_Determinism. To reproduce a failure:
//
//	JETSTREAM_ORACLE_DISAGG_CHILD=1 JETSTREAM_ORACLE_DISAGG_SEED=<seed> \
//	JETSTREAM_ORACLE_DISAGG_MODE=full go test ./internal/oracle \
//	-run '^TestDisagg_OracleChild$' -v
//
// nolint:paralleltest // one synctest bubble per process.
func TestDisagg_OracleChild(t *testing.T) {
	if os.Getenv(envDisaggChild) != "1" {
		t.Skip("runs only as a TestDisagg_Oracle child process")
	}
	if synctestBubbleUsed.Swap(true) {
		t.Fatal("the disaggregated oracle needs the process's only synctest bubble")
	}
	seed, err := strconv.ParseUint(os.Getenv(envDisaggSeed), 10, 64)
	require.NoError(t, err, envDisaggSeed)
	mode := os.Getenv(envDisaggMode)
	require.Contains(t, []string{disaggModeShort, disaggModeFull}, mode, envDisaggMode)

	var res disaggResult
	synctest.Test(t, func(t *testing.T) {
		res = runDisaggOracle(t, seed, mode)
	})
	if path := os.Getenv(envDisaggResult); path != "" && !t.Failed() {
		b, err := json.MarshalIndent(res, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, b, 0o600))
	}
}

func runDisaggChild(t *testing.T, seed, mode string) disaggResult {
	t.Helper()
	path := filepath.Join(t.TempDir(), "result.json")
	cmd := exec.CommandContext(t.Context(), os.Args[0],
		"-test.run=^TestDisagg_OracleChild$", "-test.v", "-test.count=1")
	cmd.Env = append(os.Environ(),
		envDisaggChild+"=1",
		envDisaggSeed+"="+seed,
		envDisaggMode+"="+mode,
		envDisaggResult+"="+path,
	)
	out, err := runWithTimeout(cmd, disaggChildTimeout)
	require.NoErrorf(t, err, "disaggregated oracle seed %s (%s) failed; reproduce with\n"+
		"  %s=1 %s=%s %s=%s go test ./internal/oracle -run '^TestDisagg_OracleChild$' -v\noutput:\n%s",
		seed, mode, envDisaggChild, envDisaggSeed, seed, envDisaggMode, mode, out)
	b, err := os.ReadFile(path)
	require.NoErrorf(t, err, "child wrote no result\noutput:\n%s", out)
	var res disaggResult
	require.NoError(t, json.Unmarshal(b, &res))
	return res
}

// disaggResult is what a child reports. The fields up to Fired are a pure
// function of the seed; the rest depend on the interleaving.
type disaggResult struct {
	Seed    uint64   `json:"seed"`
	Mode    string   `json:"mode"`
	Readers int      `json:"readers"`
	Leaders int      `json:"leaders"`
	Plan    []string `json:"plan"`
	// Prelude is the frames generated before the pods start and at
	// cutover.
	Prelude  []int    `json:"prelude"`
	Waves    []int    `json:"waves"`
	Fired    []string `json:"fired"`
	Events   int      `json:"events"`
	MainNext uint64   `json:"mainNext"`
	// Stream is a digest of the delivered stream without witness times,
	// which are the pod's clock.
	Stream string `json:"stream"`

	Sessions int    `json:"sessions"`
	Turns    int    `json:"turns"`
	Trace    string `json:"trace"`
}

// deterministic is the part of a result the seed decides: the plan and the
// faults fired, in order. Under the D4 fallback the interleaving is not
// replayable, and the rest follows from it: where a batch cut falls
// against a crash decides whether a commit prefix is re-archived (Stream,
// MainNext), and when a fault fires decides how many extra events its
// wave needed (Events). Each run still has to pass the model check.
func (r disaggResult) deterministic() disaggResult {
	return disaggResult{Seed: r.Seed, Mode: r.Mode, Readers: r.Readers, Leaders: r.Leaders, Plan: r.Plan, Prelude: r.Prelude, Waves: r.Waves, Fired: r.Fired}
}

// disaggFault is one scheduled fault kind.
type disaggFault string

const (
	dfCrashCut        = disaggFault("crash:" + crashpoint.AfterHotBatchCutBeforeUpload)
	dfCrashUpload     = disaggFault("crash:" + crashpoint.AfterHotBatchUploadBeforeCommit)
	dfCrashCommit     = disaggFault("crash:" + crashpoint.AfterHotBatchCommitBeforeAck)
	dfCrashFold       = disaggFault("crash:" + crashpoint.AfterFoldUploadBeforeCommit)
	dfCrashSeal       = disaggFault("crash:" + crashpoint.AfterSealFooterUploadBeforeCommit)
	dfCommitLost      = disaggFault("commit_lost")
	dfLeaseLoss       = disaggFault("lease_loss")
	dfStaleLeader     = disaggFault("stale_leader")
	dfPutError        = disaggFault("s3_put_error")
	dfPutWrongBytes   = disaggFault("s3_put_wrong_bytes")
	dfGetWrongBytes   = disaggFault("s3_get_wrong_bytes")
	dfNotifyLost      = disaggFault("notify_lost")
	dfSlowRead        = disaggFault("slow_read")
	disaggCrashPrefix = "crash:"

	// Bootstrap and merge faults (disagg_lifecycle_test.go).
	dfCrashRepoComplete    = disaggFault("crash:" + crashpoint.AfterRepoComplete)
	dfCrashDirectCut       = disaggFault("crash:" + crashpoint.AfterDirectBlockCutBeforeUpload)
	dfCrashDirectUpload    = disaggFault("crash:" + crashpoint.AfterDirectBlockUploadBeforeCommit)
	dfCrashDirectCommit    = disaggFault("crash:" + crashpoint.AfterDirectBlockCommitBeforeAck)
	dfCommitLostBlock      = disaggFault("commit_lost:block")
	dfCrashCloseBeforeSeal = disaggFault("crash:" + crashpoint.AfterBootstrapLiveCloseBeforeSeal)
	dfCrashMergeFlush      = disaggFault("crash:" + crashpoint.AfterMergeDstFlushBeforeSourceCommit)
	dfCrashMergeSeal       = disaggFault("crash:" + crashpoint.AfterMergeDstSealBeforeDiscovery)
	dfCrashMergeDiscovery  = disaggFault("crash:" + crashpoint.AfterMergeDiscoveryBeforeCleanup)
	dfCrashMergeCleanup    = disaggFault("crash:" + crashpoint.AfterMergeCleanupComplete)
	dfCrashSteadyPhase     = disaggFault("crash:" + crashpoint.AfterSteadyPhaseBeforeSteadyRun)
)

var disaggAllFaults = []disaggFault{
	dfCrashCut, dfCrashUpload, dfCrashCommit, dfCrashFold, dfCrashSeal,
	dfCommitLost, dfLeaseLoss, dfStaleLeader,
	dfPutError, dfPutWrongBytes, dfGetWrongBytes,
	dfNotifyLost, dfSlowRead,
}

// disaggPlan is the seeded schedule: pod counts, the bootstrap faults in
// order, the merge faults in phase order, one fault per steady-state wave,
// and each wave's event count. A final quiet wave has no fault.
type disaggPlan struct {
	readers, leaders int
	bootFaults       []disaggFault
	mergeFaults      []disaggFault
	faults           []disaggFault
	// pre is the frames generated before the pods start; cutover, those
	// generated while the leader holds the cutover barrier.
	pre, cutover int
	waves        []int
	quiet        int
}

func newDisaggPlan(seed uint64, mode string) disaggPlan {
	rng := rand.New(rand.NewPCG(seed, seed^0xd15a_6600_0000_0001))
	p := disaggPlan{readers: 2, leaders: 2}
	pick := func(fs ...disaggFault) disaggFault { return fs[rng.IntN(len(fs))] }
	shuffle := func(fs []disaggFault) []disaggFault {
		rng.Shuffle(len(fs), func(i, j int) { fs[i], fs[j] = fs[j], fs[i] })
		return fs
	}
	switch mode {
	case disaggModeShort:
		p.bootFaults = []disaggFault{pick(dfCrashRepoComplete, dfCrashDirectCut, dfCrashDirectUpload, dfCrashDirectCommit)}
		p.mergeFaults = []disaggFault{pick(dfCrashCloseBeforeSeal, dfCrashMergeFlush, dfCrashMergeSeal,
			dfCrashMergeDiscovery, dfCrashMergeCleanup, dfCrashSteadyPhase)}
		p.faults = []disaggFault{pick(dfCrashCut, dfCrashUpload, dfCrashCommit, dfCommitLost), dfNotifyLost}
	default:
		p.readers += rng.IntN(2)
		p.leaders += rng.IntN(2)
		// A kill after a repo completes goes first: the rest need
		// traffic, and backfill's repos all complete early.
		p.bootFaults = append([]disaggFault{dfCrashRepoComplete}, shuffle([]disaggFault{
			dfCrashDirectCut, dfCrashDirectUpload, dfCrashDirectCommit, dfCommitLostBlock, dfCrashSeal, dfLeaseLoss,
		})...)
		// Every merge crashpoint, in the order merge reaches them. The
		// last two exclude each other: the steady-phase seam fires only in
		// the session that ran merge, and a kill after cleanup leaves a
		// successor that does not.
		p.mergeFaults = []disaggFault{dfCrashCloseBeforeSeal, dfCrashMergeFlush, dfCrashMergeSeal, dfCrashMergeDiscovery,
			pick(dfCrashMergeCleanup, dfCrashSteadyPhase)}
		p.faults = shuffle(slices.Clone(disaggAllFaults))
	}
	p.pre = 6 + rng.IntN(5)
	p.cutover = 8 + rng.IntN(9)
	for range p.faults {
		p.waves = append(p.waves, 8+rng.IntN(9))
	}
	p.quiet = 8 + rng.IntN(9)
	return p
}

type disaggSession struct {
	pod   string
	epoch uint64
}

type disaggHarness struct {
	t     *testing.T
	ctx   context.Context
	seed  uint64
	plan  disaggPlan
	rng   *rand.Rand
	w     *world.World
	db    *storagefake.DB
	blob  *memblob.Blob
	sched *storagefake.Seeded

	blobFaults *disaggBlobFaults
	simClient  *http.Client
	objects    string

	crash   atomic.Pointer[disaggCrashArm]
	crashed chan *disaggPod

	mu       sync.Mutex
	sessions []disaggSession

	pods      []*disaggPod
	podCount  int
	observers []*disaggObserver
	obsCancel context.CancelFunc

	expected []segment.Event
	fired    []string
	// stream is the delivered stream every reader must agree on.
	stream []disaggKey

	// The lifecycle prelude (disagg_lifecycle_test.go). stage names the
	// phase the harness is driving, for failure reports.
	stage          string
	gate           *disaggListGate
	cutoverReached chan string
	cutoverGo      chan struct{}
	bootBarrier    chan string
	bootResume     chan struct{}
	resumeOnce     sync.Once
	// survivors are the rows generated after backfill finished, which
	// merge must keep.
	survivors []disaggKey
	// mergeReplay is set when a kill re-drained a merge source, which
	// leaves main's first mergedRows rows with duplicates.
	mergeReplay bool
	mergedRows  int
}

func runDisaggOracle(t *testing.T, seed uint64, mode string) disaggResult {
	advanceClockToSimulatorEpoch()
	plan := newDisaggPlan(seed, mode)
	sched := storagefake.NewSeeded(seed)
	schedCtx, stopSched := context.WithCancel(context.Background())
	schedDone := make(chan struct{})
	go func() { sched.Run(schedCtx); close(schedDone) }()
	t.Cleanup(func() { stopSched(); <-schedDone })

	h := &disaggHarness{
		t:          t,
		ctx:        t.Context(),
		seed:       seed,
		plan:       plan,
		rng:        rand.New(rand.NewPCG(seed^0x9ace, seed)),
		sched:      sched,
		blobFaults: &disaggBlobFaults{fired: map[*memblob.KeyPrefixFault]bool{}},
		crashed:    make(chan *disaggPod, 16),

		cutoverReached: make(chan string),
		cutoverGo:      make(chan struct{}),
		bootBarrier:    make(chan string),
		bootResume:     make(chan struct{}),
	}
	h.w = newRestartWorld(t, Config{
		Seed:              seed,
		Accounts:          seedTestAccounts,
		MinInitialRecords: 2,
		MaxInitialRecords: 5,
		LiveEventsSteady:  seedTestLive + 1024,
	})
	t.Cleanup(func() { require.NoError(t, h.w.Close()) })
	h.db = storagefake.New(storagefake.Config{
		Invariants:  catalog.InvariantOptions{MaxEventsPerBlock: seedTestBlock},
		OnViolation: func(rev uint64, err error) { t.Errorf("catalog invariant at revision %d: %v", rev, err) },
		Scheduler:   sched,
	})
	h.blob = memblob.New(memblob.WithFaultInjector(h.blobFaults))
	archiveID := h.db.Archive().ArchiveID
	h.objects = objstore.FormatUUID(archiveID) + "/objects/"
	h.initCatalog()

	simLn := newPipeListener()
	h.gate = newDisaggListGate(simhttp.NewHandler(h.w, disaggSimURL))
	simSrv := &http.Server{Handler: h.gate}
	go func() { _ = simSrv.Serve(simLn) }()
	t.Cleanup(func() { _ = simSrv.Close() })
	h.simClient = simLn.httpClient()
	t.Cleanup(h.teardown)

	t.Logf("plan: readers=%d leaders=%d bootstrap=%v merge=%v faults=%v prelude=%d+%d waves=%v",
		plan.readers, plan.leaders, plan.bootFaults, plan.mergeFaults, plan.faults, plan.pre, plan.cutover, plan.waves)
	h.runLifecycle()

	// Observers start once merge is done; the readers themselves have run
	// since bootstrap began.
	obsCtx, obsCancel := context.WithCancel(h.ctx)
	h.obsCancel = obsCancel
	for _, p := range h.pods {
		if p.reader {
			h.observers = append(h.observers, newDisaggObserver(obsCtx, p, "v2"), newDisaggObserver(obsCtx, p, "v1"))
		}
	}
	h.converge("start")
	t.Logf("steady state: %d merged events", len(h.expected))

	for i, f := range plan.faults {
		h.wave(i, f, plan.waves[i])
	}
	for range plan.quiet {
		h.generate()
	}
	h.converge("quiet wave")
	require.NoError(t, h.db.Violation())
	require.Empty(t, h.db.Unfired(), "every scheduled catalog fault fired")

	stream := h.checkStreams()
	h.checkArchives()

	snap, err := h.db.Snapshot()
	require.NoError(t, err)
	next, err := catalog.DecodeSeq(catalog.MainSeqKey, snap.Meta[catalog.MainSeqKey], true)
	require.NoError(t, err)
	trace := sched.Trace()
	sum := sha256.Sum256([]byte(strings.Join(trace, "\n")))
	h.mu.Lock()
	sessions := len(h.sessions)
	h.mu.Unlock()
	var plans []string
	for _, f := range plan.bootFaults {
		plans = append(plans, disaggBootFaultPrefix+string(f))
	}
	for _, f := range plan.mergeFaults {
		plans = append(plans, disaggMergeFaultPrefix+string(f))
	}
	for _, f := range plan.faults {
		plans = append(plans, string(f))
	}
	return disaggResult{
		Seed:     seed,
		Mode:     mode,
		Readers:  plan.readers,
		Leaders:  plan.leaders,
		Plan:     plans,
		Prelude:  []int{plan.pre, plan.cutover},
		Waves:    plan.waves,
		Fired:    h.fired,
		Events:   len(h.expected),
		MainNext: next,
		Stream:   stream,
		Sessions: sessions,
		Turns:    len(trace),
		Trace:    hex.EncodeToString(sum[:8]),
	}
}

// wave arms f, generates n events, and waits for every reader to deliver
// them. A fault that has not fired by then gets extra events, bounded.
func (h *disaggHarness) wave(i int, f disaggFault, n int) {
	fired, mid := h.arm(f)
	for j := range n {
		if j == n/2 && mid != nil {
			mid()
		}
		h.generate()
	}
	what := fmt.Sprintf("wave %d (%s)", i, f)
	h.converge(what)
	for extra := 0; !fired(); extra++ {
		if extra == disaggMaxExtraChunks {
			h.failf("%s: the fault did not fire after %d extra events", what, extra*disaggExtraChunk)
		}
		for range disaggExtraChunk {
			h.generate()
		}
		h.converge(what)
	}
	h.fired = append(h.fired, string(f))
	h.t.Logf("%s fired; %d events, sessions %v", what, len(h.expected), h.sessionList())
}

func (h *disaggHarness) sessionList() []disaggSession {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.sessions)
}

// arm schedules f. It returns the proof that f fired and, for faults the
// harness drives itself, an action to take mid-wave.
func (h *disaggHarness) arm(f disaggFault) (fired func() bool, mid func()) {
	catalogFault := func(sf *storagefake.Fault) func() bool {
		h.db.InjectFaults(sf)
		return sf.Fired
	}
	blobFault := func(bf *memblob.KeyPrefixFault) func() bool {
		bf.Prefix, bf.Ordinal = h.objects, 1
		h.blobFaults.add(bf)
		return func() bool { return h.blobFaults.hasFired(bf) }
	}
	switch f {
	case dfCommitLost:
		return catalogFault(&storagefake.Fault{Kind: storagefake.FaultCommitLost, TxKind: catalog.TxHotBatch, Ordinal: 1}), nil
	case dfCommitLostBlock:
		return catalogFault(&storagefake.Fault{Kind: storagefake.FaultCommitLost, TxKind: catalog.TxBlock, Ordinal: 1}), nil
	case dfNotifyLost:
		return catalogFault(&storagefake.Fault{Kind: storagefake.FaultNotifyLost, TxKind: catalog.TxHotBatch, Ordinal: 1}), nil
	case dfSlowRead:
		return catalogFault(&storagefake.Fault{Kind: storagefake.FaultSlowRead, Ordinal: 1, Delay: 2 * time.Second}), nil
	case dfPutError:
		return blobFault(&memblob.KeyPrefixFault{Op: memblob.OpPut, Kind: memblob.FaultError}), nil
	case dfPutWrongBytes:
		return blobFault(&memblob.KeyPrefixFault{Op: memblob.OpPut, Kind: memblob.FaultWrongBytes}), nil
	case dfGetWrongBytes:
		return blobFault(&memblob.KeyPrefixFault{Op: memblob.OpGet, Kind: memblob.FaultWrongBytes}), nil
	case dfLeaseLoss, dfStaleLeader:
		var old uint64
		expired := false
		mid = func() {
			old = h.db.Archive().WriterEpoch
			require.NoError(h.t, h.db.ExpireLease(h.ctx))
			expired = true
		}
		stale := false
		return func() bool {
			if !expired || h.maxEpoch() <= old {
				return false
			}
			if f == dfStaleLeader && !stale {
				h.staleWrites(old)
				stale = true
			}
			return true
		}, mid
	}
	if p, ok := strings.CutPrefix(string(f), disaggCrashPrefix); ok {
		arm := &disaggCrashArm{point: crashpoint.Point(p)}
		h.crash.Store(arm)
		return arm.fired.Load, nil
	}
	h.failf("unknown fault %q", f)
	return nil, nil
}

// staleWrites is a leader whose session outlived its lease: it writes with
// epoch after a successor fenced it. Both writes must fail the fence and
// leave no trace, including a hot batch that would otherwise be the next
// valid one.
func (h *disaggHarness) staleWrites(epoch uint64) {
	t := h.t
	ctx := h.ctx
	cl := h.db.Client("stale-leader")
	key := []byte("oracle/stale-leader-probe")
	_, err := catalog.NewSession(catalog.SessionConfig{DB: cl, Epoch: epoch}).
		CommitMeta(ctx, []metastore.Op{{Kind: metastore.OpSet, Key: key, Value: []byte("x")}})
	require.ErrorIs(t, err, catalog.ErrFenced, "a stale leader's metadata write")
	_, err = h.db.MetaStore(nil).Get(ctx, key)
	require.ErrorIs(t, err, metastore.ErrNotFound, "a fenced metadata write left a key")

	snap, err := h.db.Snapshot()
	require.NoError(t, err)
	next, err := catalog.DecodeSeq(catalog.MainSeqKey, snap.Meta[catalog.MainSeqKey], true)
	require.NoError(t, err)
	bb, err := segment.NewBlockBuilder(seedTestBlock)
	require.NoError(t, err)
	now := time.Now().UnixMicro()
	_, err = bb.Append(segment.Event{Seq: next, WitnessedAt: now, Kind: segment.KindIdentity, DID: "did:plc:stale-leader-phantom"})
	require.NoError(t, err)
	frame, _ := bb.Encode()
	_, err = catalog.NewSession(catalog.SessionConfig{DB: cl, Epoch: epoch}).CommitHotBatch(ctx, catalog.HotBatch{
		FirstSeq: next, LastSeq: next, MinWitnessedUS: now, MaxWitnessedUS: now, Frame: frame,
	})
	require.ErrorIs(t, err, catalog.ErrFenced, "a stale leader's hot batch")
}

// generate emits one upstream frame and appends its rows to the model.
func (h *disaggHarness) generate() {
	t := h.t
	frame, err := h.w.GenerateOneForTest(h.ctx)
	require.NoError(t, err)
	evt, err := decodeOracleFirehoseFrame(frame)
	require.NoError(t, err)
	rows, err := expectedSegmentEventsFromFirehoseEvent(h.w, evt)
	require.NoError(t, err)
	for _, row := range rows {
		row.Seq = uint64(len(h.expected)) + 1
		row.UpstreamRelayCursor = evt.Seq
		h.expected = append(h.expected, row)
	}
	h.reap()
	time.Sleep(time.Duration(1+h.rng.IntN(8)) * time.Millisecond)
}

// converge waits until the catalog covers every expected row and every
// observer has delivered the whole catalog, replacing crashed leaders as it
// goes. A row the model forbids fails at once: delivered streams only grow.
func (h *disaggHarness) converge(what string) {
	want, groups := h.model()
	deadline := time.Now().Add(disaggConvergeTimeout)
	var lastNext uint64
	var heldAt time.Time // when the catalog last changed while holding every row
	for {
		h.reap()
		h.checkPods()
		next := h.mainNext()
		done := next > uint64(len(want))
		if next != lastNext {
			lastNext, heldAt = next, time.Time{}
			if done {
				heldAt = time.Now()
			}
		}
		o0 := h.observers[0] // the first reader's v2 observer
		o0.mu.Lock()
		keys := make([]disaggKey, len(o0.v2))
		for i, ev := range o0.v2 {
			keys[i] = disaggKeyOf(ev)
		}
		o0.mu.Unlock()
		covered, rewinds, err := disaggCover(want, groups, keys)
		if err != nil {
			h.failf("%s: %s v2: %v", what, o0.pod.name, err)
		}
		if limit := h.sessionChanges(); rewinds > limit {
			h.failf("%s: %s v2: %d commit prefixes re-archived across %d leader changes", what, o0.pod.name, rewinds, limit)
		}
		done = done && covered && uint64(len(keys)) == next-1
		wantV1 := -1
		if done {
			// Reading the catalog is slow; only a caught-up stream needs it.
			wantV1 = len(disaggV1Project(keys, h.resyncSeqs()))
		}
		for _, o := range h.observers[1:] {
			if o.progress() < map[bool]int{true: wantV1, false: len(keys)}[o.proto == "v1"] {
				done = false
			}
		}
		if done {
			return
		}
		if !heldAt.IsZero() && time.Since(heldAt) > disaggServeTimeout {
			h.failf("%s: the catalog has held every row at seq/next %d for %s, but %s v2 has %d (covered=%v); v1 wants %d; %s",
				what, next, disaggServeTimeout, o0.pod.name, len(keys), covered, wantV1, h.observerProgress())
		}
		if time.Now().After(deadline) {
			h.failf("%s: not converged after %s: seq/next %d, %d model rows, %s v2 has %d (covered=%v); v1 wants %d; %s",
				what, disaggConvergeTimeout, next, len(want), o0.pod.name, len(keys), covered, wantV1, h.observerProgress())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// resyncSeqs is the set of main's seqs that hold resync replacements,
// which v1 does not deliver.
func (h *disaggHarness) resyncSeqs() map[uint64]bool {
	out := map[uint64]bool{}
	for _, ev := range h.readCatalog()[catalog.Main] {
		if ev.Kind.IsResyncReplacement() {
			out[ev.Seq] = true
		}
	}
	return out
}

// observerProgress lists how many events each observer has delivered.
func (h *disaggHarness) observerProgress() string {
	parts := make([]string, len(h.observers))
	for i, o := range h.observers {
		parts[i] = fmt.Sprintf("%s %s %d", o.pod.name, o.proto, o.progress())
	}
	return strings.Join(parts, ", ")
}

// model returns the expected rows' keys and, for each, the upstream event
// it came from; a seeded row with no upstream cursor is its own event.
func (h *disaggHarness) model() ([]disaggKey, []int64) {
	want := make([]disaggKey, len(h.expected))
	groups := make([]int64, len(h.expected))
	for i, ev := range h.expected {
		want[i] = disaggKeyOf(observedFromSegment(ev))
		groups[i] = ev.UpstreamRelayCursor
		if groups[i] == 0 {
			groups[i] = -int64(i) - 1
		}
	}
	return want, groups
}

// sessionChanges is how many times leadership moved: each move can
// re-archive at most one upstream event's committed prefix (design §10.4).
func (h *disaggHarness) sessionChanges() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return max(0, len(h.sessions)-1)
}

func (h *disaggHarness) mainNext() uint64 {
	snap, err := h.db.Snapshot()
	require.NoError(h.t, err)
	next, err := catalog.DecodeSeq(catalog.MainSeqKey, snap.Meta[catalog.MainSeqKey], true)
	require.NoError(h.t, err)
	return next
}

// reap replaces each leader killed at a crash seam with a fresh pod.
func (h *disaggHarness) reap() {
	for {
		select {
		case p := <-h.crashed:
			h.stopPod(p)
			h.startPod(false)
		default:
			return
		}
	}
}

// checkPods fails on a pod that exited without being killed: every fault
// here is one a pod must survive or leave to a successor.
func (h *disaggHarness) checkPods() {
	for _, p := range h.pods {
		if p.stopped || p.crashed.Load() {
			continue
		}
		select {
		case <-p.exited:
			h.failf("%s exited on its own: %v", p.name, p.runErr)
		default:
		}
	}
}

func (h *disaggHarness) sessionStarted(pod string, epoch uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, s := range h.sessions {
		if s.epoch >= epoch {
			h.t.Errorf("%s started a session at epoch %d after %s's at epoch %d", pod, epoch, s.pod, s.epoch)
		}
	}
	h.sessions = append(h.sessions, disaggSession{pod: pod, epoch: epoch})
}

// newestSession reports whether pod started the newest leader session. A
// pod runs one session at a time, so that session is the one calling.
func (h *disaggHarness) newestSession(pod string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.sessions) > 0 && h.sessions[len(h.sessions)-1].pod == pod
}

func (h *disaggHarness) maxEpoch() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	var m uint64
	for _, s := range h.sessions {
		m = max(m, s.epoch)
	}
	return m
}

// checkStreams compares the delivered streams with the model and with each
// other, and returns a digest of the stream's per-DID form.
//
// Against the model the check is per DID: ingest processes DIDs in
// parallel, so the archive promises per-DID order and no global order
// (docs/README.md §2, specs/invariants.md). Between readers it is exact:
// every reader serves the one catalog.
func (h *disaggHarness) checkStreams() string {
	t := h.t
	want, groups := h.model()
	ground, err := GroundTruthFromWorld(h.w)
	require.NoError(t, err)

	var ref []ObservedEvent
	var refKeys []disaggKey
	var refName string
	for _, o := range h.observers {
		if o.proto != "v2" {
			continue
		}
		o.mu.Lock()
		v2, fatal := slices.Clone(o.v2), o.fatal
		o.mu.Unlock()
		name := o.pod.name + " v2"
		require.Emptyf(t, fatal, "%s: the client saw fatal errors", name)
		keys := make([]disaggKey, len(v2))
		for i, ev := range v2 {
			keys[i] = disaggKeyOf(ev)
		}
		if ref == nil {
			disaggRequireModel(t, want, groups, keys, h.sessionChanges(), name)
			if h.mergeReplay {
				// The merged prefix repeats a source's rows; checkMerged
				// checked it as merge left it.
				require.NoErrorf(t, CheckStructuralInvariants(v2), "%s", name)
				require.NoErrorf(t, CheckInvariants(v2[min(h.mergedRows, len(v2)):]), "%s after merge", name)
			} else {
				require.NoErrorf(t, CheckInvariants(v2), "%s", name)
			}
			model, err := Reconstruct(v2)
			require.NoErrorf(t, err, "%s", name)
			require.NoErrorf(t, Compare(ground, model), "%s", name)
			ref, refKeys, refName = v2, keys, name
			h.stream = refKeys
			continue
		}
		disaggRequireKeys(t, refKeys, keys, name+" against "+refName)
		require.Equalf(t, ref, v2, "%s and %s delivered different events", refName, name)
	}
	require.NotNil(t, ref, "no v2 observer")
	refV1 := disaggV1Project(refKeys, h.resyncSeqs())
	for _, o := range h.observers {
		if o.proto != "v1" {
			continue
		}
		o.mu.Lock()
		v1, fatal := slices.Clone(o.v1), o.fatal
		o.mu.Unlock()
		name := o.pod.name + " v1"
		require.Emptyf(t, fatal, "%s: the subscriber saw bad frames", name)
		disaggRequireKeys(t, refV1, v1, name+" against "+refName)
	}

	d := sha256.New()
	for _, did := range slices.Sorted(maps.Keys(disaggByDID(ref))) {
		for _, ev := range disaggByDID(ref)[did] {
			k := disaggKeyOf(ev)
			_, _ = fmt.Fprintf(d, "%d\x00%s\x00%s\x00%s\x00%s\x00", k.Kind, k.DID, k.Collection, k.Rkey, k.Rev)
			_ = binary.Write(d, binary.BigEndian, uint64(len(ev.Payload)))
			_, _ = d.Write(ev.Payload)
		}
	}
	return hex.EncodeToString(d.Sum(nil))
}

// checkArchives downloads the whole archive from every live pod with a
// fresh client; each must match what the observers saw.
func (h *disaggHarness) checkArchives() {
	t := h.t
	want := uint64(len(h.stream))
	for _, p := range h.pods {
		if p.stopped {
			continue
		}
		client, err := jetstream.Subscribe("http://jetstream.invalid",
			jetstream.WithHTTPClient(p.ln.httpClient()),
			jetstream.WithAfterSeq(0),
			jetstream.WithBatchSize(64),
		)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(h.ctx, disaggConvergeTimeout)
		var got []disaggKey
		for batch, err := range client.Events(ctx) {
			require.NoErrorf(t, err, "%s archive download", p.name)
			for _, ev := range batch.Events() {
				oe, err := observedEventFromClientErr(ev)
				require.NoError(t, err)
				got = append(got, disaggKeyOf(oe))
			}
			if len(got) > 0 && got[len(got)-1].Seq >= want {
				break
			}
		}
		cancel()
		_ = client.Close()
		disaggRequireKeys(t, h.stream, got, p.name+" archive download")
	}
}

// teardown stops observers, then every pod, requiring a clean exit from
// each pod that was not killed.
func (h *disaggHarness) teardown() {
	if h.gate != nil {
		h.gate.release()
	}
	h.resumeBoot()
	if h.t.Failed() {
		for _, p := range h.pods {
			h.t.Logf("--- %s (reader=%t crashed=%t) log tail:\n%s", p.name, p.reader, p.crashed.Load(), p.logs.tail(16<<10))
		}
	}
	if h.obsCancel != nil {
		h.obsCancel()
	}
	for _, o := range h.observers {
		<-o.done
	}
	for _, p := range h.pods {
		if !p.stopped {
			h.stopPod(p)
		}
	}
}

func (h *disaggHarness) failf(format string, args ...any) {
	h.t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "stage: %s\nfired so far: %v\n", h.stage, h.fired)
	if snap, err := h.db.Snapshot(); err == nil {
		fmt.Fprintf(&b, "catalog: epoch %d holder %x lease until %s; %d segments, %d active blocks, %d hot batches, %d objects\n",
			snap.Archive.WriterEpoch, snap.Archive.HolderID, snap.Archive.LeaseExpiresAt.Format(time.RFC3339Nano),
			len(snap.Segments), len(snap.ActiveBlocks), len(snap.HotBatches), len(snap.Objects))
	}
	h.mu.Lock()
	fmt.Fprintf(&b, "sessions: %v\n", h.sessions)
	h.mu.Unlock()
	for _, o := range h.observers {
		o.mu.Lock()
		fmt.Fprintf(&b, "observer %s %s: %d events, last seq %d, %d recoverable errors, fatal %v\n",
			o.pod.name, o.proto, max(len(o.v1), len(o.v2)), o.last, o.recoverable, o.fatal)
		o.mu.Unlock()
	}
	if len(h.observers) > 1 {
		o := h.observers[1]
		o.mu.Lock()
		have := map[string]int{}
		for _, k := range o.v1 {
			have[k.DID]++
		}
		o.mu.Unlock()
		for _, ev := range h.expected {
			if ev.Kind == segment.KindSync {
				continue
			}
			if have[ev.DID] > 0 {
				have[ev.DID]--
				continue
			}
			fmt.Fprintf(&b, "not delivered to %s: model seq %d upstream %d kind %d %s %s/%s rev %s\n",
				o.pod.name, ev.Seq, ev.UpstreamRelayCursor, ev.Kind, ev.DID, ev.Collection, ev.Rkey, ev.Rev)
		}
	}
	trace := h.sched.Trace()
	fmt.Fprintf(&b, "last scheduler turns (%d total):\n  %s\n", len(trace), strings.Join(trace[max(0, len(trace)-40):], "\n  "))
	for _, p := range h.pods {
		fmt.Fprintf(&b, "--- %s (reader=%t crashed=%t stopped=%t) log tail:\n%s\n", p.name, p.reader, p.crashed.Load(), p.stopped, p.logs.tail(16<<10))
	}
	h.t.Fatalf("%s\n%s", fmt.Sprintf(format, args...), b.String())
}

// disaggPod is one jetstreamd process: its own catalog client, object
// store handle, and public listener.
type disaggPod struct {
	name   string
	reader bool
	client *storagefake.Client
	blob   *disaggPodBlob
	ln     *pipeListener
	rt     *jetstreamd.Runtime
	cancel context.CancelFunc
	logs   *disaggLog

	exited  chan struct{}
	runErr  error
	crashed atomic.Bool
	stopped bool
}

func (h *disaggHarness) startPod(reader bool) *disaggPod {
	h.podCount++
	p := &disaggPod{
		name:   fmt.Sprintf("pod-%d", h.podCount),
		reader: reader,
		ln:     newPipeListener(),
		logs:   &disaggLog{},
		exited: make(chan struct{}),
	}
	p.client = h.db.Client(p.name)
	p.blob = &disaggPodBlob{blob: h.blob, sched: h.sched, actor: p.name}
	newLease := func() leader.Locker { return p.client.NewLease() }
	if reader {
		newLease = func() leader.Locker { return disaggReaderLocker{} }
	}
	backend := &jetstreamd.StorageBackend{
		DB:       p.client,
		Listener: p.client,
		Blob:     p.blob,
		Archive: func(context.Context) (catalog.ArchiveRow, error) {
			if p.client.Killed() {
				return catalog.ArchiveRow{}, storagefake.ErrKilled
			}
			return h.db.Archive(), nil
		},
		NewLease:  newLease,
		MetaStore: p.client.MetaStore,
	}
	opts := disaggPodOptions(backend, p.ln, h.simClient, p.logs)
	opts.LogLevel = "info"
	opts.SteadyMaxSegmentBytes = seedTestMaxSegment
	// A small inline budget mixes pointer and inline batches.
	opts.Storage.Hot.InlineBytesPerSec = 512
	opts.OnSessionStart = func(epoch uint64) { h.sessionStarted(p.name, epoch) }
	if reader {
		opts.Storage.Leader.AcquireInterval = time.Hour
	} else {
		opts.CrashInjector = disaggCrashInjector{h: h, pod: p}
		opts.BackfillRetryBaseDelay = time.Millisecond
		// atmos holds a per-DID-shard sync.Mutex across the Store's
		// discovery write, a fenced commit that parks in the scheduler.
		// A second host's reconcile waiting on that shard is not durably
		// blocked, so synctest.Wait would never return. One active host
		// runs one reconcile at a time.
		opts.BackfillMaxActiveHosts = 1
		// A block per bootstrap-live event gives the direct-mode seams
		// traffic while backfill waits on its held repo.
		opts.BootstrapLiveMaxEventsPerBlock = 1
		opts.BootstrapLiveMaxSegmentBytes = seedTestMaxSegment
		opts.BarrierBeforeCutover = disaggBarrier(h.cutoverReached, h.cutoverGo, p.name)
		opts.BarrierAfterBootstrap = disaggBarrier(h.bootBarrier, h.bootResume, p.name)
	}
	ctx, cancel := context.WithCancel(h.ctx)
	p.cancel = cancel
	rt, err := jetstreamd.Build(ctx, opts)
	require.NoErrorf(h.t, err, "build %s", p.name)
	p.rt = rt
	go func() {
		p.runErr = rt.Run(ctx)
		close(p.exited)
	}()
	h.pods = append(h.pods, p)
	return p
}

// stopPod shuts p down. A pod killed at a crash seam may exit with any
// error; any other pod must exit cleanly.
func (h *disaggHarness) stopPod(p *disaggPod) {
	p.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	closeErr := p.rt.Close(ctx)
	<-p.exited
	_ = p.ln.Close()
	p.stopped = true
	if p.crashed.Load() {
		return
	}
	if closeErr != nil || p.runErr != nil {
		h.failf("%s did not stop cleanly: close %v, run %v", p.name, closeErr, p.runErr)
	}
}

// disaggPodOptions is a disaggregated pod on backend, serving on ln and
// reaching the simulator through simClient.
func disaggPodOptions(backend *jetstreamd.StorageBackend, ln *pipeListener, simClient *http.Client, logs io.Writer) jetstreamd.Options {
	storage := jetstreamd.DefaultStorageConfig()
	storage.Mode = jetstreamd.StorageDisaggregated
	storage.Leader.AcquireInterval = 10 * time.Millisecond
	storage.Hot.BatchMaxAge = 5 * time.Millisecond
	storage.CatalogPollInterval = 10 * time.Millisecond
	return jetstreamd.Options{
		Storage:                        storage,
		StorageBackend:                 backend,
		MemoryLimit:                    8 << 30,
		RelayURL:                       disaggSimURL,
		PLCURL:                         disaggSimURL,
		OTelServiceName:                "jetstream-oracle",
		LogLevel:                       "warn",
		LogFormat:                      "text",
		LogOutput:                      logs,
		ShutdownTimeout:                5 * time.Second,
		ClientDrainTimeout:             time.Second,
		PublicListener:                 ln,
		LiveDial:                       subscribeReposDial(simClient),
		HTTPTransport:                  simClient.Transport,
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
		SteadyMaxEventsPerBlock:        seedTestBlock,
	}
}

// disaggReaderLocker never wins the lease: a reader-role pod.
type disaggReaderLocker struct{}

func (disaggReaderLocker) Acquire(context.Context, time.Duration) error { return streaming.ErrLockHeld }
func (disaggReaderLocker) Renew(context.Context, time.Duration) error   { return streaming.ErrNotHolder }
func (disaggReaderLocker) Release(context.Context) error                { return streaming.ErrNotHolder }
func (disaggReaderLocker) Epoch() uint64                                { return 0 }

// disaggCrashArm is the one crash seam armed at a time; the first leader to
// reach it dies there. Only the newest session may take it: after lease
// loss the old session runs on until it notices, and a kill there would
// end no session the harness is waiting on.
type disaggCrashArm struct {
	point crashpoint.Point
	fired atomic.Bool
}

type disaggCrashInjector struct {
	h   *disaggHarness
	pod *disaggPod
}

// SimulateCrash kills the pod's storage connections at once, as SIGKILL
// would, so nothing after the seam reaches the catalog or the store. The
// harness then tears the process down and starts a replacement.
func (c disaggCrashInjector) SimulateCrash(_ context.Context, p crashpoint.Point) error {
	arm := c.h.crash.Load()
	if arm == nil || arm.point != p || !c.h.newestSession(c.pod.name) || !c.h.crash.CompareAndSwap(arm, nil) {
		return nil
	}
	c.pod.client.Kill()
	c.pod.blob.killed.Store(true)
	c.pod.crashed.Store(true)
	arm.fired.Store(true)
	c.h.crashed <- c.pod
	return fmt.Errorf("oracle: %s killed at %s", c.pod.name, p)
}

var errDisaggPodKilled = errors.New("oracle: pod killed")

// disaggPodBlob is one pod's handle on the shared store: every call is a
// scheduler yield point, and a killed pod's calls fail.
type disaggPodBlob struct {
	blob   *memblob.Blob
	sched  storagefake.Scheduler
	actor  string
	killed atomic.Bool
}

func (b *disaggPodBlob) enter(ctx context.Context, op memblob.Op) error {
	if b.killed.Load() {
		return errDisaggPodKilled
	}
	if err := b.sched.Yield(storagefake.WithActor(ctx, b.actor), "blob/"+string(op)); err != nil {
		return err
	}
	if b.killed.Load() {
		return errDisaggPodKilled
	}
	return nil
}

func (b *disaggPodBlob) PutKey(ctx context.Context, key string, data []byte) error {
	if err := b.enter(ctx, memblob.OpPut); err != nil {
		return err
	}
	return b.blob.PutKey(ctx, key, data)
}

func (b *disaggPodBlob) GetKey(ctx context.Context, key string) ([]byte, error) {
	if err := b.enter(ctx, memblob.OpGet); err != nil {
		return nil, err
	}
	return b.blob.GetKey(ctx, key)
}

func (b *disaggPodBlob) GetKeyRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if err := b.enter(ctx, memblob.OpGetRange); err != nil {
		return nil, err
	}
	return b.blob.GetKeyRange(ctx, key, off, n)
}

func (b *disaggPodBlob) DeleteKey(ctx context.Context, key string) error {
	if err := b.enter(ctx, memblob.OpDelete); err != nil {
		return err
	}
	return b.blob.DeleteKey(ctx, key)
}

// disaggBlobFaults lets the harness arm object store faults on the shared
// store mid-run and records which fired.
type disaggBlobFaults struct {
	mu    sync.Mutex
	armed []*memblob.KeyPrefixFault
	fired map[*memblob.KeyPrefixFault]bool
}

func (d *disaggBlobFaults) add(f *memblob.KeyPrefixFault) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.armed = append(d.armed, f)
}

func (d *disaggBlobFaults) hasFired(f *memblob.KeyPrefixFault) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.fired[f]
}

// BeforeBlobOp implements memblob.FaultInjector.
func (d *disaggBlobFaults) BeforeBlobOp(op memblob.Op, key string) (memblob.FaultKind, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	kind, err := memblob.FaultNone, error(nil)
	for _, f := range d.armed {
		k, e := f.BeforeBlobOp(op, key)
		if k != memblob.FaultNone && kind == memblob.FaultNone {
			kind, err = k, e
			d.fired[f] = true
		}
	}
	return kind, err
}

// disaggLog is a pod's log, kept for the failure report.
type disaggLog struct {
	mu  sync.Mutex
	buf []byte
}

func (l *disaggLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf = append(l.buf, p...)
	return len(p), nil
}

func (l *disaggLog) tail(n int) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return string(l.buf[max(0, len(l.buf)-n):])
}

// disaggKey is what both wire protocols carry of an event, without the
// payload and witness time.
type disaggKey struct {
	Seq        uint64
	Kind       segment.Kind
	DID        string
	Collection string
	Rkey       string
	Rev        string
}

func disaggKeyOf(ev ObservedEvent) disaggKey {
	k := disaggKey{Seq: ev.Seq, Kind: ev.Kind, DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey, Rev: ev.Rev}
	if k.Kind == segment.KindCreateResync {
		// Both protocols deliver a resync create as a create.
		k.Kind = segment.KindCreate
	}
	return k
}

// disaggRequireKeys fails with the first divergence and its neighbours;
// a full dump of two streams is unreadable.
func disaggRequireKeys(t *testing.T, want, got []disaggKey, what string) {
	t.Helper()
	i := 0
	for i < len(want) && i < len(got) && want[i] == got[i] {
		i++
	}
	if i == len(want) && i == len(got) {
		return
	}
	var b strings.Builder
	for j := max(0, i-3); j < min(max(len(want), len(got)), i+6); j++ {
		fmt.Fprintf(&b, "  [%d]\n    want %s\n    got  %s\n", j, disaggKeyAt(want, j), disaggKeyAt(got, j))
	}
	t.Fatalf("%s: %d events, want %d; first divergence at index %d:\n%s", what, len(got), len(want), i, b.String())
}

func disaggKeyAt(keys []disaggKey, i int) string {
	if i >= len(keys) {
		return "(none)"
	}
	k := keys[i]
	return fmt.Sprintf("seq=%d kind=%d %s %s/%s rev=%s", k.Seq, k.Kind, k.DID, k.Collection, k.Rkey, k.Rev)
}

// disaggRequireModel checks a delivered stream against the model: seqs
// dense from 1, and each DID's events exactly the model's, in its order,
// apart from at most rewinds re-archived commit prefixes.
func disaggRequireModel(t *testing.T, want []disaggKey, groups []int64, got []disaggKey, rewinds int, what string) {
	t.Helper()
	for i, k := range got {
		require.Equalf(t, uint64(i+1), k.Seq, "%s: seqs are dense from 1", what)
	}
	covered, n, err := disaggCover(want, groups, got)
	require.NoErrorf(t, err, "%s", what)
	require.Truef(t, covered, "%s: %d events do not cover the %d model rows", what, len(got), len(want))
	require.LessOrEqualf(t, n, rewinds, "%s: commit prefixes re-archived", what)
	if n > 0 {
		t.Logf("%s: %d commit prefixes re-archived across %d leader changes", what, n, rewinds)
	}
}

// disaggCover walks each DID's delivered events against the model's. The
// only departure it allows is design §10.4's at-least-once relay cursor:
// a leader that dies after committing a batch cut inside one upstream
// commit leaves that commit's prefix archived, and its successor, resuming
// from the last cursor committed with a whole event, archives the commit
// again in full. So an event may restart the upstream commit in progress
// from its first row. Anything else (a lost row, a reorder, a whole event
// archived twice) is an error. covered reports that every model row has
// been delivered; rewinds counts the restarts.
func disaggCover(want []disaggKey, groups []int64, got []disaggKey) (covered bool, rewinds int, err error) {
	wantByDID := map[string][]int{}
	for i, k := range want {
		wantByDID[k.DID] = append(wantByDID[k.DID], i)
	}
	pos := map[string]int{}
	for _, g := range got {
		idx := wantByDID[g.DID]
		p := pos[g.DID]
		strip := func(k disaggKey) disaggKey { k.Seq = 0; return k }
		if p < len(idx) && strip(want[idx[p]]) == strip(g) {
			pos[g.DID] = p + 1
			continue
		}
		// Restart of the upstream commit in progress?
		if p > 0 && p < len(idx) && groups[idx[p]] == groups[idx[p-1]] {
			q := p - 1
			for q > 0 && groups[idx[q-1]] == groups[idx[p]] {
				q--
			}
			if strip(want[idx[q]]) == strip(g) {
				pos[g.DID] = q + 1
				rewinds++
				continue
			}
		}
		next := "(none)"
		if p < len(idx) {
			next = disaggKeyAt(want, idx[p])
		}
		return false, rewinds, fmt.Errorf("%s: event %d of %s is not in the model; want %s",
			disaggKeyAt([]disaggKey{g}, 0), p, g.DID, next)
	}
	for did, idx := range wantByDID {
		if pos[did] < len(idx) {
			return false, rewinds, nil
		}
	}
	return true, rewinds, nil
}

func disaggByDID(events []ObservedEvent) map[string][]ObservedEvent {
	out := map[string][]ObservedEvent{}
	for _, ev := range events {
		out[ev.DID] = append(out[ev.DID], ev)
	}
	return out
}

// disaggV1Project is what a v1 subscriber sees of a stream: no sync rows,
// no resync replacements, and a rev only on commits. The v2 client
// delivers a resync replacement as a create, so resync names their seqs.
func disaggV1Project(keys []disaggKey, resync map[uint64]bool) []disaggKey {
	var out []disaggKey
	for _, k := range keys {
		if k.Kind == segment.KindSync || resync[k.Seq] {
			continue
		}
		switch k.Kind {
		case segment.KindCreate, segment.KindUpdate, segment.KindDelete:
		default:
			k.Rev = ""
		}
		out = append(out, k)
	}
	return out
}

// disaggObserver is one long-lived subscriber on a reader pod.
type disaggObserver struct {
	pod   *disaggPod
	proto string
	done  chan struct{}

	mu          sync.Mutex
	v1          []disaggKey
	v2          []ObservedEvent
	last        uint64
	recoverable int
	fatal       []string
}

func newDisaggObserver(ctx context.Context, p *disaggPod, proto string) *disaggObserver {
	o := &disaggObserver{pod: p, proto: proto, done: make(chan struct{})}
	if proto == "v1" {
		go o.runV1(ctx)
	} else {
		go o.runV2(ctx)
	}
	return o
}

func (o *disaggObserver) progress() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return max(len(o.v1), len(o.v2))
}

func (o *disaggObserver) lastSeq() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.last
}

func (o *disaggObserver) fail(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.fatal = append(o.fatal, err.Error())
}

// runV2 is the real client from seq 0: archive download, cutover, live
// tail. It resumes after its last seq when the stream ends, as a consumer
// that persists its cursor would.
func (o *disaggObserver) runV2(ctx context.Context) {
	defer close(o.done)
	for ctx.Err() == nil {
		client, err := jetstream.Subscribe("http://jetstream.invalid",
			jetstream.WithHTTPClient(o.pod.ln.httpClient()),
			jetstream.WithAfterSeq(o.lastSeq()),
			jetstream.WithBatchSize(64),
		)
		if err != nil {
			o.fail(err)
			return
		}
		for batch, err := range client.Events(ctx) {
			if err != nil {
				if ctx.Err() != nil {
					break
				}
				if errors.Is(err, jetstream.ErrFatal) {
					o.fail(err)
					break
				}
				o.mu.Lock()
				o.recoverable++
				o.mu.Unlock()
				continue
			}
			for _, ev := range batch.Events() {
				oe, err := observedEventFromClientErr(ev)
				if err != nil {
					o.fail(err)
					continue
				}
				o.mu.Lock()
				o.v2 = append(o.v2, oe)
				o.last = oe.Seq
				o.mu.Unlock()
			}
		}
		_ = client.Close()
		disaggSleep(ctx, 50*time.Millisecond)
	}
}

// runV1 is a v1 JSON subscriber replaying from seq 1 and reconnecting after
// its last seq.
func (o *disaggObserver) runV1(ctx context.Context) {
	defer close(o.done)
	for ctx.Err() == nil {
		url := fmt.Sprintf("ws://jetstream.invalid/subscribe?cursor=%d", o.lastSeq()+1)
		conn, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPClient: o.pod.ln.httpClient()})
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		if err != nil {
			o.mu.Lock()
			o.recoverable++
			o.mu.Unlock()
			disaggSleep(ctx, 50*time.Millisecond)
			continue
		}
		conn.SetReadLimit(-1)
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				break
			}
			k, err := disaggV1Key(data)
			if err != nil {
				o.fail(err)
				continue
			}
			o.mu.Lock()
			o.v1 = append(o.v1, k)
			o.last = k.Seq
			o.mu.Unlock()
		}
		_ = conn.CloseNow()
		o.mu.Lock()
		o.recoverable++
		o.mu.Unlock()
		disaggSleep(ctx, 50*time.Millisecond)
	}
}

func disaggV1Key(data []byte) (disaggKey, error) {
	var ev streaming.JetstreamEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		return disaggKey{}, fmt.Errorf("decode v1 frame: %w", err)
	}
	k := disaggKey{Seq: ev.Cursor, DID: ev.DID}
	switch ev.Kind {
	case streaming.JetstreamKindCommit:
		if ev.Commit == nil {
			return k, fmt.Errorf("v1 commit without a commit at cursor %d", ev.Cursor)
		}
		k.Collection, k.Rkey, k.Rev = ev.Commit.Collection, ev.Commit.RKey, ev.Commit.Rev
		switch ev.Commit.Operation {
		case streaming.JetstreamOpCreate:
			k.Kind = segment.KindCreate
		case streaming.JetstreamOpUpdate:
			k.Kind = segment.KindUpdate
		case streaming.JetstreamOpDelete:
			k.Kind = segment.KindDelete
		default:
			return k, fmt.Errorf("v1 commit operation %q at cursor %d", ev.Commit.Operation, ev.Cursor)
		}
	case streaming.JetstreamKindIdentity:
		k.Kind = segment.KindIdentity
	case streaming.JetstreamKindAccount:
		k.Kind = segment.KindAccount
	default:
		return k, fmt.Errorf("v1 kind %q at cursor %d", ev.Kind, ev.Cursor)
	}
	return k, nil
}

func disaggSleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
