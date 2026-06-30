package oracle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ============================================================================
// Same-seed trace determinism guard (issue #27 — "stabilize deterministic
// inputs"). READ THIS BEFORE DEBUGGING A FAILURE.
// ============================================================================
//
// WHAT THIS TEST DOES
//
// It runs the full oracle lifecycle (TestOracle_DefaultLifecycle) TWICE at the
// SAME seed, in two separate child processes, and asserts that the
// DETERMINISTIC-INPUT sections of the two run traces are byte-identical. It is
// the DoD item "a small same-seed oracle run can compare deterministic-input
// trace sections across repeated runs."
//
// WHY TWO CHILD PROCESSES (not a loop)
//
// The lifecycle runs inside a testing/synctest bubble, and the harness enforces
// ONE bubble per process (the synctestBubbleUsed guard defined in
// synctest_test.go, checked at the top of the harness). So we cannot
// run it twice in-process; instead we re-exec THIS test binary twice with
// `-test.run=^TestOracle_DefaultLifecycle$`, each child pinned to the same seed
// and writing its trace to a temp dir we then read back. This mirrors the
// restart tier's subprocess pattern (runRestartChild).
//
// WHY ONLY *SECTIONS*, NOT THE WHOLE TRACE
//
// The lifecycle does heavy concurrent work (≈100 backfill workers; the live
// consumer racing compaction at shutdown). The ORDER in which those goroutines
// finish is decided by the Go scheduler, NOT by our seed, so trace kinds like
// backfill_repo_complete / bootstrap_live_event / steady_state_event /
// compaction_pass / client_backfill_* legitimately differ run-to-run. Forcing
// them to match would mean serializing the concurrency the oracle exists to
// exercise — the wrong fix. So we compare only the kinds whose content is a
// pure function of the seed (config, fault schedule, phase progression,
// event-log equivalence counts): see
// deterministicTraceKinds below. This empirical split was measured (run twice,
// diff) before the allowlist was written; do not widen it without re-measuring.
//
// ----------------------------------------------------------------------------
// IF THIS TEST FAILS, HERE IS HOW TO DIAGNOSE IT (in likelihood order):
//
//  1. "deterministic section <kind>#<i> diverged" with a 1:/2: payload diff.
//     A trace field that USED to be a pure function of the seed is now
//     nondeterministic. By far the most likely cause is UNSORTED MAP ITERATION:
//     someone added a map-derived value to that kind's payload and ranged a Go
//     map without sorting first. Go randomizes map iteration order PER PROCESS,
//     and the two runs are separate processes, so the field differs.
//     (Note: wall-clock time is NOT a likely cause here — the lifecycle runs in
//     a synctest bubble with a deterministic fake clock, so time.Now() is
//     identical across same-seed runs. Verified: a time.Now() probe does NOT
//     trip this test; an unsorted-map probe DOES. The realistic culprits are
//     unsorted map iteration or unseeded RNG, which is exactly the #27
//     "sort map-derived output / seed jitter" surface this test polices.)
//     FIX: make that field deterministic at its source (sort the map keys, seed
//     the RNG, drop the nondeterministic field, or hash it stably). Do NOT
//     "fix" it by removing the kind from the allowlist unless you have proven
//     the field is genuinely concurrency-dependent and cannot be made stable —
//     and if so, say why in a comment next to the allowlist entry you remove.
//
//  2. "deterministic section line counts differ" (one run has more/fewer
//     allowlisted lines). A control-flow path now depends on scheduling: e.g. a
//     phase or a compaction pass fires a different NUMBER of times depending on
//     a race. Inspect which kind changed count (the failure logs both traces'
//     paths). If it's a genuinely racy count (like compaction_pass), that kind
//     should not be on the allowlist — but verify that's really the cause and
//     not a regression that made a deterministic step conditional on timing.
//
//  3. A child failed to produce a trace / exited non-zero. This is usually NOT
//     a determinism problem — it's the lifecycle itself failing at this seed
//     (look at the child's captured output in the failure log). If the default
//     seed started failing, that's a lifecycle regression to chase first;
//     determinism is unprovable until the run is green. (Seed 42 in fast+swarm
//     mode was chosen because it passes reliably; some other seeds hit known
//     seed-specific lifecycle failures unrelated to determinism.)
//
//  4. Flake / environment. The two children inherit this process's env. If you
//     set JETSTREAM_ORACLE_* in your shell, both children pick it up; that's
//     fine as long as BOTH get the same values (we force seed/mode/fault-mode
//     explicitly below, so a stray seed override is neutralized). go_version
//     and gomaxprocs are normalized out, so a different machine won't flake it.
// ----------------------------------------------------------------------------

const (
	// envTraceDeterminismChild marks a re-exec'd child of this harness so the
	// child path knows it is the inner lifecycle run, not the parent. (The
	// parent never sets the lifecycle test's own skip guard; the child is just
	// a normal `-test.run=^TestOracle_DefaultLifecycle$` invocation, so this is
	// only used to keep the parent from recursing into itself.)
	envTraceDeterminismChild = "JETSTREAM_ORACLE_TRACE_DETERMINISM_CHILD"

	// traceDeterminismSeed is a seed known to drive the fast+swarm lifecycle to
	// a clean PASS (some seeds hit known seed-specific lifecycle failures that
	// would mask the determinism signal). If you change it, re-verify the new
	// seed passes the lifecycle twice in a row before committing.
	traceDeterminismSeed = "42"

	// traceDeterminismChildTimeout bounds a single child lifecycle run. The
	// fast mode finishes in well under a second; this is a generous deadlock
	// guard, not a tight budget.
	traceDeterminismChildTimeout = 90 * time.Second
)

// deterministicTraceKinds is the allowlist of trace kinds whose payloads are a
// pure function of the seed, so two same-seed runs MUST emit them identically
// and in the same order. Everything NOT in this set is concurrency-dependent
// (goroutine completion order) and is deliberately excluded — see the file
// header for the empirical split and why widening this set requires
// re-measuring.
//
// Excluded-and-why (do not add without proof they became deterministic):
//   - backfill_repo_complete, bootstrap_live_event, steady_state_event:
//     emitted in worker/goroutine completion order.
//   - compaction_pass, compaction_over_drop_check, client_backfill_start/done:
//     depend on exactly where steady-state shutdown lands relative to the
//     compaction watermark — a timing boundary, not a seed input.
var deterministicTraceKinds = map[string]bool{
	"run_start":         true, // config echo (seed, counts, mode)
	"simulator_config":  true, // simulator world config
	"fault_plan":        true, // seeded fault schedule (also a #27 DoD: seeded jitter/faults)
	"phase":             true, // lifecycle phase progression
	"faults_fired":      true, // which scheduled faults fired (seeded)
	"event_log_compare": true, // expected/observed event-log equivalence counts per phase
	"steady_target":     true, // steady-state target seq
	"shutdown_start":    true,
	"runtime_exit":      true,
}

// normalizedTraceFields are deterministic-section payload fields that vary by
// HOST, not by seed, so they are stripped before comparison. go_version and
// gomaxprocs are recorded in run_start for debugging provenance but must not
// make a same-binary, different-machine comparison flake.
var normalizedTraceFields = []string{"go_version", "gomaxprocs"}

// deterministicSection is one allowlisted trace line reduced to (kind, payload)
// with host-dependent fields normalized out. Two same-seed runs must produce
// identical ordered slices of these.
type deterministicSection struct {
	kind    string
	payload string // canonical JSON (sorted keys) of the normalized data map
}

// TestOracle_SameSeedTraceDeterminism is the parent harness. See the file
// header for the full design and the failure-diagnosis guide.
//
// nolint:paralleltest // spawns subprocess lifecycle children; not parallel-safe with the bubble guard.
func TestOracle_SameSeedTraceDeterminism(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping same-seed trace determinism harness under -short (spawns subprocess lifecycle runs)")
	}
	if os.Getenv(envTraceDeterminismChild) == "1" {
		// Defensive: we re-exec only TestOracle_DefaultLifecycle, never this
		// test, so this branch should be unreachable. It exists so a future
		// wiring mistake skips instead of fork-bombing.
		t.Skip("trace determinism child must not re-run the parent harness")
	}

	first := runLifecycleForTrace(t, "first")
	second := runLifecycleForTrace(t, "second")

	firstSecs := deterministicSections(t, first)
	secondSecs := deterministicSections(t, second)

	// Anti-vacuity: the allowlisted sections must actually be present. If the
	// trace schema changes and these kinds vanish, an empty-vs-empty comparison
	// would pass silently and we'd be guarding nothing.
	require.NotEmptyf(t, firstSecs,
		"no deterministic trace sections found in %s — did the trace kinds change? "+
			"update deterministicTraceKinds.", first)
	require.GreaterOrEqualf(t, len(firstSecs), len(deterministicTraceKinds),
		"expected at least one line per allowlisted kind (%d kinds) but found %d sections in %s; "+
			"a deterministic kind may have stopped being emitted",
		len(deterministicTraceKinds), len(firstSecs), first)

	// Line-count check first: a count mismatch points at diagnosis case (2) in
	// the header (a control-flow path became scheduling-dependent).
	require.Equalf(t, len(firstSecs), len(secondSecs),
		"deterministic section line counts differ between same-seed runs (%d vs %d).\n"+
			"trace 1: %s\ntrace 2: %s\n"+
			"See the failure-diagnosis guide at the top of trace_determinism_test.go (case 2).",
		len(firstSecs), len(secondSecs), first, second)

	// Ordered content check: the load-bearing determinism assertion.
	for i := range firstSecs {
		a, b := firstSecs[i], secondSecs[i]
		require.Equalf(t, a, b,
			"deterministic trace section %s#%d diverged between same-seed runs:\n"+
				"  run 1: kind=%s data=%s\n"+
				"  run 2: kind=%s data=%s\n"+
				"trace 1: %s\ntrace 2: %s\n"+
				"A field that should be a pure function of the seed is now nondeterministic.\n"+
				"See the failure-diagnosis guide at the top of trace_determinism_test.go (case 1).",
			a.kind, i, a.kind, a.payload, b.kind, b.payload, first, second)
	}

	t.Logf("same-seed trace determinism: %d deterministic sections identical across two runs (seed=%s)",
		len(firstSecs), traceDeterminismSeed)
}

// runLifecycleForTrace re-execs this test binary to run ONE TestOracle_DefaultLifecycle
// at the pinned seed, writing its trace into a fresh temp dir, and returns the
// trace file path. The child is a plain lifecycle invocation; we force the
// seed/mode/fault-mode so a stray JETSTREAM_ORACLE_* in the caller's env cannot
// desynchronize the two runs.
func runLifecycleForTrace(t *testing.T, label string) string {
	t.Helper()

	traceDir := t.TempDir()
	// -test.run is anchored to the lifecycle test ONLY (not this harness), so
	// the child cannot recurse back into TestOracle_SameSeedTraceDeterminism.
	cmd := exec.CommandContext(t.Context(), os.Args[0],
		"-test.run=^TestOracle_DefaultLifecycle$", "-test.v", "-test.count=1")
	cmd.Env = append(os.Environ(),
		envTraceDeterminismChild+"=1",
		envOracleTraceDir+"="+traceDir,
		envOracleSeed+"="+traceDeterminismSeed,
		// fast mode keeps the run small; swarm mode exercises the seeded fault
		// schedule, which is itself part of #27's determinism surface.
		envOracleMode+"=fast",
		envOracleFaultMode+"="+FaultModeSwarm,
	)

	out, err := runWithTimeout(cmd, traceDeterminismChildTimeout)
	require.NoErrorf(t, err,
		"%s lifecycle child did not pass — determinism is unprovable until the run is green "+
			"(see diagnosis case 3 in trace_determinism_test.go).\noutput:\n%s", label, out)

	tracePath := filepath.Join(traceDir, "oracle-trace.jsonl")
	require.FileExistsf(t, tracePath,
		"%s lifecycle child produced no trace at %s\noutput:\n%s", label, tracePath, out)
	return tracePath
}

// runWithTimeout runs cmd to completion, capturing combined stdout+stderr, and
// kills it if it exceeds timeout (a deadlock guard — a healthy fast run
// finishes in well under a second). cmd.Wait writes both streams into one
// buffer before returning, so there is no concurrent writer to synchronize.
func runWithTimeout(cmd *exec.Cmd, timeout time.Duration) (string, error) {
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Start(); err != nil {
		return buf.String(), err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return buf.String(), err
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		<-done // reap so the buffer write has settled
		return buf.String(), fmt.Errorf("lifecycle child did not exit within %s", timeout)
	}
}

// deterministicSections reads a trace file and returns its allowlisted lines as
// canonical (kind, normalized-payload) pairs, in trace order.
func deterministicSections(t *testing.T, tracePath string) []deterministicSection {
	t.Helper()

	body, err := os.ReadFile(tracePath)
	require.NoError(t, err)

	var sections []deterministicSection
	for line := range strings.SplitSeq(string(body), "\n") {
		if line == "" {
			continue
		}
		var rec TraceRecord
		require.NoErrorf(t, json.Unmarshal([]byte(line), &rec),
			"malformed trace line in %s: %q", tracePath, line)
		if !deterministicTraceKinds[rec.Kind] {
			continue
		}
		data := make(map[string]any, len(rec.Data))
		maps.Copy(data, rec.Data)
		for _, f := range normalizedTraceFields {
			delete(data, f)
		}
		// json.Marshal sorts map keys, so this canonical form is itself
		// order-stable regardless of the payload map's iteration order.
		canon, err := json.Marshal(data)
		require.NoError(t, err)
		sections = append(sections, deterministicSection{kind: rec.Kind, payload: string(canon)})
	}
	return sections
}
