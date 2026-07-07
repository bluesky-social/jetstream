package oracle

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// TestOracle_RestartSegmentFault_FailsLoudThenRecovers is the oracle-level
// segment I/O fault tier for issue #200, mirroring the store-fault tier's
// fail-loud protocol (restart_storefault_test.go).
//
// Each case arms a deterministic segment I/O fault (op kind + process-wide
// ordinal + errno) in a first child running the full backfill->merge
// lifecycle. The contract: the runtime must FAIL LOUD — rt.Run surfaces the
// injected sentinel, the child writes the observed-marker, and the merge does
// NOT complete (anti-vacuity: the first child's after-merge barrier never
// fires). A second fault-free child then recovers idempotently to a clean
// after-merge exit, and the convergence bundle proves no event was silently
// lost to the faulted I/O.
//
// Case notes:
//   - write/enospc@1 fires on the very first segment header write (backfill
//     writer open) and must carry the #201 disk-full operator message
//     end-to-end — the parent asserts the message text recorded in the
//     observed-marker. This is the "injected ENOSPC -> clean crash -> clean
//     recovery proven end-to-end" DoD item.
//   - sync/eio@2 fires on the backfill writer's parent-dir fsync (each
//     writer open consults IOOpSync twice: init fsync + parent-dir fsync).
//   - write/shortwrite@3 fires on the first block-flush write after both
//     writers are open (backfill or bootstrap-live, whichever flushes first —
//     the fail-loud contract is path-agnostic).
//   - rename/eio@1 fires on the first Rewrite commit rename. Only
//     Patch/Rewrite ever rename, and the child's only rewrite driver is the
//     merge-tail compaction pass, so this deterministically targets the
//     compaction-rewrite path e2e (the plan's §9 open question resolves
//     without new barriers: merge-tail compaction runs before the
//     after-merge barrier). The chain spec always injects delete shapes, so
//     the pass genuinely drops rows and must rewrite.
//
// Import-patch fault coverage deliberately stays at the orchestrator level
// (segment_iofault_test.go in internal/ingest/orchestrator): the restart
// child never runs a timestamp import (operator-submitted via XRPC), and
// RunImport-level tests exercise the identical Patch seam and error path.
//
// nolint:paralleltest
func TestOracle_RestartSegmentFault_FailsLoudThenRecovers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping restart oracle under -short")
	}

	cases := []struct {
		name    string
		op      segment.IOOp
		ordinal int
		kind    string
		// wantOperatorMessage asserts the observed-marker content carries the
		// #201 disk-full operator message (ENOSPC cases only).
		wantOperatorMessage bool
		// liveEventsBetweenChildren generates fresh firehose traffic after the
		// faulted child exits and before the recovery child starts. Needed
		// when the fault lands AFTER the first child's bootstrap-live consumer
		// has archived every frame and persisted cursor == firehose tip: the
		// recovery child then resumes at the tip, observes nothing, and the
		// cutover delivery gate's zero-observations rule (deliberate — an
		// empty set normally means "consumer hasn't delivered yet") waits
		// forever. Fresh frames mirror reality (the relay doesn't stop when
		// jetstream restarts) and give the gate a non-empty observation set.
		liveEventsBetweenChildren int
		// skip marks a case that deterministically exposes a KNOWN pre-existing
		// bug; the case is the checked-in repro, re-enabled by the fix.
		skip string
	}{
		{name: "write-enospc-first-header", op: segment.IOOpWrite, ordinal: 1, kind: "enospc", wantOperatorMessage: true},
		{name: "sync-eio-writer-open", op: segment.IOOpSync, ordinal: 2, kind: "eio"},
		{name: "write-shortwrite-first-flush", op: segment.IOOpWrite, ordinal: 3, kind: "shortwrite", liveEventsBetweenChildren: 2},
		{name: "rename-eio-compaction-rewrite", op: segment.IOOpRename, ordinal: 1, kind: "eio"},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip != "" {
				t.Skip(tc.skip)
			}
			cfg := Config{
				Mode:                "restart",
				Seed:                restartSeed(10 + i),
				Accounts:            4,
				MinInitialRecords:   1,
				MaxInitialRecords:   4,
				LiveEventsBootstrap: 4,
				LiveEventsSteady:    4,
			}
			trace, _, closeTrace := newOracleTrace(t, "restart-segmentfault-"+tc.name+".jsonl")
			t.Cleanup(closeTrace)

			spec := deriveChainSpec(cfg.Seed, cfg.Accounts)
			recordTraceOrError(t, trace, "run_start", map[string]any{
				"mode":          cfg.Mode,
				"seed":          cfg.Seed,
				"go_version":    runtime.Version(),
				"gomaxprocs":    runtime.GOMAXPROCS(0),
				"accounts":      cfg.Accounts,
				"case":          "segmentfault-" + tc.name,
				"fault_op":      string(tc.op),
				"fault_ordinal": tc.ordinal,
				"fault_kind":    tc.kind,
				"chain_did_idx": spec.chainAccountIdx(),
				"chain_records": len(spec.records),
			})

			w := newRestartWorld(t, cfg)
			t.Cleanup(func() { require.NoError(t, w.Close()) })

			coord := newChainCoordinator(t, w, spec)
			srv := newRestartServer(t, w, coord.onGetRepoServed)
			t.Cleanup(srv.Close)

			dataDir := t.TempDir()
			markersDir := t.TempDir()
			observedPath := filepath.Join(markersDir, "segment-fault-observed")
			firstMergeDonePath := filepath.Join(markersDir, "first-after-merge")
			mergeDonePath := filepath.Join(markersDir, "after-merge")

			// First child: fault armed. Correct code surfaces the injected
			// error loud (sentinel observed via errors.Is on rt.Run's error);
			// a swallowing bug completes the lifecycle and reaches the
			// after-merge barrier instead, leaving the observed-marker absent.
			// The barrier is armed so that swallowed-error path exits promptly
			// (a clean fast kill) rather than timing out.
			first := runRestartChild(t, restartChildArgs{
				dataDir:                  dataDir,
				relayURL:                 srv.URL,
				mergeDonePath:            firstMergeDonePath,
				segmentFaultOp:           tc.op,
				segmentFaultOrdinal:      tc.ordinal,
				segmentFaultKind:         tc.kind,
				segmentFaultObservedPath: observedPath,
				timeout:                  30 * time.Second,
				trace:                    trace,
				runLabel:                 "first-segmentfault-" + tc.name,
			})
			recordTraceOrError(t, trace, "restart_child_result", traceRestartChildResult("first", first))
			require.NoErrorf(t, first.err,
				"first segment-fault child should exit cleanly (fail-loud is in-process, not a crash)\n%s", first.output)
			require.FileExistsf(t, observedPath,
				"runtime must FAIL LOUD on the injected segment %s fault (a swallowing bug completes the lifecycle instead)\n%s",
				tc.op, first.output)
			require.NoFileExistsf(t, firstMergeDonePath,
				"lifecycle completed despite the armed segment %s fault — fault did not bite, kill is vacuous\n%s",
				tc.op, first.output)

			if tc.wantOperatorMessage {
				// The observed-marker carries rt.Run's full error text; the
				// disk-full operator message (#201) must ride it end-to-end.
				observed, err := os.ReadFile(observedPath)
				require.NoError(t, err)
				msg := string(observed)
				require.Contains(t, msg, "fatal persistence error", "ENOSPC must carry the operator message")
				require.Contains(t, msg, "disk full")
				require.Contains(t, msg, dataDir)
				require.Contains(t, msg, "restart jetstream")
			}

			if tc.liveEventsBetweenChildren > 0 {
				generateN(t, w, tc.liveEventsBetweenChildren)
			}

			// Second child: no fault armed. Recovery re-runs the interrupted
			// phase idempotently and exits cleanly at the after-merge barrier.
			second := runRestartChild(t, restartChildArgs{
				dataDir:       dataDir,
				relayURL:      srv.URL,
				mergeDonePath: mergeDonePath,
				timeout:       30 * time.Second,
				trace:         trace,
				runLabel:      "second-segmentfault-" + tc.name,
			})
			recordTraceOrError(t, trace, "restart_child_result", traceRestartChildResult("second", second))
			require.NoErrorf(t, second.err, "recovery child should exit cleanly\n%s", second.output)
			require.FileExistsf(t, mergeDonePath, "recovery child must reach after-merge barrier before exiting")

			// Convergence: nothing the faulted I/O touched is lost or
			// silently corrupted — ObserveSegments walks every durable frame
			// (torn-tail aware for the active segment) and Compare holds.
			assertOracleMatchesAfterReplay(t, dataDir, w, cfg, "segmentfault-"+tc.name)
			assertChainDurable(t, dataDir, coord, "segmentfault-"+tc.name)
		})
	}
}

// TestOracle_RestartTornActiveSegmentTailRecovers is the post-crash
// truncate/corrupt-at-offset DoD item for issue #200: a first child is
// SIGKILLed mid-backfill (at the after-repo-complete crashpoint), the parent
// then mutates the active segment's tail the way a real torn write would —
// bytes past the last fully-durable frame — and a second child must recover
// via the torn-tail walk (resumeExistingSegment/lastGoodOffset truncates the
// tear) and converge with zero silent corruption.
//
// Only the un-fsynced tail is fair game: the store cursor advances after the
// block fsync, so a real crash can never tear an already-durable frame. Both
// mutations model that window:
//   - torn-frame: an 8-byte length prefix promising far more bytes than the
//     garbage body that follows (a partial block write with hostile content —
//     the corrupt-at-offset case; lastGoodOffset must not trust the prefix).
//   - torn-length-prefix: fewer than 8 trailing bytes (the write died inside
//     the length prefix itself).
//
// nolint:paralleltest
func TestOracle_RestartTornActiveSegmentTailRecovers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping restart oracle under -short")
	}

	cases := []struct {
		name   string
		mutate func(t *testing.T, path string)
	}{
		{
			name: "torn-frame-with-garbage-body",
			mutate: func(t *testing.T, path string) {
				var lenBuf [8]byte
				binary.LittleEndian.PutUint64(lenBuf[:], 1<<20)
				tail := append(lenBuf[:], make([]byte, 64)...)
				for i := range tail[8:] {
					tail[8+i] = byte(0xa5 ^ i)
				}
				appendBytes(t, path, tail)
			},
		},
		{
			name: "torn-length-prefix",
			mutate: func(t *testing.T, path string) {
				appendBytes(t, path, []byte{0xff, 0xee, 0xdd})
			},
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				Mode:                "restart",
				Seed:                restartSeed(20 + i),
				Accounts:            4,
				MinInitialRecords:   1,
				MaxInitialRecords:   4,
				LiveEventsBootstrap: 4,
				LiveEventsSteady:    4,
			}
			trace, _, closeTrace := newOracleTrace(t, "restart-torntail-"+tc.name+".jsonl")
			t.Cleanup(closeTrace)
			recordTraceOrError(t, trace, "run_start", map[string]any{
				"mode":       cfg.Mode,
				"seed":       cfg.Seed,
				"go_version": runtime.Version(),
				"gomaxprocs": runtime.GOMAXPROCS(0),
				"accounts":   cfg.Accounts,
				"case":       "torntail-" + tc.name,
			})

			w := newRestartWorld(t, cfg)
			t.Cleanup(func() { require.NoError(t, w.Close()) })
			srv := newRestartServer(t, w, nil)
			t.Cleanup(srv.Close)

			dataDir := t.TempDir()
			markersDir := t.TempDir()
			markerPath := filepath.Join(markersDir, "crash-marker")
			mergeDonePath := filepath.Join(markersDir, "after-merge")

			first := runRestartChild(t, restartChildArgs{
				dataDir:         dataDir,
				relayURL:        srv.URL,
				markerPath:      markerPath,
				crashPoint:      crashpoint.AfterRepoComplete,
				killAfterMarker: true,
				timeout:         30 * time.Second,
				trace:           trace,
				runLabel:        "first-torntail-" + tc.name,
			})
			recordTraceOrError(t, trace, "restart_child_result", traceRestartChildResult("first", first))
			require.True(t, wasSIGKILL(first.err),
				"first child should be killed at %s: err=%v\n%s", crashpoint.AfterRepoComplete, first.err, first.output)

			// Mutate the active (unsealed) primary segment's tail. The
			// backfill writer owns dataDir/segments; the highest-indexed file
			// is its active segment.
			activePath := activeSegmentPath(t, filepath.Join(dataDir, "segments"))
			tc.mutate(t, activePath)
			recordTraceOrError(t, trace, "torn_tail_mutation", map[string]any{
				"case": tc.name,
				"path": activePath,
			})

			second := runRestartChild(t, restartChildArgs{
				dataDir:       dataDir,
				relayURL:      srv.URL,
				mergeDonePath: mergeDonePath,
				timeout:       30 * time.Second,
				trace:         trace,
				runLabel:      "second-torntail-" + tc.name,
			})
			recordTraceOrError(t, trace, "restart_child_result", traceRestartChildResult("second", second))
			require.NoErrorf(t, second.err, "recovery child should exit cleanly despite the torn tail\n%s", second.output)
			require.FileExists(t, mergeDonePath, "recovery child must reach after-merge barrier before exiting")

			// Strict final-state assertion (mirrors the crash-points tier):
			// the torn bytes were never durable, so recovery must converge
			// exactly, not merely modulo replay.
			assertOracleMatches(t, dataDir, w, cfg, "torntail-"+tc.name)
		})
	}
}

// activeSegmentPath returns the highest-indexed segment file in dir and
// requires it to be active (unsealed): its checksum bytes at offset 4..11 are
// zero. Mutating a sealed file would model a different failure class
// (bit-rot, covered by reader checksum tests), not a torn write.
func activeSegmentPath(t *testing.T, dir string) string {
	t.Helper()
	files, err := ingest.SegmentFiles(dir)
	require.NoError(t, err)
	require.NotEmpty(t, files, "first child must have created at least one segment before the kill")
	path := files[len(files)-1].Path

	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	var checksum [8]byte
	_, err = f.ReadAt(checksum[:], 4)
	require.NoError(t, err)
	require.Zero(t, binary.LittleEndian.Uint64(checksum[:]),
		"highest segment %s is sealed; expected an active segment mid-backfill", path)
	return path
}

func appendBytes(t *testing.T, path string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	_, werr := f.Write(b)
	require.NoError(t, f.Close())
	require.NoError(t, werr)
}
