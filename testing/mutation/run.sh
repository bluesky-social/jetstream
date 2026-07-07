#!/usr/bin/env bash
# Oracle mutation campaign driver.
# See specs/mutation.md
#
# Usage:
#   testing/mutation/run.sh                 # run every mutant
#   testing/mutation/run.sh m019            # run one mutant (filename prefix match)
#   testing/mutation/run.sh m002 --seeds 5  # stress sweep over 5 random seeds
#   testing/mutation/run.sh --json out.json # also emit a machine-readable result
#   testing/mutation/run.sh --race          # run every tier under -race (#107)
#
# The optional --json file is the contract consumed by the #108 campaign gate
# (testing/mutation/gate): a stable {commit, mutants:[{id, disposition, result,
# note}]} document the gate diffs against the committed baseline. The markdown
# table on stdout stays the human-facing view; the JSON is the enforced one.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
MUTANTS_DIR="testing/mutation/mutants"

ONLY=""
SEEDS=0
JSON_OUT=""
RACE=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --seeds)
            if [[ $# -lt 2 ]]; then
                echo "error: --seeds requires a value" >&2
                exit 1
            fi
            SEEDS="$2"; shift 2 ;;
        --json)
            if [[ $# -lt 2 ]]; then
                echo "error: --json requires a path" >&2
                exit 1
            fi
            JSON_OUT="$2"; shift 2 ;;
        --race)
            RACE=1; shift ;;
        *) ONLY="$1"; shift ;;
    esac
done

# Under --race the data-race detector instruments every tier's `go test`, so a
# race-only regression in the stress/restart interleavings (the #107 gap: the
# only existing race coverage is ci.yml's default-mode lane) becomes a kill. The
# detector slows execution ~5-15x, so each tier's timeout is widened to keep a
# healthy run from racing its own bound and reading as a false liveness kill.
# A genuine hang still kills via the (larger) timeout. The default-tier -short
# run is ~1s clean, so even 10x under race stays well inside 15m; that timeout
# only fires on a real hang. RACE_FLAG expands to nothing when disabled (safe
# under set -u in bash >=4.4).
RACE_FLAG=()
default_timeout="5m"
stress_timeout="30m"
restart_timeout="10m"
# The storefault tier runs the same subprocess-restart machinery as the restart
# tier (two child runtimes), so it shares the restart timeout budget. The
# segmentfault tier is its segment-I/O sibling and shares the same shape.
storefault_timeout="10m"
segmentfault_timeout="10m"
if [[ "$RACE" -eq 1 ]]; then
    RACE_FLAG=(-race)
    default_timeout="15m"
    stress_timeout="90m"
    restart_timeout="30m"
    storefault_timeout="30m"
    segmentfault_timeout="30m"
    echo "mutation campaign: race detector ENABLED (timeouts default=$default_timeout stress=$stress_timeout restart=$restart_timeout storefault=$storefault_timeout segmentfault=$segmentfault_timeout)"
fi

if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "error: uncommitted changes to tracked files; commit or stash first" >&2
    exit 1
fi
if ! [[ "$SEEDS" =~ ^[0-9]+$ ]]; then
    echo "error: --seeds must be a non-negative integer" >&2
    exit 1
fi
if [[ "$SEEDS" -gt 0 && -z "$ONLY" ]]; then
    echo "error: --seeds requires a single mutant id" >&2
    exit 1
fi

LOG_ROOT="$(mktemp -d -t mutation-campaign-XXXXXX)"

# CURRENT_PATCH names the mutant currently applied to the working tree, or ""
# when the tree is clean. Every git-apply is paired with revert_current so this
# invariant holds at every loop boundary; the EXIT trap is the backstop.
CURRENT_PATCH=""

# revert_current undoes the applied mutant and clears CURRENT_PATCH. A failed
# reverse means the tree is dirty and we cannot trust any further result, so we
# crash loud rather than silently continuing on corrupted state (project
# directive: fail loud over corrupt). Callers run this OUTSIDE `if !` guards so
# `set -e`/the explicit exit aborts the campaign.
revert_current() {
    if [[ -z "$CURRENT_PATCH" ]]; then
        return 0
    fi
    if ! git apply --unidiff-zero -R "$CURRENT_PATCH"; then
        echo "FATAL: failed to revert $CURRENT_PATCH; working tree is DIRTY — aborting" >&2
        CURRENT_PATCH=""
        exit 2
    fi
    CURRENT_PATCH=""
}

# log_is_build_failure reports whether a `go test` log failed because the TEST
# package did not compile, rather than because an oracle assertion fired. The
# preceding `go build ./...` gate only compiles non-test code, so a mutant that
# edits a symbol used by _test.go files compiles for the build but breaks `go
# test`'s package build. Without this, that compile error is recorded as a
# KILLED — a false "the oracle detected the bug" when the oracle never ran. We
# match `go test`'s own compile-failure framing (`[build failed]` / the `FAIL
# pkg [build failed]` line / the leading `# pkg` diagnostic header) rather than
# substrings like "undefined:" that could legitimately appear inside an oracle
# assertion message.
log_is_build_failure() {
    grep -qE '\[build failed\]|^# [^ ]' "$1"
}

# EXIT is the single cleanup backstop: it fires after normal exit and after the
# signal traps below re-exit, so trapping cleanup on INT/TERM too would just
# double-invoke it. The signal traps set a conventional non-zero status so a
# Ctrl-C'd campaign does not masquerade as success.
trap revert_current EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# json_escape emits a JSON-safe rendering of its argument (sans surrounding
# quotes). Notes originate from grepped 'oracle: ...' lines, so backslashes and
# double-quotes are the realistic hazards and are backslash-escaped. Every C0
# control byte (U+0000–U+001F: tabs, newlines, carriage returns, and ANSI/
# terminal bytes that can leak in from `go test` output) is then flattened to a
# space: encoding/json in the #108 gate rejects ANY raw control byte inside a
# string, so leaving even a lone \r in a note would make the whole result
# document undecodable and silently break the gate. tr is coreutils, so this
# keeps the JSON contract free of a jq dependency on the runner.
json_escape() {
    local s=$1
    s=${s//\\/\\\\}
    s=${s//\"/\\\"}
    printf '%s' "$s" | LC_ALL=C tr '\000-\037' ' '
}

declare -a ROWS=()
declare -a JSON_ROWS=()

# record_result appends one row to both the markdown table and the JSON
# accumulator from a single normalized (id, disposition, result, note) tuple, so
# the human and machine views can never disagree. disposition is the gate's
# coarse verdict (KILLED|SURVIVED|STALE|BUILD-BROKEN); result carries the
# tier/seed detail for humans.
record_result() {
    local id=$1 disposition=$2 result=$3 note=$4
    ROWS+=("| $id | $result | $note |")
    JSON_ROWS+=("$(printf '{"id":"%s","disposition":"%s","result":"%s","note":"%s"}' \
        "$(json_escape "$id")" "$(json_escape "$disposition")" \
        "$(json_escape "$result")" "$(json_escape "$note")")")
}

for patch in "$MUTANTS_DIR"/*.patch; do
    id="$(basename "$patch" .patch)"
    if [[ -n "$ONLY" && "$id" != "$ONLY"* ]]; then
        continue
    fi

    tiers="$(sed -n 's/^tiers: *//p' "$patch" | head -n1)"
    tiers="${tiers:-default,stress}"
    echo "=== $id (tiers: $tiers) ==="

    if ! git apply --unidiff-zero --check "$patch" 2>"$LOG_ROOT/$id.apply.log"; then
        echo "    STALE (patch no longer applies — refresh needed)"
        record_result "$id" "STALE" "STALE" "patch no longer applies — refresh needed"
        continue
    fi
    git apply --unidiff-zero "$patch"
    CURRENT_PATCH="$patch"

    if ! go build ./... >"$LOG_ROOT/$id.build.log" 2>&1; then
        echo "    BUILD-BROKEN (mutant does not compile — refresh needed)"
        record_result "$id" "BUILD-BROKEN" "BUILD-BROKEN" "mutant does not compile — refresh needed"
        revert_current
        continue
    fi

    disposition=""
    result=""
    note=""
    if [[ "$SEEDS" -gt 0 ]]; then
        # A non-zero test exit counts as a kill. The preceding `go build` rules
        # out compile errors, but an infra failure (timeout, OOM) is still
        # indistinguishable from a real oracle assertion here; inspect the
        # per-seed logs before trusting a low kill count.
        kills=0
        build_broken=0
        for i in $(seq 1 "$SEEDS"); do
            seed="$(od -An -N8 -tu8 /dev/urandom | tr -d ' ')"
            echo "    seed sweep $i/$SEEDS seed=$seed"
            if ! env JETSTREAM_ORACLE_MODE=stress JETSTREAM_ORACLE_SEED="$seed" \
                go test "${RACE_FLAG[@]}" ./internal/oracle -run 'TestOracle_DefaultLifecycle$' \
                -count=1 -timeout "$stress_timeout" >"$LOG_ROOT/$id.seed$i.log" 2>&1; then
                # A test-package compile error fails every seed identically and is
                # NOT a kill (the oracle never ran); reclassify and stop sweeping.
                if log_is_build_failure "$LOG_ROOT/$id.seed$i.log"; then
                    build_broken=1
                    echo "        BUILD-BROKEN (test package does not compile)"
                    break
                fi
                kills=$((kills + 1))
                echo "        KILLED seed=$seed"
            fi
        done
        if [[ "$build_broken" -eq 1 ]]; then
            disposition="BUILD-BROKEN"; result="BUILD-BROKEN"; note="test package failed to compile — refresh needed"
        elif [[ "$kills" -gt 0 ]]; then
            disposition="KILLED"; result="KILLED@stress($kills/$SEEDS seeds)"; note="flaky detection — see logs"
        else
            disposition="SURVIVED"; result="SURVIVED($SEEDS seeds)"; note="true escape candidate"
        fi
    else
        for tier in ${tiers//,/ }; do
            case "$tier" in
                default)
                    # -timeout bounds a mutant that breaks LIVENESS rather
                    # than tripping an assertion: e.g. a delete->update mutation
                    # stalls the bootstrap seq-ack contiguity wait so the
                    # after-bootstrap barrier never releases. A healthy -short
                    # run finishes in ~1s, so the timeout only fires on a hung
                    # mutant, where it is the kill signal (non-zero exit) instead
                    # of Go's silent 10m default. (Widened under --race.)
                    cmd=(go test "${RACE_FLAG[@]}" ./internal/oracle -run 'TestOracle_DefaultLifecycle$' -count=1 -short -timeout "$default_timeout") ;;
                stress)
                    cmd=(env JETSTREAM_ORACLE_MODE=stress go test "${RACE_FLAG[@]}" ./internal/oracle
                         -run 'TestOracle_DefaultLifecycle$' -count=1 -timeout "$stress_timeout") ;;
                restart)
                    cmd=(go test "${RACE_FLAG[@]}" ./internal/oracle -run 'TestOracle_RestartCrashPointsDoNotLoseRecords$'
                         -count=1 -timeout "$restart_timeout") ;;
                restart-multisource)
                    # Multi-source restart tier (#209): kills m003, the merge
                    # source-cursor off-by-one. The test forces one
                    # bootstrap-live event per source segment, kills after a
                    # later source is flushed but before its cursor commit, and
                    # asserts a previously committed source is not reprocessed
                    # during recovery.
                    cmd=(go test "${RACE_FLAG[@]}" ./internal/oracle
                         -run 'TestOracle_RestartMultiSourceMergeCursorNoReprocess$'
                         -count=1 -timeout "$restart_timeout") ;;
                storefault)
                    # Store-fault tier (#30): kills swallowed-persistence-error
                    # mutants (m006 and kin) at two layers in one `go test`:
                    #   - oracle level: TestOracle_RestartStoreFault* drives a
                    #     real runtime through the merge with a metadata-store
                    #     fault on the source-cursor commit and asserts fail-loud
                    #     + recovery convergence;
                    #   - orchestrator unit level: TestMerge_StoreFault*,
                    #     TestMerge_MultiSourceDrainsAllSources, and
                    #     TestCompaction_StoreFault* pin the same fail-loud /
                    #     no-silent-advance contract fast and directly on
                    #     runMerge / runDeleteCompaction.
                    # Both packages run so a regression in either layer is a kill.
                    cmd=(go test "${RACE_FLAG[@]}"
                         ./internal/oracle ./internal/ingest/orchestrator
                         -run 'TestOracle_RestartStoreFault|TestMerge_StoreFault|TestMerge_MultiSourceDrainsAllSources|TestCompaction_StoreFault'
                         -count=1 -timeout "$storefault_timeout") ;;
                segmentfault)
                    # Segment-fault tier (#200): kills swallowed segment-file
                    # I/O error mutants (m044, m045) at three layers in one
                    # `go test`:
                    #   - oracle level: TestOracle_RestartSegmentFault* drives a
                    #     real runtime with a deterministic segment I/O fault
                    #     (write/sync/rename by process-wide ordinal) and asserts
                    #     fail-loud + recovery convergence; the rename case
                    #     deterministically lands on the merge-tail compaction
                    #     rewrite. TestOracle_RestartTornActiveSegmentTail*
                    #     covers post-crash truncate/corrupt-at-offset recovery.
                    #   - orchestrator unit level: TestRunDeleteCompaction_ENOSPC*
                    #     and TestRunImport_*SegmentIOFault*/ENOSPC* pin the
                    #     fail-loud + disk-full operator-message contract
                    #     directly on runDeleteCompaction / RunImport.
                    #   - segment unit level: TestFlushReturnsENOSPC* and the
                    #     Patch/Rewrite (op, ordinal) fault sweeps pin every seam
                    #     consult, so a dropped/reordered consult is a fast kill.
                    cmd=(go test "${RACE_FLAG[@]}"
                         ./internal/oracle ./internal/ingest/orchestrator ./segment
                         -run 'TestOracle_RestartSegmentFault|TestOracle_RestartTornActiveSegmentTail|TestRunDeleteCompaction_ENOSPC|TestRunImport_ENOSPC|TestRunImport_SegmentIOFaultSweep|TestFlushReturnsENOSPC|TestPatchIOFaultSweep|TestRewriteIOFaultSweep|TestNewRemovesEmptyFileWhenInitFails'
                         -count=1 -timeout "$segmentfault_timeout") ;;
                powerloss)
                    # Power-loss tier (#264): strict in-memory storage and
                    # ordering checks for fsync omission/reordering mutants.
                    # This is deliberately narrow and fast: segment/store/ingest
                    # package tests prove written-vs-synced state is discarded
                    # correctly, the oracle tests exercise the shared
                    # segment+Pebble strict FS plus the durable-op checker, and
                    # the orchestrator's TestRunMerge_StrictMemPowerLoss* pin the
                    # merge-cleanup / restart-after-cleanup guard fsync ordering
                    # (kills m051, the data-dir fsync deletion that lets a power
                    # loss re-drain already-merged survivors).
                    cmd=(go test "${RACE_FLAG[@]}"
                         ./segment ./internal/store ./internal/ingest ./internal/ingest/orchestrator ./internal/oracle
                         -run 'TestStrictMem|TestOpen_StrictMemDropsUnsyncedWrites|TestWriterStrictMem|TestWriterFlushOrdersSegmentSyncBeforeStoreCommit|TestDurableOrderRecorder|TestOracle_PowerLossStrictMemDropsUnsyncedState|TestRunMerge_StrictMemPowerLoss'
                         -count=1 -short -timeout "$default_timeout") ;;
                partb)
                    # Part-B tier (#182): kills paginated-cutover mutants
                    # (continuation-cursor off-by-one, mid-segment cut reporting
                    # the enclosing segment MaxSeq, zero-units-unadvanced
                    # livelock, the §14 below-floor 400 silently clamped, and the
                    # client treating that 400 as fatal instead of
                    # re-backfilling). These paths are NOT exercised by
                    # TestOracle_DefaultLifecycle (default config never truncates a
                    # plan or ages a cursor below the floor). Two layers run in one
                    # `go test`: the oracle's hermetic §16 end-to-end scenarios
                    # (TestPartB*) and the manifest planner's per-page truncation
                    # unit tests (TestPlanBackfill*), which kill the planner
                    # mutants fast and directly without waiting on a client-loop
                    # livelock timeout. Fast (~1s).
                    cmd=(go test "${RACE_FLAG[@]}"
                         ./internal/oracle ./internal/manifest
                         -run 'TestPartB|TestPlanBackfill'
                         -count=1 -short -timeout "$default_timeout") ;;
                tombstone)
                    # Tombstone tier (#184): kills live compaction-suppression
                    # mutants in tombstone.Snapshot.ShouldDrop / observeLocked
                    # (e.g. m022, which inverts the DID-tombstone seq guard so a
                    # superseded row survives and a reactivated row is dropped —
                    # a data-loss path). TestOracle_DefaultLifecycle no longer
                    # reaches these directly (the overlay-reconstruction oracle
                    # that used to was deleted in #177), but the tombstone unit
                    # tests assert ShouldDrop in BOTH seq directions, so they
                    # kill the inversion fast and without an end-to-end run.
                    cmd=(go test "${RACE_FLAG[@]}" ./internal/tombstone
                         -count=1 -timeout "$default_timeout") ;;
                corpus)
                    # Real-data corpus tier (#32): kills symmetric protocol
                    # bugs the closed atmos loop structurally cannot see
                    # (e.g. m009, the checksum-range off-by-one where the
                    # write and read sides shift identically so every
                    # write-then-read-back check passes). The corpus pins
                    # real network bytes and byte-exact golden outputs
                    # produced by known-good builds and foreign
                    # implementations, so a symmetric shift fails against
                    # the committed facts. Offline and fast (<1s).
                    cmd=(go test "${RACE_FLAG[@]}" ./internal/corpus
                         -count=1 -timeout "$default_timeout") ;;
                compaction)
                    # Compaction-boundary tier (#199): kills watermark boundary
                    # mutants (e.g. m002, the first-init floor off-by-one) with
                    # deterministic boundary-exact scenarios instead of stress
                    # seed luck. The orchestrator tests pin a tombstone/survivor
                    # pair EXACTLY at the first-init watermark: the mutant's
                    # floor claims that seq as already-compacted, the merge-tail
                    # pass no-ops, and the superseded row survives permanently
                    # (fold windows are exclusive below W, so the miss can never
                    # heal). Fast (<1s) and seed-independent.
                    cmd=(go test "${RACE_FLAG[@]}" ./internal/ingest/orchestrator
                         -run 'TestInitCompactionWatermarkFloor|TestMerge_FirstInitWatermarkFloor'
                         -count=1 -timeout "$default_timeout") ;;
                frames)
                    # Frame-adversity tier (#206): kills mutants that disarm
                    # the consumer's poison-frame handling (e.g. m042, the
                    # per-op-drop arm discarding well-formed siblings of a
                    # partial-CAR commit). Two layers in one `go test`: the
                    # oracle's frame-fault scenarios drive the REAL consumer
                    # against the simulator relay with injected garbage /
                    # unknown-type / error / oversized / swallowed / stripped-
                    # leaf frames and assert exact-multiset archives, labeled
                    # counters, and self-heal; the live-package unit tests pin
                    # the missing-block and malformed-event arms directly.
                    # Fast (~1s).
                    cmd=(go test "${RACE_FLAG[@]}"
                         ./internal/oracle ./internal/ingest/live
                         -run 'TestOracle_Frame|TestProcessBatch'
                         -count=1 -short -timeout "$default_timeout") ;;
                replay)
                    # Relay seq-replay tier (#205): kills mutants that disarm
                    # the replay protections (e.g. m035, the #account replay
                    # guard vacuously disabled). Two layers in one `go test`:
                    # the oracle's replay-fault scenarios drive the REAL live
                    # consumer against the simulator relay with duplicate-N /
                    # regress-to-K seq replays over an account-delete →
                    # reactivate → recreate window and assert final-state
                    # convergence + exact-multiset event log + guard-fired
                    # anti-vacuity; the live-package unit test pins the
                    # applied-seq drop contract directly. Fast (<1s).
                    cmd=(go test "${RACE_FLAG[@]}"
                         ./internal/oracle ./internal/ingest/live
                         -run 'TestOracle_RelaySeq|TestProcessBatch_ReplayedAccountEvent'
                         -count=1 -timeout "$default_timeout") ;;
                *)
                    echo "error: unknown tier '$tier' in $id" >&2
                    exit 1 ;;
            esac
            echo "    tier=$tier ..."
            if ! "${cmd[@]}" >"$LOG_ROOT/$id.$tier.log" 2>&1; then
                # A test-package compile error is NOT a kill: the oracle never
                # ran. `go build ./...` above only compiles non-test code, so a
                # mutant touching a _test.go-only symbol slips through to here.
                # Reclassify as BUILD-BROKEN (refresh needed), not KILLED.
                if log_is_build_failure "$LOG_ROOT/$id.$tier.log"; then
                    disposition="BUILD-BROKEN"; result="BUILD-BROKEN"
                    note="test package failed to compile — refresh needed"
                    echo "    BUILD-BROKEN (test package does not compile)"
                    break
                fi
                note="$(grep -m1 -o 'oracle: [^"]*' "$LOG_ROOT/$id.$tier.log" | head -c 140 || true)"
                # A liveness-breaking mutant kills via test timeout, not an
                # assertion, so there is no 'oracle:' line; surface that as the
                # reason instead of a bare "see log".
                if [[ -z "$note" ]] && grep -q 'panic: test timed out' "$LOG_ROOT/$id.$tier.log"; then
                    note="hang: test timed out, no oracle assertion (likely liveness break — e.g. barrier never releases; inspect log for the blocked goroutine)"
                fi
                disposition="KILLED"; result="KILLED@$tier"; note="${note:-see log}"
                echo "    KILLED@$tier"
                break
            fi
        done
        if [[ -z "$result" ]]; then
            disposition="SURVIVED"; result="SURVIVED"; note="escape — analyze and disposition"
            echo "    SURVIVED"
        fi
    fi

    revert_current
    record_result "$id" "$disposition" "$result" "$note"
done

if [[ ${#ROWS[@]} -eq 0 ]]; then
    echo "error: no mutants matched '$ONLY'" >&2
    exit 1
fi

echo
echo "| mutant | result | note |"
echo "|---|---|---|"
printf '%s\n' "${ROWS[@]}"
echo
echo "logs: $LOG_ROOT"

if [[ -n "$JSON_OUT" ]]; then
    # commit is recorded for provenance/triage only — it identifies the tree the
    # campaign ran against in logs and refresh PRs. The gate does NOT validate it
    # (Evaluate diffs dispositions, not commits); the CI flow runs the campaign
    # fresh at HEAD and gates the just-produced result, so the tree is HEAD by
    # construction. A partial run (a single --json with a mutant filter) is still
    # emitted; the gate decides whether partial coverage is acceptable.
    commit="$(git rev-parse HEAD)"
    {
        printf '{\n'
        printf '  "commit": "%s",\n' "$(json_escape "$commit")"
        printf '  "mutants": [\n'
        for i in "${!JSON_ROWS[@]}"; do
            sep=","
            if [[ "$i" -eq $((${#JSON_ROWS[@]} - 1)) ]]; then sep=""; fi
            printf '    %s%s\n' "${JSON_ROWS[$i]}" "$sep"
        done
        printf '  ]\n'
        printf '}\n'
    } >"$JSON_OUT"
    echo "json: $JSON_OUT"
fi
