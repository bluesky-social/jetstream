#!/usr/bin/env bash
# Oracle mutation campaign driver.
# See docs/superpowers/specs/2026-06-12-oracle-mutation-campaign-design.md
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
if [[ "$RACE" -eq 1 ]]; then
    RACE_FLAG=(-race)
    default_timeout="15m"
    stress_timeout="90m"
    restart_timeout="30m"
    echo "mutation campaign: race detector ENABLED (timeouts default=$default_timeout stress=$stress_timeout restart=$restart_timeout)"
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
