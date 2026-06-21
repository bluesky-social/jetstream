#!/usr/bin/env bash
# Oracle mutation campaign driver.
# See docs/superpowers/specs/2026-06-12-oracle-mutation-campaign-design.md
#
# Usage:
#   testing/mutation/run.sh                 # run every mutant
#   testing/mutation/run.sh m019            # run one mutant (filename prefix match)
#   testing/mutation/run.sh m002 --seeds 5  # stress sweep over 5 random seeds
#   testing/mutation/run.sh --json out.json # also emit a machine-readable result
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
        *) ONLY="$1"; shift ;;
    esac
done

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
        for i in $(seq 1 "$SEEDS"); do
            seed="$(od -An -N8 -tu8 /dev/urandom | tr -d ' ')"
            echo "    seed sweep $i/$SEEDS seed=$seed"
            if ! env JETSTREAM_ORACLE_MODE=stress JETSTREAM_ORACLE_SEED="$seed" \
                go test ./internal/oracle -run 'TestOracle_DefaultLifecycle$' \
                -count=1 -timeout 30m >"$LOG_ROOT/$id.seed$i.log" 2>&1; then
                kills=$((kills + 1))
                echo "        KILLED seed=$seed"
            fi
        done
        if [[ "$kills" -gt 0 ]]; then
            disposition="KILLED"; result="KILLED@stress($kills/$SEEDS seeds)"; note="flaky detection — see logs"
        else
            disposition="SURVIVED"; result="SURVIVED($SEEDS seeds)"; note="true escape candidate"
        fi
    else
        for tier in ${tiers//,/ }; do
            case "$tier" in
                default)
                    # -timeout 5m bounds a mutant that breaks LIVENESS rather
                    # than tripping an assertion: e.g. a delete->update mutation
                    # stalls the bootstrap seq-ack contiguity wait so the
                    # after-bootstrap barrier never releases. A healthy -short
                    # run finishes in ~1s, so the timeout only fires on a hung
                    # mutant, where it is the kill signal (non-zero exit) instead
                    # of Go's silent 10m default.
                    cmd=(go test ./internal/oracle -run 'TestOracle_DefaultLifecycle$' -count=1 -short -timeout 5m) ;;
                stress)
                    cmd=(env JETSTREAM_ORACLE_MODE=stress go test ./internal/oracle
                         -run 'TestOracle_DefaultLifecycle$' -count=1 -timeout 30m) ;;
                restart)
                    cmd=(go test ./internal/oracle -run 'TestOracle_RestartCrashPointsDoNotLoseRecords$'
                         -count=1 -timeout 10m) ;;
                *)
                    echo "error: unknown tier '$tier' in $id" >&2
                    exit 1 ;;
            esac
            echo "    tier=$tier ..."
            if ! "${cmd[@]}" >"$LOG_ROOT/$id.$tier.log" 2>&1; then
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
    # commit is recorded so the gate can confirm the result describes the tree
    # it is gating. A partial run (a single --json with a mutant filter) is still
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
