#!/usr/bin/env bash
# Oracle mutation campaign driver.
# See docs/superpowers/specs/2026-06-12-oracle-mutation-campaign-design.md
#
# Usage:
#   testing/mutation/run.sh                 # run every mutant
#   testing/mutation/run.sh m019            # run one mutant (filename prefix match)
#   testing/mutation/run.sh m002 --seeds 5  # stress sweep over 5 random seeds
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
MUTANTS_DIR="testing/mutation/mutants"

ONLY=""
SEEDS=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --seeds)
            if [[ $# -lt 2 ]]; then
                echo "error: --seeds requires a value" >&2
                exit 1
            fi
            SEEDS="$2"; shift 2 ;;
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

declare -a ROWS=()

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
        ROWS+=("| $id | STALE | patch no longer applies — refresh needed |")
        continue
    fi
    git apply --unidiff-zero "$patch"
    CURRENT_PATCH="$patch"

    if ! go build ./... >"$LOG_ROOT/$id.build.log" 2>&1; then
        echo "    BUILD-BROKEN (mutant does not compile — refresh needed)"
        ROWS+=("| $id | BUILD-BROKEN | mutant does not compile — refresh needed |")
        revert_current
        continue
    fi

    result=""
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
            result="| $id | KILLED@stress($kills/$SEEDS seeds) | flaky detection — see logs |"
        else
            result="| $id | SURVIVED($SEEDS seeds) | true escape candidate |"
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
                result="| $id | KILLED@$tier | ${note:-see log} |"
                echo "    KILLED@$tier"
                break
            fi
        done
        if [[ -z "$result" ]]; then
            result="| $id | SURVIVED | escape — analyze and disposition |"
            echo "    SURVIVED"
        fi
    fi

    revert_current
    ROWS+=("$result")
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
