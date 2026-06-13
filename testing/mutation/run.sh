#!/usr/bin/env bash
# Oracle mutation campaign driver.
# See docs/superpowers/specs/2026-06-12-oracle-mutation-campaign-design.md
#
# Usage:
#   testing/mutation/run.sh                 # run every mutant
#   testing/mutation/run.sh m007            # run one mutant (filename prefix match)
#   testing/mutation/run.sh m007 --seeds 5  # stress sweep over 5 random seeds
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
MUTANTS_DIR="testing/mutation/mutants"

ONLY=""
SEEDS=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --seeds) SEEDS="$2"; shift 2 ;;
        *) ONLY="$1"; shift ;;
    esac
done

if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "error: uncommitted changes to tracked files; commit or stash first" >&2
    exit 1
fi
if [[ "$SEEDS" -gt 0 && -z "$ONLY" ]]; then
    echo "error: --seeds requires a single mutant id" >&2
    exit 1
fi

LOG_ROOT="$(mktemp -d -t mutation-campaign-XXXXXX)"

# Safety net: any exit path (including Ctrl-C) reverts an applied mutant.
CURRENT_PATCH=""
cleanup() {
    if [[ -n "$CURRENT_PATCH" ]]; then
        git apply -R "$CURRENT_PATCH" 2>/dev/null || true
        CURRENT_PATCH=""
    fi
}
trap cleanup EXIT INT TERM

declare -a ROWS=()

for patch in "$MUTANTS_DIR"/*.patch; do
    id="$(basename "$patch" .patch)"
    if [[ -n "$ONLY" && "$id" != "$ONLY"* ]]; then
        continue
    fi

    tiers="$(sed -n 's/^tiers: *//p' "$patch" | head -n1)"
    tiers="${tiers:-default,stress}"
    echo "=== $id (tiers: $tiers) ==="

    if ! git apply --check "$patch" 2>"$LOG_ROOT/$id.apply.log"; then
        echo "    STALE (patch no longer applies — refresh needed)"
        ROWS+=("| $id | STALE | patch no longer applies — refresh needed |")
        continue
    fi
    git apply "$patch"
    CURRENT_PATCH="$patch"

    if ! go build ./... >"$LOG_ROOT/$id.build.log" 2>&1; then
        echo "    BUILD-BROKEN (mutant does not compile — refresh needed)"
        ROWS+=("| $id | BUILD-BROKEN | mutant does not compile — refresh needed |")
        git apply -R "$patch"
        CURRENT_PATCH=""
        continue
    fi

    result=""
    if [[ "$SEEDS" -gt 0 ]]; then
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
                    cmd=(go test ./internal/oracle -run 'TestOracle_DefaultLifecycle$' -count=1 -short) ;;
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

    git apply -R "$patch"
    CURRENT_PATCH=""
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
