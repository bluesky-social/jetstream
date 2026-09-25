set shell := ["bash", "-cu"]
set dotenv-load

# Runs the linter and tests
default: lint test

# Lints the code
lint:
    golangci-lint run --timeout 5m ./...

# Scans module dependencies and reachable code for known Go vulnerabilities.
vuln *ARGS="./...":
    govulncheck {{ARGS}}

# Apply Go modernization rewrites
modernize *ARGS="./...":
    modernize -fix -test {{ARGS}}

# Build static command binaries into ./bin, stamping build info
# (version/commit/date) into internal/version via -ldflags. VERSION comes from
# `git describe`; a dirty tree gets a -dirty suffix on the commit.
build:
    #!/usr/bin/env bash
    set -euo pipefail
    pkg="github.com/bluesky-social/jetstream/internal/version"
    version="$(git describe --tags --always --dirty 2>/dev/null || echo dev)"
    commit="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
    if ! git diff --quiet HEAD 2>/dev/null; then
        commit="${commit}-dirty"
    fi
    date="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    ldflags="-extldflags=-static -X ${pkg}.Version=${version} -X ${pkg}.Commit=${commit} -X ${pkg}.Date=${date}"
    export CGO_ENABLED=0
    for cmd in ./cmd/*/; do
        name="$(basename "${cmd}")"
        go build -trimpath -ldflags "${ldflags}" -o "bin/${name}" "${cmd}"
    done

    if [[ "$(go env GOOS)" == "linux" ]]; then
        for cmd in ./cmd/*/; do
            name="$(basename "${cmd}")"
            description="$(LC_ALL=C file -b "bin/${name}")"
            if [[ "${description}" != *"statically linked"* ]]; then
                echo "build: bin/${name} is not statically linked: ${description}" >&2
                exit 1
            fi
        done
    fi

    go build -o bin/ ./examples/...

# Build the Docker image locally, stamping the same build info as `just build`.
# `--load` intentionally keeps this to one platform so the image can be run
# immediately for smoke checks (`docker run --rm jetstream:local version`).
docker-build TAG="jetstream:local" PLATFORM="linux/amd64":
    #!/usr/bin/env bash
    set -euo pipefail
    version="$(git describe --tags --always --dirty 2>/dev/null || echo dev)"
    commit="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
    if ! git diff --quiet HEAD 2>/dev/null; then
        commit="${commit}-dirty"
    fi
    date="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    docker buildx build \
        --load \
        --platform "{{PLATFORM}}" \
        --build-arg "VERSION=${version}" \
        --build-arg "COMMIT=${commit}" \
        --build-arg "DATE=${date}" \
        --tag "{{TAG}}" \
        .

# Remove build artifacts and local data.
clean:
    rm -rf bin
    rm -rf data*

# Run jetstream against the local simulator (default).
# Picks up JETSTREAM_RELAY_URL and JETSTREAM_PLC_URL from .env.
run *ARGS:
    go run ./cmd/jetstream {{ARGS}}

# Run jetstream with the race detector enabled.
run-race *ARGS:
    go run -race ./cmd/jetstream {{ARGS}}

# Run jetstream against real production services.
run-prod *ARGS:
    JETSTREAM_RELAY_URL=https://bsky.network \
    JETSTREAM_PLC_URL=https://plc.directory \
    JETSTREAM_DATA_DIR=./data-prod \
    go run ./cmd/jetstream {{ARGS}}

# Run jetstream against real production services with the race detector enabled.
run-prod-race *ARGS:
    JETSTREAM_RELAY_URL=https://bsky.network \
    JETSTREAM_PLC_URL=https://plc.directory \
    JETSTREAM_DATA_DIR=./data-prod \
    go run -race ./cmd/jetstream {{ARGS}}

# Run the websocket load-test client against a running jetstream server.
run-client *ARGS:
    go run ./cmd/client {{ARGS}}

run-example PROGRAM *ARGS:
    go run ./examples/{{PROGRAM}} {{ARGS}}

# Run the local simulator (PLC + PDS + relay + firehose).
simulator *ARGS:
    go run ./cmd/simulator {{ARGS}}

# Wipe the simulator's pebble db so the next `just simulator` re-bootstraps.
simulator-reset:
    rm -rf ./data/simulator

# Runs the full, long test suite
test-long *ARGS="./...":
    gotestsum --format-hide-empty-pkg --format-icons hivis --hide-summary=skipped -- -count=1 {{ARGS}}

# Run short tests; omit the expected skipped-test summary.
test *ARGS="./...":
    gotestsum --format-hide-empty-pkg --format-icons hivis --hide-summary=skipped -- -count=1 -short {{ARGS}}

# Runs the tests with the race detector enabled
test-race *ARGS="./...":
    just test-long -race {{ARGS}}

# Runs the CI race lane with raw diagnostics preserved for flaky race reports.
test-race-ci *ARGS="./...":
    #!/usr/bin/env bash
    set -euo pipefail

    # Do not use the JETSTREAM_ prefix here. The jetstream binary reserves that
    # namespace for runtime config and intentionally rejects unknown keys.
    artifact_dir="${RACE_ARTIFACT_DIR:-race-artifacts}"
    mkdir -p "${artifact_dir}"

    echo "test-race-ci: writing diagnostics to ${artifact_dir}"
    if ! GOTRACEBACK=all gotestsum \
        --format standard-verbose \
        --jsonfile "${artifact_dir}/gotestsum.jsonl" \
        -- -count=1 -race {{ARGS}} \
        2>&1 | tee "${artifact_dir}/test-output.log"; then
        echo "test-race-ci: failed; diagnostics are in ${artifact_dir}" >&2
        exit 1
    fi

# Runs the heavier simulator oracle mode. Deterministic transient getRepo and
# steady-state subscribeRepos disconnect fault injection is ON by default
# (JETSTREAM_ORACLE_FAULT_MODE=swarm, see internal/oracle/config.go); set
# JETSTREAM_ORACLE_FAULT_MODE=none to opt out.
oracle:
    JETSTREAM_ORACLE_MODE=stress gotestsum --format-hide-empty-pkg --format-icons hivis -- -count=1 ./internal/oracle -run TestOracle_DefaultLifecycle

# Run stress and restart checks across random seeds. CI supplies one fixed
# seed per matrix job. Workload sizing comes from stress mode in
# internal/oracle/config.go; change SEEDS to control local coverage. Swarm
# faults exercise retry and reconnect recovery by default.
oracle-sweep SEEDS="10" RACE="" FIXED_SEED="":
    #!/usr/bin/env bash
    set -euo pipefail

    # Keep traces and test output outside temporary test directories so
    # failures remain diagnosable. CI uploads this directory.
    artifact_root="${ORACLE_ARTIFACT_DIR:-oracle-artifacts}"
    mkdir -p "${artifact_root}"

    # A fixed CI seed repeats the interrupted workload on retry. Local sweeps
    # use fresh seeds by default.
    fixed_seed="{{FIXED_SEED}}"
    if [[ -n "${fixed_seed}" ]]; then
        if [[ "{{SEEDS}}" != "1" ]]; then
            echo "oracle-sweep: FIXED_SEED requires SEEDS=1" >&2
            exit 2
        fi
        if [[ ! "${fixed_seed}" =~ ^[0-9]+$ ]]; then
            echo "oracle-sweep: FIXED_SEED must be an unsigned decimal integer" >&2
            exit 2
        fi
    fi

    # RACE enables the detector and raises the per-seed timeout from 30m to
    # 90m. Restart children re-execute the same binary, so they inherit race
    # instrumentation.
    race_flag=()
    per_seed_timeout="30m"
    if [[ -n "{{RACE}}" ]]; then
        race_flag=(-race)
        per_seed_timeout="90m"
        echo "oracle-sweep: race detector ENABLED (per-seed timeout ${per_seed_timeout})"
    fi

    for i in $(seq 1 "{{SEEDS}}"); do
        # Use a fresh uint64 seed unless CI supplied one. /dev/urandom works
        # on Linux and macOS; print the seed for reproduction.
        if [[ -n "${fixed_seed}" ]]; then
            seed="${fixed_seed}"
        else
            seed="$(od -An -N8 -tu8 /dev/urandom | tr -d ' ')"
        fi
        seed_dir="${artifact_root}/seed-${i}-${seed}"
        mkdir -p "${seed_dir}"
        echo "::group::oracle ${i}/{{SEEDS}} seed=${seed}"
        echo "oracle run ${i}/{{SEEDS}} seed=${seed} artifacts=${seed_dir}"
        # Keep the test timeout below CI’s 45m/110m job limit so goroutine
        # dumps and artifacts survive a hang. Persist the harness trace, raw
        # test2json output, and console output in the per-seed directory.
        if ! GOTRACEBACK=all \
            JETSTREAM_ORACLE_MODE=stress \
            JETSTREAM_ORACLE_SEED="${seed}" \
            JETSTREAM_ORACLE_TRACE_DIR="${seed_dir}" \
            gotestsum --format-hide-empty-pkg --format-icons hivis --jsonfile "${seed_dir}/gotestsum.jsonl" -- -count=1 -timeout "${per_seed_timeout}" "${race_flag[@]}" ./internal/oracle -run TestOracle_DefaultLifecycle -v \
            2>&1 | tee "${seed_dir}/test-output.log"; then
            echo "::endgroup::"
            echo "::error::oracle failed at seed ${seed} (artifacts: ${seed_dir})"
            echo "Repro (NOTE: the seed fixes the INPUTS only — the world,"
            echo "the runtime RNG, and the fault schedule. The oracle runs the"
            echo "real jetstreamd runtime concurrently against real time and"
            echo "real sockets, so goroutine scheduling, fault-vs-retry timing,"
            echo "and socket ordering are NOT seeded. A single run may pass on a"
            echo "faster or less-contended machine; the failure is interleaving-"
            echo "dependent. To surface it, force the schedule rather than"
            echo "trusting a single replay:"
            echo "  JETSTREAM_ORACLE_MODE=stress \\"
            echo "  JETSTREAM_ORACLE_SEED=${seed} \\"
            echo "  GOMAXPROCS=2 go test ./internal/oracle -run TestOracle_DefaultLifecycle \\"
            echo "    -count=200 -failfast -timeout 360m -v"
            echo "(add -race to catch a data race directly; raise -count or lower"
            echo "GOMAXPROCS to bias the scheduler toward the CI interleaving.)"
            exit 1
        fi
        echo "::endgroup::"

        # Run crash/restart coverage for every seed. The durable chains
        # include updates, deletes, syncs, and account tombstones that
        # final-state lifecycle comparison alone cannot check. This tier must
        # run without -short.
        echo "::group::oracle-restart ${i}/{{SEEDS}} seed=${seed}"
        echo "oracle-restart run ${i}/{{SEEDS}} seed=${seed} artifacts=${seed_dir}"
        if ! GOTRACEBACK=all \
            JETSTREAM_ORACLE_SEED="${seed}" \
            JETSTREAM_ORACLE_TRACE_DIR="${seed_dir}" \
            gotestsum --format-hide-empty-pkg --format-icons hivis --jsonfile "${seed_dir}/gotestsum-restart.jsonl" -- -count=1 -timeout "${per_seed_timeout}" "${race_flag[@]}" ./internal/oracle -run 'TestOracle_Restart' -v \
            2>&1 | tee "${seed_dir}/test-output-restart.log"; then
            echo "::endgroup::"
            echo "::error::oracle restart tier failed at seed ${seed} (artifacts: ${seed_dir})"
            echo "Repro (seed fixes the world + chain shape; crash TIMING is real"
            echo "wall-clock scheduling, not seeded — force the schedule):"
            echo "  JETSTREAM_ORACLE_SEED=${seed} \\"
            echo "  GOMAXPROCS=2 go test ./internal/oracle -run 'TestOracle_Restart' \\"
            echo "    -count=50 -failfast -timeout 60m -v"
            exit 1
        fi
        echo "::endgroup::"
    done

# Runs the oracle mutation campaign: applies each curated mutant patch in
# testing/mutation/mutants one at a time and verifies the oracle kills it.
# Pass a mutant id to run one (e.g. `just mutation-campaign m019`), or
# `m002 --seeds 5` for a stress-mode seed sweep of a survivor. Scorecard
# lives in testing/mutation/RESULTS.md.
mutation-campaign *ARGS="":
    testing/mutation/run.sh {{ARGS}}

# Run the mutation campaign and compare with the committed baseline. Fail on
# lost detection, stale or broken patches, or catalog drift. Improvements are
# reported for baseline refresh. CI uploads MUTATION_RESULT_JSON.
mutation-gate:
    #!/usr/bin/env bash
    set -euo pipefail
    result_json="${MUTATION_RESULT_JSON:-mutation-result.json}"
    mkdir -p "$(dirname "${result_json}")"
    testing/mutation/run.sh --json "${result_json}"
    echo "::group::mutation gate vs baseline"
    go run ./testing/mutation/gate -baseline testing/mutation/baseline.json -result "${result_json}"
    echo "::endgroup::"

# Regenerate the mutation baseline after reviewing improvements or catalog
# changes. Requires a clean tree; review and commit the result.
mutation-baseline:
    testing/mutation/run.sh --json testing/mutation/baseline.json
    @echo "baseline written to testing/mutation/baseline.json — review the diff and commit"

# Runs performance benchmarks.
bench *ARGS="./...":
    go test -bench=. -benchmem -count=1 -run='^$' {{ARGS}}

# Runs synthetic delete-compaction benchmarks.
bench-compaction *ARGS="":
    go test -bench='Compaction' -benchmem -count=1 -run='^$' ./internal/ingest/orchestrator {{ARGS}}

# Runs fuzz tests for the given duration (default 10s per target)
fuzz DURATION="10s" *ARGS="./...":
    #!/usr/bin/env bash
    set -euo pipefail
    pkgs="{{ARGS}}"
    for pkg in $(go list $pkgs); do
        targets=$(go test "$pkg" -list '^Fuzz' -run '^$' -count=1 2>/dev/null | grep '^Fuzz' || true)
        for t in $targets; do
            echo "=== FUZZ $t ($pkg) ==="
            go test "$pkg" -run='^$' -fuzz="^${t}$" -fuzztime={{DURATION}}
        done
    done

# Runs exactly one fuzz target. Scheduled CI uses this entry point for matrix
# shards so one runner loss cannot discard the other targets' completed work.
fuzz-target DURATION PACKAGE TARGET:
    #!/usr/bin/env bash
    set -euo pipefail
    pkg="{{PACKAGE}}"
    target="{{TARGET}}"
    if [[ ! "${target}" =~ ^Fuzz[[:alnum:]_]+$ ]]; then
        echo "fuzz-target: invalid target ${target}" >&2
        exit 2
    fi
    listed="$(go test "${pkg}" -list "^${target}$" -run '^$' -count=1)"
    if ! grep -Fxq -- "${target}" <<< "${listed}"; then
        echo "fuzz-target: ${target} does not exist in ${pkg}" >&2
        exit 2
    fi
    echo "=== FUZZ ${target} (${pkg}) ==="
    go test "${pkg}" -run='^$' -fuzz="^${target}$" -fuzztime={{DURATION}}

# Generate XRPC types from lexicons/. Resolve com.atproto references through a
# scratch package; atmos supplies the actual types.
lexgen:
    go run github.com/jcalabro/atmos/cmd/lexgen -lexdir lexicons -config lexgen.json
    rm -rf .lexgen-scratch

# Retrain the v2 subscribe zstd dictionary from live firehose traffic on a
# running jetstream instance's /xrpc/network.bsky.jetstream.subscribeEvents endpoint
# (a few minutes of capture; needs the zstd CLI)
train-subscribe-dict host="localhost:8080":
    go run ./testing/dicttrain --host {{host}}
