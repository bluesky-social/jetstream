set shell := ["bash", "-cu"]
set dotenv-load

# Runs the linter and tests
default: lint test

# Enters the pinned Nix development shell.
dev *ARGS="":
    exec ./dev.sh {{ARGS}}

# Lints the code
lint:
    golangci-lint run --timeout 5m ./...

# Scans module dependencies and reachable code for known Go vulnerabilities.
vuln *ARGS="./...":
    govulncheck {{ARGS}}

# Apply Go modernization rewrites
modernize *ARGS="./...":
    modernize -fix -test {{ARGS}}

# Build the jetstream binary into ./bin/jetstream, stamping build info
# (version/commit/date) into internal/version via -ldflags, mirroring the
# Dockerfile. VERSION comes from `git describe`; a dirty tree gets a -dirty
# suffix on the commit.
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
    ldflags="-X ${pkg}.Version=${version} -X ${pkg}.Commit=${commit} -X ${pkg}.Date=${date}"
    for cmd in ./cmd/*/; do
        name="$(basename "${cmd}")"
        go build -trimpath -ldflags "${ldflags}" -o "bin/${name}" "${cmd}"
    done

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

# Run the local simulator (PLC + PDS + relay + firehose).
simulator *ARGS:
    go run ./cmd/simulator {{ARGS}}

# Wipe the simulator's pebble db so the next `just simulator` re-bootstraps.
simulator-reset:
    rm -rf ./data/simulator

# Runs the full, long test suite
test-long *ARGS="./...":
    gotestsum --format-hide-empty-pkg --format-icons hivis --hide-summary=skipped -- -count=1 {{ARGS}}

# Runs the tests in -short mode. Hides the skipped-test summary because
# heavy tests (e.g. the simulator E2E) are deliberately skipped here and
# the listing is just noise.
test *ARGS="./...":
    gotestsum --format-hide-empty-pkg --format-icons hivis --hide-summary=skipped -- -count=1 -short {{ARGS}}

# Runs the tests with the race detector enabled
test-race *ARGS="./...":
    just test-long -race {{ARGS}}

# Runs the CI race lane with raw diagnostics preserved for flaky race reports.
test-race-ci *ARGS="./...":
    #!/usr/bin/env bash
    set -euo pipefail

    artifact_dir="${JETSTREAM_RACE_ARTIFACT_DIR:-race-artifacts}"
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

# Sweeps oracle stress mode across randomly-chosen seeds. Intended for the
# scheduled CI job, which runs several smaller sweeps each day to reduce the
# blast radius of ARC runner failures. The per-seed workload is governed
# entirely by JETSTREAM_ORACLE_MODE=stress (see internal/oracle/config.go) so
# there is a single source of truth: do not reintroduce account/record/event
# overrides here. Pass SEEDS explicitly to grow or shrink a local run.
#
# Deterministic transient getRepo and steady-state subscribeRepos disconnect
# fault injection is on by default (JETSTREAM_ORACLE_FAULT_MODE=swarm); the
# sweep relies on it to exercise backfill retry/recovery and live reconnect/
# resume recovery on every seed.
oracle-sweep SEEDS="10" RACE="":
    #!/usr/bin/env bash
    set -euo pipefail

    # Root for per-seed diagnostic artifacts (the JSONL trace and the captured
    # test output that carries the goroutine dump on a hang). CI sets
    # ORACLE_ARTIFACT_DIR to a path it uploads on failure; locally it defaults
    # to a repo-relative dir. The trace is the design's substitute for
    # bit-reproducible scheduling, so it must outlive the test process.
    artifact_root="${ORACLE_ARTIFACT_DIR:-oracle-artifacts}"
    mkdir -p "${artifact_root}"

    # RACE="" (default): no race detector, 30m per-seed timeout. Any non-empty
    # RACE arg enables `-race` and raises the timeout to 90m, because the race
    # detector slows execution ~5-15x and inflates memory; the #107 race lane
    # runs FEW seeds within this larger budget rather than the full nightly
    # count. The restart tier re-execs the SAME test binary as its child
    # (os.Args[0]), so -race instruments both the parent harness and the killed
    # child — the data-race coverage is real on both tiers, not just the parent.
    race_flag=()
    per_seed_timeout="30m"
    if [[ -n "{{RACE}}" ]]; then
        race_flag=(-race)
        per_seed_timeout="90m"
        echo "oracle-sweep: race detector ENABLED (per-seed timeout ${per_seed_timeout})"
    fi

    for i in $(seq 1 "{{SEEDS}}"); do
        # Draw a fresh random uint64 seed each iteration so successive nightly
        # runs explore different points in the state space instead of replaying
        # a fixed 1..N. /dev/urandom is portable across the Linux CI runner and
        # macOS dev machines; the failing seed is printed below for exact repro.
        seed="$(od -An -N8 -tu8 /dev/urandom | tr -d ' ')"
        seed_dir="${artifact_root}/seed-${i}-${seed}"
        mkdir -p "${seed_dir}"
        echo "::group::oracle ${i}/{{SEEDS}} seed=${seed}"
        echo "oracle run ${i}/{{SEEDS}} seed=${seed} artifacts=${seed_dir}"
        # GOTRACEBACK=all makes the runtime print every goroutine's stack when
        # the test -timeout fires, so a hang is diagnosable instead of a bare
        # job kill. The per-seed -timeout (30m, matching the mutation campaign)
        # is deliberately far below the job budget (timeout-minutes: 360) so the
        # dump prints and the artifact upload runs before the job is killed; a
        # healthy stress seed completes in minutes. JETSTREAM_ORACLE_TRACE_DIR
        # redirects the harness trace from an ephemeral t.ArtifactDir() into the
        # uploaded per-seed dir. --jsonfile records the raw test2json stream
        # (the timeout traceback arrives as package output events), so the dump
        # is captured regardless of how gotestsum renders its console output;
        # tee additionally mirrors the console stream for human-readable triage.
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

        # Restart/crash tier: same per-seed budget. This tier SIGKILLs a real
        # child subprocess at enumerated crashpoints and asserts recovery does
        # not lose records; the chain shapes (#113) additionally land durable
        # create/update/delete intermediates + sync/account tombstones through
        # the merge, so a nightly random seed here exercises the lost-
        # intermediate / no-permanent-tombstone / over-drop surface that
        # DefaultLifecycle does not. It reads JETSTREAM_ORACLE_SEED (default
        # 101+i) so the sweep varies the chain specifics per run. Cheap vs.
        # stress DefaultLifecycle (each crash case SIGKILLs in ~0.1s), so it
        # runs at full per-seed frequency. Not -short (that skips the tier).
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

# Runs the full mutation campaign and enforces the committed baseline (#108).
# Emits a machine-readable result and fails if any mutant regressed
# (KILLED->SURVIVED), went STALE/BUILD-BROKEN, or drifted from
# testing/mutation/baseline.json. This is the scheduled CI gate; a
# SURVIVED->KILLED improvement is reported but does not fail (refresh the
# baseline to bank it). CI sets MUTATION_RESULT_JSON to a path it uploads as an
# artifact; locally it defaults to a repo-relative file.
mutation-gate:
    #!/usr/bin/env bash
    set -euo pipefail
    result_json="${MUTATION_RESULT_JSON:-mutation-result.json}"
    mkdir -p "$(dirname "${result_json}")"
    testing/mutation/run.sh --json "${result_json}"
    echo "::group::mutation gate vs baseline"
    go run ./testing/mutation/gate -baseline testing/mutation/baseline.json -result "${result_json}"
    echo "::endgroup::"

# Regenerates testing/mutation/baseline.json from a fresh full campaign at HEAD.
# Run this (and review the diff) after intentionally adding/retiring a mutant or
# banking a SURVIVED->KILLED improvement, so the #108 gate has a current
# source of truth. Requires a clean tree.
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

# Generate Go XRPC types from the lexicons in ./lexicons
lexgen:
    go run github.com/jcalabro/atmos/cmd/lexgen -lexdir lexicons -config lexgen.json

# Retrain the /subscribe-v2 zstd dictionary from live firehose traffic on a
# running jetstream instance (a few minutes of capture; needs the zstd CLI)
train-subscribe-dict host="localhost:8080":
    go run ./testing/dicttrain --host {{host}}
