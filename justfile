set shell := ["bash", "-cu"]
set dotenv-load

# Runs the linter and tests
default: lint test

# Ensures that all tools required for local development are installed
install-tools:
    go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.10.1
    go install gotest.tools/gotestsum@v1.13.0
    go install golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@v0.20.0

# Lints the code
lint:
    golangci-lint run --timeout 5m ./...

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
    pkg="github.com/bluesky-social/jetstream-v2/internal/version"
    version="$(git describe --tags --always --dirty 2>/dev/null || echo dev)"
    commit="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
    if ! git diff --quiet HEAD 2>/dev/null; then
        commit="${commit}-dirty"
    fi
    date="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    go build -trimpath \
        -ldflags "-X ${pkg}.Version=${version} -X ${pkg}.Commit=${commit} -X ${pkg}.Date=${date}" \
        -o bin/jetstream ./cmd/jetstream

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
    gotestsum --format-hide-empty-pkg --format-icons hivis -- -count=1 {{ARGS}}

# Runs the tests in -short mode. Hides the skipped-test summary because
# heavy tests (e.g. the simulator E2E) are deliberately skipped here and
# the listing is just noise.
test *ARGS="./...":
    gotestsum --format-hide-empty-pkg --format-icons hivis --hide-summary=skipped -- -count=1 -short {{ARGS}}

# Runs the tests with the race detector enabled
test-race *ARGS="./...":
    just test-long -race {{ARGS}}

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
oracle-sweep SEEDS="10":
    #!/usr/bin/env bash
    set -euo pipefail

    for i in $(seq 1 "{{SEEDS}}"); do
        # Draw a fresh random uint64 seed each iteration so successive nightly
        # runs explore different points in the state space instead of replaying
        # a fixed 1..N. /dev/urandom is portable across the Linux CI runner and
        # macOS dev machines; the failing seed is printed below for exact repro.
        seed="$(od -An -N8 -tu8 /dev/urandom | tr -d ' ')"
        echo "::group::oracle ${i}/{{SEEDS}} seed=${seed}"
        echo "oracle run ${i}/{{SEEDS}} seed=${seed}"
        if ! JETSTREAM_ORACLE_MODE=stress \
            JETSTREAM_ORACLE_SEED="${seed}" \
            gotestsum --format-hide-empty-pkg --format-icons hivis -- -count=1 -timeout 30m ./internal/oracle -run TestOracle_DefaultLifecycle -v; then
            echo "::endgroup::"
            echo "::error::oracle failed at seed ${seed}"
            echo "Repro:"
            echo "  JETSTREAM_ORACLE_MODE=stress \\"
            echo "  JETSTREAM_ORACLE_SEED=${seed} \\"
            echo "  go test ./internal/oracle -run TestOracle_DefaultLifecycle -count=1 -timeout 30m -v"
            exit 1
        fi
        echo "::endgroup::"
    done

# Runs the oracle mutation campaign: applies each curated mutant patch in
# testing/mutation/mutants one at a time and verifies the oracle kills it.
# Pass a mutant id to run one (e.g. `just mutation-campaign m007`), or
# `m007 --seeds 5` for a stress-mode seed sweep of a survivor. Scorecard
# lives in testing/mutation/RESULTS.md.
mutation-campaign *ARGS="":
    testing/mutation/run.sh {{ARGS}}

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
