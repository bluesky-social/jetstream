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

# Runs the heavier simulator oracle mode.
oracle:
    JETSTREAM_ORACLE_MODE=stress gotestsum --format-hide-empty-pkg --format-icons hivis -- -count=1 ./internal/oracle -run TestOracle_DefaultLifecycle

# Sweeps oracle stress mode across deterministic seeds. Intended for nightly CI;
# override args locally to shrink or expand the run.
oracle-sweep SEEDS="100" ACCOUNTS="250" MAX_INITIAL_RECORDS="10000" LIVE_EVENTS_BOOTSTRAP="25000" LIVE_EVENTS_STEADY="25000":
    #!/usr/bin/env bash
    set -euo pipefail

    for seed in $(seq 1 "{{SEEDS}}"); do
        echo "::group::oracle seed ${seed}"
        echo "oracle seed ${seed}/{{SEEDS}}"
        if ! JETSTREAM_ORACLE_MODE=stress \
            JETSTREAM_ORACLE_SEED="${seed}" \
            JETSTREAM_ORACLE_ACCOUNTS="{{ACCOUNTS}}" \
            JETSTREAM_ORACLE_MAX_INITIAL_RECORDS="{{MAX_INITIAL_RECORDS}}" \
            JETSTREAM_ORACLE_LIVE_EVENTS_BOOTSTRAP="{{LIVE_EVENTS_BOOTSTRAP}}" \
            JETSTREAM_ORACLE_LIVE_EVENTS_STEADY="{{LIVE_EVENTS_STEADY}}" \
            gotestsum --format-hide-empty-pkg --format-icons hivis -- -count=1 -timeout 30m ./internal/oracle -run TestOracle_DefaultLifecycle -v; then
            echo "::endgroup::"
            echo "::error::oracle failed at seed ${seed}"
            echo "Repro:"
            echo "  JETSTREAM_ORACLE_MODE=stress \\"
            echo "  JETSTREAM_ORACLE_SEED=${seed} \\"
            echo "  JETSTREAM_ORACLE_ACCOUNTS={{ACCOUNTS}} \\"
            echo "  JETSTREAM_ORACLE_MAX_INITIAL_RECORDS={{MAX_INITIAL_RECORDS}} \\"
            echo "  JETSTREAM_ORACLE_LIVE_EVENTS_BOOTSTRAP={{LIVE_EVENTS_BOOTSTRAP}} \\"
            echo "  JETSTREAM_ORACLE_LIVE_EVENTS_STEADY={{LIVE_EVENTS_STEADY}} \\"
            echo "  go test ./internal/oracle -run TestOracle_DefaultLifecycle -count=1 -timeout 30m -v"
            exit 1
        fi
        echo "::endgroup::"
    done

# Runs performance benchmarks.
bench *ARGS="./...":
    go test -bench=. -benchmem -count=1 -run='^$' {{ARGS}}

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
