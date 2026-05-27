set shell := ["bash", "-cu"]
set dotenv-load

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

# Build the jetstream binary into ./bin/jetstream.
build:
    go build -trimpath -o bin/jetstream ./cmd/jetstream

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
    just run -race {{ARGS}}

# Run jetstream against real production services.
run-prod *ARGS:
    JETSTREAM_RELAY_URL=https://bsky.network \
    JETSTREAM_PLC_URL=https://plc.directory \
    JETSTREAM_DATA_DIR=./data-prod \
    go run ./cmd/jetstream {{ARGS}}

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
