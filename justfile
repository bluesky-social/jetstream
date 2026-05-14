set shell := ["bash", "-cu"]

default: lint test-short

# Ensures that all tools required for local development are installed
install-tools:
    go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.10.1
    go install gotest.tools/gotestsum@v1.13.0

# Lints the code
lint:
    golangci-lint run --timeout 1m ./...

# Run jetstream with arbitrary args, e.g. `just run --version` or `just run serve --addr :9090`.
run *ARGS:
    go run ./cmd/jetstream {{ARGS}}

# Run jetstream with the race detector enabled.
run-race *ARGS:
    just run -race {{ARGS}}

# Runs the tests in -short mode (skips long-running swarm/integration tests).
test-short *ARGS="./...":
    gotestsum --format-hide-empty-pkg --format-icons hivis -- -short -count=1 {{ARGS}}

# Runs the tests
test *ARGS="./...":
    gotestsum --format-hide-empty-pkg --format-icons hivis -- -count=1 {{ARGS}}

# Runs the tests with the race detector enabled
test-race *ARGS="./...":
    gotestsum --format-hide-empty-pkg --format-icons hivis -- -race -count=1 {{ARGS}}

# Build the jetstream binary into ./bin/jetstream.
build:
    go build -trimpath -o bin/jetstream ./cmd/jetstream

# Remove build artifacts.
clean:
    rm -rf bin
