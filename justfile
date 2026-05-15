set shell := ["bash", "-cu"]

default: lint test

# Ensures that all tools required for local development are installed
install-tools:
    go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.10.1
    go install gotest.tools/gotestsum@v1.13.0
    go install golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@v0.20.0

# Lints the code
lint:
    golangci-lint run --timeout 1m ./...

# Apply Go modernization rewrites
modernize *ARGS="./...":
    modernize -fix -test {{ARGS}}

# Run jetstream with arbitrary args, e.g. `just run --version` or `just run serve --addr :9090`.
run *ARGS:
    go run ./cmd/jetstream {{ARGS}}

# Run jetstream with the race detector enabled.
run-race *ARGS:
    just run -race {{ARGS}}

# Runs the full, long test suite
test-long *ARGS="./...":
    gotestsum --format-hide-empty-pkg --format-icons hivis -- -count=1 {{ARGS}}

# Runs the tests in -short mode.
test *ARGS="./...":
    just test-long -short {{ARGS}}

# Runs the tests with the race detector enabled
test-race *ARGS="./...":
    just test-long -race {{ARGS}}

# Runs performance benchmarks.
bench *ARGS="./...":
    go test -bench=. -benchmem -count=1 -run='^$' {{ARGS}}

# Build the jetstream binary into ./bin/jetstream.
build:
    go build -trimpath -o bin/jetstream ./cmd/jetstream

# Remove build artifacts.
clean:
    rm -rf bin
