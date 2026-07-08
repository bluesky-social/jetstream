package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// buildJetstreamForTest compiles the jetstream binary into a temp
// dir and returns the path. Build artifacts are scoped to the test's
// TempDir, so cleanup is automatic.
//
// When the test binary itself was built with -race (i.e. `go test
// -race` / `just test-race`), we propagate -race to the spawned
// build. Go's build cache keys include the -race flag, so without
// this the spawned build can't reuse any of the race-mode artifacts
// the parent `go test` run already produced — it would recompile
// every transitive dependency in non-race mode from a cold cache.
// CI disables Go module/build caching by policy (see ci.yml item 11),
// so that cold rebuild has been observed to exceed the build timeout
// under CPU contention from parallel tests. Matching the build mode
// lets the spawned build hit the warm cache instead.
func buildJetstreamForTest(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "jetstream")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	args := []string{"build", "-o", bin}
	if raceEnabled {
		args = append(args, "-race")
	}
	args = append(args, "./cmd/jetstream")
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = repoRoot(t)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "go build jetstream: %s", string(out))
	return bin
}

func newJetstreamCmd(ctx context.Context, bin string, args []string) *exec.Cmd {
	return exec.CommandContext(ctx, bin, args...)
}

func freePortAddr(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	lc := net.ListenConfig{}
	l, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func repoRoot(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "rev-parse", "--show-toplevel")
	out, err := cmd.Output()
	require.NoError(t, err)
	return string(bytes.TrimSpace(out))
}

// lockedBuffer is a thread-safe bytes.Buffer for capturing the
// subprocess's stdout/stderr.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// logMsgSet parses jetstream's JSON slog stream (the subprocess default
// format, see obs.ParseLogFormat) and returns the set of distinct slog
// `msg` values it emitted.
//
// The E2E warning sentinels key off this set rather than substring-
// matching the raw JSON. Substring matching is brittle: it silently
// matches structured field *values* as well as messages, and — as
// issue #283 showed — it breaks the moment a producer's message text
// drifts from what the test hard-codes. Matching whole `msg` values
// keeps the sentinels explicit: a rename in the producer surfaces as a
// sentinel miss (a maintainer must update the constant) instead of a
// silent false pass.
//
// Non-JSON lines (e.g. an early panic or Go runtime output before the
// logger is wired) are ignored here; callers that need to fail loudly
// on those inspect the raw buffer separately.
func logMsgSet(logs string) map[string]struct{} {
	msgs := make(map[string]struct{})
	for line := range strings.Lines(logs) {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var rec struct {
			Msg string `json:"msg"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			continue
		}
		if rec.Msg != "" {
			msgs[rec.Msg] = struct{}{}
		}
	}
	return msgs
}
