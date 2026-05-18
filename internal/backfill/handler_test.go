package backfill

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/repo"
	"github.com/stretchr/testify/require"
)

// TestLogHandler_HandleRepoLogsAtDebug pins the contract: the handler
// is a no-op that emits a debug log. Tests for the real segment-
// writer-backed handler will replace this in a future PR.
func TestLogHandler_HandleRepoLogsAtDebug(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	h := NewLogHandler(logger)

	commit := &repo.Commit{DID: "did:plc:abc", Rev: "rev-x"}
	err := h.HandleRepo(context.Background(), atmos.DID("did:plc:abc"), nil, commit)
	require.NoError(t, err)

	require.Contains(t, buf.String(), `"did":"did:plc:abc"`)
	require.Contains(t, buf.String(), `"rev":"rev-x"`)
	require.Contains(t, buf.String(), `"level":"DEBUG"`)
}

// TestLogHandler_NilLoggerNoPanic guards the wiring: a caller that
// forgot to plumb a logger should get a usable handler, not a crash.
// We default to slog.Default() in the constructor.
func TestLogHandler_NilLoggerNoPanic(t *testing.T) {
	t.Parallel()
	h := NewLogHandler(nil)
	err := h.HandleRepo(context.Background(), atmos.DID("did:plc:abc"), nil, &repo.Commit{Rev: "x"})
	require.NoError(t, err)
}
