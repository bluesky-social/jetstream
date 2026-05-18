// Package backfill: handler.go provides the placeholder Handler used
// by the bootstrap PR. It does no segment writing — that comes in a
// later PR. The point is to prove the engine wiring (listRepos ->
// download -> handler -> Store) works end to end.
package backfill

import (
	"context"
	"log/slog"

	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/repo"
)

// LogHandler is a no-op atmos backfill.Handler that logs each handled
// repo at debug level. It exists so we can prove the engine wiring
// works without committing to segment file format details from this
// PR.
type LogHandler struct {
	logger *slog.Logger
}

// Compile-time assertion.
var _ atmosbackfill.Handler = (*LogHandler)(nil)

// NewLogHandler returns a LogHandler. nil logger uses slog.Default().
func NewLogHandler(logger *slog.Logger) *LogHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogHandler{logger: logger}
}

// HandleRepo logs the (did, rev) pair and returns nil. The atmos
// engine then advances the DID via Store.OnComplete.
func (h *LogHandler) HandleRepo(_ context.Context, did atmos.DID, _ *repo.Repo, commit *repo.Commit) error {
	h.logger.Debug("backfill: repo handled",
		"did", string(did),
		"rev", commit.Rev,
	)
	return nil
}
