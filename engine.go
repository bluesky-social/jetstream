package jetstream

import (
	"context"
	"fmt"
)

// newEngine constructs the orchestration engine for the given host and config.
//
// The full engine (plan negotiation, segment/block download, tombstone
// suppression, backfill-to-live cutover, and the live tail) is implemented in
// the internal/client package and wired here as that work lands. Until then
// this returns a placeholder that reports the not-yet-implemented surface so
// the public API, options, and types are stable and testable now.
func newEngine(host string, cfg config) (engine, error) {
	return &placeholderEngine{host: host, cfg: cfg}, nil
}

type placeholderEngine struct {
	host string
	cfg  config
}

func (e *placeholderEngine) run(ctx context.Context, yield func(*Batch, error) bool) {
	mode := "live tail"
	if e.cfg.backfillRequested() {
		mode = "backfill + live tail"
	}
	yield(nil, fmt.Errorf("jetstream: %s engine not yet implemented", mode))
}

func (e *placeholderEngine) close() error { return nil }
