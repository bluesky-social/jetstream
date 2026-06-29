package client

import (
	"errors"
	"fmt"
)

// ErrFatal marks a terminal stream failure: the engine has aborted and will
// deliver no further events. It distinguishes a doomed stream (overlay-seed
// failure, plan rejection, a cutover guarantee broken) from a recoverable
// per-entry hiccup (a single bad segment, a transient live read error) that the
// stream continues past. Consumers test for it with errors.Is(err, ErrFatal)
// and should stop and surface a non-zero status rather than logging and
// continuing.
var ErrFatal = errors.New("jetstream: fatal stream error")

// errSnapshotMissing is the §R6.6 fail-closed condition: the client requested
// the §R4 DID-tombstone start-snapshot on page 1 but the server's response did
// not include it (didTombstonesIncluded false — a too-old server). Proceeding
// with an empty suppression set would silently retain the records of accounts
// deleted within the planned range, so the engine treats this as fatal. Crash
// over corruption.
var errSnapshotMissing = errors.New("jetstream: server did not return the requested DID-tombstone snapshot (too-old server?); refusing to backfill without it to avoid retaining deleted accounts' records")

// fatal wraps err so errors.Is(_, ErrFatal) reports true while preserving the
// original error for unwrapping and message context.
func fatal(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrFatal, err)
}
