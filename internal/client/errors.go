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

// fatal wraps err so errors.Is(_, ErrFatal) reports true while preserving the
// original error for unwrapping and message context.
func fatal(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrFatal, err)
}
