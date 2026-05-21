package livestream

import "errors"

// Sentinel errors. Callers compare with errors.Is.
var (
	// ErrInvalidConfig is returned by Open when Config has unusable
	// values.
	ErrInvalidConfig = errors.New("livestream: invalid config")

	// ErrClosed is returned by Run / Close after the Consumer has
	// already been closed.
	ErrClosed = errors.New("livestream: consumer is closed")
)
