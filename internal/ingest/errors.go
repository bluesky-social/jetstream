package ingest

import "errors"

// Sentinel errors. Callers compare with errors.Is.
var (
	// ErrInvalidConfig is returned by Open when Config has unusable values.
	ErrInvalidConfig = errors.New("ingest: invalid config")

	// ErrClosed is returned by Append and Close after the Writer has
	// already been closed.
	ErrClosed = errors.New("ingest: writer is closed")
)
