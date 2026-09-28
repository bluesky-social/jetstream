package ingest

import "errors"

// Sentinel errors. Callers compare with errors.Is.
var (
	// ErrInvalidConfig is returned by Open when Config has unusable values.
	ErrInvalidConfig = errors.New("ingest: invalid config")

	// ErrClosed is returned by Append and Close after the Writer has
	// already been closed.
	ErrClosed = errors.New("ingest: writer is closed")

	// ErrAppendCancelled is returned, wrapping the context's error, when a
	// disaggregated append stops waiting for room because its ctx ended.
	// The writer is unharmed: events of the batch admitted before the
	// wait are appended, and the rest are not.
	ErrAppendCancelled = errors.New("ingest: append cancelled while waiting for room")
)
