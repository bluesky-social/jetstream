package live

import "errors"

// Sentinel errors. Callers compare with errors.Is.
var (
	// ErrInvalidConfig is returned by Open when Config has unusable
	// values.
	ErrInvalidConfig = errors.New("livestream: invalid config")

	// ErrClosed is returned by Run / Close after the Consumer has
	// already been closed.
	ErrClosed = errors.New("livestream: consumer is closed")

	// ErrUnknownEventKind is returned by ConvertEvent when no field of
	// streaming.Event is set to a kind we recognize. Run treats this
	// as non-fatal (we deliberately do not crash on a future relay
	// adding a new event variant) but does NOT advance the upstream
	// cursor for such events, so a later jetstream build that knows
	// the new kind can be replayed from the gap.
	ErrUnknownEventKind = errors.New("livestream: unknown event kind")
)
