package subscribe

import "errors"

// errSkipEvent signals that the encoder intentionally produced no
// frame for this event (e.g. #sync events, which Jetstream v1 never
// emitted on the wire). The subscriber pull loop treats this as
// "advance the cursor; keep the connection alive."
var errSkipEvent = errors.New("subscribe: skip event")

// ErrInvalidOptions wraps validation failures from ParseQuery and
// ParseUpdatePayload. Callers (the handler, plus tests outside this
// package) errors.Is against it to distinguish bad-input failures from
// other errors.
var ErrInvalidOptions = errors.New("subscribe: invalid options")
