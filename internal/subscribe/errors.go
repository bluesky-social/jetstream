package subscribe

import "errors"

// errSkipEvent signals that the encoder intentionally produced no
// frame for this event (e.g. #sync events, which Jetstream v1 never
// emitted on the wire). The handler's writer loop treats this as
// "advance the channel; keep the connection alive."
//
//nolint:unused
var errSkipEvent = errors.New("subscribe: skip event")
