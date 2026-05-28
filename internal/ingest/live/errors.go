package live

import (
	"errors"
	"fmt"
)

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

// DroppedOp describes one create/update op that ConvertEvent had to
// drop because the commit's CAR diff did not carry its record
// block. CID is the parsed-and-well-formed record CID the op
// claimed; correlating it with the upstream PDS's logs is the
// quickest way to identify a misbehaving repo.
type DroppedOp struct {
	DID        string
	Collection string
	RKey       string
	Action     string
	CID        string
}

// DroppedMissingBlocksError is returned by ConvertEvent when one or
// more create/update ops in a #commit referenced a CID whose record
// block was absent from the commit's CAR diff. Partial CARs are
// spec-permitted (a record block may be omitted e.g. when the new
// CID equals the old CID after a no-op update, or when a non-
// canonical PDS just doesn't include it), so the drop is
// informational rather than fatal: the well-formed ops in the same
// commit are still returned alongside the error and the caller is
// expected to fall through and archive them.
//
// Reach the value via errors.AsType[*DroppedMissingBlocksError].
type DroppedMissingBlocksError struct {
	Dropped []DroppedOp
}

func (e *DroppedMissingBlocksError) Error() string {
	if len(e.Dropped) == 1 {
		d := e.Dropped[0]
		return fmt.Sprintf(
			"livestream: dropped 1 op (did=%s collection=%s rkey=%s action=%s): record block missing from CAR diff",
			d.DID, d.Collection, d.RKey, d.Action,
		)
	}
	return fmt.Sprintf("livestream: dropped %d ops: record blocks missing from CAR diff", len(e.Dropped))
}
