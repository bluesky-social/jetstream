package live

import (
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/ingest"
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

// InvalidEventError is returned by ConvertEvent when a whole upstream
// event fails the ingest validation gate and must not be archived in
// any part — e.g. a #commit or #sync whose rev is not a spec-valid
// TID (rev ordering drives merge-filter and compaction decisions, so
// rows under a garbage rev would silently corrupt them). Unlike
// ErrUnknownEventKind, the event's shape IS recognized; the consumer
// counts it on the shared drop metric and advances the cursor past
// it — a later build cannot make the input valid.
//
// Reach the value via errors.AsType[*InvalidEventError].
type InvalidEventError struct {
	Reason ingest.DropReason
	DID    string
	Detail string
}

func (e *InvalidEventError) Error() string {
	return fmt.Sprintf("livestream: dropped event (did=%s reason=%s): %s", e.DID, e.Reason, e.Detail)
}

// DroppedOp describes one op that ConvertEvent had to drop while its
// well-formed siblings survived: a create/update whose record block
// was absent from the commit's CAR diff, or an op whose wire path
// failed spec validation. CID is the record CID the op claimed (empty
// when unavailable); correlating it with the upstream PDS's logs is
// the quickest way to identify a misbehaving repo.
type DroppedOp struct {
	Reason     ingest.DropReason
	DID        string
	Collection string
	RKey       string
	Action     string
	CID        string
}

// DroppedOpsError is returned by ConvertEvent when one or more ops in
// an event were dropped while the rest survived. Two producers share
// it: create/update ops whose record block was absent from the
// commit's CAR diff (partial CARs are spec-permitted — a block may be
// omitted e.g. when the new CID equals the old CID after a no-op
// update, or when a non-canonical PDS just doesn't include it), and
// ops whose collection/rkey failed atproto spec validation. The drop
// is informational rather than fatal: the well-formed ops in the same
// event are still returned alongside the error and the caller is
// expected to fall through and archive them.
//
// Reach the value via errors.AsType[*DroppedOpsError].
type DroppedOpsError struct {
	Dropped []DroppedOp
}

func (e *DroppedOpsError) Error() string {
	if len(e.Dropped) == 1 {
		d := e.Dropped[0]
		return fmt.Sprintf(
			"livestream: dropped 1 op (did=%s collection=%s rkey=%s action=%s reason=%s)",
			d.DID, d.Collection, d.RKey, d.Action, d.Reason,
		)
	}
	return fmt.Sprintf("livestream: dropped %d ops", len(e.Dropped))
}

// CountByReason returns the number of dropped ops per reason, for
// bulk metric increments.
func (e *DroppedOpsError) CountByReason() map[ingest.DropReason]int {
	out := make(map[ingest.DropReason]int, 2)
	for _, d := range e.Dropped {
		out[d.Reason]++
	}
	return out
}
