// package live: events.go is the pure converter from atmos's
// upstream streaming event shape to the segment.Event shape jetstream
// writes to disk. No I/O, no allocation beyond the result slice and
// CBOR marshalling. Safe to fuzz against arbitrary input — every
// branch returns an error rather than panicking on malformed bytes.
//
// All segment.Events derived from a single upstream event share the
// same indexedAt timestamp. Per-record timestamps would imply false
// ordering (DESIGN.md §3.4 requires per-DID ingest order is preserved).
//
// Seq is left zero on the returned events — ingest.Writer.Append
// allocates the value at write time.
package live

import (
	"fmt"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos/streaming"
)

// ConvertEvent translates one atmos streaming.Event into zero or
// more segment.Events. See the per-kind mapping in the spec
// (§4.3 of the design doc).
func ConvertEvent(evt streaming.Event, indexedAt int64) ([]segment.Event, error) {
	switch {
	case evt.Commit != nil:
		return convertCommit(evt, indexedAt)
	case evt.Identity != nil:
		return convertIdentity(evt, indexedAt)
	case evt.Account != nil:
		return convertAccount(evt, indexedAt)
	case evt.Sync != nil:
		return convertSync(evt, indexedAt)
	case evt.Info != nil:
		// #info is informational: archival no-op, but the seq is
		// still ours to record so we let the caller advance the
		// cursor past it.
		return nil, nil
	default:
		// No recognized field set. Either atmos shipped a new event
		// variant ahead of jetstream, or the wire shape changed.
		// Either way the safe thing is to refuse to advance the
		// cursor past this seq so a future build can replay it.
		return nil, ErrUnknownEventKind
	}
}

func convertCommit(evt streaming.Event, indexedAt int64) ([]segment.Event, error) {
	commit := evt.Commit
	ops := make([]segment.Event, 0, len(commit.Ops))

	for op, err := range evt.Operations() {
		if err != nil {
			return nil, fmt.Errorf("livestream: decode commit ops for did=%s: %w", commit.Repo, err)
		}

		kind, err := actionKind(op.Action)
		if err != nil {
			return nil, fmt.Errorf("livestream: did=%s: %w", commit.Repo, err)
		}

		segEv := segment.Event{
			IndexedAt:  indexedAt,
			Kind:       kind,
			DID:        commit.Repo,
			Collection: string(op.Collection),
			Rkey:       string(op.RKey),
			Rev:        commit.Rev,
		}
		// Deletes have no record bytes; everything else carries the
		// raw CBOR record block exactly as atmos extracted it from
		// the commit's CAR. atmos returns BlockData()==nil silently
		// when the op's CID is missing from the CAR diff (truncated
		// CAR, hash mismatch, or relay bug). We refuse rather than
		// archive a Create/Update with no payload — PRACTICES.md:
		// crashing > silent corruption.
		if kind != segment.KindDelete {
			block := op.BlockData()
			if block == nil {
				return nil, fmt.Errorf(
					"livestream: did=%s collection=%s rkey=%s: %s op references CID missing from CAR diff",
					commit.Repo, op.Collection, op.RKey, op.Action,
				)
			}
			segEv.Payload = append([]byte(nil), block...)
		}
		ops = append(ops, segEv)
	}
	return ops, nil
}

func actionKind(a streaming.Action) (segment.Kind, error) {
	switch a {
	case streaming.ActionCreate:
		return segment.KindCreate, nil
	case streaming.ActionUpdate:
		return segment.KindUpdate, nil
	case streaming.ActionDelete:
		return segment.KindDelete, nil
	case streaming.ActionResync:
		// After Sync 1.1, atmos's verifier resync worker yields each
		// record currently in the repo as ActionResync with the live
		// record bytes. Mapping to KindCreate is the brainstorming-
		// locked decision: the segment is an event log, not a state
		// table, so emitting a duplicate Create over a record we've
		// already archived is acceptable. Downstream consumers can
		// dedupe on (DID, Collection, Rkey, Rev).
		return segment.KindCreate, nil
	default:
		return 0, fmt.Errorf("livestream: unknown commit action %q", a)
	}
}

func convertIdentity(evt streaming.Event, indexedAt int64) ([]segment.Event, error) {
	payload, err := evt.Identity.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("livestream: marshal identity: %w", err)
	}
	return []segment.Event{{
		IndexedAt: indexedAt,
		Kind:      segment.KindIdentity,
		DID:       evt.Identity.DID,
		Payload:   payload,
	}}, nil
}

func convertAccount(evt streaming.Event, indexedAt int64) ([]segment.Event, error) {
	payload, err := evt.Account.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("livestream: marshal account: %w", err)
	}
	return []segment.Event{{
		IndexedAt: indexedAt,
		Kind:      segment.KindAccount,
		DID:       evt.Account.DID,
		Payload:   payload,
	}}, nil
}

func convertSync(evt streaming.Event, indexedAt int64) ([]segment.Event, error) {
	payload, err := evt.Sync.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("livestream: marshal sync: %w", err)
	}
	return []segment.Event{{
		IndexedAt: indexedAt,
		Kind:      segment.KindSync,
		DID:       evt.Sync.DID,
		Rev:       evt.Sync.Rev,
		Payload:   payload,
	}}, nil
}
