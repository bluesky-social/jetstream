// package live: events.go is the pure converter from atmos's
// upstream streaming event shape to the segment.Event shape jetstream
// writes to disk. No I/O, no allocation beyond the result slice and
// CBOR marshalling. Safe to fuzz against arbitrary input — every
// branch returns an error rather than panicking on malformed bytes.
//
// All segment.Events derived from a single upstream event share the
// same witnessedAt timestamp. Per-record timestamps would imply false
// ordering (DESIGN.md §3.4 requires per-DID ingest order is preserved).
//
// Seq is left zero on the returned events — ingest.Writer.Append
// allocates the value at write time.
package live

import (
	"fmt"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/streaming"
)

// ConvertEvent translates one atmos streaming.Event into zero or
// more segment.Events. See the per-kind mapping in the spec
// (§4.3 of the design doc).
//
// When a #commit's CAR diff omits the record block for one or more
// create/update ops, ConvertEvent returns ErrDroppedMissingBlocks
// alongside the surviving events; use errors.AsType to retrieve
// per-op detail. The error is informational (the surviving events
// in the slice are still archivable); callers should fall through
// rather than discard the result. See the consumer's processBatch
// for the canonical handling.
func ConvertEvent(evt streaming.Event, witnessedAt int64) ([]segment.Event, error) {
	switch {
	case evt.Resync != streaming.ResyncNone && evt.Sync == nil:
		return nil, fmt.Errorf("livestream: resync event missing sync envelope: %w", ErrUnknownEventKind)
	case evt.Commit != nil:
		return convertCommit(evt, witnessedAt)
	case evt.Identity != nil:
		return convertIdentity(evt, witnessedAt)
	case evt.Account != nil:
		return convertAccount(evt, witnessedAt)
	case evt.Sync != nil:
		return convertSync(evt, witnessedAt)
	case evt.Info != nil:
		// #info is informational: archival no-op, but the seq is
		// still ours to record so we let the caller advance the
		// cursor past it.
		return nil, nil
	default:
		// No public envelope is set. Two cases:
		//
		//   1. Older local atmos checkouts emitted async resync
		//      replacement ops without a public envelope. The current
		//      local atmos API emits ResyncAsync with Sync populated,
		//      but keeping this fallback makes the converter tolerant
		//      during branch bisects and local replace churn.
		//
		//   2. A future relay variant we don't know how to decode.
		//      Operations() yields nothing in that case, so we fall
		//      through to ErrUnknownEventKind and the consumer refuses
		//      to advance its cursor past this seq.
		return convertVerifiedOps(evt, witnessedAt)
	}
}

// validateRev enforces the #197 ingest gate on a commit/sync rev: it
// must be a spec-valid TID, because rev ordering drives the merge
// filter, the stale-resync guard, and compaction tombstone folding.
// Returns a typed *InvalidEventError so the caller drops the whole
// event and counts it — every row of the event shares the rev, so no
// part of it is archivable.
func validateRev(did, rev string) error {
	if _, err := atmos.ParseTID(rev); err != nil {
		return &InvalidEventError{
			Reason: ingest.DropReasonInvalidRev,
			DID:    did,
			Detail: err.Error(),
		}
	}
	return nil
}

// validateOpPath enforces the per-op half of the gate: collection
// must be a spec-valid NSID (which also rejects `$`-prefixed names
// that could shadow the $account/$identity/$sync sentinels) and rkey
// a spec-valid record key. Returns the DroppedOp to record, or nil if
// the op is clean. Wire op paths are attacker-controlled and atmos
// deliberately does not re-validate them (streaming.Operation doc).
func validateOpPath(op *streaming.Operation) *DroppedOp {
	var reason ingest.DropReason
	switch {
	case op.Collection.Validate() != nil:
		reason = ingest.DropReasonInvalidCollection
	case op.RKey.Validate() != nil:
		reason = ingest.DropReasonInvalidRkey
	default:
		return nil
	}
	return &DroppedOp{
		Reason:     reason,
		DID:        string(op.Repo),
		Collection: string(op.Collection),
		RKey:       string(op.RKey),
		Action:     string(op.Action),
		CID:        opCIDString(op),
	}
}

// validateOp gates one op whose rev is archived from op.Rev itself
// (the verified-ops and sync-resync paths; convertCommit archives
// the already-validated commit.Rev instead and skips the rev half).
// Returns the DroppedOp to record, or nil if the op is clean.
func validateOp(op *streaming.Operation) *DroppedOp {
	if op.Rev.Validate() != nil {
		return &DroppedOp{
			Reason:     ingest.DropReasonInvalidRev,
			DID:        string(op.Repo),
			Collection: string(op.Collection),
			RKey:       string(op.RKey),
			Action:     string(op.Action),
			CID:        opCIDString(op),
		}
	}
	return validateOpPath(op)
}

// opCIDString renders the op's CID for drop diagnostics; deletes have
// no CID and render empty rather than a garbage zero-value encoding.
func opCIDString(op *streaming.Operation) string {
	if !op.CID.Defined() {
		return ""
	}
	return op.CID.String()
}

// convertVerifiedOps drains evt.Operations() and converts each op
// into a segment.Event. Used for the verifier-resync emission path
// where the upstream wire envelope is absent and the only signal is
// the iterator yielding ops.
func convertVerifiedOps(evt streaming.Event, witnessedAt int64) ([]segment.Event, error) {
	var out []segment.Event
	var dropped []DroppedOp
	for op, err := range evt.Operations() {
		if err != nil {
			return nil, fmt.Errorf("livestream: decode resync ops: %w", err)
		}

		kind, err := actionKind(op.Action)
		if err != nil {
			return nil, fmt.Errorf("livestream: did=%s: %w", op.Repo, err)
		}

		// No shared envelope rev exists on this path; each op carries
		// its own. An invalid field drops just that op.
		if d := validateOp(&op); d != nil {
			dropped = append(dropped, *d)
			continue
		}

		segEv := segment.Event{
			WitnessedAt:         witnessedAt,
			UpstreamRelayCursor: evt.Seq,
			Kind:                kind,
			DID:                 string(op.Repo),
			Collection:          string(op.Collection),
			Rkey:                string(op.RKey),
			Rev:                 string(op.Rev),
		}
		// Resync ops carry the live record bytes for create/update;
		// deletes are not part of a resync result (atmos's resync
		// worker only emits records present in the post-resync repo).
		// A nil block here would mean atmos's resync worker emitted
		// an op without payload bytes, which it shouldn't — but we
		// drop rather than crash to keep the property uniform with
		// convertCommit: a misbehaving upstream should not be able
		// to take the firehose down. The drop is surfaced via
		// DroppedOpsError so it is never silent: when the
		// event also carries a KindSync DID tombstone, a record
		// skipped here is permanently absent from the post-compaction
		// archive, and the operator needs the signal.
		if kind != segment.KindDelete {
			block := op.BlockData()
			if block == nil {
				dropped = append(dropped, DroppedOp{
					Reason:     ingest.DropReasonMissingBlock,
					DID:        string(op.Repo),
					Collection: string(op.Collection),
					RKey:       string(op.RKey),
					Action:     string(op.Action),
					CID:        op.CID.String(),
				})
				continue
			}
			segEv.Payload = append([]byte(nil), block...)
		}
		out = append(out, segEv)
	}

	if len(dropped) > 0 {
		return out, &DroppedOpsError{Dropped: dropped}
	}
	if len(out) == 0 {
		// Iterator yielded nothing at all — this is a true unknown
		// event kind (no Commit/Sync/Identity/Account/Info, no
		// verified ops). Refuse to advance the cursor past it.
		return nil, ErrUnknownEventKind
	}
	return out, nil
}

func convertCommit(evt streaming.Event, witnessedAt int64) ([]segment.Event, error) {
	commit := evt.Commit
	if err := validateRev(commit.Repo, commit.Rev); err != nil {
		return nil, err
	}
	ops := make([]segment.Event, 0, len(commit.Ops))
	var dropped []DroppedOp

	for op, err := range evt.Operations() {
		if err != nil {
			return nil, fmt.Errorf("livestream: decode commit ops for did=%s: %w", commit.Repo, err)
		}

		kind, err := actionKind(op.Action)
		if err != nil {
			return nil, fmt.Errorf("livestream: did=%s: %w", commit.Repo, err)
		}

		if d := validateOpPath(&op); d != nil {
			dropped = append(dropped, *d)
			continue
		}

		segEv := segment.Event{
			WitnessedAt:         witnessedAt,
			UpstreamRelayCursor: evt.Seq,
			Kind:                kind,
			DID:                 commit.Repo,
			Collection:          string(op.Collection),
			Rkey:                string(op.RKey),
			Rev:                 commit.Rev,
		}
		// Deletes have no record bytes; everything else carries the
		// raw CBOR record block exactly as atmos extracted it from
		// the commit's CAR. atmos returns BlockData()==nil silently
		// when the op's CID is missing from the CAR diff — partial
		// CARs are spec-permitted (a record block may be omitted
		// e.g. when the new CID equals the old CID after a no-op
		// update, or when a non-canonical PDS just doesn't include
		// it). We drop the op rather than archive a Create/Update
		// with nil payload (which would be data-corruption-shaped),
		// and rather than abort the whole commit (which would let a
		// single misbehaving PDS DoS the firehose consumer). The
		// drop is surfaced to the caller via ErrDroppedMissingBlocks
		// alongside the well-formed events.
		if kind != segment.KindDelete {
			block := op.BlockData()
			if block == nil {
				dropped = append(dropped, DroppedOp{
					Reason:     ingest.DropReasonMissingBlock,
					DID:        commit.Repo,
					Collection: string(op.Collection),
					RKey:       string(op.RKey),
					Action:     string(op.Action),
					CID:        op.CID.String(),
				})
				continue
			}
			segEv.Payload = append([]byte(nil), block...)
		}
		ops = append(ops, segEv)
	}
	if len(dropped) > 0 {
		return ops, &DroppedOpsError{Dropped: dropped}
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
		// record bytes. Persist a distinct create-shaped kind so the
		// v1 /subscribe presentation can hide these replacement rows
		// while /subscribe-v2 and archive readers still see the full
		// Sync 1.1 state-repair stream.
		return segment.KindCreateResync, nil
	default:
		return 0, fmt.Errorf("livestream: unknown commit action %q", a)
	}
}

func convertIdentity(evt streaming.Event, witnessedAt int64) ([]segment.Event, error) {
	payload, err := evt.Identity.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("livestream: marshal identity: %w", err)
	}
	return []segment.Event{{
		WitnessedAt:         witnessedAt,
		UpstreamRelayCursor: evt.Seq,
		Kind:                segment.KindIdentity,
		DID:                 evt.Identity.DID,
		Payload:             payload,
	}}, nil
}

func convertAccount(evt streaming.Event, witnessedAt int64) ([]segment.Event, error) {
	payload, err := evt.Account.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("livestream: marshal account: %w", err)
	}
	return []segment.Event{{
		WitnessedAt:         witnessedAt,
		UpstreamRelayCursor: evt.Seq,
		Kind:                segment.KindAccount,
		DID:                 evt.Account.DID,
		Payload:             payload,
	}}, nil
}

func convertSync(evt streaming.Event, witnessedAt int64) ([]segment.Event, error) {
	// The #sync rev seeds a DID tombstone that compaction folds by
	// rev comparison; a garbage rev would give the tombstone wrong
	// ordering power, so the whole event (tombstone + replacement
	// rows, which share the rev) is dropped.
	if err := validateRev(evt.Sync.DID, evt.Sync.Rev); err != nil {
		return nil, err
	}
	payload, err := evt.Sync.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("livestream: marshal sync: %w", err)
	}
	out := []segment.Event{{
		WitnessedAt:         witnessedAt,
		UpstreamRelayCursor: evt.Seq,
		Kind:                segment.KindSync,
		DID:                 evt.Sync.DID,
		Rev:                 evt.Sync.Rev,
		Payload:             payload,
	}}

	// When the verifier performs a synchronous resync for this #sync
	// event, Operations yields the authoritative post-resync record set
	// without extra I/O. (atmos's resync is all-or-nothing — a missing
	// record block fails the whole resync — and live.Config requires a
	// verifier, so Operations never falls back to lazy network I/O
	// here.) The KindSync row must remain first so its seq is below
	// every replacement record assigned by ingest.Writer.
	var dropped []DroppedOp
	for op, err := range evt.Operations() {
		if err != nil {
			return nil, fmt.Errorf("livestream: decode sync resync ops for did=%s: %w", evt.Sync.DID, err)
		}
		kind, err := actionKind(op.Action)
		if err != nil {
			return nil, fmt.Errorf("livestream: did=%s: %w", op.Repo, err)
		}
		// Resync ops materialize records repaired under this #sync's
		// tombstone; a spec-invalid field drops just that record (the
		// tombstone and its siblings stay archivable — replacement
		// rows carry their own op.Rev, hence the per-op rev check).
		if d := validateOp(&op); d != nil {
			dropped = append(dropped, *d)
			continue
		}
		segEv := segment.Event{
			WitnessedAt:         witnessedAt,
			UpstreamRelayCursor: evt.Seq,
			Kind:                kind,
			DID:                 string(op.Repo),
			Collection:          string(op.Collection),
			Rkey:                string(op.RKey),
			Rev:                 string(op.Rev),
		}
		if kind != segment.KindDelete {
			block := op.BlockData()
			if block == nil {
				// Should be unreachable for resync ops (see above),
				// but the KindSync row is a DID tombstone: silently
				// skipping a replacement record here would make
				// compaction permanently erase it from the archive.
				// Surface the drop so the consumer counts and logs it.
				dropped = append(dropped, DroppedOp{
					Reason:     ingest.DropReasonMissingBlock,
					DID:        string(op.Repo),
					Collection: string(op.Collection),
					RKey:       string(op.RKey),
					Action:     string(op.Action),
					CID:        op.CID.String(),
				})
				continue
			}
			segEv.Payload = append([]byte(nil), block...)
		}
		out = append(out, segEv)
	}
	if len(dropped) > 0 {
		return out, &DroppedOpsError{Dropped: dropped}
	}
	return out, nil
}
