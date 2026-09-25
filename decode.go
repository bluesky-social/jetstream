package jetstream

import (
	"bytes"
	"encoding/base64"
	"fmt"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
)

// decodeSegmentEvent converts a decoded segment row into the engine's
// region-agnostic Event, allocating a fresh *Commit for a commit row. It is the
// single-event helper used by tests and one-off callers; the hot decode path
// uses decodeSegmentEventInto with a per-block Commit slab to avoid one heap
// allocation per commit.
//
// segment.Event payloads alias a shared decompressed block buffer, so any
// bytes retained in the returned Event (notably RecordCBOR) are copied.
func decodeSegmentEvent(ev *segment.Event) (Event, error) {
	var commit Commit
	return decodeSegmentEventInto(ev, &commit, recordDecodeMode{})
}

// decodeSegmentEventInto is decodeSegmentEvent but writes a commit row's data
// into the caller-provided *commit instead of allocating one. The caller passes
// a pointer into a per-block []Commit slab (see decodeFrame), so a whole block's
// commits cost one slice allocation rather than one *Commit each. commit is only
// written (and referenced by the returned Event) for commit-kind rows; for
// identity/account/sync rows it is ignored and those shapes are allocated
// individually (they are comparatively rare). The caller MUST ensure commit's
// backing slab is not grown/reallocated while the returned Event is reachable.
// mode is forwarded to decodeCommitInto (raw vs. map record materialization).
func decodeSegmentEventInto(ev *segment.Event, commit *Commit, mode recordDecodeMode) (Event, error) {
	out := Event{
		DID:           ev.DID,
		Seq:           ev.Seq,
		TimeUS:        ev.DisplayTimeUS(),
		WitnessedAtUS: ev.WitnessedAt,
	}
	switch ev.Kind {
	case segment.KindCreate, segment.KindUpdate, segment.KindDelete, segment.KindCreateResync:
		if err := decodeCommitInto(ev, commit, mode); err != nil {
			return Event{}, err
		}
		out.Kind = KindCommit
		out.Commit = commit
	case segment.KindIdentity:
		id, err := decodeIdentity(ev)
		if err != nil {
			return Event{}, err
		}
		out.Kind = KindIdentity
		out.Identity = id
	case segment.KindAccount:
		acct, err := decodeAccount(ev)
		if err != nil {
			return Event{}, err
		}
		out.Kind = KindAccount
		out.Account = acct
	case segment.KindSync:
		sync, err := decodeSync(ev)
		if err != nil {
			return Event{}, err
		}
		out.Kind = KindSync
		out.Sync = sync
	default:
		return Event{}, fmt.Errorf("jetstream: unknown event kind %d (did=%s seq=%d)", ev.Kind, ev.DID, ev.Seq)
	}
	return out, nil
}

// decodeCommit decodes a commit row into a freshly-allocated *Commit. The hot
// path uses decodeCommitInto with a slab slot; this allocating form is kept for
// tests and single-event callers.
func decodeCommit(ev *segment.Event) (*Commit, error) {
	commit := &Commit{}
	if err := decodeCommitInto(ev, commit, recordDecodeMode{}); err != nil {
		return nil, err
	}
	return commit, nil
}

// decodeCommitInto fills *commit from a commit row. On error *commit may hold
// partial data, but the caller discards the event (and the slab slot is never
// read) on a decode failure, so the partial write is harmless.
//
// mode selects record materialization. In the default (map) mode it builds the
// generic Record map[string]any, computes the CID, and clones RecordCBOR so the
// caller may retain it. In raw mode it SKIPS the map build entirely (the
// dominant decode allocation): Record stays nil, RecordCBOR aliases ev.Payload
// zero-copy, and the CID is computed only if mode.wantCIDs. The raw aliasing is
// safe because ev.Payload aliases the decompressed block buffer, which outlives
// the batch (the segment events already alias it for DID/Collection/Rkey/Rev);
// the contract — valid for the batch lifetime, copy to retain — is the same one
// the default Record (built via UnmarshalNoCopy) already carries.
func decodeCommitInto(ev *segment.Event, commit *Commit, mode recordDecodeMode) error {
	*commit = Commit{
		Operation:  commitOperation(ev.Kind),
		Collection: ev.Collection,
		Rkey:       ev.Rkey,
		Rev:        ev.Rev,
	}
	if ev.Kind == segment.KindDelete {
		return nil
	}

	if mode.raw {
		if mode.wantCIDs {
			commit.CID = cbor.ComputeCID(cbor.CodecDagCBOR, ev.Payload).String()
		}
		if mode.copyCBOR {
			commit.RecordCBOR = bytes.Clone(ev.Payload) // safe to retain (WithRawRecordsCopied)
		} else {
			commit.RecordCBOR = ev.Payload // zero-copy alias; see doc + WithRawRecords contract
		}
		return nil
	}

	record, err := decodeRecordMap(ev.Payload)
	if err != nil {
		return fmt.Errorf("jetstream: decode record (did=%s collection=%s rkey=%s seq=%d): %w",
			ev.DID, ev.Collection, ev.Rkey, ev.Seq, err)
	}
	commit.Record = record
	commit.CID = cbor.ComputeCID(cbor.CodecDagCBOR, ev.Payload).String()
	commit.RecordCBOR = bytes.Clone(ev.Payload)
	return nil
}

// decodeRecordMap converts DAG-CBOR directly to the atproto JSON data model,
// avoiding a JSON marshal/unmarshal round trip. Numbers become float64, bytes
// become {"$bytes": base64-raw}, and links become {"$link": cid-string}.
//
// UnmarshalNoCopy avoids reader indirection and string copies, and rejects
// trailing bytes. The returned strings and bytes alias the read-only
// decompressed block. References in the Event and Record map keep that buffer
// alive; callers must not mutate it. decodeCommit separately clones
// RecordCBOR.
func decodeRecordMap(payload []byte) (map[string]any, error) {
	val, err := cbor.UnmarshalNoCopy(payload)
	if err != nil {
		return nil, fmt.Errorf("cbor decode: %w", err)
	}
	// A valid atproto record is always a CBOR map, so require the top-level value
	// to be one and fail closed otherwise: a malformed payload must not surface as
	// a non-delete commit with a nil/garbage Record. For a map, the converted
	// output is identical to the canonical cbor.ToJSON shape the server emits on
	// /subscribe — that semantic equivalence (not bug-for-bug parity with the old
	// client code) is the contract here.
	//
	// This is intentionally stricter than the prior JSON round-trip, which
	// inconsistently accepted a top-level byte string or CID (cbor.ToJSON wraps
	// them as the JSON objects {"$bytes":..}/{"$link":..}) while rejecting a
	// top-level scalar/array/null. Neither is a valid record; rejecting all
	// non-map top-level payloads is the deliberate, consistent fail-closed choice.
	m, ok := val.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("cbor decode: record is not an object")
	}
	if !jsonShapeMap(m) {
		return nil, fmt.Errorf("cbor decode: record contains a value outside the atproto data model")
	}
	return m, nil
}

// jsonShapeValue converts one decoded CBOR value into its ATProto JSON-shaped
// form, recursively. The bool is false for values outside the atproto data
// model. The mapping mirrors cbor.ToJSON followed by encoding/json
// round-tripping:
//   - int64 -> float64 (encoding/json represents all JSON numbers as float64
//     when decoding into any; the old path went through JSON text, so a CBOR
//     integer surfaced as a float64 — preserved for byte-for-byte compatibility).
//   - []byte        -> {"$bytes": base64.RawStdEncoding}
//   - cbor.CID      -> {"$link": cid.String()}
//   - []any / map   -> converted element-wise, in place.
//   - string/bool/nil -> passed through unchanged (original box reused).
//   - float64 and unknown values -> rejected (atproto has integers, not floats).
//
// It mutates maps and slices in place and reuses the decoder's existing any
// boxes for unchanged scalars, so it allocates only where the value's JSON shape
// actually differs from its CBOR form (integers, byte strings, CID links).
func jsonShapeValue(v any) (any, bool) {
	switch val := v.(type) {
	case int64:
		// CBOR integers surface as JSON float64 (the canonical cbor.ToJSON +
		// encoding/json contract). This is the one pass-through-shaped arm that
		// MUST re-box: the stored kind genuinely changes (int64 box -> float64).
		return float64(val), true
	case []byte:
		return map[string]any{"$bytes": base64.RawStdEncoding.EncodeToString(val)}, true
	case cbor.CID:
		return map[string]any{"$link": val.String()}, true
	case []any:
		// Rewrite in place rather than allocating a second slice: the slice came
		// from the decoder and is not retained elsewhere. Only elements that
		// actually change kind re-box, via the recursion.
		for i := range val {
			shaped, ok := jsonShapeValue(val[i])
			if !ok {
				return nil, false
			}
			val[i] = shaped
		}
		return val, true
	case map[string]any:
		return val, jsonShapeMap(val)
	case string, bool, nil:
		// Already-JSON-shaped scalar:
		// pass through UNCHANGED — and we return the ORIGINAL box v, not a
		// re-asserted value. Writing `case string: return val` would unbox to a
		// concrete string and then heap-allocate a FRESH any box on return
		// (runtime.convTstring), once per scalar leaf — ~22% of decode allocations
		// (#142). Returning v reuses the box the decoder already produced; output
		// is byte-identical (same kind, same value).
		return v, true
	default:
		return nil, false
	}
}

// jsonShapeMap converts a CBOR map in place into its JSON-shaped form. The map
// from ReadValue is freshly allocated and not retained elsewhere, so rewriting
// its values avoids a second map allocation per object. It returns false if a
// nested value is outside the atproto data model.
func jsonShapeMap(m map[string]any) bool {
	for k, v := range m {
		shaped, ok := jsonShapeValue(v)
		if !ok {
			return false
		}
		m[k] = shaped
	}
	return true
}

func commitOperation(k segment.Kind) Operation {
	switch k {
	case segment.KindCreate, segment.KindCreateResync:
		return OpCreate
	case segment.KindUpdate:
		return OpUpdate
	case segment.KindDelete:
		return OpDelete
	default:
		return ""
	}
}

func decodeIdentity(ev *segment.Event) (*Identity, error) {
	var id comatproto.SyncSubscribeRepos_Identity
	if err := id.UnmarshalCBOR(ev.Payload); err != nil {
		return nil, fmt.Errorf("jetstream: decode identity (did=%s seq=%d): %w", ev.DID, ev.Seq, err)
	}
	return &Identity{
		DID:    id.DID,
		Handle: id.Handle.ValOr(""),
		Seq:    id.Seq,
		Time:   id.Time,
	}, nil
}

func decodeAccount(ev *segment.Event) (*Account, error) {
	var acct comatproto.SyncSubscribeRepos_Account
	if err := acct.UnmarshalCBOR(ev.Payload); err != nil {
		return nil, fmt.Errorf("jetstream: decode account (did=%s seq=%d): %w", ev.DID, ev.Seq, err)
	}
	return &Account{
		DID:    acct.DID,
		Active: acct.Active,
		Status: acct.Status.ValOr(""),
		Seq:    acct.Seq,
		Time:   acct.Time,
	}, nil
}

func decodeSync(ev *segment.Event) (*Sync, error) {
	var sync comatproto.SyncSubscribeRepos_Sync
	if err := sync.UnmarshalCBOR(ev.Payload); err != nil {
		return nil, fmt.Errorf("jetstream: decode sync (did=%s seq=%d): %w", ev.DID, ev.Seq, err)
	}
	return &Sync{
		DID:  sync.DID,
		Rev:  sync.Rev,
		Seq:  sync.Seq,
		Time: sync.Time,
	}, nil
}
