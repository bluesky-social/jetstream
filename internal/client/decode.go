package client

import (
	"bytes"
	"encoding/base64"
	"fmt"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
)

// decodeSegmentEvent converts a decoded segment row into the engine's
// region-agnostic Event. It decodes the raw CBOR payload into a generic
// record map (for commits) and the typed identity/account/sync shapes.
//
// segment.Event payloads alias a shared decompressed block buffer, so any
// bytes retained in the returned Event (notably RecordCBOR) are copied.
func decodeSegmentEvent(ev *segment.Event) (Event, error) {
	out := Event{
		DID:    ev.DID,
		Seq:    ev.Seq,
		TimeUS: ev.IndexedAt,
	}
	switch ev.Kind {
	case segment.KindCreate, segment.KindUpdate, segment.KindDelete, segment.KindCreateResync:
		commit, err := decodeCommit(ev)
		if err != nil {
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

func decodeCommit(ev *segment.Event) (*Commit, error) {
	commit := &Commit{
		Operation:  commitOperation(ev.Kind),
		Collection: ev.Collection,
		Rkey:       ev.Rkey,
		Rev:        ev.Rev,
	}
	if ev.Kind == segment.KindDelete {
		return commit, nil
	}

	record, err := decodeRecordMap(ev.Payload)
	if err != nil {
		return nil, fmt.Errorf("jetstream: decode record (did=%s collection=%s rkey=%s seq=%d): %w",
			ev.DID, ev.Collection, ev.Rkey, ev.Seq, err)
	}
	commit.Record = record
	commit.CID = cbor.ComputeCID(cbor.CodecDagCBOR, ev.Payload).String()
	commit.RecordCBOR = bytes.Clone(ev.Payload)
	return commit, nil
}

// decodeRecordMap decodes DAG-CBOR record bytes into a generic JSON-shaped
// object: the same shape callers see in the "record" field on /subscribe.
//
// It converts the CBOR data model directly into the JSON-shaped value, rather
// than round-tripping through JSON text (cbor.ToJSON -> json.Unmarshal). The
// round-trip dominated backfill decode CPU (see #142): marshalling the decoded
// value to JSON bytes and reparsing them was ~half of per-record decode cost
// for no benefit, since ReadValue already produced the structured value. The
// output is deep-equal to the old path: numbers are float64 (matching
// encoding/json's number handling), []byte becomes {"$bytes": base64-raw}, and
// a CID link becomes {"$link": cid-string} — the ATProto JSON sentinels.
func decodeRecordMap(payload []byte) (map[string]any, error) {
	r := bytes.NewReader(payload)
	val, err := cbor.NewDecoder(r).ReadValue()
	if err != nil {
		return nil, fmt.Errorf("cbor decode: %w", err)
	}
	// A record payload must be exactly one CBOR item. Trailing bytes mean the
	// frame is corrupt: the map we decoded would not match RecordCBOR/CID
	// (which retain the full payload), so reject rather than silently diverge.
	if r.Len() != 0 {
		return nil, fmt.Errorf("cbor decode: %d trailing bytes after record", r.Len())
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
	return jsonShapedMap(m), nil
}

// jsonShapedValue converts one cbor.ReadValue result into its ATProto
// JSON-shaped form, recursively. The mapping mirrors cbor.ToJSON followed by
// encoding/json round-tripping exactly:
//   - int64/float64 -> float64 (encoding/json represents all JSON numbers as
//     float64 when decoding into any; the old path went through JSON text, so a
//     CBOR integer surfaced as a float64 — preserved here to stay byte-for-byte
//     compatible with existing consumers).
//   - []byte        -> {"$bytes": base64.RawStdEncoding}
//   - cbor.CID      -> {"$link": cid.String()}
//   - []any / map   -> converted element-wise.
//   - string/bool/nil -> unchanged.
func jsonShapedValue(v any) any {
	switch val := v.(type) {
	case nil:
		return nil
	case bool:
		return val
	case string:
		return val
	case int64:
		return float64(val)
	case float64:
		return val
	case []byte:
		return map[string]any{"$bytes": base64.RawStdEncoding.EncodeToString(val)}
	case cbor.CID:
		return map[string]any{"$link": val.String()}
	case []any:
		out := make([]any, len(val))
		for i := range val {
			out[i] = jsonShapedValue(val[i])
		}
		return out
	case map[string]any:
		return jsonShapedMap(val)
	default:
		// ReadValue only ever returns the cases above; a new kind would be a
		// library change. Return as-is rather than dropping data silently.
		return val
	}
}

// jsonShapedMap converts a CBOR map in place into its JSON-shaped form. The map
// from ReadValue is freshly allocated and not retained elsewhere, so rewriting
// its values avoids a second map allocation per object.
func jsonShapedMap(m map[string]any) map[string]any {
	for k, v := range m {
		m[k] = jsonShapedValue(v)
	}
	return m
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
