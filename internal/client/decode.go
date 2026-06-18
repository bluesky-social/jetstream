package client

import (
	"bytes"
	"encoding/json"
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
func decodeRecordMap(payload []byte) (map[string]any, error) {
	val, err := cbor.NewDecoder(bytes.NewReader(payload)).ReadValue()
	if err != nil {
		return nil, fmt.Errorf("cbor decode: %w", err)
	}
	jsonBytes, err := cbor.ToJSON(val)
	if err != nil {
		return nil, fmt.Errorf("cbor to json: %w", err)
	}
	var m map[string]any
	if err := json.Unmarshal(jsonBytes, &m); err != nil {
		return nil, fmt.Errorf("json unmarshal record: %w", err)
	}
	return m, nil
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
