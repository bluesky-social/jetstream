package subscribe

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/streaming"
)

// Encode renders evt as the Jetstream v1 JSON wire format.
//
// Returns errSkipEvent for kinds that v1 deliberately did not emit
// (currently #sync events). The handler treats errSkipEvent as
// "advance, don't disconnect."
//
// Pure: no I/O, no goroutines. Safe to fuzz against arbitrary input.
func Encode(evt *segment.Event) ([]byte, error) {
	switch evt.Kind {
	case segment.KindCreate, segment.KindUpdate, segment.KindDelete:
		return encodeCommit(evt)
	case segment.KindIdentity:
		return encodeIdentity(evt)
	case segment.KindAccount:
		return encodeAccount(evt)
	case segment.KindSync:
		// v1 jetstream did not emit #sync events. The archive path is
		// authoritative for #sync; the v1-compat wire format stays clean.
		return nil, errSkipEvent
	default:
		return nil, fmt.Errorf("subscribe: unknown event kind %d", evt.Kind)
	}
}

// EncodeExtended renders evt as the extended Jetstream v2 JSON wire
// format. It is a superset of the v1-compatible shape for v1-visible
// events, and additionally emits archived #sync events.
func EncodeExtended(evt *segment.Event) ([]byte, error) {
	switch evt.Kind {
	case segment.KindCreate, segment.KindUpdate, segment.KindDelete:
		return encodeExtendedCommit(evt)
	case segment.KindIdentity:
		return encodeExtendedIdentity(evt)
	case segment.KindAccount:
		return encodeExtendedAccount(evt)
	case segment.KindSync:
		return encodeExtendedSync(evt)
	default:
		return nil, fmt.Errorf("subscribe: unknown event kind %d", evt.Kind)
	}
}

func encodeCommit(evt *segment.Event) ([]byte, error) {
	commit := &streaming.JetstreamCommit{
		Rev:        evt.Rev,
		Operation:  commitOpString(evt.Kind),
		Collection: evt.Collection,
		RKey:       evt.Rkey,
	}

	if evt.Kind != segment.KindDelete {
		recordVal, err := cbor.NewDecoder(bytes.NewReader(evt.Payload)).ReadValue()
		if err != nil {
			return nil, fmt.Errorf("subscribe: decode record cbor: %w", err)
		}
		recordJSON, err := cbor.ToJSON(recordVal)
		if err != nil {
			return nil, fmt.Errorf("subscribe: marshal record json: %w", err)
		}
		commit.Record = recordJSON
		commit.CID = cbor.ComputeCID(cbor.CodecDagCBOR, evt.Payload).String()
	}

	env := &streaming.JetstreamEvent{
		DID:    evt.DID,
		TimeUS: evt.IndexedAt,
		Cursor: evt.Seq,
		Kind:   streaming.JetstreamKindCommit,
		Commit: commit,
	}
	return json.Marshal(env)
}

type extendedEvent struct {
	DID                 string                                  `json:"did"`
	TimeUS              int64                                   `json:"time_us"`
	Cursor              uint64                                  `json:"cursor"`
	Kind                string                                  `json:"kind"`
	Seq                 uint64                                  `json:"seq"`
	UpstreamRelayCursor int64                                   `json:"upstream_relay_cursor"`
	Commit              *extendedCommit                         `json:"commit,omitempty"`
	Account             *comatproto.SyncSubscribeRepos_Account  `json:"account,omitempty"`
	Identity            *comatproto.SyncSubscribeRepos_Identity `json:"identity,omitempty"`
	Sync                *comatproto.SyncSubscribeRepos_Sync     `json:"sync,omitempty"`
}

type extendedCommit struct {
	Rev        string          `json:"rev"`
	Operation  string          `json:"operation"`
	Collection string          `json:"collection"`
	RKey       string          `json:"rkey"`
	Record     json.RawMessage `json:"record,omitempty"`
	CID        string          `json:"cid,omitempty"`
	RecordCBOR string          `json:"record_cbor,omitempty"`
}

func extendedEnvelope(evt *segment.Event, kind string) extendedEvent {
	return extendedEvent{
		DID:                 evt.DID,
		TimeUS:              evt.IndexedAt,
		Cursor:              evt.Seq,
		Kind:                kind,
		Seq:                 evt.Seq,
		UpstreamRelayCursor: evt.UpstreamRelayCursor,
	}
}

func encodeExtendedCommit(evt *segment.Event) ([]byte, error) {
	commit := &extendedCommit{
		Rev:        evt.Rev,
		Operation:  commitOpString(evt.Kind),
		Collection: evt.Collection,
		RKey:       evt.Rkey,
	}

	if evt.Kind != segment.KindDelete {
		recordVal, err := cbor.NewDecoder(bytes.NewReader(evt.Payload)).ReadValue()
		if err != nil {
			return nil, fmt.Errorf("subscribe: decode record cbor: %w", err)
		}
		recordJSON, err := cbor.ToJSON(recordVal)
		if err != nil {
			return nil, fmt.Errorf("subscribe: marshal record json: %w", err)
		}
		commit.Record = recordJSON
		commit.CID = cbor.ComputeCID(cbor.CodecDagCBOR, evt.Payload).String()
		commit.RecordCBOR = base64.StdEncoding.EncodeToString(evt.Payload)
	}

	env := extendedEnvelope(evt, streaming.JetstreamKindCommit)
	env.Commit = commit
	return json.Marshal(&env)
}

func commitOpString(k segment.Kind) string {
	switch k {
	case segment.KindCreate:
		return streaming.JetstreamOpCreate
	case segment.KindUpdate:
		return streaming.JetstreamOpUpdate
	case segment.KindDelete:
		return streaming.JetstreamOpDelete
	default:
		return ""
	}
}

func encodeExtendedIdentity(evt *segment.Event) ([]byte, error) {
	var id comatproto.SyncSubscribeRepos_Identity
	if err := id.UnmarshalCBOR(evt.Payload); err != nil {
		return nil, fmt.Errorf("subscribe: decode identity: %w", err)
	}
	env := extendedEnvelope(evt, streaming.JetstreamKindIdentity)
	env.Identity = &id
	return json.Marshal(&env)
}

func encodeExtendedAccount(evt *segment.Event) ([]byte, error) {
	var acct comatproto.SyncSubscribeRepos_Account
	if err := acct.UnmarshalCBOR(evt.Payload); err != nil {
		return nil, fmt.Errorf("subscribe: decode account: %w", err)
	}
	env := extendedEnvelope(evt, streaming.JetstreamKindAccount)
	env.Account = &acct
	return json.Marshal(&env)
}

func encodeExtendedSync(evt *segment.Event) ([]byte, error) {
	var sync comatproto.SyncSubscribeRepos_Sync
	if err := sync.UnmarshalCBOR(evt.Payload); err != nil {
		return nil, fmt.Errorf("subscribe: decode sync: %w", err)
	}
	env := extendedEnvelope(evt, "sync")
	env.Sync = &sync
	return json.Marshal(&env)
}

func encodeIdentity(evt *segment.Event) ([]byte, error) {
	var id comatproto.SyncSubscribeRepos_Identity
	if err := id.UnmarshalCBOR(evt.Payload); err != nil {
		return nil, fmt.Errorf("subscribe: decode identity: %w", err)
	}
	env := &streaming.JetstreamEvent{
		DID:      evt.DID,
		TimeUS:   evt.IndexedAt,
		Cursor:   evt.Seq,
		Kind:     streaming.JetstreamKindIdentity,
		Identity: &id,
	}
	return json.Marshal(env)
}

func encodeAccount(evt *segment.Event) ([]byte, error) {
	var acct comatproto.SyncSubscribeRepos_Account
	if err := acct.UnmarshalCBOR(evt.Payload); err != nil {
		return nil, fmt.Errorf("subscribe: decode account: %w", err)
	}
	env := &streaming.JetstreamEvent{
		DID:     evt.DID,
		TimeUS:  evt.IndexedAt,
		Cursor:  evt.Seq,
		Kind:    streaming.JetstreamKindAccount,
		Account: &acct,
	}
	return json.Marshal(env)
}
