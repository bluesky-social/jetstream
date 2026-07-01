package subscribe

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/bluesky-social/jetstream/segment"
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
	case segment.KindCreate, segment.KindUpdate, segment.KindDelete, segment.KindCreateResync:
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

// EncodeV2 renders evt as the Jetstream /subscribe-v2 JSON wire format.
// It is a superset of the v1-compatible shape for v1-visible events
// (adding seq and commit.record_cbor), and additionally emits archived
// #sync events.
func EncodeV2(evt *segment.Event) ([]byte, error) {
	switch evt.Kind {
	case segment.KindCreate, segment.KindUpdate, segment.KindDelete, segment.KindCreateResync:
		return encodeV2Commit(evt)
	case segment.KindIdentity:
		return encodeV2Identity(evt)
	case segment.KindAccount:
		return encodeV2Account(evt)
	case segment.KindSync:
		return encodeV2Sync(evt)
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

type v2Event struct {
	DID      string                                  `json:"did"`
	TimeUS   int64                                   `json:"time_us"`
	Cursor   uint64                                  `json:"cursor"`
	Kind     string                                  `json:"kind"`
	Seq      uint64                                  `json:"seq"`
	Commit   *v2Commit                               `json:"commit,omitempty"`
	Account  *comatproto.SyncSubscribeRepos_Account  `json:"account,omitempty"`
	Identity *comatproto.SyncSubscribeRepos_Identity `json:"identity,omitempty"`
	Sync     *v2Sync                                 `json:"sync,omitempty"`
}

type v2Commit struct {
	Rev        string          `json:"rev"`
	Operation  string          `json:"operation"`
	Collection string          `json:"collection"`
	RKey       string          `json:"rkey"`
	Record     json.RawMessage `json:"record,omitempty"`
	CID        string          `json:"cid,omitempty"`
	RecordCBOR string          `json:"record_cbor,omitempty"`
}

// Keep Jetstream's v2 wire shape independent from atmos's generated
// atproto JSON encoding for bytes, which uses DAG-JSON {"$bytes": "..."}.
type v2Sync struct {
	LexiconTypeID string `json:"$type,omitempty"`
	Blocks        []byte `json:"blocks"`
	DID           string `json:"did"`
	Rev           string `json:"rev"`
	Seq           int64  `json:"seq"`
	Time          string `json:"time"`
}

func v2Envelope(evt *segment.Event, kind string) v2Event {
	return v2Event{
		DID:    evt.DID,
		TimeUS: evt.IndexedAt,
		Cursor: evt.Seq,
		Kind:   kind,
		Seq:    evt.Seq,
	}
}

func encodeV2Commit(evt *segment.Event) ([]byte, error) {
	commit := &v2Commit{
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

	env := v2Envelope(evt, streaming.JetstreamKindCommit)
	env.Commit = commit
	return json.Marshal(&env)
}

func commitOpString(k segment.Kind) string {
	switch k {
	case segment.KindCreate, segment.KindCreateResync:
		return streaming.JetstreamOpCreate
	case segment.KindUpdate:
		return streaming.JetstreamOpUpdate
	case segment.KindDelete:
		return streaming.JetstreamOpDelete
	default:
		return ""
	}
}

func encodeV2Identity(evt *segment.Event) ([]byte, error) {
	var id comatproto.SyncSubscribeRepos_Identity
	if err := id.UnmarshalCBOR(evt.Payload); err != nil {
		return nil, fmt.Errorf("subscribe: decode identity: %w", err)
	}
	env := v2Envelope(evt, streaming.JetstreamKindIdentity)
	env.Identity = &id
	return json.Marshal(&env)
}

func encodeV2Account(evt *segment.Event) ([]byte, error) {
	var acct comatproto.SyncSubscribeRepos_Account
	if err := acct.UnmarshalCBOR(evt.Payload); err != nil {
		return nil, fmt.Errorf("subscribe: decode account: %w", err)
	}
	env := v2Envelope(evt, streaming.JetstreamKindAccount)
	env.Account = &acct
	return json.Marshal(&env)
}

func encodeV2Sync(evt *segment.Event) ([]byte, error) {
	var sync comatproto.SyncSubscribeRepos_Sync
	if err := sync.UnmarshalCBOR(evt.Payload); err != nil {
		return nil, fmt.Errorf("subscribe: decode sync: %w", err)
	}
	env := v2Envelope(evt, "sync")
	env.Sync = &v2Sync{
		LexiconTypeID: sync.LexiconTypeID,
		Blocks:        sync.Blocks,
		DID:           sync.DID,
		Rev:           sync.Rev,
		Seq:           sync.Seq,
		Time:          sync.Time,
	}
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
