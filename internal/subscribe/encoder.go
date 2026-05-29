package subscribe

import (
	"bytes"
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
