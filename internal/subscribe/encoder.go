package subscribe

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream/api/jetstream"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/gt"
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

// wireTimeLayout is the canonical rendering of an event's witnessed
// timestamp on the network.bsky.jetstream.subscribeEvents wire: RFC 3339 UTC
// with exactly six fractional digits, so the unix-microseconds value a
// timestamp cursor compares against round-trips byte-stably (goldens,
// the shared zstd dictionary, and client-side µs recovery all rely on
// one rendering per instant).
const wireTimeLayout = "2006-01-02T15:04:05.000000Z"

// wireTime renders a unix-microseconds timestamp in wireTimeLayout.
func wireTime(us int64) string {
	return time.UnixMicro(us).UTC().Format(wireTimeLayout)
}

// messageFramePrefix opens an xrpc.v1.json message frame; the payload's
// JSON follows, then messageFrameSuffix.
const (
	messageFramePrefix = `{"$type":"message","payload":`
	messageFrameSuffix = `}`
)

// EncodeV2 renders evt as one xrpc.v1.json message frame for the
// network.bsky.jetstream.subscribeEvents wire (atproto proposal 0015):
//
//	{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#<kind>", ...}}
//
// The payload is built through the lexgen-generated message union so the
// wire can never drift from the published lexicon. Unlike the v1 wire it
// emits archived #sync events.
func EncodeV2(evt *segment.Event) ([]byte, error) {
	var msg jetstream.JetstreamSubscribeEvents_Message
	switch evt.Kind {
	case segment.KindCreate, segment.KindUpdate, segment.KindDelete, segment.KindCreateResync:
		commit, err := v2Commit(evt)
		if err != nil {
			return nil, err
		}
		msg.JetstreamSubscribeEvents_Commit = gt.SomeRef(commit)
	case segment.KindIdentity:
		var id comatproto.SyncSubscribeRepos_Identity
		if err := id.UnmarshalCBOR(evt.Payload); err != nil {
			return nil, fmt.Errorf("subscribe: decode identity: %w", err)
		}
		msg.JetstreamSubscribeEvents_Identity = gt.SomeRef(jetstream.JetstreamSubscribeEvents_Identity{
			Seq:      int64(evt.Seq),
			DID:      evt.DID,
			Time:     wireTime(evt.DisplayTimeUS()),
			Identity: id,
		})
	case segment.KindAccount:
		var acct comatproto.SyncSubscribeRepos_Account
		if err := acct.UnmarshalCBOR(evt.Payload); err != nil {
			return nil, fmt.Errorf("subscribe: decode account: %w", err)
		}
		msg.JetstreamSubscribeEvents_Account = gt.SomeRef(jetstream.JetstreamSubscribeEvents_Account{
			Seq:     int64(evt.Seq),
			DID:     evt.DID,
			Time:    wireTime(evt.DisplayTimeUS()),
			Account: acct,
		})
	case segment.KindSync:
		var sync comatproto.SyncSubscribeRepos_Sync
		if err := sync.UnmarshalCBOR(evt.Payload); err != nil {
			return nil, fmt.Errorf("subscribe: decode sync: %w", err)
		}
		msg.JetstreamSubscribeEvents_Sync = gt.SomeRef(jetstream.JetstreamSubscribeEvents_Sync{
			Seq:  int64(evt.Seq),
			DID:  evt.DID,
			Time: wireTime(evt.DisplayTimeUS()),
			Sync: sync,
		})
	default:
		return nil, fmt.Errorf("subscribe: unknown event kind %d", evt.Kind)
	}

	buf := make([]byte, 0, 256+len(evt.Payload))
	buf = append(buf, messageFramePrefix...)
	buf, err := msg.AppendJSON(buf)
	if err != nil {
		return nil, fmt.Errorf("subscribe: encode v2 frame: %w", err)
	}
	return append(buf, messageFrameSuffix...), nil
}

// v2Commit builds the generated #commit payload from a commit-kind event.
func v2Commit(evt *segment.Event) (jetstream.JetstreamSubscribeEvents_Commit, error) {
	commit := jetstream.JetstreamSubscribeEvents_Commit{
		Seq:        int64(evt.Seq),
		DID:        evt.DID,
		Time:       wireTime(evt.DisplayTimeUS()),
		Rev:        evt.Rev,
		Operation:  commitOpString(evt.Kind),
		Collection: evt.Collection,
		Rkey:       evt.Rkey,
	}

	if evt.Kind != segment.KindDelete {
		recordVal, err := cbor.NewDecoder(bytes.NewReader(evt.Payload)).ReadValue()
		if err != nil {
			return commit, fmt.Errorf("subscribe: decode record cbor: %w", err)
		}
		recordJSON, err := cbor.ToJSON(recordVal)
		if err != nil {
			return commit, fmt.Errorf("subscribe: marshal record json: %w", err)
		}
		commit.Record = recordJSON
		commit.CID = gt.Some(cbor.ComputeCID(cbor.CodecDagCBOR, evt.Payload).String())
		commit.RecordCbor = evt.Payload
	}
	return commit, nil
}

// EncodeV2Error renders an xrpc.v1.json error frame:
// {"$type":"error","error":code,"message":message}. Per the event-stream
// spec the connection closes immediately after one is sent. message may
// be empty.
func EncodeV2Error(code, message string) []byte {
	buf := append([]byte(nil), `{"$type":"error","error":`...)
	buf = cbor.AppendJSONString(buf, code)
	if message != "" {
		buf = append(buf, `,"message":`...)
		buf = cbor.AppendJSONString(buf, message)
	}
	return append(buf, '}')
}

// EncodeV2Info renders an #info advisory as an xrpc.v1.json message
// frame. Info frames carry no seq and do not advance the cursor.
func EncodeV2Info(name, message string) ([]byte, error) {
	info := jetstream.JetstreamSubscribeEvents_Info{Name: name}
	if message != "" {
		info.Message = gt.Some(message)
	}
	msg := jetstream.JetstreamSubscribeEvents_Message{
		JetstreamSubscribeEvents_Info: gt.SomeRef(info),
	}
	buf := append([]byte(nil), messageFramePrefix...)
	buf, err := msg.AppendJSON(buf)
	if err != nil {
		return nil, fmt.Errorf("subscribe: encode info frame: %w", err)
	}
	return append(buf, messageFrameSuffix...), nil
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
		TimeUS: evt.DisplayTimeUS(),
		Cursor: evt.Seq,
		Kind:   streaming.JetstreamKindCommit,
		Commit: commit,
	}
	return json.Marshal(env)
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

func encodeIdentity(evt *segment.Event) ([]byte, error) {
	var id comatproto.SyncSubscribeRepos_Identity
	if err := id.UnmarshalCBOR(evt.Payload); err != nil {
		return nil, fmt.Errorf("subscribe: decode identity: %w", err)
	}
	env := &streaming.JetstreamEvent{
		DID:      evt.DID,
		TimeUS:   evt.DisplayTimeUS(),
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
		TimeUS:  evt.DisplayTimeUS(),
		Cursor:  evt.Seq,
		Kind:    streaming.JetstreamKindAccount,
		Account: &acct,
	}
	return json.Marshal(env)
}
