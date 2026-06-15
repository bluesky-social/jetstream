package oracle

import (
	"fmt"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/world"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/streaming"
)

func ExpectedEventLogFromFirehose(w *world.World, cursor int64, limit int) ([]EventLogRow, error) {
	frames, err := w.FirehoseRange(cursor, limit)
	if err != nil {
		return nil, err
	}

	var out []EventLogRow
	for _, frame := range frames {
		rows, err := expectedEventLogRowsFromFrame(frame)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}
	return out, nil
}

func expectedEventLogRowsFromFrame(frame []byte) ([]EventLogRow, error) {
	evt, err := decodeOracleFirehoseFrame(frame)
	if err != nil {
		return nil, err
	}

	segEvents, err := expectedSegmentEventsFromFirehoseEvent(evt)
	if err != nil {
		return nil, err
	}
	for i := range segEvents {
		segEvents[i].Seq = uint64(evt.Seq)
	}
	return NormalizeEventLog(observedEventsFromSegments(segEvents)), nil
}

func expectedSegmentEventsFromFirehoseEvent(evt streaming.Event) ([]segment.Event, error) {
	switch {
	case evt.Commit != nil:
		out := make([]segment.Event, 0, len(evt.Commit.Ops))
		for op, err := range evt.Operations() {
			if err != nil {
				return nil, fmt.Errorf("oracle: decode expected commit ops did=%s seq=%d: %w",
					evt.Commit.Repo, evt.Commit.Seq, err)
			}
			kind, err := expectedActionKind(op.Action)
			if err != nil {
				return nil, err
			}
			row := segment.Event{
				Kind:       kind,
				DID:        evt.Commit.Repo,
				Collection: string(op.Collection),
				Rkey:       string(op.RKey),
				Rev:        evt.Commit.Rev,
			}
			if kind != segment.KindDelete {
				payload := op.BlockData()
				if payload == nil {
					return nil, fmt.Errorf("oracle: expected commit op missing payload did=%s seq=%d %s/%s action=%s",
						evt.Commit.Repo, evt.Commit.Seq, op.Collection, op.RKey, op.Action)
				}
				row.Payload = append([]byte(nil), payload...)
			}
			out = append(out, row)
		}
		return out, nil
	case evt.Sync != nil:
		payload, err := evt.Sync.MarshalCBOR()
		if err != nil {
			return nil, fmt.Errorf("oracle: marshal expected sync seq=%d did=%s: %w", evt.Sync.Seq, evt.Sync.DID, err)
		}
		return []segment.Event{{Kind: segment.KindSync, DID: evt.Sync.DID, Rev: evt.Sync.Rev, Payload: payload}}, nil
	case evt.Identity != nil:
		payload, err := evt.Identity.MarshalCBOR()
		if err != nil {
			return nil, fmt.Errorf("oracle: marshal expected identity seq=%d did=%s: %w", evt.Identity.Seq, evt.Identity.DID, err)
		}
		return []segment.Event{{Kind: segment.KindIdentity, DID: evt.Identity.DID, Payload: payload}}, nil
	case evt.Account != nil:
		payload, err := evt.Account.MarshalCBOR()
		if err != nil {
			return nil, fmt.Errorf("oracle: marshal expected account seq=%d did=%s: %w", evt.Account.Seq, evt.Account.DID, err)
		}
		return []segment.Event{{Kind: segment.KindAccount, DID: evt.Account.DID, Payload: payload}}, nil
	case evt.Info != nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("oracle: unknown expected firehose event")
	}
}

func expectedActionKind(action streaming.Action) (segment.Kind, error) {
	switch action {
	case streaming.ActionCreate:
		return segment.KindCreate, nil
	case streaming.ActionUpdate:
		return segment.KindUpdate, nil
	case streaming.ActionDelete:
		return segment.KindDelete, nil
	default:
		return 0, fmt.Errorf("oracle: unknown expected firehose action %q", action)
	}
}

func observedEventsFromSegments(events []segment.Event) []ObservedEvent {
	out := make([]ObservedEvent, 0, len(events))
	for _, ev := range events {
		out = append(out, observedEventFromSegment(ev))
	}
	return out
}

func decodeOracleFirehoseFrame(frame []byte) (streaming.Event, error) {
	typ, bodyStart, err := decodeOracleFrameHeader(frame)
	if err != nil {
		return streaming.Event{}, err
	}
	body := frame[bodyStart:]

	switch typ {
	case "#commit":
		var v comatproto.SyncSubscribeRepos_Commit
		if err := v.UnmarshalCBOR(body); err != nil {
			return streaming.Event{}, fmt.Errorf("oracle: decode commit frame: %w", err)
		}
		return streaming.Event{Seq: v.Seq, Commit: &v}, nil
	case "#sync":
		var v comatproto.SyncSubscribeRepos_Sync
		if err := v.UnmarshalCBOR(body); err != nil {
			return streaming.Event{}, fmt.Errorf("oracle: decode sync frame: %w", err)
		}
		return streaming.Event{Seq: v.Seq, Sync: &v}, nil
	case "#identity":
		var v comatproto.SyncSubscribeRepos_Identity
		if err := v.UnmarshalCBOR(body); err != nil {
			return streaming.Event{}, fmt.Errorf("oracle: decode identity frame: %w", err)
		}
		return streaming.Event{Seq: v.Seq, Identity: &v}, nil
	case "#account":
		var v comatproto.SyncSubscribeRepos_Account
		if err := v.UnmarshalCBOR(body); err != nil {
			return streaming.Event{}, fmt.Errorf("oracle: decode account frame: %w", err)
		}
		return streaming.Event{Seq: v.Seq, Account: &v}, nil
	case "#info":
		var v comatproto.SyncSubscribeRepos_Info
		if err := v.UnmarshalCBOR(body); err != nil {
			return streaming.Event{}, fmt.Errorf("oracle: decode info frame: %w", err)
		}
		return streaming.Event{Info: &v}, nil
	default:
		return streaming.Event{}, fmt.Errorf("oracle: unknown firehose frame type %q", typ)
	}
}

func decodeOracleFrameHeader(frame []byte) (typ string, bodyStart int, err error) {
	count, pos, err := cbor.ReadMapHeader(frame, 0)
	if err != nil {
		return "", 0, fmt.Errorf("oracle: decode frame header: %w", err)
	}

	var op int64
	for range count {
		key, next, err := cbor.ReadText(frame, pos)
		if err != nil {
			return "", 0, fmt.Errorf("oracle: decode frame header key: %w", err)
		}
		pos = next
		switch key {
		case "op":
			op, pos, err = cbor.ReadInt(frame, pos)
			if err != nil {
				return "", 0, fmt.Errorf("oracle: decode frame op: %w", err)
			}
		case "t":
			typ, pos, err = cbor.ReadText(frame, pos)
			if err != nil {
				return "", 0, fmt.Errorf("oracle: decode frame type: %w", err)
			}
		default:
			pos, err = cbor.SkipValue(frame, pos)
			if err != nil {
				return "", 0, fmt.Errorf("oracle: skip frame header field %q: %w", key, err)
			}
		}
	}
	if op != 1 {
		return "", 0, fmt.Errorf("oracle: unsupported firehose frame op %d", op)
	}
	if typ == "" {
		return "", 0, fmt.Errorf("oracle: firehose frame missing type")
	}
	return typ, pos, nil
}
