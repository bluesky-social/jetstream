package oracle

import (
	"fmt"
	"sort"

	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/streaming"
)

// ExpectedEventLogFromFirehose derives the event log Jetstream should produce by
// decoding the simulator world's firehose frames after cursor (up to limit) and
// expanding them into normalized rows, including the per-record rows a sync frame
// implies. Rows the world's adversarial ledger marks as intentional gate drops
// (#204) are excluded — jetstream is contractually required never to archive
// them, and the exclusion is one-directional-safe: a wrongly-archived lie still
// fails the multiset compare as an extra observed row.
func ExpectedEventLogFromFirehose(w *world.World, cursor int64, limit int) ([]EventLogRow, error) {
	frames, err := w.FirehoseRange(cursor, limit)
	if err != nil {
		return nil, err
	}

	filter := newAdversarialFilter(w.AdversarialLedger().Entries())
	var out []EventLogRow
	for _, frame := range frames {
		rows, err := expectedEventLogRowsFromFrame(w, frame)
		if err != nil {
			return nil, err
		}
		out = append(out, filter.FilterExpectedRows(rows)...)
	}
	return out, nil
}

func expectedEventLogRowsFromFrame(w *world.World, frame []byte) ([]EventLogRow, error) {
	evt, err := decodeOracleFirehoseFrame(frame)
	if err != nil {
		return nil, err
	}

	segEvents, err := expectedSegmentEventsFromFirehoseEvent(w, evt)
	if err != nil {
		return nil, err
	}
	for i := range segEvents {
		segEvents[i].Seq = uint64(evt.Seq)
	}
	return NormalizeEventLog(observedEventsFromSegments(segEvents)), nil
}

func expectedSegmentEventsFromFirehoseEvent(w *world.World, evt streaming.Event) ([]segment.Event, error) {
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
					// Partial-CAR commit: the op references a CID whose
					// record block is absent from the frame's CAR diff.
					// Jetstream's contract is to drop exactly that op and
					// archive the siblings (DropReasonMissingBlock), so
					// the expected log mirrors the drop. This cannot mask
					// a simulator bug that omits blocks unintentionally:
					// the record still exists in world ground truth, so
					// the final-state convergence fold fails unless the
					// scenario deliberately reconciles the divergence.
					continue
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
		out := []segment.Event{{Kind: segment.KindSync, DID: evt.Sync.DID, Rev: evt.Sync.Rev, Payload: payload}}
		replacements, err := expectedSyncReplacementRows(w, evt.Sync.DID, evt.Sync.Rev)
		if err != nil {
			return nil, err
		}
		return append(out, replacements...), nil
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

func expectedSyncReplacementRows(w *world.World, did, rev string) ([]segment.Event, error) {
	indices, err := w.AccountIndicesForTest()
	if err != nil {
		return nil, err
	}
	for _, idx := range indices {
		acct, err := w.LoadAccount(idx)
		if err != nil {
			return nil, err
		}
		if string(acct.DID) != did {
			continue
		}
		rp, _, err := w.LoadRepo(idx)
		if err != nil {
			return nil, err
		}
		snap, err := snapshotRepo(did, rp)
		if err != nil {
			return nil, err
		}
		keys := make([]RecordKey, 0, len(snap.Records))
		for key := range snap.Records {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].Collection != keys[j].Collection {
				return keys[i].Collection < keys[j].Collection
			}
			return keys[i].Rkey < keys[j].Rkey
		})
		out := make([]segment.Event, 0, len(keys))
		for _, key := range keys {
			value := snap.Records[key]
			out = append(out, segment.Event{
				Kind:       segment.KindCreateResync,
				DID:        did,
				Collection: key.Collection,
				Rkey:       key.Rkey,
				Rev:        rev,
				Payload:    append([]byte(nil), value.Payload...),
			})
		}
		return out, nil
	}
	return nil, fmt.Errorf("oracle: sync frame did %s not found in simulator world", did)
}

func expectedActionKind(action streaming.Action) (segment.Kind, error) {
	switch action {
	case streaming.ActionCreate:
		return segment.KindCreate, nil
	case streaming.ActionUpdate:
		return segment.KindUpdate, nil
	case streaming.ActionDelete:
		return segment.KindDelete, nil
	case streaming.ActionResync:
		return segment.KindCreateResync, nil
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
