package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/bluesky-social/jetstream/api/jetstream"
)

// errSkipFrame signals a frame that is valid but carries no caller-visible
// event: an #info advisory (logged by the caller), or an unknown message
// $type from a newer server. The consumer advances past it without
// emitting.
var errSkipFrame = errors.New("jetstream: skip live frame")

// liveEnvelope is the xrpc.v1.json frame envelope (atproto proposal 0015):
// exactly one self-describing object per text frame, discriminated by
// $type ("message" or "error").
type liveEnvelope struct {
	Type    string          `json:"$type"`
	Payload json.RawMessage `json:"payload"` // message frames
	Error   string          `json:"error"`   // error frames: bare error type name
	Message string          `json:"message"` // error frames: optional description
}

// liveInfo is surfaced to the session loop when the server sends an #info
// advisory (e.g. OutdatedCursor on a clamped timestamp resume). Info
// frames carry no seq and are not events; the consumer logs and continues.
type liveInfo struct {
	Name    string
	Message string
}

// liveStreamError is a terminal xrpc.v1.json error frame ({"$type":
// "error",...}). Per the event-stream spec the server closes immediately
// after sending one; the consumer's reconnect loop handles the close.
type liveStreamError struct {
	Code    string
	Message string
}

// Bounds on untrusted server-supplied diagnostic strings (error codes,
// #info names/messages) before they enter error strings and logs: a
// hostile frame can be as large as the read limit (32 MiB), and one
// advisory must not be able to push that into an operator's log pipeline
// (AGENTS.md: log bounded diagnostic fields).
const (
	maxLiveDiagNameBytes    = 128
	maxLiveDiagMessageBytes = 1024
)

// boundLiveString truncates an untrusted string to limit bytes,
// rune-aligned, marking any cut with an ellipsis.
func boundLiveString(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

func (e *liveStreamError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("jetstream: live stream error: %s", e.Code)
	}
	return fmt.Sprintf("jetstream: live stream error: %s: %s", e.Code, e.Message)
}

// decodeLiveFrame parses one xrpc.v1.json frame into an engine Event.
//
//   - message frames decode through the lexgen-generated union — the same
//     types the server encodes with, so the two sides cannot drift.
//   - #info advisories return (info, errSkipFrame) so the caller can log.
//   - unknown payload $types return errSkipFrame (forward compat: a newer
//     server's new message kind must not break an old client).
//   - error frames return *liveStreamError.
//   - malformed frames return a wrapped decode error.
//
// mode selects raw vs. map record materialization (see recordDecodeMode),
// matching the backfill path so a typed consumer sees the same shape
// across the cutover.
func decodeLiveFrame(data []byte, mode recordDecodeMode) (Event, *liveInfo, error) {
	var env liveEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Event{}, nil, fmt.Errorf("jetstream: decode live frame: %w", err)
	}

	switch env.Type {
	case "message":
		// fall through below
	case "error":
		if env.Error == "" {
			return Event{}, nil, errors.New("jetstream: live error frame missing error code")
		}
		return Event{}, nil, &liveStreamError{
			Code:    boundLiveString(env.Error, maxLiveDiagNameBytes),
			Message: boundLiveString(env.Message, maxLiveDiagMessageBytes),
		}
	case "":
		// No $type at all is not a newer protocol revision — it is a
		// malformed frame (e.g. a pre-lexicon /subscribe-v2 server, or v1
		// JSON). Skipping it would make a wrong endpoint look healthy
		// while delivering nothing; surface it so the consumer sees the
		// decode errors.
		return Event{}, nil, errors.New("jetstream: live frame missing envelope $type; is the server a network.bsky.jetstream.subscribe endpoint?")
	default:
		// A well-formed frame with an unknown envelope $type is a newer
		// protocol revision; skip rather than break.
		return Event{}, nil, errSkipFrame
	}

	if env.Payload == nil {
		return Event{}, nil, errors.New("jetstream: live message frame missing payload")
	}

	var msg jetstream.JetstreamSubscribe_Message
	if err := msg.UnmarshalJSON(env.Payload); err != nil {
		return Event{}, nil, fmt.Errorf("jetstream: decode live payload: %w", err)
	}

	switch {
	case msg.JetstreamSubscribe_Commit.HasVal():
		return liveCommitToEvent(msg.JetstreamSubscribe_Commit.Val(), mode)
	case msg.JetstreamSubscribe_Identity.HasVal():
		v := msg.JetstreamSubscribe_Identity.Val()
		// The generated decoder does not enforce lexicon `required`; a
		// frame missing the wrapped upstream event would otherwise emit a
		// zero-valued Identity (with orDID papering over the missing DID)
		// and advance the dedup cursor. The check is presence-of-payload
		// (did is set by every producer), NOT full required-field
		// validation: jetstream archives synthetic envelopes that
		// legitimately omit other upstream-required fields (e.g. an atmos
		// async-resync #sync carries no time/seq), and scalar requireds
		// like account's `active` bool are indistinguishable from their
		// zero value anyway — enforcing those belongs in lexgen.
		// The outer DID is checked too: it is lexicon-required and is the
		// Event.DID every filter and fold keys on. (No equality check
		// against the payload DID — orDID's payload-preference is the
		// documented contract, and the two always match on a conforming
		// server.)
		if v.DID == "" || v.Identity.DID == "" {
			return Event{}, nil, errors.New("jetstream: live identity frame missing required DID or identity payload")
		}
		seq, timeUS, err := liveEnvelopeFields(v.Seq, v.Time)
		if err != nil {
			return Event{}, nil, err
		}
		return Event{
			DID: v.DID, Seq: seq, TimeUS: timeUS, Kind: KindIdentity,
			Identity: &Identity{
				DID:    orDID(v.Identity.DID, v.DID),
				Handle: v.Identity.Handle.ValOr(""),
				Seq:    v.Identity.Seq,
				Time:   v.Identity.Time,
			},
		}, nil, nil
	case msg.JetstreamSubscribe_Account.HasVal():
		v := msg.JetstreamSubscribe_Account.Val()
		// See the identity branch: outer-DID + payload-presence check.
		if v.DID == "" || v.Account.DID == "" {
			return Event{}, nil, errors.New("jetstream: live account frame missing required DID or account payload")
		}
		seq, timeUS, err := liveEnvelopeFields(v.Seq, v.Time)
		if err != nil {
			return Event{}, nil, err
		}
		return Event{
			DID: v.DID, Seq: seq, TimeUS: timeUS, Kind: KindAccount,
			Account: &Account{
				DID:    orDID(v.Account.DID, v.DID),
				Active: v.Account.Active,
				Status: v.Account.Status.ValOr(""),
				Seq:    v.Account.Seq,
				Time:   v.Account.Time,
			},
		}, nil, nil
	case msg.JetstreamSubscribe_Sync.HasVal():
		v := msg.JetstreamSubscribe_Sync.Val()
		// See the identity branch: outer-DID + payload-presence check.
		// Archived #sync payloads from an atmos async resync legitimately
		// carry an empty time/seq, so did is the only reliable presence
		// marker.
		if v.DID == "" || v.Sync.DID == "" {
			return Event{}, nil, errors.New("jetstream: live sync frame missing required DID or sync payload")
		}
		seq, timeUS, err := liveEnvelopeFields(v.Seq, v.Time)
		if err != nil {
			return Event{}, nil, err
		}
		return Event{
			DID: v.DID, Seq: seq, TimeUS: timeUS, Kind: KindSync,
			Sync: &Sync{
				DID:  orDID(v.Sync.DID, v.DID),
				Rev:  v.Sync.Rev,
				Seq:  v.Sync.Seq,
				Time: v.Sync.Time,
			},
		}, nil, nil
	case msg.JetstreamSubscribe_Info.HasVal():
		v := msg.JetstreamSubscribe_Info.Val()
		// Bounded at decode so every consumer of liveInfo (currently the
		// session-loop logger) inherits the bound.
		return Event{}, &liveInfo{
			Name:    boundLiveString(v.Name, maxLiveDiagNameBytes),
			Message: boundLiveString(v.Message.ValOr(""), maxLiveDiagMessageBytes),
		}, errSkipFrame
	default:
		// A payload with NO $type is malformed, not a future addition: the
		// generated union parks it in Unknown with an empty Type (the
		// PeekJSONType not-found sentinel), and skipping it would be silent
		// event loss. A NONEMPTY unknown $type is a newer server's message
		// kind and skips for forward compat.
		if msg.Unknown.HasVal() && msg.Unknown.Val().Type == "" {
			return Event{}, nil, errors.New("jetstream: live message payload missing $type")
		}
		return Event{}, nil, errSkipFrame
	}
}

// liveEnvelopeFields validates the jetstream envelope fields shared by
// every message kind: seq (int64 on the wire, uint64 internally) and the
// canonical datetime, parsed back to unix-µs (the engine's clock domain
// and the timestamp-cursor unit).
func liveEnvelopeFields(seq int64, timeStr string) (uint64, int64, error) {
	// Seqs are 1-based on the wire (seq 0 is the server's never-allocated
	// sentinel), so 0 here means the required field was absent — the
	// generated decoder does not enforce lexicon `required`. Accepting it
	// would hand session() an event the `ev.Seq <= lastSeq` dedup silently
	// swallows; error instead so the malformed frame is surfaced.
	if seq <= 0 {
		return 0, 0, fmt.Errorf("jetstream: live frame with invalid seq %d", seq)
	}
	ts, err := time.Parse(time.RFC3339Nano, timeStr)
	if err != nil {
		return 0, 0, fmt.Errorf("jetstream: live frame time %q: %w", timeStr, err)
	}
	return uint64(seq), ts.UnixMicro(), nil
}

func liveCommitToEvent(c *jetstream.JetstreamSubscribe_Commit, mode recordDecodeMode) (Event, *liveInfo, error) {
	seq, timeUS, err := liveEnvelopeFields(c.Seq, c.Time)
	if err != nil {
		return Event{}, nil, err
	}
	// All four identifiers are lexicon-required, and a folding consumer
	// cannot key a mutation without them; the generated decoder doesn't
	// enforce `required`, so a frame omitting one must error rather than
	// emit an unfoldable event that advances the dedup cursor.
	if c.DID == "" || c.Rev == "" || c.Collection == "" || c.Rkey == "" {
		return Event{}, nil, errors.New("jetstream: live commit frame missing required did, rev, collection, or rkey")
	}
	commit := &Commit{
		Operation:  Operation(c.Operation),
		Collection: c.Collection,
		Rkey:       c.Rkey,
		Rev:        c.Rev,
		CID:        c.CID.ValOr(""),
	}
	switch commit.Operation {
	case OpCreate, OpUpdate:
		if len(c.RecordCbor) == 0 {
			return Event{}, nil, fmt.Errorf("jetstream: live %s commit missing recordCbor (collection=%s rkey=%s); is the server a network.bsky.jetstream.subscribe endpoint?", c.Operation, c.Collection, c.Rkey)
		}
		// The generated decode already unwrapped {"$bytes": ...} into a
		// fresh owned buffer, so it is safe to retain regardless of mode.
		// In raw mode we skip the map build and leave Record nil; the
		// consumer decodes RecordCBOR into a typed struct.
		commit.RecordCBOR = c.RecordCbor
		if !mode.raw {
			record, err := decodeRecordMap(c.RecordCbor)
			if err != nil {
				return Event{}, nil, fmt.Errorf("jetstream: decode live record (collection=%s rkey=%s): %w", c.Collection, c.Rkey, err)
			}
			commit.Record = record
		}
	case OpDelete:
		// No record payload on deletes.
	default:
		return Event{}, nil, fmt.Errorf("jetstream: unknown live commit operation %q", c.Operation)
	}
	return Event{DID: c.DID, Seq: seq, TimeUS: timeUS, Kind: KindCommit, Commit: commit}, nil, nil
}

// orDID prefers the payload-level DID, falling back to the envelope DID.
func orDID(payloadDID, envelopeDID string) string {
	if payloadDID != "" {
		return payloadDID
	}
	return envelopeDID
}
