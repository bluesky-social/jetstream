package client

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

// errSkipFrame signals a frame that is valid but carries no caller-visible
// event: a control frame (heartbeat, segment_sealed, ...) or an error frame
// the consumer handles out of band. The consumer advances past it without
// emitting.
var errSkipFrame = errors.New("jetstream: skip live frame")

// liveFrame is the Jetstream /subscribe-v2 JSON wire shape.
// Only the fields the client consumes are modeled; unknown fields are ignored,
// and unknown kinds are skipped so future control frames don't break old
// clients.
type liveFrame struct {
	DID      string        `json:"did"`
	TimeUS   int64         `json:"time_us"`
	Cursor   uint64        `json:"cursor"`
	Seq      uint64        `json:"seq"`
	Kind     string        `json:"kind"`
	Commit   *liveCommit   `json:"commit"`
	Account  *liveAccount  `json:"account"`
	Identity *liveIdentity `json:"identity"`
	Sync     *liveSync     `json:"sync"`
	ErrorTyp string        `json:"error"`
	ErrorMsg string        `json:"message"`
}

type liveCommit struct {
	Rev        string `json:"rev"`
	Operation  string `json:"operation"`
	Collection string `json:"collection"`
	Rkey       string `json:"rkey"`
	CID        string `json:"cid"`
	RecordCBOR string `json:"record_cbor"`
}

type liveAccount struct {
	DID    string  `json:"did"`
	Active bool    `json:"active"`
	Status *string `json:"status"`
	Seq    int64   `json:"seq"`
	Time   string  `json:"time"`
}

type liveIdentity struct {
	DID    string  `json:"did"`
	Handle *string `json:"handle"`
	Seq    int64   `json:"seq"`
	Time   string  `json:"time"`
}

type liveSync struct {
	DID  string `json:"did"`
	Rev  string `json:"rev"`
	Seq  int64  `json:"seq"`
	Time string `json:"time"`
}

// decodeLiveFrame parses one /subscribe-v2 JSON frame into an engine Event. It
// returns errSkipFrame for control frames and unknown kinds, and a wrapped
// error for malformed data frames or server error frames. mode selects raw vs.
// map record materialization (see recordDecodeMode), matching the backfill path
// so a typed consumer sees the same shape across the cutover.
func decodeLiveFrame(data []byte, mode recordDecodeMode) (Event, error) {
	var f liveFrame
	if err := json.Unmarshal(data, &f); err != nil {
		return Event{}, fmt.Errorf("jetstream: decode live frame: %w", err)
	}
	if f.ErrorTyp != "" {
		return Event{}, fmt.Errorf("jetstream: live error frame: %s: %s", f.ErrorTyp, f.ErrorMsg)
	}

	out := Event{DID: f.DID, Seq: f.Seq, TimeUS: f.TimeUS}
	// The v1 wire format omits seq and carries the value only in cursor;
	// v2 always sets both. Fall back to cursor so a server that only
	// populates cursor still yields a usable seq.
	if out.Seq == 0 {
		out.Seq = f.Cursor
	}

	switch Kind(f.Kind) {
	case KindCommit:
		commit, err := liveCommitToEvent(f.Commit, mode)
		if err != nil {
			return Event{}, err
		}
		out.Kind = KindCommit
		out.Commit = commit
	case KindIdentity:
		if f.Identity == nil {
			return Event{}, fmt.Errorf("jetstream: live identity frame missing identity payload (seq=%d)", out.Seq)
		}
		out.Kind = KindIdentity
		out.Identity = &Identity{DID: orDID(f.Identity.DID, f.DID), Handle: deref(f.Identity.Handle), Seq: f.Identity.Seq, Time: f.Identity.Time}
	case KindAccount:
		if f.Account == nil {
			return Event{}, fmt.Errorf("jetstream: live account frame missing account payload (seq=%d)", out.Seq)
		}
		out.Kind = KindAccount
		out.Account = &Account{DID: orDID(f.Account.DID, f.DID), Active: f.Account.Active, Status: deref(f.Account.Status), Seq: f.Account.Seq, Time: f.Account.Time}
	case KindSync:
		if f.Sync == nil {
			return Event{}, fmt.Errorf("jetstream: live sync frame missing sync payload (seq=%d)", out.Seq)
		}
		out.Kind = KindSync
		out.Sync = &Sync{DID: orDID(f.Sync.DID, f.DID), Rev: f.Sync.Rev, Seq: f.Sync.Seq, Time: f.Sync.Time}
	default:
		// Unknown or control kind (heartbeat, segment_sealed, ...): skip so a
		// future server addition doesn't break an old client.
		return Event{}, errSkipFrame
	}
	return out, nil
}

func liveCommitToEvent(c *liveCommit, mode recordDecodeMode) (*Commit, error) {
	if c == nil {
		return nil, fmt.Errorf("jetstream: live commit frame missing commit payload")
	}
	commit := &Commit{
		Operation:  Operation(c.Operation),
		Collection: c.Collection,
		Rkey:       c.Rkey,
		Rev:        c.Rev,
		CID:        c.CID,
	}
	switch commit.Operation {
	case OpCreate, OpUpdate:
		if c.RecordCBOR == "" {
			return nil, fmt.Errorf("jetstream: live %s commit missing record_cbor (collection=%s rkey=%s); is the server a /subscribe-v2 endpoint?", c.Operation, c.Collection, c.Rkey)
		}
		raw, err := base64.StdEncoding.DecodeString(c.RecordCBOR)
		if err != nil {
			return nil, fmt.Errorf("jetstream: decode live record_cbor (collection=%s rkey=%s): %w", c.Collection, c.Rkey, err)
		}
		// raw is a fresh base64-decoded buffer (already owned), so it is safe to
		// retain regardless of mode. In raw mode we skip the map build and leave
		// Record nil; the consumer decodes RecordCBOR into a typed struct. CID is
		// already on the wire here, so raw mode keeps it (no extra work).
		commit.RecordCBOR = raw
		if !mode.raw {
			record, err := decodeRecordMap(raw)
			if err != nil {
				return nil, fmt.Errorf("jetstream: decode live record (collection=%s rkey=%s): %w", c.Collection, c.Rkey, err)
			}
			commit.Record = record
		}
	case OpDelete:
		// No record payload on deletes.
	default:
		return nil, fmt.Errorf("jetstream: unknown live commit operation %q", c.Operation)
	}
	return commit, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// orDID prefers the payload-level DID, falling back to the envelope DID.
func orDID(payloadDID, envelopeDID string) string {
	if payloadDID != "" {
		return payloadDID
	}
	return envelopeDID
}
