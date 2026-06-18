package client

import (
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/gt"
)

// segmentViewOf builds the minimal segment.Event the Matcher and Suppressor
// read from a decoded engine Event. Filtering reads Seq/Kind/DID/Collection/
// Rkey; account-delete tombstone folding additionally needs the account
// Payload, so for account events we re-marshal the account so the suppressor's
// accountDeleted(active=false,status=deleted) check works on live events too.
func segmentViewOf(ev *Event) segment.Event {
	se := segment.Event{
		Seq: ev.Seq,
		DID: ev.DID,
	}
	switch ev.Kind {
	case KindCommit:
		se.Kind = commitSegmentKind(ev.Commit)
		if ev.Commit != nil {
			se.Collection = ev.Commit.Collection
			se.Rkey = ev.Commit.Rkey
		}
	case KindIdentity:
		se.Kind = segment.KindIdentity
	case KindAccount:
		se.Kind = segment.KindAccount
		se.Payload = accountPayload(ev.Account)
	case KindSync:
		se.Kind = segment.KindSync
	}
	return se
}

// accountPayload re-marshals a decoded Account into the DAG-CBOR the tombstone
// folder decodes to detect account deletions. Returns nil on any error
// (treated as a non-delete account, which is the safe default — it just won't
// produce a DID tombstone). nil/empty Account yields nil.
func accountPayload(a *Account) []byte {
	if a == nil {
		return nil
	}
	acct := comatproto.SyncSubscribeRepos_Account{
		DID:    a.DID,
		Active: a.Active,
		Seq:    a.Seq,
		Time:   a.Time,
	}
	if a.Status != "" {
		acct.Status = gt.Some(a.Status)
	}
	payload, err := acct.MarshalCBOR()
	if err != nil {
		return nil
	}
	return payload
}

func commitSegmentKind(c *Commit) segment.Kind {
	if c == nil {
		return segment.KindCreate
	}
	switch c.Operation {
	case OpCreate:
		return segment.KindCreate
	case OpUpdate:
		return segment.KindUpdate
	case OpDelete:
		return segment.KindDelete
	default:
		return segment.KindCreate
	}
}
