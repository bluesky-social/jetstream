package client

import (
	"github.com/bluesky-social/jetstream/segment"
)

// segmentViewOf builds the minimal segment.Event the Matcher reads from a
// decoded engine Event: the matcher filters on Seq/Kind/DID/Collection/Rkey.
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
	case KindSync:
		se.Kind = segment.KindSync
	}
	return se
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
