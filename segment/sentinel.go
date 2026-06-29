package segment

// Reserved sentinel collection names for DID-level marker events.
//
// DID-level markers (#account, #identity, #sync) carry an empty
// Collection on the wire (see Event), so they are never indexed under a
// real NSID. A collection-filtered backfill selects blocks by
// collection, so without help it would never select the blocks that
// hold these markers — and because the live tail starts above the
// sealed tip, a collection-filtered consumer would never learn that an
// account was deleted and would keep its records forever.
//
// To make those blocks selectable without a side channel, the seal and
// rewrite index paths add a reserved sentinel collection name to a
// block's collection set for each DID-level marker kind the block
// contains. The planner unions these sentinels into every
// collection-filtered query (see manifest.collectionIDsForSegment), so
// marker-bearing blocks are always selected; the per-block DID bloom
// still narrows the selection by DID. The markers then ride inline
// through the normal segment/block download and a folding consumer
// converges — the same path record-level deletes already take.
//
// The names begin with '$', which makes them invalid NSIDs: atmos.ParseNSID
// requires at least three dot-separated segments. A client's requested
// collections are always validated as exact NSIDs or NSID-authority
// wildcard prefixes, so a request can neither name nor prefix-match a
// sentinel — they can never collide with real collection traffic. This
// invariant is locked by TestSentinelCollectionsAreInvalidNSIDs; if a
// future NSID grammar ever accepted a '$'-leading label, that test fails
// loudly rather than silently opening a collision.
//
// These strings are written into sealed segment footers' collection
// string tables and are therefore part of the on-disk format: once a
// segment is sealed with a sentinel, the value is load-bearing and must
// not be renamed without re-sealing every affected segment.
const (
	SentinelCollectionAccount  = "$account"
	SentinelCollectionIdentity = "$identity"
	SentinelCollectionSync     = "$sync"
)

// didMarkerSentinel returns the reserved sentinel collection name to
// index for a DID-level marker kind, or "" for kinds that carry (or
// would carry) a real collection. Used by the seal and rewrite index
// paths.
func didMarkerSentinel(k Kind) string {
	switch k {
	case KindAccount:
		return SentinelCollectionAccount
	case KindIdentity:
		return SentinelCollectionIdentity
	case KindSync:
		return SentinelCollectionSync
	default:
		return ""
	}
}

// IsDIDMarkerSentinelCollection reports whether name is one of the
// reserved DID-level marker sentinel collection names. The planner uses
// it to union sentinel collection ids into every collection-filtered
// query so marker-bearing blocks are always selected.
func IsDIDMarkerSentinelCollection(name string) bool {
	switch name {
	case SentinelCollectionAccount, SentinelCollectionIdentity, SentinelCollectionSync:
		return true
	default:
		return false
	}
}
