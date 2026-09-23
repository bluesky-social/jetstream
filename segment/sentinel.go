package segment

// Reserved collection names index DID-level markers (#account, #identity,
// #sync), whose Collection fields are empty. Without these entries,
// collection-filtered archive plans could omit account deletion or sync
// markers and leave consumers with stale records.
//
// Seal and rewrite add each marker kind's sentinel to its block's collection
// set. The planner includes sentinels unless kinds excludes them;
// kinds=commit selects none. DID blooms still apply. Markers arrive through
// ordinary block downloads in seq order.
//
// The $ prefix is invalid in NSIDs and validated request prefixes, preventing
// collisions with real collections. TestSentinelCollectionsAreInvalidNSIDs
// checks this assumption.
//
// Sentinels are stored in sealed footer string tables. Renaming them requires
// re-sealing affected segments.
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
// it to recognize marker-kind IDs in footer metadata.
func IsDIDMarkerSentinelCollection(name string) bool {
	switch name {
	case SentinelCollectionAccount, SentinelCollectionIdentity, SentinelCollectionSync:
		return true
	default:
		return false
	}
}
