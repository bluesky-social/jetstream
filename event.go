package jetstream

// Kind discriminates the firehose event type carried by an Event. The string
// values match the Jetstream JSON wire format.
type Kind string

const (
	// KindCommit is a record create, update, or delete. Commit is non-nil.
	KindCommit Kind = "commit"
	// KindIdentity is a #identity event (handle/DID-doc change). Identity is non-nil.
	KindIdentity Kind = "identity"
	// KindAccount is a #account event (hosting-status change). Account is non-nil.
	KindAccount Kind = "account"
	// KindSync is a #sync event (repo divergence / resync). Sync is non-nil.
	// Sync events are delivered on backfill and on the extended live tail.
	KindSync Kind = "sync"
)

// Operation is the kind of mutation a commit Event carries.
type Operation string

const (
	OpCreate Operation = "create"
	OpUpdate Operation = "update"
	OpDelete Operation = "delete"
)

// Event is a single, decoded firehose event delivered to the caller. It is
// identical in shape regardless of whether it originated from the sealed
// archive (backfill) or the live tail, so callers never need to know which
// region produced it.
//
// Exactly one of Commit, Identity, Account, or Sync is non-nil, selected by
// Kind.
type Event struct {
	// DID is the repository (account) this event belongs to.
	DID string

	// Seq is Jetstream's monotonic per-event sequence number (the cursor).
	// Persist the last seen Seq (via Batch.LastCursor) to resume later.
	Seq uint64

	// TimeUS is Jetstream's own indexed-at timestamp, microseconds since the
	// Unix epoch. This is the server's ingest time, not the record's
	// client-supplied createdAt.
	TimeUS int64

	// Kind selects which of the payload pointers below is populated.
	Kind Kind

	Commit   *Commit
	Identity *Identity
	Account  *Account
	Sync     *Sync
}

// Commit describes a single record mutation.
type Commit struct {
	// Operation is create, update, or delete.
	Operation Operation

	// Collection is the record's NSID, e.g. "app.bsky.feed.post".
	Collection string

	// Rkey is the record key within the collection.
	Rkey string

	// Rev is the repo revision that produced this commit.
	Rev string

	// CID is the content identifier of the record. Empty for deletes.
	CID string

	// Record is the decoded record as a generic atproto object. nil for
	// deletes. Callers that want typed records can decode RecordCBOR with
	// their own lexicon codegen.
	Record map[string]any

	// RecordCBOR is the raw, byte-exact DAG-CBOR encoding of the record,
	// suitable for verifying against a PDS or reconstructing the MST. nil for
	// deletes. It is populated on both the backfill and live paths.
	RecordCBOR []byte
}

// Identity is a #identity event: a change to an account's handle or DID
// document.
type Identity struct {
	DID    string
	Handle string // empty if not present in the event
	Seq    int64  // upstream relay sequence number carried by the event
	Time   string // RFC3339 timestamp from the upstream event
}

// Account is a #account event: a change to an account's hosting status.
type Account struct {
	DID    string
	Active bool
	// Status is the inactive reason (e.g. "deleted", "suspended",
	// "takendown") when Active is false; empty when Active is true.
	Status string
	Seq    int64
	Time   string
}

// Sync is a #sync event: the upstream signaled a repo divergence requiring a
// resync. The authoritative replacement records follow as their own commit
// events.
type Sync struct {
	DID  string
	Rev  string
	Seq  int64
	Time string
}
