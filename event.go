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
	DID string `json:"did"`

	// Seq is Jetstream's monotonic per-event sequence number (the cursor).
	// Persist the last seen Seq (via Batch.LastCursor) to resume later.
	Seq uint64 `json:"cursor"`

	// TimeUS is the event's display timestamp, microseconds since the Unix
	// epoch: the operator-imported indexed_at value if one was set, otherwise
	// the witnessed_at time Jetstream first saw the event. It is not the
	// record's client-supplied createdAt. Absent any timestamp import, this
	// is simply the server's ingest (witnessed) time.
	TimeUS int64 `json:"time_us"`

	// Kind selects which of the payload pointers below is populated.
	Kind Kind `json:"kind"`

	Commit   *Commit   `json:"commit,omitempty"`
	Identity *Identity `json:"identity,omitempty"`
	Account  *Account  `json:"account,omitempty"`
	Sync     *Sync     `json:"sync,omitempty"`
}

// Commit describes a single record mutation. The JSON tags mirror the
// Jetstream wire shape so json.Marshal(Event) yields the familiar payload.
type Commit struct {
	// Operation is create, update, or delete.
	Operation Operation `json:"operation"`

	// Collection is the record's NSID, e.g. "app.bsky.feed.post".
	Collection string `json:"collection"`

	// Rkey is the record key within the collection.
	Rkey string `json:"rkey"`

	// Rev is the repo revision that produced this commit.
	Rev string `json:"rev"`

	// CID is the content identifier of the record. Empty for deletes.
	CID string `json:"cid,omitempty"`

	// Record is the decoded record as a generic atproto object. nil for
	// deletes. Callers that want typed records can decode RecordCBOR with
	// their own lexicon codegen.
	Record map[string]any `json:"record,omitempty"`

	// RecordCBOR is the raw, byte-exact DAG-CBOR encoding of the record,
	// suitable for verifying against a PDS or reconstructing the MST. nil for
	// deletes. It is populated on both the backfill and live paths. Marshals
	// to base64 (matching the extended wire's record_cbor).
	RecordCBOR []byte `json:"record_cbor,omitempty"`
}

// Identity is a #identity event: a change to an account's handle or DID
// document.
type Identity struct {
	DID    string `json:"did"`
	Handle string `json:"handle,omitempty"` // empty if not present in the event
	Seq    int64  `json:"seq"`              // upstream relay sequence number carried by the event
	Time   string `json:"time"`             // RFC3339 timestamp from the upstream event
}

// Account is a #account event: a change to an account's hosting status.
type Account struct {
	DID    string `json:"did"`
	Active bool   `json:"active"`
	// Status is the inactive reason (e.g. "deleted", "suspended",
	// "takendown") when Active is false; empty when Active is true.
	Status string `json:"status,omitempty"`
	Seq    int64  `json:"seq"`
	Time   string `json:"time"`
}

// Sync is a #sync event: the upstream signaled a repo divergence requiring a
// resync. The authoritative replacement records follow as their own commit
// events.
type Sync struct {
	DID  string `json:"did"`
	Rev  string `json:"rev"`
	Seq  int64  `json:"seq"`
	Time string `json:"time"`
}
