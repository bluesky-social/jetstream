package oracle

import "github.com/bluesky-social/jetstream/segment"

// RecordKey identifies a single repo record by its DID, collection, and rkey.
type RecordKey struct {
	DID        string
	Collection string
	Rkey       string
}

// RecordValue holds the bytes of a record and, when known, the rev of the
// event that last wrote it.
type RecordValue struct {
	// Rev is the event rev that produced this record when known. Ground-truth
	// snapshots from current repo state leave it empty because the final MST
	// exposes record bytes, not the commit rev that last touched each record.
	// Final-state comparisons should ignore Rev unless both sides populate it.
	Rev     string
	Payload []byte
}

// RepoSnapshot is the set of live records for a single account.
type RepoSnapshot struct {
	Records map[RecordKey]RecordValue
}

// Model is the final-state view of all accounts and their records, used as
// both the reconstructed and ground-truth side of oracle comparisons.
type Model struct {
	Accounts map[string]RepoSnapshot
}

// ObservedEvent is a single durable event read back from segments, flattened
// into the fields the oracle compares and reconstructs from.
type ObservedEvent struct {
	Seq        uint64
	IndexedAt  int64
	Kind       segment.Kind
	DID        string
	Collection string
	Rkey       string
	Rev        string
	Payload    []byte
}
