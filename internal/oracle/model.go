package oracle

import "github.com/bluesky-social/jetstream-v2/segment"

type RecordKey struct {
	DID        string
	Collection string
	Rkey       string
}

type RecordValue struct {
	// Rev is the event rev that produced this record when known. Ground-truth
	// snapshots from current repo state leave it empty because the final MST
	// exposes record bytes, not the commit rev that last touched each record.
	// Final-state comparisons should ignore Rev unless both sides populate it.
	Rev     string
	Payload []byte
}

type RepoSnapshot struct {
	Records map[RecordKey]RecordValue
}

type Model struct {
	Accounts map[string]RepoSnapshot
}

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
