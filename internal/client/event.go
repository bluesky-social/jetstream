package client

// Kind discriminates the firehose event type carried by an Event. Values
// match the Jetstream JSON wire format. The root jetstream package mirrors
// these into its public jetstream.Kind.
type Kind string

const (
	KindCommit   Kind = "commit"
	KindIdentity Kind = "identity"
	KindAccount  Kind = "account"
	KindSync     Kind = "sync"
)

// Operation is the kind of mutation a commit Event carries.
type Operation string

const (
	OpCreate Operation = "create"
	OpUpdate Operation = "update"
	OpDelete Operation = "delete"
)

// Event is the engine's decoded, region-agnostic event. It is identical in
// shape whether decoded from a sealed segment (backfill) or a live frame; the
// root package translates it 1:1 into the public jetstream.Event.
//
// Exactly one of Commit/Identity/Account/Sync is non-nil, selected by Kind.
type Event struct {
	DID    string
	Seq    uint64
	TimeUS int64
	Kind   Kind

	Commit   *Commit
	Identity *Identity
	Account  *Account
	Sync     *Sync
}

// Commit describes a single record mutation.
type Commit struct {
	Operation  Operation
	Collection string
	Rkey       string
	Rev        string
	CID        string
	Record     map[string]any
	RecordCBOR []byte
}

// Identity is a #identity event.
type Identity struct {
	DID    string
	Handle string
	Seq    int64
	Time   string
}

// Account is a #account event.
type Account struct {
	DID    string
	Active bool
	Status string
	Seq    int64
	Time   string
}

// Sync is a #sync event.
type Sync struct {
	DID  string
	Rev  string
	Seq  int64
	Time string
}
