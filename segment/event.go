package segment

// Kind discriminates which firehose event type a row represents.
// Values are the on-disk wire format from DESIGN.md §3.2.
type Kind uint8

const (
	KindCreate       Kind = 1
	KindUpdate       Kind = 2
	KindDelete       Kind = 3
	KindIdentity     Kind = 4
	KindAccount      Kind = 5
	KindSync         Kind = 6
	KindCreateResync Kind = 7
)

// Valid reports whether k is one of the persisted event kinds.
func (k Kind) Valid() bool {
	return k >= KindCreate && k <= KindCreateResync
}

// IsCommit reports whether k is rendered as a commit-shaped Jetstream event.
func (k Kind) IsCommit() bool {
	switch k {
	case KindCreate, KindUpdate, KindDelete, KindCreateResync:
		return true
	default:
		return false
	}
}

// IsMaterialization reports whether k carries record bytes that materialize
// the current value for a repo path.
func (k Kind) IsMaterialization() bool {
	switch k {
	case KindCreate, KindUpdate, KindCreateResync:
		return true
	default:
		return false
	}
}

// IsResyncReplacement reports whether k is a Sync 1.1 resync replacement row.
func (k Kind) IsResyncReplacement() bool {
	return k == KindCreateResync
}

// Event is one row inside a segment block.
//
// Variable-length fields are constrained to fit in their on-disk
// length columns:
//
//	DID:        up to 65535 bytes (uint16 column)
//	Collection: up to 255   bytes (uint8  column)
//	Rkey:       up to 255   bytes (uint8  column)
//	Rev:        up to 255   bytes (uint8  column)
//	Payload:    up to math.MaxUint32 bytes
//
// IndexedAt and RenderedAt are unix microseconds. RenderedAt == 0
// means "no operator-supplied timestamp" (DESIGN.md §3.2).
//
// For non-commit kinds (Identity, Account, Sync), Collection, Rkey,
// Rev, and Payload are typically empty / nil. The encoder accepts
// any combination; emptiness is not enforced as a per-Kind invariant
// at this layer.
type Event struct {
	Seq        uint64
	IndexedAt  int64
	RenderedAt int64
	// UpstreamRelayCursor is the relay subscribeRepos cursor that produced
	// this event. It is carried in memory for extended websocket clients;
	// the current segment block format does not persist it.
	UpstreamRelayCursor int64
	Kind                Kind

	DID        string
	Collection string
	Rkey       string
	Rev        string

	// Payload holds the raw DAG-CBOR record bytes. Treat as
	// read-only: on the decode path Payload aliases the decompressed
	// block buffer, so mutating it would mutate the source for every
	// other event sharing that block. Callers that need to modify
	// must clone via append([]byte(nil), p...) first.
	Payload []byte
}
