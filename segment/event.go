package segment

// Kind discriminates which firehose event type a row represents.
// Values are the on-disk wire format from DESIGN.md §3.2.
type Kind uint8

const (
	KindCreate   Kind = 1
	KindUpdate   Kind = 2
	KindDelete   Kind = 3
	KindIdentity Kind = 4
	KindAccount  Kind = 5
	KindSync     Kind = 6
)

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
	Kind       Kind

	DID        string
	Collection string
	Rkey       string
	Rev        string

	Payload []byte // raw drisl (the DAG-CBOR subset used by atproto)
}
