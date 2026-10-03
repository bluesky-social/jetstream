package catalog

import "time"

// Read is one read statement of a leader transaction, sent with others in
// one round trip by Tx.FenceBump or Tx.Read. The backend writes the result
// into the Read itself. A result is valid only once the call that carried
// it returned nil; a fence that matched nothing never runs it.
type Read interface{ read() }

// MetaRead reads and row-locks one metadata_kv key.
type MetaRead struct {
	Key []byte

	Value []byte
	Found bool
}

// ActiveSegmentRead reads ns's active segment row, row-locked.
type ActiveSegmentRead struct {
	Namespace Namespace

	Row   SegmentRow
	Found bool
}

// LastActiveBlockRead reads the highest-ordinal active block of ns's active
// segment.
type LastActiveBlockRead struct {
	Namespace Namespace

	Row   ActiveBlockRow
	Found bool
}

// AvailableObjectsRead reads and row-locks the available object with each
// hash; the objects_sha256_available index allows at most one. A positive
// MaxUnrefAge also requires unreferenced_at to be NULL or newer than
// now()-MaxUnrefAge (§7.3 step 2).
type AvailableObjectsRead struct {
	SHA256      [][32]byte
	MaxUnrefAge time.Duration

	// Rows maps each hash that matched to its row.
	Rows map[[32]byte]ObjectRow
}

// ObjectsRead reads and row-locks the object rows with the given IDs, in
// any state.
type ObjectsRead struct {
	IDs []uint64

	// Rows maps each ID that has a row to it.
	Rows map[uint64]ObjectRow
}

func (*MetaRead) read()             {}
func (*ActiveSegmentRead) read()    {}
func (*LastActiveBlockRead) read()  {}
func (*AvailableObjectsRead) read() {}
func (*ObjectsRead) read()          {}
