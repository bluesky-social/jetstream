package segment

// IOOp names a segment writer I/O operation that can be failed by IOFaultInjector.
type IOOp string

const (
	IOOpWrite IOOp = "write"
	IOOpSync  IOOp = "sync"
)

// IOFaultInjector is a test seam for deterministic segment writer I/O
// failures. Production passes nil, so every check is a no-op.
type IOFaultInjector interface {
	BeforeSegmentIO(path string, op IOOp) error
}
