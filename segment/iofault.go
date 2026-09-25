package segment

// IOOp names a segment writer I/O operation that can be failed by IOFaultInjector.
type IOOp string

const (
	IOOpWrite IOOp = "write"
	IOOpSync  IOOp = "sync"
	// IOOpRename covers the tmp-over-original rename that commits a Rewrite.
	// The active-writer path never renames, so this op fires only on that
	// path.
	IOOpRename IOOp = "rename"
)

// IOFaultInjector is a test seam for deterministic segment writer I/O
// failures. Production passes nil, so every check is a no-op.
type IOFaultInjector interface {
	BeforeSegmentIO(path string, op IOOp) error
}

// beforeSegmentIO checks for an injected failure before I/O. Config and
// Rewrite share this helper.
func beforeSegmentIO(faults IOFaultInjector, path string, op IOOp) error {
	if faults == nil {
		return nil
	}
	return faults.BeforeSegmentIO(path, op)
}
