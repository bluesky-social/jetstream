package segment

// IOOp names a segment writer I/O operation that can be failed by IOFaultInjector.
type IOOp string

const (
	IOOpWrite IOOp = "write"
	IOOpSync  IOOp = "sync"
	// IOOpRename covers the tmp-over-original rename that commits a Patch or
	// Rewrite. The active-writer path never renames, so this op fires only on
	// those two paths.
	IOOpRename IOOp = "rename"
)

// IOFaultInjector is a test seam for deterministic segment writer I/O
// failures. Production passes nil, so every check is a no-op.
type IOFaultInjector interface {
	BeforeSegmentIO(path string, op IOOp) error
}

// beforeSegmentIO consults faults ahead of a segment I/O operation. Patch and
// Rewrite hold a bare injector (not a Config), so this free helper is the
// single seam; Config.beforeIO delegates here to keep one code path.
func beforeSegmentIO(faults IOFaultInjector, path string, op IOOp) error {
	if faults == nil {
		return nil
	}
	return faults.BeforeSegmentIO(path, op)
}
