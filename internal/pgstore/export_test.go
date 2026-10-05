package pgstore

// SetReadLimits shrinks the per-statement ID caps of s's read transactions.
func SetReadLimits(s *Store, objects, generations int) {
	s.readLimits = readLimits{objects: objects, generations: generations}
}
