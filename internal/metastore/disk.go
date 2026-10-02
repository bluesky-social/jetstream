package metastore

// DiskStats is an optional Store capability for implementations that own
// local disk (only the Pebble store). The status page reports it when
// present and omits it otherwise.
type DiskStats interface {
	DiskBytes() (int64, error)
}

// DiskStatsOf returns s's DiskStats, looking through wrappers such as
// WithFaults.
func DiskStatsOf(s Store) (DiskStats, bool) {
	for s != nil {
		if d, ok := s.(DiskStats); ok {
			return d, true
		}
		u, ok := s.(Unwrapper)
		if !ok {
			return nil, false
		}
		s = u.Unwrap()
	}
	return nil, false
}
