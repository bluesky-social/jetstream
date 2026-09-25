package manifest

import (
	"unsafe"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/gloom"
)

// ResidentBytes estimates the heap the resident segment metadata holds.
// Disaggregated pods keep every footer in memory, so startup adds this to
// the configured budgets (design §17). It counts what grows with the
// archive (block indexes, blooms, collection tables) and ignores slice and
// map overhead, so it undercounts slightly. It blocks until the manifest
// is loaded and returns 0 if the load failed.
func (m *Manifest) ResidentBytes() int64 {
	if err := m.waitReady(); err != nil {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var n int64
	for i := range m.segments {
		n += m.segments[i].residentBytes()
	}
	return n
}

func (s *SegmentMetadata) residentBytes() int64 {
	n := int64(unsafe.Sizeof(*s)) + int64(len(s.Path))
	n += int64(len(s.Blocks)) * int64(unsafe.Sizeof(segment.BlockInfo{}))
	n += bloomBytes(s.SegmentBloom)
	for _, b := range s.BlockBlooms {
		n += bloomBytes(b)
	}
	for _, c := range s.Collections {
		n += int64(unsafe.Sizeof(c)) + int64(len(c))
	}
	n += 4 * int64(len(s.CollectionEventCounts))
	for _, bc := range s.BlockCollections {
		n += int64(unsafe.Sizeof(bc)) + 4*int64(len(bc))
	}
	return n
}

// bloomBytes is a filter's bit array: 64-byte blocks.
func bloomBytes(f *gloom.Filter) int64 {
	if f == nil {
		return 0
	}
	return int64(f.NumBlocks()) * 64
}
