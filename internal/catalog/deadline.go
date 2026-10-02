package catalog

import (
	"encoding/binary"
	"fmt"
	"time"
)

// CompactionDeadlineKey is the metadata_kv key under which the compaction
// scheduler publishes its Cache-Control deadline (design §12.7). Followers
// read it with the rest of a tick so every pod computes Cache-Control the
// way local mode does from its in-memory schedule.
const CompactionDeadlineKey = "compaction/deadline"

// EncodeCompactionDeadline encodes the scheduler's published timestamp: the
// next pass while idle, or the running pass's start while one runs, exactly
// the single value local mode's CompactionScheduleState holds. The zero time
// means unknown or disabled.
func EncodeCompactionDeadline(at time.Time) []byte {
	var n int64
	if !at.IsZero() {
		n = at.UnixNano()
	}
	return binary.BigEndian.AppendUint64(nil, uint64(n))
}

// DecodeCompactionDeadline decodes a CompactionDeadlineKey value. ok is
// false for an unknown or disabled schedule, which callers must treat as "do
// not advertise a cache lifetime".
func DecodeCompactionDeadline(val []byte) (at time.Time, ok bool, err error) {
	if len(val) != 8 {
		return time.Time{}, false, fmt.Errorf("catalog: %s value is %d bytes, want 8", CompactionDeadlineKey, len(val))
	}
	n := int64(binary.BigEndian.Uint64(val))
	if n <= 0 {
		return time.Time{}, false, nil
	}
	return time.Unix(0, n).UTC(), true, nil
}
