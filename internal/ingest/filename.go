package ingest

import (
	"strconv"
	"strings"
)

const (
	segmentFilenamePrefix = "seg_"
	segmentFilenameSuffix = ".jss"
	segmentIndexDigits    = 10
)

// SegmentFilename formats idx as the on-disk segment filename
// "seg_<10-digit base36>.jss". Names sort lexicographically in
// creation order so a directory scan reproduces the segment manifest.
func SegmentFilename(idx uint64) string {
	enc := strconv.FormatUint(idx, 36)
	if len(enc) < segmentIndexDigits {
		enc = strings.Repeat("0", segmentIndexDigits-len(enc)) + enc
	}
	return segmentFilenamePrefix + enc + segmentFilenameSuffix
}

// ParseSegmentIndex returns the index encoded in name and whether the
// name has the expected segment shape.
func ParseSegmentIndex(name string) (uint64, bool) {
	if !strings.HasPrefix(name, segmentFilenamePrefix) {
		return 0, false
	}
	if !strings.HasSuffix(name, segmentFilenameSuffix) {
		return 0, false
	}
	mid := name[len(segmentFilenamePrefix) : len(name)-len(segmentFilenameSuffix)]
	if len(mid) != segmentIndexDigits {
		return 0, false
	}
	idx, err := strconv.ParseUint(mid, 36, 64)
	if err != nil {
		return 0, false
	}
	if SegmentFilename(idx) != name {
		return 0, false
	}
	return idx, true
}
