package ingest

import (
	"strconv"
	"strings"
)

// segmentFilenamePrefix and segmentFilenameSuffix bracket the
// 10-digit base-36 index in seg_<base36>.jss (DESIGN.md §3.4).
const (
	segmentFilenamePrefix = "seg_"
	segmentFilenameSuffix = ".jss"
	segmentIndexDigits    = 10
)

// segmentFilename formats idx as the on-disk segment filename.
// Names sort lexicographically in creation order so a directory
// scan reproduces the segment manifest.
func segmentFilename(idx uint64) string {
	enc := strconv.FormatUint(idx, 36)
	if len(enc) < segmentIndexDigits {
		enc = strings.Repeat("0", segmentIndexDigits-len(enc)) + enc
	}
	return segmentFilenamePrefix + enc + segmentFilenameSuffix
}

// parseSegmentIndex returns the index encoded in name and whether
// the name has the expected segment shape. Used by Open's directory
// scan to recover the highest active index.
func parseSegmentIndex(name string) (uint64, bool) {
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
	return idx, true
}
