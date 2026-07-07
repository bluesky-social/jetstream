package ingest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSegmentFilename_BaseFormat pins the on-disk filename format
// (docs/README.md §3.4: 10-digit zero-padded base-36 string).
func TestSegmentFilename_BaseFormat(t *testing.T) {
	t.Parallel()
	tests := []struct {
		idx  uint64
		want string
	}{
		{0, "seg_0000000000.jss"},
		{1, "seg_0000000001.jss"},
		{35, "seg_000000000z.jss"},
		{36, "seg_0000000010.jss"},
	}
	for _, tc := range tests {
		require.Equal(t, tc.want, SegmentFilename(tc.idx))
	}
}

// TestParseSegmentIndex round-trips the parser against segmentFilename.
func TestParseSegmentIndex(t *testing.T) {
	t.Parallel()
	for _, idx := range []uint64{0, 1, 35, 36, 1234, 1<<48 - 1} {
		got, ok := ParseSegmentIndex(SegmentFilename(idx))
		require.True(t, ok, "parse %d", idx)
		require.Equal(t, idx, got)
	}
}

// TestParseSegmentIndex_Rejects pins the rejection cases.
func TestParseSegmentIndex_Rejects(t *testing.T) {
	t.Parallel()
	bad := []string{
		"",
		"seg_.jss",
		"seg_00000.jss",       // too short (5 digits)
		"seg_00000000000.jss", // too long (11 digits)
		"seg_0000000000.txt",
		"shard_0000000000.jss",
		"seg_000000000Z.jss",
		"seg_!@#$%^&*().jss",
	}
	for _, s := range bad {
		_, ok := ParseSegmentIndex(s)
		require.False(t, ok, "unexpected accept: %q", s)
	}
}
