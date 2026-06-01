package manifest_test

import (
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

type sealedFixture struct {
	minSeq, maxSeq             uint64
	minIndexedAt, maxIndexedAt int64
	eventCount                 int
}

func mustWriteEmptyActiveSegment(t *testing.T, path string) {
	t.Helper()
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 4096})
	require.NoError(t, err)
	require.NoError(t, w.Close())
}

func mustWriteSealedSegment(t *testing.T, path string, f sealedFixture) {
	t.Helper()
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 4096})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	for i := 0; i < f.eventCount; i++ {
		seq := f.minSeq + uint64(i)*((f.maxSeq-f.minSeq+1)/uint64(f.eventCount))
		if i == f.eventCount-1 {
			seq = f.maxSeq
		}
		ts := f.minIndexedAt + int64(i)*((f.maxIndexedAt-f.minIndexedAt+1)/int64(f.eventCount))
		if i == f.eventCount-1 {
			ts = f.maxIndexedAt
		}
		_, err := w.Append(segment.Event{
			Seq: seq, IndexedAt: ts, Kind: segment.KindCreate,
			DID:        "did:plc:fixture",
			Collection: "app.bsky.feed.post",
			Rkey:       "abc",
			Rev:        "rev",
			Payload:    []byte{0xa0},
		})
		require.NoError(t, err)
	}
	_, err = w.Seal()
	require.NoError(t, err)

	_ = filepath.Base // anchor the import; helpers may grow later
}
