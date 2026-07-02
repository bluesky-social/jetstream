package manifest_test

import (
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

type sealedFixture struct {
	minSeq, maxSeq                 uint64
	minWitnessedAt, maxWitnessedAt int64
	eventCount                     int
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
		ts := f.minWitnessedAt + int64(i)*((f.maxWitnessedAt-f.minWitnessedAt+1)/int64(f.eventCount))
		if i == f.eventCount-1 {
			ts = f.maxWitnessedAt
		}
		_, err := w.Append(segment.Event{
			Seq: seq, WitnessedAt: ts, Kind: segment.KindCreate,
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
}

// mustWriteSealedSegmentWithEvents writes one sealed segment containing
// exactly the given events, flushing after every maxPerBlock events so
// callers control the block boundaries (and thus which per-block bloom
// each DID lands in).
func mustWriteSealedSegmentWithEvents(t *testing.T, path string, maxPerBlock int, events []segment.Event) {
	t.Helper()
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: maxPerBlock})
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	for _, ev := range events {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
}
