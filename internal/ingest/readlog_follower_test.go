package ingest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/segment"
)

// A follower log reports out-of-order input instead of panicking: its input
// is shared storage, not this process.
func TestFollowerLog_RejectsOutOfOrder(t *testing.T) {
	t.Parallel()
	l := NewFollowerLog(5, 0, nil)
	require.Error(t, l.Append(&segment.Event{Seq: 6}))
	require.NoError(t, l.Append(&segment.Event{Seq: 5}))
	require.NoError(t, l.Append(&segment.Event{Seq: 6}))
	require.Error(t, l.AdvanceDurable(8))
	require.NoError(t, l.AdvanceDurable(7))
	require.Error(t, l.AdvanceDurable(6))
	// A zero budget evicts everything durable.
	require.Equal(t, uint64(7), l.Log().FloorSeq())
}
