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

// Seqs compaction removed before the follower read them are vacant: readers
// skip them, a cursor on a vacancy resumes at the next resident seq, and a
// cursor with only vacancies above it waits at the tip.
func TestFollowerLog_Vacancies(t *testing.T) {
	t.Parallel()
	l := NewFollowerLog(5, 1<<20, nil)
	require.NoError(t, l.Append(&segment.Event{Seq: 5}))
	require.Error(t, l.Skip(4), "skip below tip")
	require.NoError(t, l.Skip(8))
	require.Error(t, l.Append(&segment.Event{Seq: 7}), "append into a skipped seq")
	require.NoError(t, l.Append(&segment.Event{Seq: 8}))
	require.NoError(t, l.Skip(10))
	require.Equal(t, uint64(10), l.Log().TipSeq())

	seqs := func(cursor uint64, max int) []uint64 {
		es, _, ok, _ := l.Log().ReadFrom(cursor, max)
		require.True(t, ok, "cursor %d", cursor)
		var out []uint64
		for _, e := range es {
			out = append(out, e.Event().Seq)
		}
		return out
	}
	require.Equal(t, []uint64{5, 8}, seqs(5, 10))
	require.Equal(t, []uint64{5}, seqs(5, 1))
	require.Equal(t, []uint64{8}, seqs(6, 10), "a cursor on a vacancy")

	_, notify, ok, atTip := l.Log().ReadFrom(9, 10)
	require.False(t, ok)
	require.True(t, atTip, "only vacancies up to the tip")
	require.NoError(t, l.Append(&segment.Event{Seq: 10}))
	select {
	case <-notify:
	default:
		t.Fatal("the next append must wake a reader parked on a vacancy")
	}
	require.Equal(t, []uint64{10}, seqs(9, 10))

	// Durable vacancies at the floor are dropped even under budget; resident
	// entries stay.
	require.NoError(t, l.AdvanceDurable(11))
	require.Equal(t, uint64(5), l.Log().FloorSeq())
	require.Empty(t, l.Log().PendingForDID(""))
}

// A leading vacancy run below durable does not pin the log's floor.
func TestFollowerLog_VacantFloorEvicts(t *testing.T) {
	t.Parallel()
	l := NewFollowerLog(1, 1<<20, nil)
	require.NoError(t, l.Skip(100))
	require.NoError(t, l.AdvanceDurable(100))
	require.Equal(t, uint64(100), l.Log().FloorSeq())
	_, _, ok, atTip := l.Log().ReadFrom(100, 1)
	require.False(t, ok)
	require.True(t, atTip)
}
