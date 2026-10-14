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

// AppendRun leaves the same log as Skip and Append one event at a time, and
// wakes a parked reader once.
func TestFollowerLog_AppendRunMatchesAppend(t *testing.T) {
	t.Parallel()
	run := NewFollowerLog(5, 1<<20, nil)
	one := NewFollowerLog(5, 1<<20, nil)
	_, notify, _, atTip := run.Log().ReadFrom(5, 10)
	require.True(t, atTip)

	evs := []segment.Event{{Seq: 5}, {Seq: 6}, {Seq: 9}, {Seq: 12}}
	require.NoError(t, run.AppendRun(evs, 15))
	for i := range evs {
		require.NoError(t, one.Skip(evs[i].Seq))
		require.NoError(t, one.Append(&evs[i]))
	}
	require.NoError(t, one.Skip(15))
	select {
	case <-notify:
	default:
		t.Fatal("a run must wake a reader parked at the old tip")
	}

	for _, l := range []*FollowerLog{run, one} {
		require.Equal(t, uint64(15), l.Log().TipSeq())
		es, _, ok, _ := l.Log().ReadFrom(5, 10)
		require.True(t, ok)
		var got []uint64
		for _, e := range es {
			got = append(got, e.Event().Seq)
		}
		require.Equal(t, []uint64{5, 6, 9, 12}, got)
	}

	// An empty run only moves the tip and wakes nobody.
	_, notify, _, _ = run.Log().ReadFrom(15, 10)
	require.NoError(t, run.AppendRun(nil, 17))
	require.Equal(t, uint64(17), run.Log().TipSeq())
	select {
	case <-notify:
		t.Fatal("a run of vacancies must not wake readers")
	default:
	}
}

// AppendRun rejects a run that is out of order, starts below the tip, or
// ends before its last event, and leaves the log as it was.
func TestFollowerLog_AppendRunRejectsDisorder(t *testing.T) {
	t.Parallel()
	l := NewFollowerLog(5, 1<<20, nil)
	require.Error(t, l.AppendRun([]segment.Event{{Seq: 4}}, 6), "below the tip")
	require.Error(t, l.AppendRun([]segment.Event{{Seq: 6}, {Seq: 6}}, 8), "repeated seq")
	require.Error(t, l.AppendRun([]segment.Event{{Seq: 7}, {Seq: 6}}, 8), "descending")
	require.Error(t, l.AppendRun([]segment.Event{{Seq: 6}}, 6), "ends before its last event")
	require.Equal(t, uint64(5), l.Log().TipSeq())
}
