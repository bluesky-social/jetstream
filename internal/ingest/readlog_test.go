package ingest

import (
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func TestReadLog_AppendedEventVisibleBeforeDurabilityAndEvictedAfterFlush(t *testing.T) {
	t.Parallel()

	w := newTestWriter(t, Config{
		MaxEventsPerBlock:     10,
		ReadLogRetentionBytes: 0,
	})

	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "1", Payload: []byte{1}}
	require.NoError(t, w.Append(t.Context(), &ev))

	entries, _, ok, atTip := w.ReadLog().ReadFrom(ev.Seq, 10)
	require.True(t, ok)
	require.False(t, atTip)
	require.Len(t, entries, 1)
	require.Equal(t, ev.Seq, entries[0].Event().Seq)
	require.Equal(t, uint64(1), w.ReadLog().FloorSeq(), "zero retention cannot evict not-yet-durable entries")
	require.Equal(t, uint64(1), w.ReadLog().DurableSeq())

	require.NoError(t, w.Flush(t.Context()))
	require.Equal(t, w.NextSeq(), w.ReadLog().DurableSeq())
	require.Equal(t, w.NextSeq(), w.ReadLog().FloorSeq(), "zero retention evicts once durable")
}

// brutePinned recomputes pinned bytes the old O(n) way: every resident
// entry with Seq >= durable. Used to assert the incremental pinnedBytes
// accumulator never drifts.
func brutePinned(l *ReadableLog) int64 {
	var sum int64
	for _, e := range l.entries {
		if e.event.Seq >= l.durable {
			sum += e.bytes
		}
	}
	return sum
}

// TestReadLog_PinnedBytesAccumulatorMatchesScan drives interleaved
// append / advanceDurable / evict cycles and asserts the incremental
// pinnedBytes accumulator equals a full rescan at every step. This is the
// invariant behind replacing publishMetricsLocked's per-append O(n) scan
// (which throttled live ingest into an O(n^2) collapse) with an O(1) read.
func TestReadLog_PinnedBytesAccumulatorMatchesScan(t *testing.T) {
	t.Parallel()

	// Small byte cap so eviction actually fires while entries stay pinned
	// above durable — the exact regime where the old scan blew up.
	l := newReadableLog(1, 512, nil)

	seq := uint64(1)
	appendN := func(n int) {
		for range n {
			ev := &segment.Event{
				Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c",
				Rkey: "r", Rev: "1", Payload: []byte{byte(seq)}, Seq: seq,
			}
			l.append(ev)
			seq++
			require.Equal(t, brutePinned(l), l.pinnedBytes, "pinned drift after append")
		}
	}

	appendN(20)
	require.Equal(t, brutePinned(l), l.pinnedBytes)

	// Advance durability in uneven chunks; each advance moves entries from
	// pinned to durable and lets eviction reclaim below-durable bytes.
	for _, d := range []uint64{5, 5, 12, 21} { // 21 == tipSeq after 20 appends
		l.advanceDurable(d)
		require.Equal(t, brutePinned(l), l.pinnedBytes, "pinned drift after advanceDurable(%d)", d)
	}

	// Interleave more appends after eviction has moved baseSeq forward.
	appendN(15)
	l.advanceDurable(l.tipSeq)
	require.Equal(t, brutePinned(l), l.pinnedBytes, "pinned drift after tail drain")
	require.GreaterOrEqual(t, l.pinnedBytes, int64(0), "pinned must never go negative")
}

func TestReadLog_DeepCopiesPayload(t *testing.T) {
	t.Parallel()

	w := newTestWriter(t, Config{MaxEventsPerBlock: 10})
	payload := []byte{1, 2, 3}
	ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r", Rev: "1", Payload: payload}
	require.NoError(t, w.Append(t.Context(), &ev))
	payload[0] = 9

	entries, _, ok, _ := w.ReadLog().ReadFrom(ev.Seq, 1)
	require.True(t, ok)
	require.Equal(t, []byte{1, 2, 3}, entries[0].Event().Payload)
}

func TestReadLog_AppendErrorAfterSeqAllocationStillVisible(t *testing.T) {
	t.Parallel()

	want := errors.New("observe failed")
	st := newTestStore(t)
	w, err := Open(Config{
		SegmentsDir:           filepath.Join(t.TempDir(), "segments"),
		Store:                 st,
		Logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxEventsPerBlock:     10,
		ReadLogRetentionBytes: 1 << 20,
		OnAppend: func(ev *segment.Event) error {
			if ev.Seq == 2 {
				return want
			}
			return nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	err = w.AppendBatch(t.Context(), []segment.Event{
		{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r1", Rev: "1"},
		{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r2", Rev: "1"},
		{Kind: segment.KindCreate, DID: "did:plc:a", Collection: "c", Rkey: "r3", Rev: "1"},
	})
	require.ErrorIs(t, err, want)

	entries, _, ok, _ := w.ReadLog().ReadFrom(1, 10)
	require.True(t, ok)
	require.Len(t, entries, 2)
	require.Equal(t, uint64(1), entries[0].Event().Seq)
	require.Equal(t, uint64(2), entries[1].Event().Seq)
}
