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
