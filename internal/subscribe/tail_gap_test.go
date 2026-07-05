package subscribe

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// These tests pin the hot ring's behavior on a NON-DENSE feed (#244). The
// ring's dense-seq assumption is enforced by the ingest writer's ordered
// post-append hook, but the ring itself must degrade safely — reset, log,
// count — if any future producer violates it, instead of serving wrong-seq
// entries or panicking while holding the tail mutex (which wedges every
// subscriber AND the ingest hot path that calls Append synchronously).

func gapEvent(seq uint64) *segment.Event {
	return &segment.Event{Seq: seq, Kind: segment.KindCreate, DID: "did:plc:gap", Payload: []byte{0xa0}}
}

// TestHotRing_NonDenseAppendResets: a gapped append must not leave the ring
// claiming residency it does not have. The ring resets to the new event.
func TestHotRing_NonDenseAppendResets(t *testing.T) {
	t.Parallel()
	r := newHotRing(1 << 20)
	for seq := uint64(1); seq <= 5; seq++ {
		require.False(t, r.append(ringEntry(seq, 10)), "dense append must not report a reset")
	}
	// Seqs 6-8 went to the durable writer out of band; 9 arrives next.
	require.True(t, r.append(ringEntry(9, 10)), "gapped append must report a reset")

	// Old residency is gone: seqs 1-5 would be served at wrong indices,
	// and the hole 6-8 is not claimed. Only the post-reset event remains.
	for cursor := uint64(1); cursor <= 8; cursor++ {
		_, ok := r.lookup(cursor)
		require.Falsef(t, ok, "cursor %d must miss after reset", cursor)
	}
	got, ok := r.lookup(9)
	require.True(t, ok)
	require.Len(t, got, 1)
	require.Equal(t, uint64(9), got[0].Event.Seq)
	require.Equal(t, uint64(9), r.base())
	require.Equal(t, uint64(10), r.tip())
}

// TestHotRing_RegressingSeqAlsoResets: a seq at or below the resident tip is
// the same invariant violation as a forward gap (a duplicate or regressing
// allocator would corrupt the idx math identically).
func TestHotRing_RegressingSeqAlsoResets(t *testing.T) {
	t.Parallel()
	r := newHotRing(1 << 20)
	for seq := uint64(10); seq <= 14; seq++ {
		r.append(ringEntry(seq, 10))
	}
	require.True(t, r.append(ringEntry(12, 10)), "regressing append must report a reset")
	got, ok := r.lookup(12)
	require.True(t, ok)
	require.Len(t, got, 1)
	require.Equal(t, uint64(12), got[0].Event.Seq)
	_, ok = r.lookup(13)
	require.False(t, ok, "pre-reset residency above the reset point must be gone")
}

// TestTail_GapFeedNeverServesWrongSeq is the wrong-serving regression test
// for #244: with a hole punched in the feed, a reader inside the old window
// must be sent to the cold (disk) path — never handed an entry whose seq
// differs from its cursor.
func TestTail_GapFeedNeverServesWrongSeq(t *testing.T) {
	t.Parallel()
	cold := func(_ context.Context, cursor uint64, _ int) ([]*Entry, uint64, error) {
		return []*Entry{newEntry(gapEvent(cursor))}, cursor + 1, nil
	}
	tl := newTail(tailConfig{
		hotBytes: 1 << 20,
		cold:     cold,
		nextSeq:  func() uint64 { return 11 },
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	for _, seq := range []uint64{1, 2, 3, 4, 5, 9, 10} { // 6-8 bypassed the tail
		tl.Append(gapEvent(seq))
	}

	for cursor := uint64(1); cursor <= 10; cursor++ {
		done := make(chan struct{})
		var batch []*Entry
		var err error
		go func() {
			defer close(done)
			batch, _, err = tl.ReadFrom(context.Background(), cursor, 1)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("cursor=%d: ReadFrom wedged (mutex held through a panic or lost wakeup)", cursor)
		}
		require.NoErrorf(t, err, "cursor=%d", cursor)
		require.Lenf(t, batch, 1, "cursor=%d", cursor)
		require.Equalf(t, cursor, batch[0].Event.Seq,
			"cursor=%d served seq=%d: hot ring handed back the wrong event", cursor, batch[0].Event.Seq)
	}
}

// TestTail_GapFeedDoesNotWedgeAppend guards the crash blast radius: even if
// a reader hits the gap window concurrently, Append (called synchronously on
// the ingest hot path) must never block behind a poisoned tail mutex.
func TestTail_GapFeedDoesNotWedgeAppend(t *testing.T) {
	t.Parallel()
	cold := func(_ context.Context, cursor uint64, _ int) ([]*Entry, uint64, error) {
		return []*Entry{newEntry(gapEvent(cursor))}, cursor + 1, nil
	}
	tl := newTail(tailConfig{
		hotBytes: 1 << 20,
		cold:     cold,
		nextSeq:  func() uint64 { return 11 },
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	for _, seq := range []uint64{1, 2, 3, 4, 5, 9, 10} {
		tl.Append(gapEvent(seq))
	}

	// Drive readers across the hole; recover any panic so this goroutine
	// cannot fail the test on its own — the assertion is that Append below
	// stays live.
	readersDone := make(chan struct{})
	go func() {
		defer close(readersDone)
		for cursor := uint64(1); cursor <= 10; cursor++ {
			func() {
				defer func() { _ = recover() }()
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				_, _, _ = tl.ReadFrom(ctx, cursor, 1)
			}()
		}
	}()
	<-readersDone

	appended := make(chan struct{})
	go func() {
		defer close(appended)
		tl.Append(gapEvent(11))
	}()
	select {
	case <-appended:
	case <-time.After(2 * time.Second):
		t.Fatal("tail.Append wedged after readers crossed a gapped ring window (#244 mutex poisoning)")
	}
}
