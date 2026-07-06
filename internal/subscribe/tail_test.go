package subscribe

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func noCold(context.Context, uint64, int) ([]*Entry, uint64, error) {
	return nil, 0, errColdUnavailable
}

func TestTail_ReadFrom_HotHit(t *testing.T) {
	t.Parallel()
	tl, w := newReadLogTail(t, 1<<20, noCold)
	for seq := uint64(1); seq <= 5; seq++ {
		appendToWriter(t, w, &segment.Event{Kind: segment.KindCreate, DID: "did:plc:h", Payload: []byte{0xa0}})
	}
	batch, next, err := tl.ReadFrom(context.Background(), 3, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(6), next)
	seqs := make([]uint64, len(batch))
	for i, e := range batch {
		seqs[i] = e.Event.Seq
	}
	require.Equal(t, []uint64{3, 4, 5}, seqs)
}

func TestTail_ReadFrom_RespectsMaxBatch(t *testing.T) {
	t.Parallel()
	tl, w := newReadLogTail(t, 1<<20, noCold)
	for range 10 {
		appendToWriter(t, w, &segment.Event{Kind: segment.KindCreate, DID: "did:plc:h", Payload: []byte{0xa0}})
	}
	batch, next, err := tl.ReadFrom(context.Background(), 1, 4)
	require.NoError(t, err)
	require.Len(t, batch, 4)
	require.Equal(t, uint64(5), next, "next cursor resumes after the truncated batch")
}

func TestTail_ReadFrom_BlocksAtTipThenWakes(t *testing.T) {
	t.Parallel()
	tl, w := newReadLogTail(t, 1<<20, noCold)
	appendToWriter(t, w, &segment.Event{Kind: segment.KindCreate, DID: "did:plc:h", Payload: []byte{0xa0}})

	done := make(chan struct{})
	var got uint64
	go func() {
		defer close(done)
		batch, _, err := tl.ReadFrom(context.Background(), 2, 100)
		if err == nil && len(batch) == 1 {
			got = batch[0].Event.Seq
		}
	}()

	waitTailBlocked(t, tl)
	appendToWriter(t, w, &segment.Event{Kind: segment.KindCreate, DID: "did:plc:h", Payload: []byte{0xa0}})

	select {
	case <-done:
		require.Equal(t, uint64(2), got)
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not wake on append")
	}
}

func TestTail_ReadFrom_CtxCancelWhileBlocked(t *testing.T) {
	t.Parallel()
	tl, w := newReadLogTail(t, 1<<20, noCold)
	appendToWriter(t, w, &segment.Event{Kind: segment.KindCreate, DID: "did:plc:h", Payload: []byte{0xa0}})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		waitTailBlocked(t, tl)
		cancel()
	}()
	_, _, err := tl.ReadFrom(ctx, 2, 100)
	require.ErrorIs(t, err, context.Canceled)
}

func TestTail_ReadFrom_ColdDelegatesBelowReadLogFloor(t *testing.T) {
	t.Parallel()
	cold := func(_ context.Context, cursor uint64, _ int) ([]*Entry, uint64, error) {
		return []*Entry{newEntry(&segment.Event{Seq: cursor})}, cursor + 1, nil
	}
	tl, w := newReadLogTail(t, 0, cold)
	appendToWriter(t, w, &segment.Event{Kind: segment.KindCreate, DID: "did:plc:h", Payload: []byte{0xa0}})
	require.NoError(t, w.Flush(t.Context()))
	appendToWriter(t, w, &segment.Event{Kind: segment.KindCreate, DID: "did:plc:h", Payload: []byte{0xa0}})

	batch, next, err := tl.ReadFrom(context.Background(), 1, 100)
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.Equal(t, uint64(1), batch[0].Event.Seq)
	require.Equal(t, uint64(2), next)
}

func TestTail_ReadFrom_NoReadLogCursorReplayGoesCold(t *testing.T) {
	t.Parallel()
	cold := func(_ context.Context, cursor uint64, _ int) ([]*Entry, uint64, error) {
		return []*Entry{newEntry(&segment.Event{Seq: cursor})}, cursor + 1, nil
	}
	tl := newTail(tailConfig{
		cold:    cold,
		nextSeq: func() uint64 { return 10 },
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	batch, next, err := tl.ReadFrom(context.Background(), 5, 100)
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.Equal(t, uint64(5), batch[0].Event.Seq)
	require.Equal(t, uint64(6), next)
}

func TestTail_ReadFrom_ConcurrentEvictionNoRace(t *testing.T) {
	t.Parallel()
	cold := func(_ context.Context, cursor uint64, _ int) ([]*Entry, uint64, error) {
		return []*Entry{newEntry(&segment.Event{Seq: cursor, Kind: segment.KindCreate, DID: "did:plc:c"})}, cursor + 1, nil
	}
	tl, w := newReadLogTail(t, 4096, cold)

	const total = 50_000
	ctx := t.Context()

	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() {
			cursor := uint64(1)
			for cursor < total {
				batch, next, err := tl.ReadFrom(ctx, cursor, 64)
				if err != nil {
					return
				}
				for _, e := range batch {
					_ = e.Event.Seq
				}
				cursor = next
			}
		})
	}

	for range total {
		appendToWriter(t, w, &segment.Event{Kind: segment.KindCreate, DID: "did:plc:c", Payload: make([]byte, 32)})
	}
	wg.Wait()
}

func TestTail_TipUsesWriterNextSeqBeforeReadLogPublished(t *testing.T) {
	t.Parallel()
	tl := newTail(tailConfig{
		nextSeq: func() uint64 { return 12345 },
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.Equal(t, uint64(12345), tl.Tip(), "live subscriber must start at the durable next-seq, not 0")
}

func TestTail_LiveSubscriberAtTipBlocksThenWakes(t *testing.T) {
	t.Parallel()
	tl, w := newReadLogTail(t, 1<<20, noCold)
	start := tl.Tip()
	require.Equal(t, uint64(1), start)

	done := make(chan struct{})
	var got uint64
	go func() {
		defer close(done)
		batch, _, err := tl.ReadFrom(context.Background(), start, 100)
		if err == nil && len(batch) == 1 {
			got = batch[0].Event.Seq
		}
	}()

	waitTailBlocked(t, tl)
	appendToWriter(t, w, &segment.Event{Kind: segment.KindCreate, DID: "did:plc:x", Payload: []byte{0xa0}})
	select {
	case <-done:
		require.Equal(t, uint64(1), got)
	case <-time.After(2 * time.Second):
		t.Fatal("live subscriber did not wake on append at tip")
	}
}

func waitTailBlocked(t *testing.T, tl *Tail) {
	t.Helper()
	select {
	case <-tl.blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("tail reader did not park at live tip")
	}
}

func TestTail_SetReadLogSourceWakesBlockedReader(t *testing.T) {
	t.Parallel()
	tl := newTail(tailConfig{
		nextSeq: func() uint64 { return 1 },
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	done := make(chan error, 1)
	go func() {
		_, _, err := tl.ReadFrom(context.Background(), 1, 1)
		done <- err
	}()
	waitTailBlocked(t, tl)

	_, w := newReadLogTail(t, 1<<20, noCold)
	tl.SetReadLogSource(func() *ingest.ReadableLog { return w.ReadLog() })
	appendToWriter(t, w, &segment.Event{Kind: segment.KindCreate, DID: "did:plc:x", Payload: []byte{0xa0}})

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("blocked reader did not wake when read log was installed")
	}
}
