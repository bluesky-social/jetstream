package subscribe

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func newTestTail(t *testing.T, maxBytes int, cold coldReader) *Tail {
	t.Helper()
	return newTail(tailConfig{
		hotBytes: maxBytes,
		cold:     cold,
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

func noCold(context.Context, uint64, int) ([]*Entry, uint64, error) {
	return nil, 0, errColdUnavailable
}

func TestTail_ReadFrom_HotHit(t *testing.T) {
	t.Parallel()
	tl := newTestTail(t, 1<<20, noCold)
	for seq := uint64(1); seq <= 5; seq++ {
		tl.Append(&segment.Event{Seq: seq, Kind: segment.KindCreate, DID: "did:plc:h", Payload: []byte{0xa0}})
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
	tl := newTestTail(t, 1<<20, noCold)
	for seq := uint64(1); seq <= 10; seq++ {
		tl.Append(&segment.Event{Seq: seq, Kind: segment.KindCreate, DID: "did:plc:h", Payload: []byte{0xa0}})
	}
	batch, next, err := tl.ReadFrom(context.Background(), 1, 4)
	require.NoError(t, err)
	require.Len(t, batch, 4)
	require.Equal(t, uint64(5), next, "next cursor resumes after the truncated batch")
}

func TestTail_ReadFrom_BlocksAtTipThenWakes(t *testing.T) {
	t.Parallel()
	tl := newTestTail(t, 1<<20, noCold)
	tl.Append(&segment.Event{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:h", Payload: []byte{0xa0}})

	done := make(chan struct{})
	var got uint64
	go func() {
		defer close(done)
		batch, _, err := tl.ReadFrom(context.Background(), 2, 100) // cursor at tip; blocks
		if err == nil && len(batch) == 1 {
			got = batch[0].Event.Seq
		}
	}()

	time.Sleep(20 * time.Millisecond) // let the reader park at the tip
	tl.Append(&segment.Event{Seq: 2, Kind: segment.KindCreate, DID: "did:plc:h", Payload: []byte{0xa0}})

	select {
	case <-done:
		require.Equal(t, uint64(2), got)
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not wake on append")
	}
}

func TestTail_ReadFrom_CtxCancelWhileBlocked(t *testing.T) {
	t.Parallel()
	tl := newTestTail(t, 1<<20, noCold)
	tl.Append(&segment.Event{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:h", Payload: []byte{0xa0}})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	_, _, err := tl.ReadFrom(ctx, 2, 100)
	require.ErrorIs(t, err, context.Canceled)
}

func TestTail_ReadFrom_ColdDelegates(t *testing.T) {
	t.Parallel()
	cold := func(_ context.Context, cursor uint64, _ int) ([]*Entry, uint64, error) {
		return []*Entry{newEntry(&segment.Event{Seq: cursor})}, cursor + 1, nil
	}
	tl := newTestTail(t, 200, cold) // tiny ring forces eviction → cold path
	for seq := uint64(1); seq <= 20; seq++ {
		tl.Append(&segment.Event{Seq: seq, Kind: segment.KindCreate, DID: "did:plc:h", Payload: []byte{0xa0}})
	}
	batch, next, err := tl.ReadFrom(context.Background(), 1, 100) // seq 1 evicted
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.Equal(t, uint64(1), batch[0].Event.Seq)
	require.Equal(t, uint64(2), next)
}

func TestTail_ReadFrom_EmptyRingCursorReplayGoesCold(t *testing.T) {
	t.Parallel()
	// Ring empty, but authoritative tip says 10 durable events exist.
	cold := func(_ context.Context, cursor uint64, _ int) ([]*Entry, uint64, error) {
		return []*Entry{newEntry(&segment.Event{Seq: cursor})}, cursor + 1, nil
	}
	tl := newTail(tailConfig{
		hotBytes: 1 << 20,
		cold:     cold,
		nextSeq:  func() uint64 { return 10 },
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	// Ring empty so the cold threshold is the durable tip (10); cursor 5 < 10
	// -> cold replay, not block.
	batch, next, err := tl.ReadFrom(context.Background(), 5, 100)
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.Equal(t, uint64(5), batch[0].Event.Seq)
	require.Equal(t, uint64(6), next)
}

// TestTail_ReadFrom_InFlightTipBlocksNotCold is the regression test for the
// in-flight window between the ingest loop's writer.Append (which advances the
// durable NextSeq) and tail.Append (which puts the event in the ring). A
// caught-up live subscriber sitting at that in-flight seq must BLOCK for the
// imminent Append and then be served from the hot ring — it must NOT dive to
// the cold (disk) path, which would re-decode/re-encode per subscriber and
// defeat the encode-once sharing during bursts.
func TestTail_ReadFrom_InFlightTipBlocksNotCold(t *testing.T) {
	t.Parallel()
	// Cold reader that fails the test if ever called: a caught-up live reader
	// must never touch disk.
	coldCalled := make(chan struct{}, 1)
	cold := func(_ context.Context, cursor uint64, _ int) ([]*Entry, uint64, error) {
		select {
		case coldCalled <- struct{}{}:
		default:
		}
		return []*Entry{newEntry(&segment.Event{Seq: cursor})}, cursor + 1, nil
	}
	// Ring already holds seqs 10..14 (tip 15). The durable writer is two events
	// ahead (NextSeq 17): seqs 15 and 16 are in flight, appended to the writer
	// but not yet to the ring. The OLD max(ringTip,nextSeq) logic would treat
	// cursor 15 as cold (15 < 17); the fix blocks instead.
	var next atomic.Uint64
	next.Store(17)
	tl := newTail(tailConfig{
		hotBytes: 1 << 20,
		cold:     cold,
		nextSeq:  func() uint64 { return next.Load() },
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	for seq := uint64(10); seq <= 14; seq++ {
		tl.Append(&segment.Event{Seq: seq, Kind: segment.KindCreate, DID: "did:plc:x", Payload: []byte{0xa0}})
	}

	done := make(chan struct{})
	var got uint64
	go func() {
		defer close(done)
		batch, _, err := tl.ReadFrom(context.Background(), 15, 100) // at the in-flight tip
		if err == nil && len(batch) == 1 {
			got = batch[0].Event.Seq
		}
	}()

	// The reader must be parked (blocked), not have gone cold.
	select {
	case <-coldCalled:
		t.Fatal("caught-up live reader at the in-flight tip went cold; must block for the imminent Append")
	case <-done:
		t.Fatal("reader returned before the in-flight event was appended")
	case <-time.After(50 * time.Millisecond):
		// Good: parked at the tip.
	}

	// The in-flight event lands in the ring. The reader must wake and serve it hot.
	tl.Append(&segment.Event{Seq: 15, Kind: segment.KindCreate, DID: "did:plc:x", Payload: []byte{0xa0}})
	select {
	case <-done:
		require.Equal(t, uint64(15), got, "must serve the in-flight event from the ring")
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not wake on the in-flight Append")
	}
	select {
	case <-coldCalled:
		t.Fatal("reader went cold after waking; must serve hot")
	default:
	}
}

// TestTail_ReadFrom_EvictedCursorStillGoesCold guards the other side of the
// fix: a cursor below the ring's base (genuinely evicted to disk) must still
// take the cold path even when the ring is populated.
func TestTail_ReadFrom_EvictedCursorStillGoesCold(t *testing.T) {
	t.Parallel()
	coldServed := false
	cold := func(_ context.Context, cursor uint64, _ int) ([]*Entry, uint64, error) {
		coldServed = true
		return []*Entry{newEntry(&segment.Event{Seq: cursor})}, cursor + 1, nil
	}
	tl := newTestTail(t, 200, cold) // tiny ring forces eviction
	for seq := range uint64(50) {
		tl.Append(&segment.Event{Seq: seq, Kind: segment.KindCreate, DID: "did:plc:x", Payload: []byte{0xa0}})
	}
	// seq 0 was evicted (below ring base): must serve cold.
	batch, next, err := tl.ReadFrom(context.Background(), 0, 100)
	require.NoError(t, err)
	require.True(t, coldServed, "evicted cursor must take the cold path")
	require.Len(t, batch, 1)
	require.Equal(t, uint64(0), batch[0].Event.Seq)
	require.Equal(t, uint64(1), next)
}

// TestTail_ReadFrom_ConcurrentEvictionNoRace is the regression test for the
// slice-aliasing race: hotRing.lookup returns a slice over the ring's backing
// array, and a concurrent Append→evict nils + reslices that array. Readers
// that held the returned slice past the unlock would race on (and could
// nil-deref) those entries. Run under -race. The reader dereferences
// e.Event.Seq on every returned entry to surface any torn read.
func TestTail_ReadFrom_ConcurrentEvictionNoRace(t *testing.T) {
	t.Parallel()
	// Small ring so Append churns eviction continuously while readers hold
	// slices. Cold serves any miss so readers always make forward progress.
	cold := func(_ context.Context, cursor uint64, max int) ([]*Entry, uint64, error) {
		return []*Entry{newEntry(&segment.Event{Seq: cursor, Kind: segment.KindCreate, DID: "did:plc:c"})}, cursor + 1, nil
	}
	tl := newTestTail(t, 4096, cold)

	const total = 50_000
	ctx := t.Context()

	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() {
			cursor := uint64(0)
			for cursor < total {
				batch, next, err := tl.ReadFrom(ctx, cursor, 64)
				if err != nil {
					return
				}
				// Touch every entry to force a real read of the shared memory.
				for _, e := range batch {
					_ = e.Event.Seq
				}
				cursor = next
			}
		})
	}

	for s := range uint64(total) {
		tl.Append(&segment.Event{Seq: s, Kind: segment.KindCreate, DID: "did:plc:c", Payload: make([]byte, 32)})
	}
	wg.Wait()
}

func TestTail_TipUsesWriterNextSeqWhenRingEmpty(t *testing.T) {
	t.Parallel()
	tl := newTail(tailConfig{
		hotBytes: 1 << 20,
		nextSeq:  func() uint64 { return 12345 },
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.Equal(t, uint64(12345), tl.Tip(), "live subscriber must start at the durable next-seq, not 0")
}

func TestTail_LiveSubscriberAtTipBlocksThenWakes(t *testing.T) {
	t.Parallel()
	// The durable tip from the writer is 100; with an empty ring a live
	// subscriber starts at Tip()=100 and blocks. After an Append of seq 100,
	// it must wake and receive it.
	// next is read by the reader goroutine via nextSeq and mutated by the main
	// goroutine below, so it must be synchronized (atomic) to stay race-free.
	var next atomic.Uint64
	next.Store(100)
	tl := newTail(tailConfig{
		hotBytes: 1 << 20,
		nextSeq:  func() uint64 { return next.Load() },
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	start := tl.Tip()
	require.Equal(t, uint64(100), start)

	done := make(chan struct{})
	var got uint64
	go func() {
		defer close(done)
		batch, _, err := tl.ReadFrom(context.Background(), start, 100)
		if err == nil && len(batch) == 1 {
			got = batch[0].Event.Seq
		}
	}()
	time.Sleep(20 * time.Millisecond)
	// Append seq 100. NOTE: the ring seeds baseSeq from the first appended
	// event's Seq, so the ring becomes [100..]; lookup(100) now serves it.
	next.Store(101)
	tl.Append(&segment.Event{Seq: 100, Kind: segment.KindCreate, DID: "did:plc:x", Payload: []byte{0xa0}})
	select {
	case <-done:
		require.Equal(t, uint64(100), got)
	case <-time.After(2 * time.Second):
		t.Fatal("live subscriber did not wake on append at tip")
	}
}
