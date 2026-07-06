package subscribe

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// TestTail_BurstDoesNotDropSlowButLiveReaders simulates a #sync-like burst:
// many events append quickly while readers consume steadily. Readers that fall
// below the writer read-log floor transparently read cold and still receive the
// whole stream.
func TestTail_BurstDoesNotDropSlowButLiveReaders(t *testing.T) {
	t.Parallel()

	const total = 50_000
	var coldReads atomic.Int64
	cold := func(_ context.Context, cursor uint64, max int) ([]*Entry, uint64, error) {
		coldReads.Add(1)
		out := make([]*Entry, 0, max)
		next := cursor
		for len(out) < max && next <= uint64(total) {
			out = append(out, newEntry(&segment.Event{
				Seq: next, Kind: segment.KindCreate, DID: "did:plc:burst",
				Payload: make([]byte, 64),
			}))
			next++
		}
		return out, next, nil
	}

	tl, w := newReadLogTail(t, 64<<10, cold)

	const readers = 8
	var wg sync.WaitGroup
	received := make([]int, readers)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	appendEvent := func() {
		appendToWriter(t, w, &segment.Event{
			Kind: segment.KindCreate, DID: "did:plc:burst",
			Payload: make([]byte, 64),
		})
		// Keep the durable watermark moving so old entries can fall below the
		// retention floor and exercise cold replay.
		if w.NextSeq()%512 == 0 {
			require.NoError(t, w.Flush(t.Context()))
		}
	}

	const preburst = 2000
	for range preburst {
		appendEvent()
	}
	require.NoError(t, w.Flush(t.Context()))

	for r := range readers {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			cursor := uint64(1)
			for cursor <= total {
				batch, next, err := tl.ReadFrom(ctx, cursor, 256)
				if err != nil {
					return
				}
				received[r] += len(batch)
				cursor = next
			}
		}(r)
	}

	for s := preburst; s < total; s++ {
		appendEvent()
	}
	require.NoError(t, w.Flush(t.Context()))
	wg.Wait()

	require.Positive(t, coldReads.Load(), "burst must force at least one cold replay")
	for r := range readers {
		require.Equal(t, total, received[r], "reader %d must receive every event, zero drops", r)
	}
}
