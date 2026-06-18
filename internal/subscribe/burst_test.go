package subscribe

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

// TestTail_BurstDoesNotDropSlowButLiveReaders simulates the #sync backfill
// burst: 500k events appended fast, while N readers consume steadily but
// slower than the append rate. With the pull model, readers that fall behind
// the byte-bounded ring transparently read "cold" (here the injected cold
// reader replays from an in-memory log), and NONE are dropped. The old push
// model dropped them once the 16384-slot channel overflowed.
func TestTail_BurstDoesNotDropSlowButLiveReaders(t *testing.T) {
	t.Parallel()

	const total = 50_000
	// In-memory cold log so the test needs no disk fixtures: it records every
	// appended event and replays Seq >= cursor on a cold miss. Seq == index.
	var logMu sync.RWMutex
	var coldReads atomic.Int64
	logged := make([]*segment.Event, 0, total)
	cold := func(_ context.Context, cursor uint64, max int) ([]*Entry, uint64, error) {
		coldReads.Add(1)
		logMu.RLock()
		defer logMu.RUnlock()
		n := uint64(len(logged))
		if cursor >= n {
			return nil, cursor, nil
		}
		end := min(cursor+uint64(max), n)
		out := make([]*Entry, 0, end-cursor)
		for s := cursor; s < end; s++ {
			out = append(out, newEntry(logged[s]))
		}
		return out, end, nil
	}

	tl := newTail(tailConfig{
		hotBytes: 64 << 10, // deliberately small so readers fall cold
		cold:     cold,
		nextSeq:  func() uint64 { logMu.RLock(); defer logMu.RUnlock(); return uint64(len(logged)) },
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	const readers = 8
	var wg sync.WaitGroup
	received := make([]int, readers)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for r := range readers {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			cursor := uint64(0)
			for cursor < total {
				batch, next, err := tl.ReadFrom(ctx, cursor, 256)
				if err != nil {
					return
				}
				received[r] += len(batch)
				cursor = next
			}
		}(r)
	}

	// Producer: append everything, recording into the cold log FIRST so a cold
	// miss can always be served. The byte budget must hold throughout.
	for s := range total {
		ev := &segment.Event{
			Seq: uint64(s), Kind: segment.KindCreate, DID: "did:plc:burst",
			Payload: make([]byte, 64),
		}
		logMu.Lock()
		logged = append(logged, ev)
		logMu.Unlock()
		tl.Append(ev)
		require.LessOrEqual(t, tl.ringBytes(), 64<<10, "ring must stay within budget mid-burst")
	}

	wg.Wait()
	require.Positive(t, coldReads.Load(), "burst must force at least one cold replay")
	for r := range readers {
		require.Equal(t, total, received[r], "reader %d must receive every event, zero drops", r)
	}
}
