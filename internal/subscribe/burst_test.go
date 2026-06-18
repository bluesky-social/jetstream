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

	// appendEvent records ev into the cold log FIRST (so a cold miss can always
	// be served) then into the hot ring. The byte budget must hold throughout.
	appendEvent := func(s int) {
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

	// Pre-burst: append far more than the ~319-entry ring can hold (64KiB /
	// ~205B per entry) BEFORE launching any reader. This forces eviction so the
	// ring base is well above seq 0, which deterministically makes each reader's
	// first read (at cursor 0) a cold miss. Without this head start the 8 fast
	// readers (256/batch) outrun the single 1-at-a-time producer, reach the tip,
	// and block hot forever — so a cold replay would only ever occur by chance
	// scheduling, making the coldReads assertion flaky.
	const preburst = 2000
	for s := range preburst {
		appendEvent(s)
	}

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

	// Producer continues the burst concurrently with the now-running readers,
	// which consume via cold replay until they catch up into the resident ring.
	for s := preburst; s < total; s++ {
		appendEvent(s)
	}

	wg.Wait()
	require.Positive(t, coldReads.Load(), "burst must force at least one cold replay")
	for r := range readers {
		require.Equal(t, total, received[r], "reader %d must receive every event, zero drops", r)
	}
}
