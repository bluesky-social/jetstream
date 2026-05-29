package subscribe

import (
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

// TestBroadcaster_Swarm exercises Subscribe / Publish / unsubscribe
// concurrency under -race. Catches lock-ordering and channel-close
// races. Long test only (skipped under -short).
func TestBroadcaster_Swarm(t *testing.T) {
	t.Parallel()

	iters := 1000
	if testing.Short() {
		iters = 10
	}

	b, err := New(Config{
		Logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		SubscriberBufferSize: 64,
	})
	require.NoError(t, err)

	const numChurners = 8

	stop := make(chan struct{})

	// One goroutine produces events forever.
	var publishedSeq atomic.Uint64
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				seq := publishedSeq.Add(1)
				b.Publish(&segment.Event{Seq: seq, DID: "did:plc:swarm"})
			}
		}
	}()

	// N churners spin: subscribe, read a few events, unsubscribe.
	var wg sync.WaitGroup
	for c := range numChurners {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(c), 1))
			for i := 0; i < iters; i++ {
				ch, done, unsub := b.Subscribe()
				toRead := rng.IntN(8)
				for range toRead {
					select {
					case <-ch:
					case <-done:
					case <-time.After(50 * time.Millisecond):
					}
				}
				unsub()
			}
		})
	}
	wg.Wait()
	close(stop)
}
