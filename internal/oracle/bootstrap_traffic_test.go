package oracle

import (
	"context"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// TestBootstrapTrafficGeneratorRunGeneratesTargetPacedByAck verifies the
// post-R2 contract: Run (on its own goroutine) generates exactly `target`
// events, paced by the ack — it does not advance past a batch until every
// cursor up to the last generated seq has been observed. A feeder goroutine
// plays the role of the bootstrap-live consumer, acking each generated seq.
func TestBootstrapTrafficGeneratorRunGeneratesTargetPacedByAck(t *testing.T) {
	t.Parallel()

	ack := newSeqAck()
	var generated int
	genCh := make(chan int64, 1)
	gen := newBootstrapTrafficGenerator(4, 10, ack, time.Minute, func(context.Context) (int64, error) {
		generated++
		seq := int64(generated)
		genCh <- seq
		return seq, nil
	})

	// Feeder: ack every generated seq, mirroring OnBootstrapLiveEvent delivery,
	// so Run's WaitContiguousFrom can make progress.
	feederDone := make(chan struct{})
	go func() {
		defer close(feederDone)
		for seq := range genCh {
			ack.Observe(&segment.Event{UpstreamRelayCursor: seq})
		}
	}()

	// AfterRepoComplete is the start signal; Run blocks until it fires.
	require.NoError(t, gen.AfterRepoComplete(t.Context(), "did:plc:test"))

	require.NoError(t, gen.Run(t.Context()))
	close(genCh)
	<-feederDone

	require.Equal(t, 10, generated, "Run must generate exactly the target event count")
	_, snapGenerated := gen.Snapshot()
	require.Equal(t, 10, snapGenerated)
}

// TestBootstrapTrafficGeneratorAfterRepoCompleteIsNonBlocking is the crux of the
// R2 fix: AfterRepoComplete runs under the ingest writer lock, so it must return
// promptly and never block on consumer progress. It only signals/counts.
func TestBootstrapTrafficGeneratorAfterRepoCompleteIsNonBlocking(t *testing.T) {
	t.Parallel()

	ack := newSeqAck()
	gen := newBootstrapTrafficGenerator(4, 10, ack, time.Minute, func(context.Context) (int64, error) {
		return 1, nil
	})

	// No feeder acks anything; if AfterRepoComplete blocked on the ack (the old
	// deadlock-prone behavior) this would hang to the test timeout. It must
	// return immediately and just bump the completed counter.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 5 {
			require.NoError(t, gen.AfterRepoComplete(t.Context(), "did:plc:test"))
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("AfterRepoComplete blocked — it must not wait under the writer lock")
	}

	completed, _ := gen.Snapshot()
	require.Equal(t, 5, completed)
}

func TestBootstrapTrafficGeneratorNoopsWhenTargetIsZero(t *testing.T) {
	t.Parallel()

	var generated int
	gen := newBootstrapTrafficGenerator(4, 0, newSeqAck(), time.Minute, func(context.Context) (int64, error) {
		generated++
		return int64(generated), nil
	})

	require.NoError(t, gen.AfterRepoComplete(t.Context(), "did:plc:test"))
	require.NoError(t, gen.Run(t.Context()))
	require.Zero(t, generated)
}
