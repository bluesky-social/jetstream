package ingest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// sizedEvent is an event whose raw size does not depend on rng, so tests
// can set caps in whole events.
func sizedEvent(rng *rand.Rand) segment.Event {
	payload := make([]byte, 200)
	for i := range payload {
		payload[i] = byte(rng.Uint32())
	}
	return segment.Event{
		WitnessedAt: time.Now().UnixMicro(),
		Kind:        segment.KindCreate,
		DID:         "did:plc:p0",
		Collection:  "app.bsky.feed.post",
		Rkey:        fmt.Sprintf("r%010d", rng.Uint32()),
		Rev:         "3l",
		Payload:     payload,
	}
}

func sizedEvents(rng *rand.Rand, n int) []segment.Event {
	evs := make([]segment.Event, n)
	for i := range evs {
		evs[i] = sizedEvent(rng)
	}
	return evs
}

var sizedRaw = func() int64 {
	ev := sizedEvent(rand.New(rand.NewPCG(0, 0)))
	return rawEventBytes(&ev)
}()

// gatedUploader holds every upload until gate closes, then sleeps delay,
// standing in for a slow or stalled S3.
type gatedUploader struct {
	up    ObjectUploader
	gate  chan struct{}
	delay time.Duration
}

func (g *gatedUploader) Upload(ctx context.Context, s *catalog.Session, objs [][]byte) ([]catalog.ObjectRef, error) {
	if g.gate != nil {
		select {
		case <-g.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if g.delay > 0 {
		time.Sleep(g.delay)
	}
	return g.up.Upload(ctx, s, objs)
}

type rowShape struct {
	first, last uint64
	inline      bool
}

func shapes(rows []catalog.HotBatchRow) []rowShape {
	out := make([]rowShape, len(rows))
	for i, r := range rows {
		out[i] = rowShape{r.FirstSeq, r.LastSeq, r.Inline}
	}
	return out
}

// Rule 1: a live append that arrives while a bulk chunk holds the lock runs
// right after that chunk. A chunk never spans a batch or a block, so live
// events land only on bulk batch boundaries.
func TestHot_LiveYieldsBulk(t *testing.T) {
	t.Parallel()
	// Blocks of 6 and chunks of 4. Two bulk events open a bulk batch, then
	// ten more follow in one AppendBatch, with a live append arriving while
	// the event at trigger is appended.
	for name, tc := range map[string]struct{ trigger, live uint64 }{
		"batch room": {3, 5}, // the chunk only fills the open batch: 3-4
		"block room": {5, 7}, // the chunk only fills the block: 5-6
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				env := newHotEnv(t)
				rng := rand.New(rand.NewPCG(5, tc.trigger))
				live := sizedEvent(rng)
				liveDone := make(chan error, 1)
				var w *Writer
				started := false // under the writer's lock, like OnAppend
				onAppend := func(ev *segment.Event) error {
					if ev.Seq == tc.trigger && !started {
						started = true
						go func() { liveDone <- w.Append(t.Context(), &live) }()
						for w.hot.liveWaiting.Load() == 0 {
							runtime.Gosched()
						}
					}
					return nil
				}
				w = env.open(Config{
					MaxEventsPerBlock: 6,
					OnAppend:          onAppend,
					Hot:               &HotConfig{Uploader: env.up, BulkChunkMaxEvents: 4, BatchMaxAge: time.Hour},
				})
				ctx := WithClass(t.Context(), ClassBulk)
				require.NoError(t, w.AppendBatch(ctx, sizedEvents(rng, 2)))
				require.NoError(t, w.AppendBatch(ctx, sizedEvents(rng, 10)))
				require.NoError(t, <-liveDone)
				require.Equal(t, tc.live, live.Seq)
				require.NoError(t, w.Close())
				rows := env.rows()
				requireTiles(t, rows, 14)
				for _, r := range rows {
					require.Equal(t, r.FirstSeq == tc.live, r.Inline, "row %d-%d", r.FirstSeq, r.LastSeq)
				}
			})
		})
	}
}

// Rules 3 and 4: a live batch the bucket cannot pay for grows into an
// overflow pointer batch, and overflow ends once the bucket refills.
func TestHot_TokenBucketOverflow(t *testing.T) {
	t.Parallel()
	t.Run("size", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			env := newHotEnv(t)
			m := NewMetrics(prometheus.NewRegistry())
			rate := 4*sizedRaw + 100
			w := env.open(Config{MaxEventsPerBlock: 64, Metrics: m, Hot: &HotConfig{
				Uploader:          env.up,
				BatchMaxEvents:    4,
				BatchMaxAge:       time.Hour,
				OverflowMaxEvents: 10,
				OverflowMaxAge:    time.Hour,
				InlineBytesPerSec: rate,
			}})
			rng := rand.New(rand.NewPCG(6, 6))
			// 1-4 pay; 5-8 cannot, so they overflow and freeze at 10 events.
			require.NoError(t, w.AppendBatch(t.Context(), sizedEvents(rng, 14)))
			require.NoError(t, w.Flush(t.Context()))
			// The bucket settled from raw bytes to the frame's size.
			first := env.rows()[0]
			require.NotEqual(t, sizedRaw*4, int64(len(first.Frame)))
			require.Equal(t, float64(rate-int64(len(first.Frame))), testutil.ToFloat64(m.HotInlineTokens))
			time.Sleep(2 * time.Second)
			require.NoError(t, w.AppendBatch(t.Context(), sizedEvents(rng, 4)))
			require.NoError(t, w.Close())
			require.Equal(t, []rowShape{{1, 4, true}, {5, 14, false}, {15, 18, true}}, shapes(env.rows()))
			require.Equal(t, 2.0, testutil.ToFloat64(m.HotBatches.WithLabelValues("live", "inline")))
			require.Equal(t, 1.0, testutil.ToFloat64(m.HotBatches.WithLabelValues("live", "pointer")))
		})
	})
	t.Run("age", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			env := newHotEnv(t)
			w := env.open(Config{MaxEventsPerBlock: 64, Hot: &HotConfig{
				Uploader:          env.up,
				BatchMaxAge:       10 * time.Millisecond,
				OverflowMaxAge:    100 * time.Millisecond,
				InlineBytesPerSec: 1,
			}})
			rng := rand.New(rand.NewPCG(7, 7))
			ev := sizedEvent(rng)
			require.NoError(t, w.Append(t.Context(), &ev))
			time.Sleep(10 * time.Millisecond)
			synctest.Wait()
			require.Equal(t, uint64(1), env.committedNext(), "the batch age cut found the bucket short: overflow")
			time.Sleep(89 * time.Millisecond)
			synctest.Wait()
			require.Equal(t, uint64(1), env.committedNext())
			time.Sleep(time.Millisecond)
			synctest.Wait()
			require.Equal(t, uint64(2), env.committedNext(), "the overflow age cut commits it")

			// A cut that cannot wait (here a class change) with the bucket
			// short makes a pointer batch at once.
			ev = sizedEvent(rng)
			require.NoError(t, w.Append(t.Context(), &ev))
			bulk := sizedEvent(rng)
			require.NoError(t, w.Append(WithClass(t.Context(), ClassBulk), &bulk))
			require.NoError(t, w.Close())
			require.Equal(t, []rowShape{{1, 1, false}, {2, 2, false}, {3, 3, false}}, shapes(env.rows()))
		})
	})
	t.Run("limits below ordinary", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			// A batch that reaches its ordinary cut already at the overflow
			// limits freezes as a pointer batch rather than grow past them.
			env := newHotEnv(t)
			w := env.open(Config{MaxEventsPerBlock: 64, Hot: &HotConfig{
				Uploader:          env.up,
				BatchMaxEvents:    4,
				BatchMaxAge:       time.Hour,
				OverflowMaxEvents: 2,
				OverflowMaxAge:    time.Hour,
				InlineBytesPerSec: 1,
			}})
			require.NoError(t, w.AppendBatch(t.Context(), sizedEvents(rand.New(rand.NewPCG(8, 9)), 8)))
			require.NoError(t, w.Close())
			require.Equal(t, []rowShape{{1, 4, false}, {5, 8, false}}, shapes(env.rows()))
		})
	})
	t.Run("disabled", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			env := newHotEnv(t)
			w := env.open(Config{Hot: &HotConfig{Uploader: env.up, BatchMaxEvents: 4, InlineBytesPerSec: -1}})
			require.NoError(t, w.AppendBatch(t.Context(), sizedEvents(rand.New(rand.NewPCG(8, 8)), 40)))
			require.NoError(t, w.Close())
			for _, r := range env.rows() {
				require.True(t, r.Inline)
			}
		})
	})
}

// Rule 6: bulk chunks wait for permits, which commits release. Live appends
// do not take permits.
func TestHot_BulkPermits(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		env := newHotEnv(t)
		gate := make(chan struct{})
		w := env.open(Config{MaxEventsPerBlock: 64, Hot: &HotConfig{
			Uploader:           &gatedUploader{up: env.up, gate: gate},
			BatchMaxAge:        time.Millisecond,
			BlockMaxAge:        time.Hour,
			BulkChunkMaxEvents: 4,
			BulkPendingBytes:   8 * sizedRaw,
		}})
		rng := rand.New(rand.NewPCG(9, 9))
		bulkDone := make(chan error, 1)
		go func() { bulkDone <- w.AppendBatch(WithClass(t.Context(), ClassBulk), sizedEvents(rng, 20)) }()
		synctest.Wait()
		require.Equal(t, uint64(9), w.NextSeq(), "two uncommitted chunks hold every permit")

		live := sizedEvent(rand.New(rand.NewPCG(9, 10)))
		require.NoError(t, w.Append(t.Context(), &live))
		require.Equal(t, uint64(9), live.Seq)
		synctest.Wait()
		select {
		case err := <-bulkDone:
			t.Fatalf("bulk append returned early: %v", err)
		default:
		}

		released := time.Now()
		close(gate)
		require.NoError(t, <-bulkDone)
		require.Less(t, time.Since(released), 10*time.Millisecond, "commits release permits at once")

		// A chunk that fails before any event lands still gives back its
		// permits: a full pool's worth fails here, then bulk carries on.
		for range 2 {
			bad := sizedEvents(rng, 4)
			bad[0].Kind = 0
			require.Error(t, w.AppendBatch(WithClass(t.Context(), ClassBulk), bad))
		}
		ctx, cancel := context.WithTimeout(WithClass(t.Context(), ClassBulk), time.Second)
		defer cancel()
		require.NoError(t, w.AppendBatch(ctx, sizedEvents(rng, 8)))
		require.NoError(t, w.Close())
		requireTiles(t, env.rows(), 30)
	})
}

// Rule 8: past the total frozen-uncommitted cap every append waits, live
// included, until commits catch up.
func TestHot_PendingCap(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		env := newHotEnv(t)
		m := NewMetrics(prometheus.NewRegistry())
		gate := make(chan struct{})
		w := env.open(Config{MaxEventsPerBlock: 64, Metrics: m, Hot: &HotConfig{
			Uploader:           &gatedUploader{up: env.up, gate: gate},
			BlockMaxAge:        time.Hour,
			BulkChunkMaxEvents: 4,
			PendingBytes:       4*sizedRaw - 1,
		}})
		rng := rand.New(rand.NewPCG(10, 10))
		require.NoError(t, w.AppendBatch(WithClass(t.Context(), ClassBulk), sizedEvents(rng, 4)))
		require.Equal(t, float64(4*sizedRaw), testutil.ToFloat64(m.HotPendingBytes.WithLabelValues("bulk")))

		live := sizedEvent(rng)
		liveDone := make(chan error, 1)
		go func() { liveDone <- w.Append(t.Context(), &live) }()
		bulkDone := make(chan error, 1)
		go func() { bulkDone <- w.AppendBatch(WithClass(t.Context(), ClassBulk), sizedEvents(rng, 2)) }()
		synctest.Wait()
		require.Equal(t, uint64(5), w.NextSeq(), "nothing is admitted over the cap")

		released := time.Now()
		close(gate)
		require.NoError(t, <-liveDone)
		require.NoError(t, <-bulkDone)
		require.Less(t, time.Since(released), time.Second, "commits release the cap at once")
		require.NoError(t, w.Flush(t.Context()))
		require.Zero(t, testutil.ToFloat64(m.HotPendingBytes.WithLabelValues("bulk")))
		require.Zero(t, testutil.ToFloat64(m.HotPendingBytes.WithLabelValues("live")))
		require.NoError(t, w.Close())
		requireTiles(t, env.rows(), 8)
	})
}

// Rule 9: past the unfolded cap every append waits for a fold. A waiting
// append also ends with its context or with Close.
func TestHot_UnfoldedCap(t *testing.T) {
	t.Parallel()
	t.Run("nil sink", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			// With no sink, a closed block counts as folded once it commits.
			env := newHotEnv(t)
			w := env.open(Config{MaxEventsPerBlock: 4, Hot: &HotConfig{MaxUnfoldedEvents: 4}})
			rng := rand.New(rand.NewPCG(11, 10))
			for range 5 {
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				require.NoError(t, w.AppendBatch(ctx, sizedEvents(rng, 4)))
				cancel()
				synctest.Wait() // the block commits
			}
			require.NoError(t, w.Close())
			requireTiles(t, env.rows(), 21)
		})
	})
	t.Run("sink", func(t *testing.T) {
		t.Parallel()
		unfoldedCapWithSink(t)
	})
}

func unfoldedCapWithSink(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		env := newHotEnv(t)
		m := NewMetrics(prometheus.NewRegistry())
		sink := &recSink{}
		w := env.open(Config{MaxEventsPerBlock: 4, Metrics: m, Hot: &HotConfig{Sink: sink, MaxUnfoldedEvents: 4}})
		rng := rand.New(rand.NewPCG(11, 11))
		require.NoError(t, w.AppendBatch(t.Context(), sizedEvents(rng, 8)))
		require.NoError(t, w.Flush(t.Context()))
		require.Equal(t, 8.0, testutil.ToFloat64(m.HotUnfoldedEvents))

		ctx, cancel := context.WithCancel(t.Context())
		canceled := make(chan error, 1)
		go func() {
			ev := sizedEvent(rand.New(rand.NewPCG(11, 12)))
			canceled <- w.Append(ctx, &ev)
		}()
		ev := sizedEvent(rng)
		done := make(chan error, 1)
		go func() { done <- w.Append(t.Context(), &ev) }()
		synctest.Wait()
		require.Equal(t, uint64(9), w.NextSeq(), "over the cap nothing is admitted")
		cancel()
		require.ErrorIs(t, <-canceled, context.Canceled)

		blocks, _ := sink.snapshot()
		require.Len(t, blocks, 2)
		blocks[0].Folded()
		require.NoError(t, <-done)
		require.Equal(t, uint64(9), ev.Seq)
		blocks[0].Folded() // idempotent
		require.Equal(t, 4.0, testutil.ToFloat64(m.HotUnfoldedEvents))

		// Committing seq 9 puts the writer over the cap again; Close
		// releases whoever waits.
		require.NoError(t, w.Flush(t.Context()))
		go func() {
			ev := sizedEvent(rng)
			done <- w.Append(t.Context(), &ev)
		}()
		synctest.Wait()
		require.NoError(t, w.Close())
		require.ErrorIs(t, <-done, ErrClosed)
	})
}

// Live latency stays bounded while a bulk flood runs: bulk permits bound the
// pointer uploads a live batch can queue behind (design §10.5).
func TestHot_LiveLatencyUnderBulkFlood(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		env := newHotEnv(t)
		const (
			upload   = 50 * time.Millisecond
			batchAge = 15 * time.Millisecond
			chunk    = 64
		)
		type commit struct {
			at      time.Time
			durable uint64
		}
		var mu sync.Mutex
		var commits []commit
		var w *Writer
		w = env.open(Config{Hot: &HotConfig{
			Uploader:           &gatedUploader{up: env.up, delay: upload},
			BatchMaxAge:        batchAge,
			BulkChunkMaxEvents: chunk,
			BulkPendingBytes:   4 * chunk * sizedRaw,
			OnCommit: func(uint64) {
				mu.Lock()
				defer mu.Unlock()
				commits = append(commits, commit{time.Now(), w.ReadLog().DurableSeq()})
			},
		}})

		var wg sync.WaitGroup
		bulkRng := rand.New(rand.NewPCG(12, 12))
		start := time.Now()
		var bulkEnd time.Time
		wg.Go(func() {
			ctx := WithClass(t.Context(), ClassBulk)
			for range 100 {
				require.NoError(t, w.AppendBatch(ctx, sizedEvents(bulkRng, chunk)))
			}
			bulkEnd = time.Now()
		})
		liveRng := rand.New(rand.NewPCG(12, 13))
		appendedAt := map[uint64]time.Time{}
		wg.Go(func() {
			for time.Since(start) < 2*time.Second {
				ev := sizedEvent(liveRng)
				at := time.Now()
				require.NoError(t, w.Append(t.Context(), &ev))
				appendedAt[ev.Seq] = at
				time.Sleep(5 * time.Millisecond)
			}
		})
		wg.Wait()
		require.NoError(t, w.Close())
		requireTiles(t, env.rows(), w.NextSeq())

		var worst time.Duration
		for seq, at := range appendedAt {
			i := 0
			for i < len(commits) && commits[i].durable <= seq {
				i++
			}
			require.Less(t, i, len(commits), "live seq %d committed", seq)
			worst = max(worst, commits[i].at.Sub(at))
		}
		// A live batch waits at most its age, then for the pointer batches
		// frozen before it. Permits cap those at four chunks, all uploading
		// at once.
		require.LessOrEqual(t, worst, batchAge+upload+5*time.Millisecond, "worst live latency")
		require.Greater(t, worst, batchAge, "live batches did queue behind pointer uploads")
		require.Greater(t, bulkEnd.Sub(start), 500*time.Millisecond, "the flood overlaps the live traffic")
	})
}

// Rule 7: bulk waits while as many bulk batches are frozen and uncommitted
// as uploads may be in flight, however much of the byte pool is left.
func TestHot_BulkFrozenCap(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		env := newHotEnv(t)
		gate := make(chan struct{})
		w := env.open(Config{MaxEventsPerBlock: 64, Hot: &HotConfig{
			Uploader:           &gatedUploader{up: env.up, gate: gate},
			UploadConcurrency:  2,
			BatchMaxAge:        time.Millisecond,
			BlockMaxAge:        time.Hour,
			BulkChunkMaxEvents: 4,
			BulkPendingBytes:   1 << 30,
		}})
		rng := rand.New(rand.NewPCG(12, 12))
		bulkDone := make(chan error, 1)
		go func() { bulkDone <- w.AppendBatch(WithClass(t.Context(), ClassBulk), sizedEvents(rng, 20)) }()
		synctest.Wait()
		require.Equal(t, uint64(9), w.NextSeq(), "two frozen bulk batches fill the upload slots")

		// Live is not held back by the cap.
		live := sizedEvent(rng)
		require.NoError(t, w.Append(t.Context(), &live))
		require.Equal(t, uint64(9), live.Seq)

		close(gate)
		require.NoError(t, <-bulkDone)
		require.NoError(t, w.Close())
		requireTiles(t, env.rows(), 22)
	})
}
