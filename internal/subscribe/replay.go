package subscribe

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/manifest"
	"github.com/bluesky-social/jetstream-v2/segment"
)

// WalkInput is the parameter bundle for WalkFromCursor.
type WalkInput struct {
	// StartSeq is the smallest seq the walker will emit. Events with
	// Seq < StartSeq are skipped silently.
	StartSeq uint64

	// Manifest is the in-memory segment manifest. May be nil; callers
	// without sealed segments still walk the active segment + pending.
	Manifest *manifest.Manifest

	// Writer is the ingest writer; the walker reads its active
	// segment's flushed blocks and SnapshotPending() events to extend
	// past the sealed-segment region. Required.
	Writer *ingest.Writer
}

// WalkFromCursor invokes emit for every durable event with
// Seq >= input.StartSeq, in seq order, across:
//
//  1. the sealed-segment region from the manifest,
//  2. the active segment's flushed blocks,
//  3. the active segment's in-memory pending block.
//
// Halts when emit returns a non-nil error and surfaces the error
// (errors.Is is honored).
//
// Pure-function design: WalkFromCursor never touches the broadcaster
// or per-subscriber ring. The replay state machine in a subsequent
// task composes WalkFromCursor with ring management and overflow
// handling.
func WalkFromCursor(ctx context.Context, input WalkInput, emit func(*segment.Event) error) error {
	current := input.StartSeq

	// 1. Sealed segments.
	if input.Manifest != nil {
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			bounds, ok := input.Manifest.SegmentForSeq(current)
			if !ok {
				break
			}
			next, err := walkSealedSegment(input.Manifest, bounds, current, emit)
			if err != nil {
				return err
			}
			current = next
		}
	}

	// 2. Active segment's flushed blocks.
	activeIdx := input.Writer.ActiveIndex()
	activePath := filepath.Join(input.Writer.SegmentsDir(), ingest.SegmentFilename(activeIdx))
	walkErr := segment.WalkActive(activePath, func(events []segment.Event) error {
		for i := range events {
			if events[i].Seq < current {
				continue
			}
			ev := events[i]
			if err := emit(&ev); err != nil {
				return err
			}
			current = events[i].Seq + 1
		}
		return nil
	})
	if walkErr != nil && !errors.Is(walkErr, os.ErrNotExist) {
		return fmt.Errorf("walk active: %w", walkErr)
	}

	// 3. Pending in-memory block.
	pending := input.Writer.SnapshotPending()
	for i := range pending {
		if pending[i].Seq < current {
			continue
		}
		if err := emit(&pending[i]); err != nil {
			return err
		}
		current = pending[i].Seq + 1
	}
	return nil
}

func walkSealedSegment(m *manifest.Manifest, bounds manifest.SegmentBounds, current uint64, emit func(*segment.Event) error) (uint64, error) {
	blocks, err := m.BlockIndex(bounds.Idx)
	if err != nil {
		return current, fmt.Errorf("block index for seg %d: %w", bounds.Idx, err)
	}

	r, err := segment.Open(segment.ReaderConfig{Path: bounds.Path, SkipChecksum: true})
	if err != nil {
		return current, fmt.Errorf("open seg %d: %w", bounds.Idx, err)
	}
	defer func() { _ = r.Close() }()

	for i, block := range blocks {
		if block.MaxSeq < current {
			continue
		}
		events, err := r.DecodeBlock(i)
		if err != nil {
			return current, fmt.Errorf("decode seg %d block %d: %w", bounds.Idx, i, err)
		}
		for j := range events {
			if events[j].Seq < current {
				continue
			}
			ev := events[j]
			if err := emit(&ev); err != nil {
				return current, err
			}
			current = events[j].Seq + 1
		}
	}
	return current, nil
}

// ReplayerInput configures a single per-subscriber replay run.
type ReplayerInput struct {
	Broadcaster *Broadcaster
	Manifest    *manifest.Manifest
	Writer      *ingest.Writer

	// StartSeq is the resolved cursor (post-clamp). The replayer
	// emits events with Seq >= StartSeq.
	StartSeq uint64

	// RingSize is the per-subscriber bounded ring capacity used during
	// lookback. On overflow the ring is dropped and replay restarts
	// from an updated cursor.
	RingSize int

	// MaxIters bounds the overflow-restart loop. Exceeding it returns
	// ErrLookbackTooSlow.
	MaxIters int

	// Metrics is optional. nil means no metric increments.
	Metrics *Metrics
}

// Replayer composes WalkFromCursor with broadcaster lookback
// subscription, ring overflow handling, drain, and switchover to
// live. The handler's writer loop consumes events from the emit
// callback in seq order; switchover is transparent.
type Replayer struct {
	in ReplayerInput
}

// NewReplayer constructs a Replayer for one subscriber. Zero/negative
// RingSize and MaxIters fall back to package defaults.
func NewReplayer(in ReplayerInput) *Replayer {
	if in.RingSize <= 0 {
		in.RingSize = DefaultLookbackRingSize
	}
	if in.MaxIters <= 0 {
		in.MaxIters = DefaultMaxLookbackIterations
	}
	return &Replayer{in: in}
}

// Run drives the state machine to completion (or error). emit is
// called once per event in seq order; the caller is expected to
// forward the event to the websocket. Run blocks until live-mode
// has fully taken over and the subscriber is detached, the context
// is cancelled, or an error occurs.
func (r *Replayer) Run(ctx context.Context, emit func(*segment.Event) error) error {
	start := time.Now()
	defer func() {
		r.in.Metrics.observeLookbackSeconds(time.Since(start).Seconds())
	}()

	// 1. Subscribe in lookback mode FIRST so no live events are missed.
	//    The ring is drained via SwitchToLive at the handoff, so we don't
	//    retain the handle here.
	subID, _ := r.in.Broadcaster.SubscribeForLookback(r.in.RingSize)
	defer r.in.Broadcaster.Unsubscribe(subID)

	currentCursor := r.in.StartSeq
	var lastEmittedSeq uint64
	var hasEmitted bool

	for iter := 0; iter < r.in.MaxIters; iter++ {
		// 2. Run the disk walker. Intercept emit to forward to the
		//    caller while checking ring overflow between events.
		walkErr := WalkFromCursor(ctx, WalkInput{
			StartSeq: currentCursor,
			Manifest: r.in.Manifest,
			Writer:   r.in.Writer,
		}, func(ev *segment.Event) error {
			if r.in.Broadcaster.SubscriberOverflowed(subID) {
				return errOverflow
			}
			if err := emit(ev); err != nil {
				return err
			}
			lastEmittedSeq = ev.Seq
			hasEmitted = true
			r.in.Metrics.incLookbackEvents()
			return nil
		})

		if errors.Is(walkErr, errOverflow) {
			// Drop the saturated ring; replay restarts from the next
			// untouched seq.
			r.in.Metrics.incRingOverflows()
			r.in.Metrics.incLookbackIterations()
			// Install a fresh ring + clear overflow on the subscriber's
			// phase. We don't retain the handle: SwitchToLive drains via
			// the live phase pointer at the eventual handoff.
			r.in.Broadcaster.ResetSubscriberOverflow(subID, r.in.RingSize)
			if hasEmitted {
				currentCursor = lastEmittedSeq + 1
			}
			continue
		}
		if walkErr != nil {
			return walkErr
		}

		// 3. Disk replay completed. Atomically clear the phase, seal the
		//    ring, and drain it. SealAndDrain (inside SwitchToLive) closes
		//    the window where a live event could be pushed into a ring the
		//    replay goroutine has stopped reading: any racing Publish
		//    either lands in the returned slice or reroutes to liveCh.
		liveCh, drained := r.in.Broadcaster.SwitchToLive(subID)
		if liveCh == nil {
			// Subscriber was unsubscribed under our feet; treat as a
			// clean disconnect.
			return nil
		}
		for _, ev := range drained {
			if hasEmitted && ev.Seq <= lastEmittedSeq {
				continue
			}
			if err := emit(ev); err != nil {
				return err
			}
			lastEmittedSeq = ev.Seq
			hasEmitted = true
			r.in.Metrics.incLookbackEvents()
		}

		// 5. Pump live events until ctx is cancelled.
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ev, ok := <-liveCh:
				if !ok {
					return nil
				}
				if hasEmitted && ev.Seq <= lastEmittedSeq {
					// Defensive: filter any boundary-time duplicate.
					continue
				}
				if err := emit(ev); err != nil {
					return err
				}
				lastEmittedSeq = ev.Seq
				hasEmitted = true
				r.in.Metrics.incLookbackEvents()
			}
		}
	}

	// We hit MaxIters without making it through replay.
	r.in.Metrics.incLookbackTerminated("too_slow")
	return ErrLookbackTooSlow
}

// errOverflow is the sentinel WalkFromCursor's emit callback returns
// to abort the walk on ring overflow. It never escapes Replayer.Run.
var errOverflow = errors.New("subscribe: lookback ring overflow")
