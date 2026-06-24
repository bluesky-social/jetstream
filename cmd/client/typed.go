package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/bluesky-social/jetstream"
	"github.com/jcalabro/atmos/api/bsky"
)

// reportTypedLikeThroughput consumes the stream via the typed fast path
// (jetstream.TypedEvents[bsky.FeedLike]) and prints periodic throughput, plus
// how many records actually decoded vs. failed. It mirrors reportThroughput but
// over typed batches, and exists to measure the typed-decode path end to end:
// it touches a decoded field so the compiler cannot elide the decode.
func reportTypedLikeThroughput(ctx context.Context, out io.Writer, client *jetstream.Client, interval time.Duration) error {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	var events, decoded, decodeErrs uint64
	var lastCursor uint64
	var subjectChars uint64 // touched so the decode isn't dead-code-eliminated

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	start := time.Now()
	lastAt := start
	lastEvents := uint64(0)
	report := func(label string) {
		now := time.Now()
		secs := now.Sub(lastAt).Seconds()
		if secs <= 0 {
			secs = 1
		}
		eps := uint64(float64(events-lastEvents)/secs + 0.5)
		_, _ = fmt.Fprintf(out, "%s elapsed=%s events=%s events_per_second=%s decoded=%s decode_errs=%s last_cursor=%d\n",
			label, now.Sub(start).Round(time.Second), formatCount(events), formatCount(eps),
			formatCount(decoded), formatCount(decodeErrs), lastCursor)
		lastAt = now
		lastEvents = events
	}

	done := ctx.Done()
	stream, stop := typedIterPull(jetstream.TypedEvents[bsky.FeedLike](ctx, client, "app.bsky.feed.like"))
	defer stop()
	for {
		select {
		case <-done:
			report("final")
			return nil
		case <-ticker.C:
			report("stats")
		case item, ok := <-stream:
			if !ok {
				report("final")
				return nil
			}
			if item.err != nil {
				if errors.Is(item.err, jetstream.ErrFatal) {
					report("final")
					return fmt.Errorf("event stream aborted: %w", item.err)
				}
				fmt.Fprintln(os.Stderr, "event error:", item.err)
				continue
			}
			evs := item.batch.Events()
			events += uint64(len(evs))
			for i := range evs {
				switch {
				case evs[i].DecodeErr != nil:
					decodeErrs++
				case evs[i].Record != nil:
					decoded++
					subjectChars += uint64(len(evs[i].Record.Subject.URI))
				}
			}
			if c := item.batch.LastCursor(); c > lastCursor {
				lastCursor = c
			}
		}
	}
}

// typedStreamItem carries one typed-batch-or-error from the pump.
type typedStreamItem struct {
	batch *jetstream.TypedBatch[bsky.FeedLike]
	err   error
}

// typedIterPull is iterPull for the typed iterator (see iterPull).
func typedIterPull(seq func(func(*jetstream.TypedBatch[bsky.FeedLike], error) bool)) (<-chan typedStreamItem, func()) {
	ch := make(chan typedStreamItem)
	done := make(chan struct{})
	go func() {
		defer close(ch)
		seq(func(b *jetstream.TypedBatch[bsky.FeedLike], err error) bool {
			select {
			case ch <- typedStreamItem{batch: b, err: err}:
				return true
			case <-done:
				return false
			}
		})
	}()
	return ch, func() { close(done) }
}
