package follower

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jcalabro/gt"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"github.com/bluesky-social/jetstream/segment"
)

// The readable log is fed after the mirror swap, apart from the tick, so a
// mirror's freshness costs one catalog read however many events a revision
// brings. On 2026-10-14 bulk resyncs committed 300k-800k events within
// seconds; feeding them inside the tick held every pod's mirror back for
// 25-90s, past MaxViewAge, and every pod answered 503 at once.

// runFeeder feeds the readable log whenever a tick rings feedBell, until
// ctx ends.
func (f *Follower) runFeeder(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-f.feedBell:
		}
		_ = f.feedLocked(ctx)
	}
}

// ringFeeder asks the feeder for a pass without waiting for it.
func (f *Follower) ringFeeder() {
	select {
	case f.feedBell <- struct{}{}:
	default:
	}
}

// feedLocked runs one feed pass under feedMu. Corruption stops the
// follower; any other failure keeps what the pass appended and leaves the
// rest to the next pass, which the next tick's bell starts. Every tick
// retries, so only the first failure and the recovery are logged;
// feed_errors_total counts the rest.
func (f *Follower) feedLocked(ctx context.Context) error {
	select {
	case f.feedMu <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-f.feedMu }()
	err := f.feed(ctx)
	switch {
	case err == nil:
		if f.feedFailing {
			f.feedFailing = false
			f.log.Info("readable log feed recovered")
		}
	case ctx.Err() != nil:
	case isFatal(err):
		f.fail(err)
	default:
		f.cfg.Metrics.feedFailed()
		if !f.feedFailing {
			f.feedFailing = true
			f.log.Warn("readable log feed failed; retrying on every tick", slog.Any("error", err))
		}
	}
	return err
}

// feed appends the events [log tip, mirror Main tip) of the current mirror
// to the readable log, in seq order: blocks folded (or sealed) since the
// last pass first, read through RefsFrom, then the hot batches (design
// §11.1). Every seq appears at most once. Hot batches and active blocks are
// dense; a sealed block may lack seqs inside its envelope, because a
// compaction pass dropped them (design §12). Two blocks may also sit either
// side of a vacancy a migrated local archive registered. The log leaves all
// those seqs vacant.
//
// Blocks are fetched and decoded concurrently, a window at a time, and each
// window is appended in one run and made durable at once: every seq in it
// is in the published mirror, so a reader that sees the log evict it finds
// it there.
func (f *Follower) feed(ctx context.Context) error {
	m, flog := f.cur.Load(), f.flog.Load()
	if m == nil || flog == nil || m.phase != lifecycle.PhaseSteadyState {
		return nil
	}
	from := flog.Log().TipSeq()
	tip := m.view.TipSeq(catalog.Main)
	if tip <= from {
		if tip != 0 && tip < from {
			return catalog.Corruptf(catalog.SourceInvariant, "main tip %d is below the readable log tip %d", tip, from)
		}
		f.fedAt.Store(f.cfg.Now().UnixNano())
		return nil
	}

	start := f.cfg.Now()
	// The mirror's own rows come first: a newer mirror may already have
	// dropped the rows of hot batches it folded.
	rd, err := f.reader(tickRows{objects: m.objects, db: protocol.DBRows{DB: f.cfg.DB}}, f.cfg.Blob)
	if err != nil {
		return err
	}
	fetch := tickFetcher{rd: rd}
	want, fed := from, 0
	window := make([]catalog.BlockRef, 0, f.feedWindow())
	flush := func() error {
		if len(window) == 0 {
			return nil
		}
		phase := f.cfg.Now()
		decoded, err := gt.ConcurrentN(ctx, window, f.cfg.ReadConcurrency, func(ref catalog.BlockRef) ([]segment.Event, error) {
			return decodeMain(ctx, fetch, ref)
		})
		if err != nil {
			return err
		}
		var out []segment.Event
		for i, ref := range window {
			if out, want, err = appendRef(out, decoded[i], ref, want, m); err != nil {
				return err
			}
		}
		f.cfg.Metrics.observeFeedPhase("fetch_decode", f.cfg.Now().Sub(phase).Seconds())

		phase = f.cfg.Now()
		if err := flog.AppendRun(out, want); err != nil {
			return catalog.Corruptf(catalog.SourceInvariant, "%v", err)
		}
		if err := flog.AdvanceDurable(want); err != nil {
			return catalog.Corruptf(catalog.SourceInvariant, "%v", err)
		}
		now := f.cfg.Now()
		f.cfg.Metrics.observeFeedPhase("append", now.Sub(phase).Seconds())
		f.cfg.Metrics.fedEvents(len(out))
		f.fedAt.Store(now.UnixNano())
		for i := range out {
			f.cfg.Metrics.observeVisibility(now.Sub(time.UnixMicro(out[i].WitnessedAt)).Seconds())
		}
		fed += len(out)
		window = window[:0]
		return nil
	}
	for ref := range m.view.RefsFrom(catalog.Main, from) {
		window = append(window, ref)
		if len(window) == cap(window) {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if want != tip {
		return catalog.Corruptf(catalog.SourceInvariant, "main refs from %d end at %d, before tip %d", from, want, tip)
	}
	if d := f.cfg.Now().Sub(start); d > slowRefreshDuration {
		f.log.Warn("slow readable log feed", slog.Duration("duration", d), slog.Int("events", fed),
			slog.Uint64("from_seq", from), slog.Uint64("to_seq", tip))
	}
	return nil
}

// feedWindow is how many blocks one feed window fetches and decodes
// concurrently before appending them.
func (f *Follower) feedWindow() int { return 2 * f.cfg.ReadConcurrency }

// decodeMain fetches and decodes one Main block for the feed. A block
// shared storage cannot decode is corruption; any other failure is
// transient.
func decodeMain(ctx context.Context, fetch tickFetcher, ref catalog.BlockRef) ([]segment.Event, error) {
	events, err := catalog.DecodeRef(ctx, fetch, ref)
	if err == nil {
		return events, nil
	}
	if _, ok := catalog.IsCorruption(err); ok {
		return nil, err
	}
	if _, inline := ref.Loc.(catalog.InlineBlock); inline || errors.Is(err, segment.ErrCorruptSegment) {
		return nil, catalog.Corruptf(catalog.SourceHotBatch, "decode main block [%d,%d]: %v", ref.MinSeq, ref.MaxSeq, err)
	}
	return nil, fmt.Errorf("follower: read main block [%d,%d]: %w", ref.MinSeq, ref.MaxSeq, err)
}

// appendRef appends ref's events at or above want to out, checking that the
// block continues the stream, and returns the seq the stream is due at next.
func appendRef(out, events []segment.Event, ref catalog.BlockRef, want uint64, m *mirror) ([]segment.Event, uint64, error) {
	if want < ref.MinSeq {
		if !catalog.Bridges(m.gaps, want, ref.MinSeq) {
			return nil, 0, catalog.Corruptf(catalog.SourceHotBatch, "main block [%d,%d] starts after seq %d", ref.MinSeq, ref.MaxSeq, want)
		}
		want = ref.MinSeq
	}
	// Only a sealed generation can have been compacted.
	sealed := ref.Generation != 0
	for i := range events {
		ev := &events[i]
		if ev.Seq < want {
			continue
		}
		if ev.Seq > ref.MaxSeq || (ev.Seq != want && !sealed) {
			return nil, 0, catalog.Corruptf(catalog.SourceHotBatch, "main block [%d,%d] holds seq %d where %d was due", ref.MinSeq, ref.MaxSeq, ev.Seq, want)
		}
		out = append(out, *ev)
		want = ev.Seq + 1
	}
	if sealed {
		want = ref.MaxSeq + 1
	}
	if want != ref.MaxSeq+1 {
		return nil, 0, catalog.Corruptf(catalog.SourceHotBatch, "main block [%d,%d] ends at seq %d", ref.MinSeq, ref.MaxSeq, want)
	}
	return out, want, nil
}

// tickFetcher reads blocks through a reader whose rows cover a mirror that
// may not be the current one, without the current mirror's generation
// check: a generation compacted away since stays readable until GC.
type tickFetcher struct{ rd *protocol.Reader }

func (t tickFetcher) Fetch(ctx context.Context, ref catalog.BlockRef) ([]byte, error) {
	switch loc := ref.Loc.(type) {
	case catalog.InlineBlock:
		return loc.Frame, nil
	case catalog.ObjectBlock:
		return t.rd.Get(ctx, loc.ObjectID)
	default:
		return nil, fmt.Errorf("follower: unsupported locator %T", ref.Loc)
	}
}
