package client

import (
	"bytes"
	"context"
	"fmt"

	"github.com/bluesky-social/jetstream/api/jetstream"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/xrpc"
	"golang.org/x/sync/errgroup"
)

// RowSelector decides whether a decoded segment row should be kept (emitted)
// or skipped. The downloader consults it before the expensive CBOR decode so
// filtered-out and suppressed rows are never materialized. A nil RowSelector
// keeps every row.
//
// Keep is called concurrently across entries and must be safe for concurrent
// use. The bool is keep; the string is a drop reason for diagnostics.
type RowSelector interface {
	Keep(ev *segment.Event) (bool, string)
}

// Downloader fetches sealed-archive plan entries over XRPC and decodes them
// into events. It downloads with bounded concurrency but emits entries in
// strict plan order, preserving the archive's per-DID ordering invariant.
type Downloader struct {
	xc          *xrpc.Client
	concurrency int
	selector    RowSelector
}

// NewDownloader returns a Downloader using xc for getSegment/getBlock calls.
// concurrency bounds in-flight downloads; values < 1 are clamped to 1.
// selector (may be nil) filters/suppresses rows before decode.
func NewDownloader(xc *xrpc.Client, concurrency int, selector RowSelector) *Downloader {
	if concurrency < 1 {
		concurrency = 1
	}
	return &Downloader{xc: xc, concurrency: concurrency, selector: selector}
}

// EntryResult is the decoded output of one plan entry, tagged with its plan
// position so the consumer can emit in order. Err is non-nil if the entry
// could not be downloaded or decoded; Events is then nil.
type EntryResult struct {
	Index  int // position in the plan's Entries slice
	Entry  PlanEntry
	Events []Event
	Err    error
}

// Download fetches and decodes every entry in plan order, invoking emit once
// per entry in ascending plan order regardless of completion order. Downloads
// run concurrently up to the configured bound; emission is serialized and
// ordered. If emit returns false, Download stops early: it cancels in-flight
// and pending downloads and returns without fetching the remaining entries.
// Download returns the first context error encountered; per-entry
// download/decode failures are reported through EntryResult.Err, not returned,
// so one bad entry does not abort the whole backfill.
func (d *Downloader) Download(ctx context.Context, entries []PlanEntry, emit func(EntryResult) bool) error {
	if len(entries) == 0 {
		return nil
	}

	// Each slot is completed by a worker and consumed by the ordered emitter.
	// ready[i] is closed when slot i is filled.
	results := make([]EntryResult, len(entries))
	ready := make([]chan struct{}, len(entries))
	for i := range ready {
		ready[i] = make(chan struct{})
	}

	// Derive a cancelable context so the emitter can stop in-flight and pending
	// downloads when the consumer asks to stop early (emit returns false). We
	// keep the caller's ctx separate for the final error check: an emit-driven
	// cancel is a clean early stop (Download returns nil), not an error.
	dlCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, gctx := errgroup.WithContext(dlCtx)
	g.SetLimit(d.concurrency)

	// Consumer: emit slots in order as they become ready. Runs concurrently
	// with the producers so a slow early entry doesn't stall downloads of
	// later entries (only their emission waits on order). Every ready[i] is
	// guaranteed to be closed exactly once — by its worker, or by the
	// producer's cancellation path for entries never launched — so the
	// emitter waits on ready[i] directly. It must NOT also select on
	// gctx.Done(): errgroup cancels gctx the instant Wait returns, which would
	// race the emitter into dropping an already-ready final slot.
	emitDone := make(chan struct{})
	go func() {
		defer close(emitDone)
		for i := range entries {
			<-ready[i]
			if !emit(results[i]) {
				// Consumer asked to stop. Cancel pending/in-flight downloads
				// so Download returns promptly instead of fetching the rest of
				// the archive. The producer loop observes gctx.Err() and fills
				// the remaining slots; nothing else reads them after we return.
				cancel()
				return
			}
		}
	}()

	// Producers: launched from the calling goroutine (not a group member, so
	// it never occupies a limiter slot — avoids a deadlock at low
	// concurrency). g.Go blocks once `concurrency` downloads are in flight. A
	// download/decode failure is recorded in the slot, not propagated as a
	// group error; the group error is reserved for context cancellation.
	for i := range entries {
		if gctx.Err() != nil {
			// Fill remaining slots so the emitter can drain and exit.
			for j := i; j < len(entries); j++ {
				results[j] = EntryResult{Index: j, Entry: entries[j], Err: gctx.Err()}
				close(ready[j])
			}
			break
		}
		idx := i
		entry := entries[i]
		g.Go(func() error {
			events, err := d.downloadEntry(gctx, entry)
			results[idx] = EntryResult{Index: idx, Entry: entry, Events: events, Err: err}
			close(ready[idx])
			return nil
		})
	}

	_ = g.Wait()
	<-emitDone
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

// downloadEntry fetches and decodes a single plan entry into ordered events.
func (d *Downloader) downloadEntry(ctx context.Context, entry PlanEntry) ([]Event, error) {
	switch entry.Mode {
	case ModeWholeSegment:
		return d.downloadWholeSegment(ctx, entry)
	case ModeBlocks:
		return d.downloadBlocks(ctx, entry)
	default:
		return nil, fmt.Errorf("jetstream: unknown download mode %v for segment %q", entry.Mode, entry.SegmentName)
	}
}

func (d *Downloader) downloadWholeSegment(ctx context.Context, entry PlanEntry) ([]Event, error) {
	raw, err := jetstream.JetstreamGetSegment(ctx, d.xc, entry.SegmentName)
	if err != nil {
		return nil, fmt.Errorf("jetstream: getSegment %q: %w", entry.SegmentName, err)
	}
	r := bytes.NewReader(raw)
	hdr, err := segment.ReadSealedHeader(r)
	if err != nil {
		return nil, fmt.Errorf("jetstream: parse segment header %q: %w", entry.SegmentName, err)
	}
	var events []Event
	for idx := 0; idx < int(hdr.BlockCount); idx++ {
		frame, err := segment.ReadBlockFrame(r, hdr, idx)
		if err != nil {
			return nil, fmt.Errorf("jetstream: read block %d of %q: %w", idx, entry.SegmentName, err)
		}
		blockEvents, err := d.decodeFrame(frame, entry.SegmentName, idx)
		if err != nil {
			return nil, err
		}
		events = append(events, blockEvents...)
	}
	return events, nil
}

func (d *Downloader) downloadBlocks(ctx context.Context, entry PlanEntry) ([]Event, error) {
	var events []Event
	for _, br := range entry.Blocks {
		// idx is widened to uint64 so a range ending at the uint32 max
		// (math.MaxUint32 passes the planner's `> MaxUint32` validation) does
		// not wrap back to 0 on the final increment and loop forever. The body
		// only runs for idx <= br.Last <= MaxUint32, so int64/int narrowing is safe.
		for idx := uint64(br.First); idx <= uint64(br.Last); idx++ {
			frame, err := jetstream.JetstreamGetBlock(ctx, d.xc, int64(idx), entry.SegmentName)
			if err != nil {
				return nil, fmt.Errorf("jetstream: getBlock %d of %q: %w", idx, entry.SegmentName, err)
			}
			blockEvents, err := d.decodeFrame(frame, entry.SegmentName, int(idx))
			if err != nil {
				return nil, err
			}
			events = append(events, blockEvents...)
		}
	}
	return events, nil
}

// decodeFrame decompresses one block frame, applies the row selector before
// decode (so filtered/suppressed rows are never materialized), and converts
// the survivors to events.
func (d *Downloader) decodeFrame(frame []byte, segName string, blockIdx int) ([]Event, error) {
	rows, err := segment.DecodeBlockFrame(frame)
	if err != nil {
		return nil, fmt.Errorf("jetstream: decode block %d of %q: %w", blockIdx, segName, err)
	}
	out := make([]Event, 0, len(rows))
	for i := range rows {
		if d.selector != nil {
			if keep, _ := d.selector.Keep(&rows[i]); !keep {
				continue
			}
		}
		ev, err := decodeSegmentEvent(&rows[i])
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, nil
}
