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

// entryChanBuffer is the per-entry result-channel buffer depth. A buffer of 1
// lets a worker decode one block ahead while the ordered consumer processes the
// previous one (so download/decode is not fully serialized with emission),
// while keeping the live decoded-event set tightly bounded: with the
// concurrency limit, at most ~concurrency × (entryChanBuffer+1) blocks of
// decoded events are alive at once, independent of how many segments the plan
// spans. This is the bound that fixes the full-archive backfill OOM (#142).
const entryChanBuffer = 1

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

// EntryResult is one unit of decoded output, tagged with its plan position so
// the consumer can emit in order. A single plan entry streams as MULTIPLE
// EntryResults — one per decoded block — all carrying the same Index/Entry, so
// the downloader never holds a whole segment's decoded events in memory at
// once. Err is non-nil if a block could not be downloaded or decoded; Events is
// then nil, and that entry stops streaming after the error (any earlier blocks
// of the same entry were already emitted).
type EntryResult struct {
	Index  int // position in the plan's Entries slice
	Entry  PlanEntry
	Events []Event
	Err    error
}

// blockResult is the internal per-block hand-off from a worker to the ordered
// consumer: one decoded block's events, or a terminal error for that entry.
// Exactly one of events/err is set (events is nil when err != nil).
type blockResult struct {
	events []Event
	err    error
}

// Download fetches and decodes every entry in plan order, invoking emit once
// per decoded BLOCK in ascending plan order (entry 0's blocks in order, then
// entry 1's, …) regardless of completion order. Downloads run concurrently up
// to the configured bound; emission is serialized and ordered, and each block's
// decoded events become eligible for GC as soon as emit returns — so peak
// memory is bounded by the concurrency window, not the size of the archive.
//
// If emit returns false, Download stops early: it cancels in-flight and pending
// downloads and returns without fetching the remaining entries. Download
// returns the first context error encountered; per-block download/decode
// failures are reported through EntryResult.Err, not returned, so one bad block
// does not abort the whole backfill (the good prefix of that entry's blocks is
// still emitted, then the error).
//
// Ordering: blocks within a segment are read in ascending index, segments are
// drained in ascending plan order, and rows within a block preserve their
// stored (ingestion) order. Per the segment format this yields per-DID
// ingestion order (docs/README.md §2 invariant #2, §3.1.1), the invariant the
// whole client rests on.
func (d *Downloader) Download(ctx context.Context, entries []PlanEntry, emit func(EntryResult) bool) error {
	if len(entries) == 0 {
		return nil
	}

	// One result channel per entry. Each entry's worker streams its decoded
	// blocks into chans[i] (one blockResult per block) and closes it when done;
	// the ordered consumer drains chans[0] to close, then chans[1], etc. The
	// small buffer plus the concurrency limit is what bounds the live decoded set
	// (see entryChanBuffer). Channels are cheap (a slice header + error each), so
	// allocating one per entry up front is fine — the plan is bounded by the
	// segment count.
	chans := make([]chan blockResult, len(entries))
	for i := range chans {
		chans[i] = make(chan blockResult, entryChanBuffer)
	}

	// Derive a cancelable context so the consumer can stop in-flight and pending
	// downloads on an early stop (emit returns false). The caller's ctx stays
	// separate for the final error check: an emit-driven cancel is a clean early
	// stop (Download returns nil), not an error.
	dlCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, gctx := errgroup.WithContext(dlCtx)
	g.SetLimit(d.concurrency)

	// Consumer: drain entries in strict plan order, emitting each block as it
	// arrives. Every chans[i] is guaranteed to be closed exactly once — by its
	// worker (deferred), or by the producer's cancellation path for entries never
	// launched — so the range loop always terminates. It must NOT also select on
	// gctx.Done(): a clean drain relies on reading every buffered block before the
	// close, and racing the cancel could drop an already-ready final block.
	emitDone := make(chan struct{})
	go func() {
		defer close(emitDone)
		for i := range entries {
			for br := range chans[i] {
				res := EntryResult{Index: i, Entry: entries[i], Events: br.events, Err: br.err}
				if !emit(res) {
					// Consumer asked to stop. Cancel pending/in-flight downloads so
					// Download returns promptly instead of fetching the rest of the
					// archive. Workers blocked on send observe gctx.Done() and exit;
					// the producer fills any unlaunched channels. Nothing reads the
					// channels after we return, so we do not drain them here.
					cancel()
					return
				}
			}
		}
	}()

	// Producers: launched from the calling goroutine (not a group member, so it
	// never occupies a limiter slot — avoids a deadlock at low concurrency).
	// g.Go blocks once `concurrency` workers are in flight; a worker holds its
	// slot until the consumer has drained its channel (bounded-buffer
	// backpressure), so at most `concurrency` segments are being downloaded/
	// decoded at any moment. The head entry's worker is always among the running
	// set, so the consumer always makes progress: deadlock-free including
	// concurrency=1. A download/decode failure is reported in-band on the channel
	// (EntryResult.Err), not as a group error; the group error is reserved for
	// context cancellation.
	for i := range entries {
		if gctx.Err() != nil {
			// Cancelled before launching the rest: fill the unlaunched channels with
			// the context error and close them so the consumer can drain and exit.
			// These channels have no worker, and the buffer (>=1) absorbs the single
			// send without a reader, so this never blocks.
			for j := i; j < len(entries); j++ {
				chans[j] <- blockResult{err: gctx.Err()}
				close(chans[j])
			}
			break
		}
		idx := i
		entry := entries[i]
		g.Go(func() error {
			ch := chans[idx]
			defer close(ch)
			// send hands one block to the consumer, unblocking on cancellation so a
			// worker stuck on backpressure does not leak when the consumer stops.
			send := func(br blockResult) bool {
				select {
				case ch <- br:
					return true
				case <-gctx.Done():
					return false
				}
			}
			d.streamEntry(gctx, entry, send)
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

// streamEntry downloads and decodes entry block-by-block, handing each decoded
// block to send in ascending block order. It stops at the first download/decode
// error (after emitting that error) or when send reports the consumer stopped.
func (d *Downloader) streamEntry(ctx context.Context, entry PlanEntry, send func(blockResult) bool) {
	switch entry.Mode {
	case ModeWholeSegment:
		d.streamWholeSegment(ctx, entry, send)
	case ModeBlocks:
		d.streamBlocks(ctx, entry, send)
	default:
		send(blockResult{err: fmt.Errorf("jetstream: unknown download mode %v for segment %q", entry.Mode, entry.SegmentName)})
	}
}

// streamWholeSegment fetches the whole sealed file once, then decodes and emits
// its blocks one at a time. The compressed file stays resident for the entry's
// lifetime (bounding peak by ~concurrency × file size — a separate follow-up,
// #143), but the decoded events are released per block as the consumer drains
// them rather than accumulated for the whole segment.
func (d *Downloader) streamWholeSegment(ctx context.Context, entry PlanEntry, send func(blockResult) bool) {
	raw, err := jetstream.JetstreamGetSegment(ctx, d.xc, entry.SegmentName)
	if err != nil {
		send(blockResult{err: fmt.Errorf("jetstream: getSegment %q: %w", entry.SegmentName, err)})
		return
	}
	r := bytes.NewReader(raw)
	hdr, err := segment.ReadSealedHeader(r)
	if err != nil {
		send(blockResult{err: fmt.Errorf("jetstream: parse segment header %q: %w", entry.SegmentName, err)})
		return
	}
	for idx := 0; idx < int(hdr.BlockCount); idx++ {
		frame, err := segment.ReadBlockFrame(r, hdr, idx)
		if err != nil {
			send(blockResult{err: fmt.Errorf("jetstream: read block %d of %q: %w", idx, entry.SegmentName, err)})
			return
		}
		events, err := d.decodeFrame(frame, entry.SegmentName, idx)
		if err != nil {
			send(blockResult{err: err})
			return
		}
		if len(events) == 0 {
			continue // wholly filtered/suppressed block: nothing to emit
		}
		if !send(blockResult{events: events}) {
			return // consumer stopped
		}
	}
}

// streamBlocks fetches the listed block ranges via getBlock and emits each
// block's events as it is decoded, in ascending block index.
func (d *Downloader) streamBlocks(ctx context.Context, entry PlanEntry, send func(blockResult) bool) {
	for _, br := range entry.Blocks {
		// idx is widened to uint64 so a range ending at the uint32 max
		// (math.MaxUint32 passes the planner's `> MaxUint32` validation) does
		// not wrap back to 0 on the final increment and loop forever. The body
		// only runs for idx <= br.Last <= MaxUint32, so int64/int narrowing is safe.
		for idx := uint64(br.First); idx <= uint64(br.Last); idx++ {
			frame, err := jetstream.JetstreamGetBlock(ctx, d.xc, int64(idx), entry.SegmentName)
			if err != nil {
				send(blockResult{err: fmt.Errorf("jetstream: getBlock %d of %q: %w", idx, entry.SegmentName, err)})
				return
			}
			events, err := d.decodeFrame(frame, entry.SegmentName, int(idx))
			if err != nil {
				send(blockResult{err: err})
				return
			}
			if len(events) == 0 {
				continue // wholly filtered/suppressed block: nothing to emit
			}
			if !send(blockResult{events: events}) {
				return // consumer stopped
			}
		}
	}
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
