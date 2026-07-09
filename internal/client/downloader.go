package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/bluesky-social/jetstream/api/jetstream"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/xrpc"
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

// prefetchDepth is how many whole-segment files the prefetcher may fetch ahead
// of the framer. It overlaps the next segment's network download with the
// current segment's decode so the decode pool does not starve waiting on I/O.
// Depth 2 (one in-flight download + one ready) fully pipelines fetch with decode
// when a single segment's fetch is no slower than its decode; the cost is up to
// prefetchDepth resident ~280 MB segment buffers. Bounding the compressed-file
// footprint further (streaming the body) is tracked separately as #143.
const prefetchDepth = 2

// Downloader fetches sealed-archive plan entries over XRPC and decodes them into
// events. Block decode runs in parallel across a worker pool while a single
// reassembler emits results in strict plan order, so decode scales across cores
// without violating the archive's per-DID ordering invariant.
type Downloader struct {
	xc          *xrpc.Client
	concurrency int
	selector    RowSelector
	// transform, when non-nil, runs ON the decode-pool workers (in parallel)
	// immediately after a block is decoded, turning that block's []Event into an
	// opaque, ready-to-deliver payload that the reassembler forwards in seq order
	// as EntryResult.Payload. It exists to move expensive per-event work (notably
	// the internal→public event conversion + batch assembly, ~⅔ of the old serial
	// reassembler's CPU) OFF the single ordered goroutine and onto the parallel
	// pool, lifting the scaling ceiling (#142). The return type is `any` — not a
	// concrete type — precisely so this internal package never names the root
	// jetstream.Event/Batch types (root imports internal/client; the reverse would
	// be an import cycle). The root package supplies the closure and type-asserts
	// the payload back on the way out. When nil, the legacy []Event path is used
	// unchanged (direct Download callers / unit tests). A nil return for a block
	// means "nothing to emit" (empty/filtered), matching the len(events)>0 skip.
	transform func(entryIdx int, evs []Event) any
	// mode controls how commit records are materialized (raw vs. map[string]any).
	// Zero value = the default map-building path, so existing callers/tests are
	// unaffected. Set via SetRecordMode.
	mode recordDecodeMode
	// Striped whole-segment fetch tuning (#296). Set to the segment* defaults
	// by NewDownloader; tests shrink them to exercise multi-part behavior on
	// small fixtures without megabyte inputs.
	segPartSize    int64
	segStripes     int
	partRetryDelay time.Duration
}

// SetTransform installs the per-block worker-side transform (see Downloader.transform).
// It is set separately from NewDownloader so the constructor signature stays
// stable for the many direct callers/tests that do not need it.
func (d *Downloader) SetTransform(fn func(entryIdx int, evs []Event) any) { d.transform = fn }

// SetRecordMode selects raw vs. map record decode (see recordDecodeMode). The
// zero value (default map build) applies when unset, keeping existing callers
// unchanged.
func (d *Downloader) SetRecordMode(m recordDecodeMode) { d.mode = m }

// NewDownloader returns a Downloader using xc for getSegment/getBlock calls.
// concurrency bounds in-flight downloads; values < 1 are clamped to 1.
// selector (may be nil) filters/suppresses rows before decode.
func NewDownloader(xc *xrpc.Client, concurrency int, selector RowSelector) *Downloader {
	if concurrency < 1 {
		concurrency = 1
	}
	return &Downloader{
		xc:             xc,
		concurrency:    concurrency,
		selector:       selector,
		segPartSize:    segmentPartSize,
		segStripes:     defaultSegmentStripes,
		partRetryDelay: 500 * time.Millisecond,
	}
}

// SetSegmentStripes overrides how many parallel range requests fetch each
// whole segment. 1 (the default) is a single resumable stream; see the
// segmentfetch.go doc comment for when >1 helps and when it actively hurts.
func (d *Downloader) SetSegmentStripes(n int) {
	if n > 0 {
		d.segStripes = n
	}
}

// EntryResult is one unit of decoded output, tagged with its plan position so
// the consumer can emit in order. A single plan entry streams as MULTIPLE
// EntryResults — one per decoded block — all carrying the same Index/Entry, so
// the downloader never holds a whole segment's decoded events in memory at
// once. Err is non-nil for recoverable failures. If a whole block could not be
// downloaded or structurally decoded, Events/Payload are nil and that entry
// stops streaming after the error. If only individual selected rows were
// semantically malformed, Err is non-nil alongside the valid decoded rows, so
// consumers can surface the warning without dropping the rest of the block.
type EntryResult struct {
	Index  int // position in the plan's Entries slice
	Entry  PlanEntry
	Events []Event
	Err    error
	// Payload is the opaque output of the Downloader's transform (when one is
	// set), already computed in parallel on the decode workers and forwarded here
	// in seq order. nil when no transform is set (legacy []Event path) or for an
	// error result with no valid decoded rows. The caller that supplied the
	// transform type-asserts it.
	Payload any
}

// decodeJob is one raw block frame queued for the decode pool, tagged with the
// dense global sequence the framer assigned (for in-order reassembly) and its
// plan entry index (to build the EntryResult). err != nil is a fetch/read
// failure the framer surfaces in order without decoding (frame is then nil).
type decodeJob struct {
	seq      uint64
	entryIdx int
	blockIdx int
	frame    []byte
	err      error
}

// decodeResult is a decoded block leaving the pool, keyed by the same global
// seq for reassembly. emit is false for a wholly filtered/suppressed block (no
// events, no error): the reassembler still consumes its seq to keep the space
// dense but calls no emit. events/payload may be present with err when one or
// more selected rows in the block were semantically malformed but other rows
// decoded cleanly.
type decodeResult struct {
	seq      uint64
	entryIdx int
	events   []Event
	err      error
	emit     bool
	payload  any // transform output (worker-computed); nil on the legacy path
}

// Download fetches and decodes every entry in plan order, invoking emit once per
// decoded block in strict plan order (entry 0's blocks ascending, then entry
// 1's, …) regardless of decode-completion order. It runs a three-stage pipeline:
//
//  1. a single PREFETCHER fetches whole-segment files a little ahead (bounded),
//     overlapping the next segment's download with the current one's decode,
//     while a BLOCK-FETCH COORDINATOR runs block-mode getBlock round trips on
//     its own parallel pool across blocks and entries (#292);
//  2. a POOL of d.concurrency DECODE workers decompresses + CBOR-decodes block
//     frames in parallel — the CPU-heavy work, fanned out across cores;
//  3. a single REASSEMBLER emits decoded blocks in global-seq order, so output
//     order is independent of which worker finished first.
//
// Parallel decode is the throughput lever (#142): a likes backfill is decode-
// bound, and serial decode pinned it to ~1 core. d.concurrency now sizes the
// decode pool — the parallelism knob.
//
// If emit returns false, Download stops early: it cancels in-flight and pending
// work and returns nil (a clean stop, not an error). It returns the first
// context error encountered; per-block download/decode failures are reported
// through EntryResult.Err in order (not returned), so one bad block does not
// abort the backfill — the good prefix of that entry's blocks is emitted, then
// the error, then the entry stops and the next entry continues.
//
// Ordering invariant: the framer assigns a dense, monotonically increasing seq
// while walking entries in plan order and blocks in ascending index, and the
// reassembler emits strictly in that seq order; rows within a block keep their
// stored order. Per the segment format this yields per-DID ingestion order
// (docs/README.md §2 invariant #2, §3.1.1), the contract the client rests on.
// Parallel decode only reorders completion, never emission.
//
// Memory bound: at most inFlightWindow(d.concurrency) block frames are live
// between dispatch and emission (a global semaphore the reassembler drains in
// order), so decoded events held at once are O(window × block-size), not
// O(archive). Compressed whole-segment buffers are bounded to ~prefetchDepth
// resident files (a separate, larger term tracked by #143), independent of plan
// size. This is what keeps the full-archive backfill from the OOM in #142.
func (d *Downloader) Download(ctx context.Context, entries []PlanEntry, emit func(EntryResult) bool) error {
	if len(entries) == 0 {
		return nil
	}

	// One cancelable context drives the whole pipeline. An emit-driven early stop
	// cancels it (clean stop, Download returns nil); a caller-ctx cancel is
	// surfaced as the Download error via the final ctx.Err() check.
	dlCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	window := inFlightWindow(d.concurrency)
	// jobs/results are bounded so neither stage races arbitrarily far ahead; the
	// window semaphore is the real memory bound, these just smooth handoff.
	jobs := make(chan decodeJob, d.concurrency)
	results := make(chan decodeResult, d.concurrency)
	sem := make(chan struct{}, window) // in-flight block tokens; freed at reassembly

	// Stage 1+2 framing: the framer fetches (via the prefetcher) and slices block
	// frames, assigning a dense global seq, and feeds the decode pool. It closes
	// jobs when the plan is exhausted or the context is cancelled.
	go d.runFramer(dlCtx, entries, jobs, sem)

	// Stage 2: parallel decode pool. Each worker pulls jobs, decodes in parallel,
	// and forwards results. After all workers exit, results is closed so the
	// reassembler's range terminates.
	var pool sync.WaitGroup
	pool.Add(d.concurrency)
	for range d.concurrency {
		go func() {
			defer pool.Done()
			d.runDecodeWorker(dlCtx, entries, jobs, results, sem)
		}()
	}
	go func() {
		pool.Wait()
		close(results)
	}()

	// Stage 3: in-order reassembly + emission, on this goroutine. Returns when
	// results is closed (pipeline drained) — which is guaranteed: the framer
	// always closes jobs, every worker drains jobs to close, and close(results)
	// follows pool.Wait().
	reassemble(entries, results, sem, emit, cancel)

	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

// inFlightWindow is the cap on block frames live between dispatch and emission.
// It is the decoded-memory bound: ~window × one-block-decoded. Two per decode
// worker lets each worker hold one block while one more waits, keeping the pool
// fed without letting the reorder buffer (or decoded-events footprint) grow with
// the archive. A small floor keeps a 1-worker pipeline from starving itself.
func inFlightWindow(concurrency int) int {
	return max(2*concurrency, 4)
}

// runFramer is stages 1+2: walk entries in plan order, fetch each (whole-segment
// files prefetched a little ahead; block-mode frames via the parallel
// block-fetch coordinator), slice block frames, and dispatch them to the decode
// pool with a dense ascending global seq. It closes jobs on return. A per-entry
// fetch/read error is dispatched as an in-order error job and that entry stops
// (the next continues).
func (d *Downloader) runFramer(ctx context.Context, entries []PlanEntry, jobs chan<- decodeJob, sem chan struct{}) {
	defer close(jobs)

	// The block-fetch coordinator (nil when the plan has no block-mode entries)
	// runs getBlock round trips on a parallel pool, across blocks AND entries,
	// handing the framer per-entry ordered future streams (#292).
	bc := d.startBlockFetches(ctx, entries)
	prefetch := d.startPrefetch(ctx, entries)
	var seq uint64

	// dispatch hands one frame (or an in-order error) to the pool, first taking a
	// window token so in-flight frames stay bounded. It unblocks on cancellation
	// so the framer never wedges after an early stop. Returns false to stop.
	dispatch := func(entryIdx, blockIdx int, frame []byte, ferr error) bool {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return false
		}
		select {
		case jobs <- decodeJob{seq: seq, entryIdx: entryIdx, blockIdx: blockIdx, frame: frame, err: ferr}:
			seq++
			return true
		case <-ctx.Done():
			<-sem // release the token we took but could not enqueue
			return false
		}
	}

	for f := range prefetch {
		if ctx.Err() != nil {
			return
		}
		switch {
		case f.err != nil:
			// Fetch failed: surface one in-order error for this entry, skip it.
			if !dispatch(f.idx, 0, nil, f.err) {
				return
			}
		case f.entry.Mode == ModeWholeSegment:
			if !d.frameWholeSegment(f.idx, f.entry, f.raw, dispatch) {
				return
			}
		case f.entry.Mode == ModeBlocks:
			if !frameBlocks(ctx, f.idx, bc.futuresByEntry[f.idx], bc.inflight, dispatch) {
				return
			}
		default:
			if !dispatch(f.idx, 0, nil, fmt.Errorf("jetstream: unknown download mode %v for segment %q", f.entry.Mode, f.entry.SegmentName)) {
				return
			}
		}
	}
}

// frameWholeSegment slices the prefetched sealed file into block frames and
// dispatches them in ascending index. A header/read error is dispatched in order
// and stops this entry. Returns false if the pipeline was cancelled.
func (d *Downloader) frameWholeSegment(entryIdx int, entry PlanEntry, raw []byte, dispatch func(entryIdx, blockIdx int, frame []byte, ferr error) bool) bool {
	r := bytes.NewReader(raw)
	hdr, err := segment.ReadSealedHeader(r)
	if err != nil {
		return dispatch(entryIdx, 0, nil, fmt.Errorf("jetstream: parse segment header %q: %w", entry.SegmentName, err))
	}
	for idx := 0; idx < int(hdr.BlockCount); idx++ {
		frame, err := segment.ReadBlockFrame(r, hdr, idx)
		if err != nil {
			return dispatch(entryIdx, idx, nil, fmt.Errorf("jetstream: read block %d of %q: %w", idx, entry.SegmentName, err))
		}
		if !dispatch(entryIdx, idx, frame, nil) {
			return false
		}
	}
	return true
}

// frameBlocks consumes one entry's block-fetch futures in ascending block index
// and dispatches each frame. The fetches themselves run in parallel on the
// coordinator's pool (#292); consuming strictly in order HERE is what preserves
// the framer's ordered-dispatch contract — head-of-line waiting on the next
// future is fine because the round trips behind it are already overlapped. A
// fetch error is dispatched in order and stops this entry: the remaining
// futures are drained without dispatching. The drain still awaits each fetch
// (the coordinator keeps enqueueing the entry's tail as tokens free), so an
// entry error wastes that entry's remaining getBlocks — acceptable because
// errors survive 3 xrpc retries first, and the serial alternative would
// reintroduce the head-of-line stall this pool exists to remove. One in-flight
// token is released per future consumed, drained or not, so the coordinator
// keeps looking ahead. Returns false if the pipeline was cancelled.
func frameBlocks(ctx context.Context, entryIdx int, futures <-chan blockFuture, inflight <-chan struct{}, dispatch func(entryIdx, blockIdx int, frame []byte, ferr error) bool) bool {
	failed := false
	for fut := range futures {
		var res blockFetch
		select {
		case res = <-fut.result:
		case <-ctx.Done():
			return false
		}
		<-inflight
		if failed {
			continue
		}
		// int narrowing is safe: blockIdx <= math.MaxUint32 by the planner's
		// validation (the widened-uint64 loop in the coordinator).
		switch {
		case res.err != nil:
			failed = true
			if !dispatch(entryIdx, int(fut.blockIdx), nil, res.err) {
				return false
			}
		default:
			if !dispatch(entryIdx, int(fut.blockIdx), res.frame, nil) {
				return false
			}
		}
	}
	return true
}

// maxBlockFetchConc caps the parallel getBlock fetch pool. 64 stays comfortably
// under the transport's MaxConnsPerHost (100) while collapsing a sparse WAN
// backfill's block-per-RTT serialization (#292); getBlock is nearly free
// server-side (one stored frame read), so the client optimizes its own
// end-to-end latency rather than protecting the server here.
const maxBlockFetchConc = 64

// blockFetchPoolSize sizes the getBlock fetch pool from the decode concurrency:
// fetches are IO-bound round trips, not CPU work, so they deserve more
// parallelism than the decode pool itself.
func blockFetchPoolSize(concurrency int) int {
	return min(2*concurrency, maxBlockFetchConc)
}

// blockFetch is the outcome of one getBlock fetch.
type blockFetch struct {
	frame []byte
	err   error
}

// blockFuture is the framer's in-order handle to one parallel getBlock fetch.
// result has capacity 1 and is written exactly once by a fetch worker, so
// workers never block on delivery and an abandoned result is simply GC'd.
type blockFuture struct {
	blockIdx uint64
	result   chan blockFetch
}

// blockJob pairs one getBlock fetch with the future its result resolves.
type blockJob struct {
	segmentName string
	future      blockFuture
}

// blockFetchCoordinator runs the plan's getBlock fetches on a parallel worker
// pool, ahead of and independently from the framer, while preserving the
// framer's in-order consumption: futuresByEntry[i] streams entry i's futures in
// ascending block index, and the coordinator fills entries in plan order. Jobs
// are FIFO across entries, so plan-order blocks fetch first and later entries'
// blocks fill idle workers — cross-entry parallelism, which is what collapses a
// sparse (DID-filtered) plan of many few-block entries from one RTT per block
// to one RTT per pool-width (#292).
type blockFetchCoordinator struct {
	futuresByEntry []<-chan blockFuture // nil for non-block-mode entries
	// inflight bounds fetched-or-fetching frames: acquired before a job is
	// enqueued, released by the framer as it consumes each future. Without it
	// the coordinator could race arbitrarily far ahead of the framer, holding
	// one fetched frame per future across unboundedly many entries.
	inflight chan struct{}
}

// startBlockFetches launches the block-fetch coordinator, or returns nil when
// the plan has no block-mode entries. Workers exit when the coordinator has
// enqueued every block (or on cancellation, when in-flight requests fail fast
// via ctx and the per-entry channels are closed so the framer never hangs).
func (d *Downloader) startBlockFetches(ctx context.Context, entries []PlanEntry) *blockFetchCoordinator {
	if !slices.ContainsFunc(entries, func(e PlanEntry) bool { return e.Mode == ModeBlocks }) {
		return nil
	}
	size := blockFetchPoolSize(d.concurrency)
	bc := &blockFetchCoordinator{
		futuresByEntry: make([]<-chan blockFuture, len(entries)),
		inflight:       make(chan struct{}, 2*size),
	}
	chans := make([]chan blockFuture, len(entries))
	for i := range entries {
		if entries[i].Mode == ModeBlocks {
			chans[i] = make(chan blockFuture, size)
			bc.futuresByEntry[i] = chans[i]
		}
	}

	jobs := make(chan blockJob, size)
	for range size {
		go func() {
			for j := range jobs {
				frame, err := jetstream.JetstreamGetBlock(ctx, d.xc, int64(j.future.blockIdx), j.segmentName)
				if err != nil {
					err = fmt.Errorf("jetstream: getBlock %d of %q: %w", j.future.blockIdx, j.segmentName, err)
				}
				j.future.result <- blockFetch{frame: frame, err: err}
			}
		}()
	}

	// The coordinator: walk block-mode entries in plan order, enqueue one job
	// per block in ascending index, stream the future to that entry's channel.
	go func() {
		defer close(jobs)
		for i := range entries {
			if chans[i] == nil {
				continue
			}
			if !enqueueEntryBlocks(ctx, entries[i], jobs, chans[i], bc.inflight) {
				// Cancelled: close every remaining channel so a framer mid-range
				// never hangs (it will exit via ctx anyway, but never wedge).
				for j := i; j < len(entries); j++ {
					if chans[j] != nil {
						close(chans[j])
						chans[j] = nil
					}
				}
				return
			}
			close(chans[i])
			chans[i] = nil
		}
	}()
	return bc
}

// enqueueEntryBlocks submits one fetch job per listed block of one entry, in
// ascending index order, streaming each future to the framer's channel as it
// is issued. Returns false on cancellation (the caller closes the channel).
func enqueueEntryBlocks(ctx context.Context, entry PlanEntry, jobs chan<- blockJob, futures chan<- blockFuture, inflight chan<- struct{}) bool {
	for _, br := range entry.Blocks {
		// idx is widened to uint64 so a range ending at the uint32 max
		// (math.MaxUint32 passes the planner's `> MaxUint32` validation) does not
		// wrap back to 0 on the final increment and loop forever. The body only
		// runs for idx <= br.Last <= MaxUint32, so int64/int narrowing is safe.
		for idx := uint64(br.First); idx <= uint64(br.Last); idx++ {
			select {
			case inflight <- struct{}{}:
			case <-ctx.Done():
				return false
			}
			fut := blockFuture{blockIdx: idx, result: make(chan blockFetch, 1)}
			select {
			case jobs <- blockJob{segmentName: entry.SegmentName, future: fut}:
			case <-ctx.Done():
				return false
			}
			select {
			case futures <- fut:
			case <-ctx.Done():
				return false
			}
		}
	}
	return true
}

// fetchedEntry is one whole-segment file fetched ahead of framing (raw nil for
// non-whole-segment entries, whose blocks arrive via the block-fetch
// coordinator), or a fetch error.
type fetchedEntry struct {
	idx   int
	entry PlanEntry
	raw   []byte
	err   error
}

// startPrefetch launches a single goroutine that fetches whole-segment files in
// plan order, a little ahead of the framer, so the next segment's download
// overlaps the current one's decode. The depth-bounded channel caps how far
// ahead it runs (and thus how many ~280 MB buffers are resident). Block-mode
// entries pass through without fetching — their getBlock fetches run on the
// block-fetch coordinator's pool, which looks ahead independently of this
// depth bound (#292). It closes the channel on completion or cancellation.
func (d *Downloader) startPrefetch(ctx context.Context, entries []PlanEntry) <-chan fetchedEntry {
	out := make(chan fetchedEntry, prefetchDepth)
	go func() {
		defer close(out)
		for i := range entries {
			if ctx.Err() != nil {
				return
			}
			f := fetchedEntry{idx: i, entry: entries[i]}
			if entries[i].Mode == ModeWholeSegment {
				f.raw, f.err = d.fetchSegment(ctx, entries[i].SegmentName)
				if f.err != nil {
					f.err = fmt.Errorf("jetstream: getSegment %q: %w", entries[i].SegmentName, f.err)
				}
			}
			select {
			case out <- f:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// runDecodeWorker is one decode-pool worker: pull jobs, decode in parallel,
// forward results keyed by seq. A fetch/read error job (frame nil, err set) is
// forwarded without decoding so the reassembler can surface it in order. On
// cancellation the worker stops pulling; it does not need to free window tokens
// because the framer has also stopped acquiring them (no one is waiting).
func (d *Downloader) runDecodeWorker(ctx context.Context, entries []PlanEntry, jobs <-chan decodeJob, results chan<- decodeResult, sem chan struct{}) {
	_ = sem // window tokens are freed by the reassembler, in seq order
	for j := range jobs {
		res := decodeResult{seq: j.seq, entryIdx: j.entryIdx}
		switch {
		case j.err != nil:
			res.err = j.err
			res.emit = true
		default:
			events, err := d.decodeFrame(j.frame, entries[j.entryIdx].SegmentName, j.blockIdx)
			res.err = err
			if len(events) > 0 && d.transform != nil {
				// Fast path: run the caller's per-block transform HERE, in parallel,
				// so the expensive per-event work is off the serial reassembler.
				// The transform is caller-supplied (root) code running on a pool
				// goroutine; if it panics, a dead worker would leave pool.Wait ->
				// close(results) hung and wedge the whole Download. Convert a panic
				// into an in-order recoverable error instead (crash-visible to the
				// consumer, pipeline still drains) — never a silent hang.
				var transformErr error
				res.payload, transformErr = d.runTransform(j.entryIdx, events)
				res.err = errors.Join(res.err, transformErr)
			} else if len(events) > 0 {
				res.events = events
			}
			res.emit = len(events) > 0 || res.err != nil
		}
		select {
		case results <- res:
		case <-ctx.Done():
			return
		}
	}
}

// runTransform invokes d.transform on a decoded block, recovering a panic into a
// returned error. The transform runs caller-supplied (root) code on a decode-pool
// goroutine; a panic that killed the worker would hang pool.Wait → close(results)
// and wedge Download forever. Converting it to an in-order recoverable error keeps
// the pipeline draining and surfaces the failure to the consumer (crash-loud, not
// a silent hang). payload is nil for an empty/filtered block (nothing to emit).
func (d *Downloader) runTransform(entryIdx int, evs []Event) (payload any, err error) {
	defer func() {
		if r := recover(); r != nil {
			payload = nil
			err = fmt.Errorf("jetstream: backfill transform panicked: %v", r)
		}
	}()
	return d.transform(entryIdx, evs), nil
}

// reassemble is stage 3: read decoded blocks (arriving in any order), emit them
// strictly in global-seq order, and free one in-flight window token per block so
// the framer can dispatch more. It is the SOLE caller of emit, so emission is
// serialized and ordered. When emit returns false it cancels the pipeline and
// switches to drain mode: it keeps reading results until close (so decode workers
// never block on a full results channel) but emits nothing further.
func reassemble(entries []PlanEntry, results <-chan decodeResult, sem chan struct{}, emit func(EntryResult) bool, cancel context.CancelFunc) {
	pending := make(map[uint64]decodeResult)
	var next uint64
	stopped := false
	for r := range results {
		if stopped {
			continue // drain remaining results without emitting
		}
		pending[r.seq] = r
		// Flush every contiguous result from next upward that has arrived.
		for {
			nr, ok := pending[next]
			if !ok {
				break
			}
			delete(pending, next)
			next++
			<-sem // free this block's in-flight token
			if !nr.emit {
				continue // wholly filtered/suppressed block: consume seq, emit nothing
			}
			res := EntryResult{Index: nr.entryIdx, Entry: entries[nr.entryIdx], Events: nr.events, Err: nr.err, Payload: nr.payload}
			if !emit(res) {
				stopped = true
				cancel() // stop the framer + prefetch + decode workers
				break
			}
		}
	}
}

// decodeFrame decompresses one block frame, applies the row selector before
// decode (so filtered/suppressed rows are never materialized), and converts
// the survivors to events. Malformed selected rows are dropped and returned as a
// joined recoverable error alongside any valid decoded rows; one bad upstream
// record must not make the client lose the rest of the block.
//
// Commit rows dominate the archive (e.g. app.bsky.feed.like), and each commit's
// *Commit was one heap allocation. To cut that, the surviving commit rows of a
// block draw their Commit structs from a single per-block []Commit slab and the
// events reference &slab[i] — collapsing N allocations to one per block. The
// slab is sized exactly to the kept-commit count and NEVER appended to, so the
// &slab[i] pointers stay valid (a realloc would dangle them). It is a fresh
// allocation per block, dropped to GC with the batch; it is never pooled/reused,
// so a consumer still holding an earlier batch can never observe a mutation.
func (d *Downloader) decodeFrame(frame []byte, segName string, blockIdx int) ([]Event, error) {
	rows, err := segment.DecodeBlockFrame(frame)
	if err != nil {
		return nil, fmt.Errorf("jetstream: decode block %d of %q: %w", blockIdx, segName, err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	// One Commit slab + one Event slab for the whole block, instead of a separate
	// *Commit heap allocation per commit row. Each surviving commit takes the next
	// slab slot (&commits[ci]); the events reference those pointers. The slab is
	// sized to len(rows) and NEVER appended to, so &commits[ci] never dangles (a
	// realloc would). It is sized to the row count rather than the surviving-
	// commit count to keep this a single pass that runs the selector exactly once;
	// when a collection/DID filter drops rows, the unused tail slots are zeroed
	// Commits that are never referenced and are dropped to GC with the block. The
	// slab is a fresh per-block allocation, never pooled, so a consumer still
	// holding an earlier batch can never observe a mutation.
	commits := make([]Commit, len(rows))
	out := make([]Event, 0, len(rows))
	var decodeErrs []error
	ci := 0
	for i := range rows {
		if d.selector != nil {
			if keep, _ := d.selector.Keep(&rows[i]); !keep {
				continue
			}
		}
		var commit *Commit
		if isCommitKind(rows[i].Kind) {
			commit = &commits[ci]
			ci++
		}
		ev, err := decodeSegmentEventInto(&rows[i], commit, d.mode)
		if err != nil {
			decodeErrs = append(decodeErrs, err)
			continue
		}
		out = append(out, ev)
	}
	if len(out) == 0 {
		return nil, errors.Join(decodeErrs...)
	}
	return out, errors.Join(decodeErrs...)
}

// isCommitKind reports whether a segment row kind decodes to a KindCommit event
// (and therefore consumes a Commit slab slot in decodeFrame).
func isCommitKind(k segment.Kind) bool {
	switch k {
	case segment.KindCreate, segment.KindUpdate, segment.KindDelete, segment.KindCreateResync:
		return true
	default:
		return false
	}
}
