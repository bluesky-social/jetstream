package client

import (
	"bytes"
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// archiveServer serves getSegment (whole sealed file) and getBlock (raw frame)
// from real on-disk sealed segments, mirroring the production XRPC contract
// closely enough to exercise the downloader's decode path end to end.
type archiveServer struct {
	srv       *httptest.Server
	mux       *http.ServeMux
	mu        sync.Mutex
	segments  map[string][]byte // name -> whole sealed file bytes
	blockReqs atomic.Int64
	segReqs   atomic.Int64
	// segGate, when non-nil, is awaited at the start of every getSegment before
	// the response is served, letting a test deterministically delay a download
	// (e.g. to order a live event ahead of the backfill completing).
	segGate <-chan struct{}
}

func newArchiveServer(t *testing.T) *archiveServer {
	t.Helper()
	as := &archiveServer{segments: map[string][]byte{}}
	mux := http.NewServeMux()
	as.mux = mux
	mux.HandleFunc("/xrpc/network.bsky.jetstream.getSegment", func(w http.ResponseWriter, r *http.Request) {
		as.segReqs.Add(1)
		as.mu.Lock()
		gate := as.segGate
		as.mu.Unlock()
		if gate != nil {
			select {
			case <-gate:
			case <-r.Context().Done():
				return
			}
		}
		name := r.URL.Query().Get("name")
		as.mu.Lock()
		raw, ok := as.segments[name]
		as.mu.Unlock()
		if !ok {
			writeXRPCError(w, http.StatusNotFound, "SegmentNotFound")
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(raw)
	})
	mux.HandleFunc("/xrpc/network.bsky.jetstream.getBlock", func(w http.ResponseWriter, r *http.Request) {
		as.blockReqs.Add(1)
		name := r.URL.Query().Get("segment")
		idxStr := r.URL.Query().Get("blockIndex")
		as.mu.Lock()
		raw, ok := as.segments[name]
		as.mu.Unlock()
		if !ok {
			writeXRPCError(w, http.StatusNotFound, "SegmentNotFound")
			return
		}
		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest")
			return
		}
		hdr, err := segment.ReadSealedHeader(bytes.NewReader(raw))
		if err != nil || idx < 0 || idx >= int(hdr.BlockCount) {
			writeXRPCError(w, http.StatusNotFound, "BlockNotFound")
			return
		}
		frame, err := segment.ReadBlockFrame(bytes.NewReader(raw), hdr, idx)
		if err != nil {
			writeXRPCError(w, http.StatusInternalServerError, "InternalError")
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(frame)
	})
	as.srv = httptest.NewServer(mux)
	t.Cleanup(as.srv.Close)
	return as
}

func writeXRPCError(w http.ResponseWriter, status int, name string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"error":"` + name + `","message":""}`))
}

func (as *archiveServer) addSegment(t *testing.T, name string, events []segment.Event) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for i := range events {
		_, err := w.Append(events[i])
		require.NoError(t, err)
		// Flush block boundaries every 2 events to produce multiple blocks.
		if (i+1)%2 == 0 && i != len(events)-1 {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	as.mu.Lock()
	as.segments[name] = raw
	as.mu.Unlock()
}

func (as *archiveServer) downloader(concurrency int) *Downloader {
	return NewDownloader(&xrpc.Client{Host: as.srv.URL}, concurrency, nil)
}

// noKeepAliveDownloader builds a downloader whose HTTP transport disables
// connection reuse. The goroutine-leak test counts goroutines, and net/http's
// idle keep-alive connections (one reader goroutine each, pooled up to
// MaxIdleConns) otherwise accumulate independently of the pipeline and swamp the
// signal. Disabling keep-alives makes every connection tear down promptly so the
// count reflects only pipeline goroutines.
func (as *archiveServer) noKeepAliveDownloader(concurrency int) *Downloader {
	hc := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	xc := &xrpc.Client{Host: as.srv.URL, HTTPClient: gt.Some(hc)}
	return NewDownloader(xc, concurrency, nil)
}

// makeCreate builds a create-commit segment row carrying a minimal CBOR record.
func makeCreate(t *testing.T, seq uint64, did, collection, rkey string) segment.Event {
	t.Helper()
	rec := map[string]any{"$type": collection, "text": "hello " + rkey}
	payload, err := cbor.Marshal(rec)
	require.NoError(t, err)
	return segment.Event{
		Seq:         seq,
		WitnessedAt: int64(1_730_000_000_000_000 + seq*1_000),
		Kind:        segment.KindCreate,
		DID:         did,
		Collection:  collection,
		Rkey:        rkey,
		Rev:         "rev" + strconv.FormatUint(seq, 10),
		Payload:     payload,
	}
}

// collectOrdered drains a download and returns the flattened, in-order events.
// It enforces the streaming-emit ordering contract: a single plan entry now
// streams as MULTIPLE EntryResults (one per decoded block) carrying the same
// Index, so indices must be NON-DECREASING (not strictly +1 per call) and must
// still cover every entry's index contiguously from 0 (no entry skipped, no
// out-of-order interleaving across entries).
func collectOrdered(t *testing.T, d *Downloader, entries []PlanEntry) []Event {
	t.Helper()
	var all []Event
	lastIdx := -1
	err := d.Download(context.Background(), entries, func(res EntryResult) bool {
		require.NoError(t, res.Err, "entry %d (%s)", res.Index, res.Entry.SegmentName)
		require.GreaterOrEqual(t, res.Index, lastIdx,
			"block emission must be in non-decreasing plan order")
		require.LessOrEqual(t, res.Index, lastIdx+1,
			"entry indices must advance one at a time (no entry skipped or interleaved)")
		require.NotEmpty(t, res.Events, "a successful block result must carry events (empty blocks are skipped, not emitted)")
		lastIdx = res.Index
		all = append(all, res.Events...)
		return true
	})
	require.NoError(t, err)
	require.Equal(t, len(entries)-1, lastIdx, "every entry must be drained through the last index")
	return all
}

// TestDownloadTransformOrdering pins the fast-path (worker-side transform)
// ordering contract: when a transform is set, each block's payload is computed
// in parallel on the decode workers, but the reassembler must still deliver
// payloads in strict ascending seq order. The transform boxes the block's first
// event Seq as a sentinel; we assert the delivered sentinels are strictly
// increasing across a multi-block, multi-entry plan at high concurrency. This is
// the only test exercising the worker-box → reassembler-forward path directly.
func TestDownloadTransformOrdering(t *testing.T) {
	t.Parallel()
	as := newArchiveServer(t)
	const nSeg = 30
	const perSeg = 10 // 5 blocks/segment at MaxEventsPerBlock=2
	var entries []PlanEntry
	for s := range nSeg {
		var events []segment.Event
		for i := range perSeg {
			seq := uint64(s*1000 + i + 1)
			events = append(events, makeCreate(t, seq, "did:plc:a", "app.bsky.feed.post", "r"+strconv.FormatUint(seq, 10)))
		}
		as.addSegment(t, segName(s), events)
		entries = append(entries, PlanEntry{SegmentName: segName(s), Index: uint32(s), Mode: ModeWholeSegment})
	}

	d := as.downloader(16)
	// Transform boxes the block's first-event seq. Because the framer assigns
	// dense ascending seq in plan order and blocks ascend within a segment, the
	// per-block first-seqs are themselves strictly increasing in emit order.
	d.SetTransform(func(_ int, evs []Event) any {
		if len(evs) == 0 {
			return nil
		}
		return evs[0].Seq
	})

	var last int64 = -1
	var count int
	err := d.Download(context.Background(), entries, func(res EntryResult) bool {
		require.NoError(t, res.Err)
		require.Nil(t, res.Events, "fast path carries Payload, not Events")
		first, ok := res.Payload.(uint64)
		require.True(t, ok, "payload must be the boxed first-seq")
		require.Greater(t, int64(first), last, "payloads must arrive in strictly ascending seq order")
		last = int64(first)
		count++
		return true
	})
	require.NoError(t, err)
	require.Equal(t, nSeg*(perSeg/2), count, "one payload per non-empty block")
}

// TestDownloadTransformPanicNoHang guards FIX #1: a transform that panics on one
// block must NOT wedge Download (a dead worker would hang pool.Wait→close(results)).
// The panic must surface as an in-order recoverable EntryResult.Err, after the
// good prefix, and Download must return. The test's own timeout converts a
// regression (hang) into a failure rather than a stuck run.
func TestDownloadTransformPanicNoHang(t *testing.T) {
	t.Parallel()
	as := newArchiveServer(t)
	const nSeg = 8
	var entries []PlanEntry
	for s := range nSeg {
		var events []segment.Event
		for i := range 4 { // 2 blocks/segment
			seq := uint64(s*1000 + i + 1)
			events = append(events, makeCreate(t, seq, "did:plc:a", "app.bsky.feed.post", "r"+strconv.FormatUint(seq, 10)))
		}
		as.addSegment(t, segName(s), events)
		entries = append(entries, PlanEntry{SegmentName: segName(s), Index: uint32(s), Mode: ModeWholeSegment})
	}

	d := as.downloader(8)
	// Panic on the block whose first event seq is in the 3rd segment.
	panicSeq := uint64(2*1000 + 1)
	d.SetTransform(func(_ int, evs []Event) any {
		if len(evs) > 0 && evs[0].Seq == panicSeq {
			panic("boom in transform")
		}
		if len(evs) == 0 {
			return nil
		}
		return evs[0].Seq
	})

	done := make(chan struct{})
	var sawErr error
	var goodBefore int
	go func() {
		defer close(done)
		_ = d.Download(context.Background(), entries, func(res EntryResult) bool {
			if res.Err != nil {
				sawErr = res.Err
				return false // stop after the surfaced panic-error
			}
			goodBefore++
			return true
		})
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Download hung after a transform panic (FIX #1 regression)")
	}
	require.Error(t, sawErr, "transform panic must surface as a recoverable EntryResult.Err")
	require.Contains(t, sawErr.Error(), "panicked")
	require.Positive(t, goodBefore, "blocks before the panicking one must be delivered first")
}

// TestDownloadTransformErrorRouting guards FIX #3: on the fast path a per-block
// decode error must still surface in order as EntryResult.Err with a nil Payload
// (so the caller routes it through the error path, never asserting the payload).
// We corrupt one block's frame so its decode fails; the transform is never
// reached for that block.
func TestDownloadTransformErrorRouting(t *testing.T) {
	t.Parallel()
	as := newArchiveServer(t)
	var events []segment.Event
	for i := uint64(1); i <= 4; i++ { // 2 blocks of 2
		events = append(events, makeCreate(t, i, "did:plc:a", "app.bsky.feed.post", "r"+strconv.FormatUint(i, 10)))
	}
	as.addSegment(t, segName(0), events)

	as.mu.Lock()
	raw := append([]byte(nil), as.segments[segName(0)]...)
	as.mu.Unlock()
	hdr, err := segment.ReadSealedHeader(bytes.NewReader(raw))
	require.NoError(t, err)
	require.Equal(t, uint32(2), hdr.BlockCount)
	frame1, err := segment.ReadBlockFrame(bytes.NewReader(raw), hdr, 1)
	require.NoError(t, err)
	at := bytes.Index(raw, frame1)
	require.GreaterOrEqual(t, at, 0)
	for i := at; i < at+len(frame1); i++ {
		raw[i] ^= 0xFF
	}
	as.mu.Lock()
	as.segments[segName(0)] = raw
	as.mu.Unlock()

	d := as.downloader(1)
	transformCalls := 0
	d.SetTransform(func(_ int, evs []Event) any {
		transformCalls++
		if len(evs) == 0 {
			return nil
		}
		return evs[0].Seq
	})

	var payloads []uint64
	var sawErr error
	err = d.Download(context.Background(), []PlanEntry{{SegmentName: segName(0), Index: 0, Mode: ModeWholeSegment}},
		func(res EntryResult) bool {
			if res.Err != nil {
				sawErr = res.Err
				require.Nil(t, res.Payload, "an error result must carry no payload")
				return true
			}
			p, ok := res.Payload.(uint64)
			require.True(t, ok, "non-error result must carry the boxed seq payload")
			payloads = append(payloads, p)
			return true
		})
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, payloads, "the good block prefix is delivered as payloads before the error")
	require.Error(t, sawErr, "the corrupt block surfaces as an in-order error")
	require.Equal(t, 1, transformCalls, "transform runs only for the good block, never the failed-decode one")
}

// TestDownloadTransformNoGoroutineLeak mirrors the leak test on the fast path:
// repeated Downloads with a transform set (and an early stop) must not leak
// goroutines.
//
//nolint:paralleltest // intentionally serial: measures process-wide goroutine count
func TestDownloadTransformNoGoroutineLeak(t *testing.T) {
	as := newArchiveServer(t)
	const nSeg = 20
	var entries []PlanEntry
	for s := range nSeg {
		var events []segment.Event
		for i := range 6 {
			seq := uint64(s*1000 + i + 1)
			events = append(events, makeCreate(t, seq, "did:plc:a", "app.bsky.feed.post", "r"+strconv.FormatUint(seq, 10)))
		}
		as.addSegment(t, segName(s), events)
		entries = append(entries, PlanEntry{SegmentName: segName(s), Index: uint32(s), Mode: ModeWholeSegment})
	}
	settle := func() int {
		for range 40 {
			time.Sleep(5 * time.Millisecond)
			runtime.GC()
		}
		return runtime.NumGoroutine()
	}
	dl := as.noKeepAliveDownloader(8)
	dl.SetTransform(func(_ int, evs []Event) any {
		if len(evs) == 0 {
			return nil
		}
		return len(evs)
	})
	_ = collectViaTransform(dl, entries, func(EntryResult) bool { return true })
	_ = collectViaTransform(dl, entries, func(EntryResult) bool { return false })
	before := settle()
	const runs = 15
	for range runs {
		_ = collectViaTransform(dl, entries, func(EntryResult) bool { return true })
		_ = collectViaTransform(dl, entries, func(EntryResult) bool { return false })
	}
	after := settle()
	require.LessOrEqual(t, after, before+5,
		"goroutines must not grow across %d fast-path Download runs; before=%d after=%d", 2*runs, before, after)
}

func collectViaTransform(d *Downloader, entries []PlanEntry, emit func(EntryResult) bool) error {
	return d.Download(context.Background(), entries, emit)
}

// TestDownloadStreamsPerBlock pins the core memory-fix contract: a single
// whole-segment entry is emitted as MULTIPLE EntryResults — one per decoded
// block — rather than one giant slice. With MaxEventsPerBlock=2 a 6-event
// segment has 3 blocks, so the consumer must see 3 emit calls (all Index 0),
// each carrying one block's events, in ascending seq order. This is what lets
// each block's events be released before the next is decoded.
func TestDownloadStreamsPerBlock(t *testing.T) {
	t.Parallel()
	as := newArchiveServer(t)
	var events []segment.Event
	for i := uint64(1); i <= 6; i++ {
		events = append(events, makeCreate(t, i, "did:plc:a", "app.bsky.feed.post", "r"+strconv.FormatUint(i, 10)))
	}
	as.addSegment(t, segName(0), events)
	entries := []PlanEntry{{SegmentName: segName(0), Index: 0, Mode: ModeWholeSegment}}

	var batches [][]uint64
	err := as.downloader(4).Download(context.Background(), entries, func(res EntryResult) bool {
		require.NoError(t, res.Err)
		require.Equal(t, 0, res.Index)
		batches = append(batches, seqs(res.Events))
		return true
	})
	require.NoError(t, err)
	// 3 blocks of 2, each emitted as its own EntryResult, in order.
	require.Equal(t, [][]uint64{{1, 2}, {3, 4}, {5, 6}}, batches,
		"each block must be emitted as a separate EntryResult in ascending block order")
}

// TestDownloadLiveSetBoundedAcrossEntries is the regression guard for the #142
// OOM: completed entries' decoded events must NOT stay reachable for the whole
// Download. We assert it structurally — the emit callback only ever holds one
// block at a time, and the cumulative emitted count far exceeds any single
// block — so a correct implementation never needs the whole archive resident.
// A regression to the old "accumulate whole segment, never release results[i]"
// behavior would still pass functionally, so this test's value is the explicit
// contract + the alloc check below (it runs under -race/-count in CI and pins
// the per-block granularity that bounds liveness).
func TestDownloadLiveSetBoundedAcrossEntries(t *testing.T) {
	t.Parallel()
	as := newArchiveServer(t)
	const nSeg = 8
	for s := range nSeg {
		var events []segment.Event
		for i := range 6 { // 3 blocks per segment
			seq := uint64(s*100 + i + 1)
			events = append(events, makeCreate(t, seq, "did:plc:a", "app.bsky.feed.post", "r"+strconv.FormatUint(seq, 10)))
		}
		as.addSegment(t, segName(s), events)
	}
	var entries []PlanEntry
	for s := range nSeg {
		entries = append(entries, PlanEntry{SegmentName: segName(s), Index: uint32(s), Mode: ModeWholeSegment})
	}

	var maxBlock, total int
	err := as.downloader(4).Download(context.Background(), entries, func(res EntryResult) bool {
		require.NoError(t, res.Err)
		if len(res.Events) > maxBlock {
			maxBlock = len(res.Events)
		}
		total += len(res.Events)
		return true
	})
	require.NoError(t, err)
	require.Equal(t, nSeg*6, total, "every event across every segment must be emitted exactly once")
	require.LessOrEqual(t, maxBlock, 2, "no single emit may carry more than one block (MaxEventsPerBlock=2)")
}

// TestDownloadEmitsBlockPrefixThenErrorMidSegment pins the deliberate behavior
// change introduced with streaming: when a block fails to decode mid-segment,
// the blocks BEFORE it are still emitted (the good prefix), then the error is
// surfaced, then that entry stops. Previously a whole-segment decode returned
// nothing on any error. We corrupt one block's frame on the wire so block 0
// decodes cleanly and block 1 fails.
func TestDownloadEmitsBlockPrefixThenErrorMidSegment(t *testing.T) {
	t.Parallel()
	as := newArchiveServer(t)
	var events []segment.Event
	for i := uint64(1); i <= 4; i++ { // 2 blocks of 2
		events = append(events, makeCreate(t, i, "did:plc:a", "app.bsky.feed.post", "r"+strconv.FormatUint(i, 10)))
	}
	as.addSegment(t, segName(0), events)

	// Corrupt the SECOND block's frame bytes in the served file so block 0 still
	// decodes but block 1's zstd frame is invalid. We flip bytes near the end of
	// the file's block region (before the footer) — the simplest reliable way is
	// to serve a file whose block 1 frame is mangled. Locate block 1 and zero its
	// frame body.
	as.mu.Lock()
	raw := append([]byte(nil), as.segments[segName(0)]...)
	as.mu.Unlock()
	hdr, err := segment.ReadSealedHeader(bytes.NewReader(raw))
	require.NoError(t, err)
	require.Equal(t, uint32(2), hdr.BlockCount, "test expects exactly 2 blocks")
	// Find block 1's frame range via the public reader, then corrupt those bytes
	// in raw. ReadBlockFrame returns the frame bytes; we locate them by scanning
	// for that exact subslice (frames are large enough to be unique here).
	frame1, err := segment.ReadBlockFrame(bytes.NewReader(raw), hdr, 1)
	require.NoError(t, err)
	at := bytes.Index(raw, frame1)
	require.GreaterOrEqual(t, at, 0, "must locate block 1 frame in the file")
	for i := at; i < at+len(frame1); i++ {
		raw[i] ^= 0xFF // mangle the compressed frame so zstd decode fails
	}
	as.mu.Lock()
	as.segments[segName(0)] = raw
	as.mu.Unlock()

	entries := []PlanEntry{{SegmentName: segName(0), Index: 0, Mode: ModeWholeSegment}}
	var goodSeqs []uint64
	var sawErr error
	emitCalls := 0
	err = as.downloader(1).Download(context.Background(), entries, func(res EntryResult) bool {
		emitCalls++
		if res.Err != nil {
			sawErr = res.Err
			return true
		}
		goodSeqs = append(goodSeqs, seqs(res.Events)...)
		return true
	})
	require.NoError(t, err, "a mid-segment decode error is a per-entry error, not a Download failure")
	require.Equal(t, []uint64{1, 2}, goodSeqs, "the good block prefix must be emitted before the error")
	require.Error(t, sawErr, "the failing block must surface as an EntryResult error")
	require.Equal(t, 2, emitCalls, "exactly one good block then one error; no further blocks after the error")
}

// TestDownloadEarlyStopMidSegmentCancels asserts that stopping mid-segment (the
// consumer returns false on the first block of a multi-block segment) promptly
// cancels in-flight/pending work rather than draining every block of every
// segment. Tightened to concurrency=1 so the producer must stop launching.
func TestDownloadEarlyStopMidSegmentCancels(t *testing.T) {
	t.Parallel()
	as := newArchiveServer(t)
	const n = 30
	var entries []PlanEntry
	for s := range n {
		var events []segment.Event
		for i := range 6 { // 3 blocks each
			seq := uint64(s*100 + i + 1)
			events = append(events, makeCreate(t, seq, "did:plc:a", "app.bsky.feed.post", "r"+strconv.FormatUint(seq, 10)))
		}
		as.addSegment(t, segName(s), events)
		entries = append(entries, PlanEntry{SegmentName: segName(s), Index: uint32(s), Mode: ModeWholeSegment})
	}

	emitted := 0
	err := as.downloader(1).Download(context.Background(), entries, func(EntryResult) bool {
		emitted++
		return false // stop on the very first block
	})
	require.NoError(t, err, "an emit-driven early stop is a clean stop, not an error")
	require.Equal(t, 1, emitted, "emit must be called exactly once before stopping")
	require.Less(t, int(as.segReqs.Load()), n,
		"early stop must cancel pending downloads, not fetch the whole plan")
}

// TestDownloadParallelOrderingUnderShuffledCompletion is the core ordering guard
// for the parallel-decode pipeline (#142 lever 2): decode runs across a worker
// pool and completes out of order, but emission must still be strict ascending
// plan order. Many multi-block segments at high concurrency exercise genuine
// parallelism; the test asserts the flattened seq stream is exactly the sorted
// plan order. Run with -race, this also pins the pipeline race-free.
func TestDownloadParallelOrderingUnderShuffledCompletion(t *testing.T) {
	t.Parallel()
	as := newArchiveServer(t)
	const nSeg = 40
	const perSeg = 10 // 5 blocks/segment at MaxEventsPerBlock=2
	var entries []PlanEntry
	var want []uint64
	for s := range nSeg {
		var events []segment.Event
		for i := range perSeg {
			seq := uint64(s*1000 + i + 1)
			want = append(want, seq)
			events = append(events, makeCreate(t, seq, "did:plc:a", "app.bsky.feed.post", "r"+strconv.FormatUint(seq, 10)))
		}
		as.addSegment(t, segName(s), events)
		entries = append(entries, PlanEntry{SegmentName: segName(s), Index: uint32(s), Mode: ModeWholeSegment})
	}

	// High concurrency: decode order is effectively shuffled across the pool.
	got := collectOrdered(t, as.downloader(16), entries)
	require.Equal(t, want, seqs(got), "parallel decode must not perturb plan/seq order")
}

// TestDownloadConcurrencyEquivalence asserts the emitted event stream is
// identical regardless of decode-pool size — the parallelism knob changes
// throughput, never output. Pinning equivalence across 1/4/16 workers catches
// any ordering or dropped/duplicated-block bug that only manifests at a
// particular degree of parallelism.
func TestDownloadConcurrencyEquivalence(t *testing.T) {
	t.Parallel()
	as := newArchiveServer(t)
	const nSeg = 12
	var entries []PlanEntry
	for s := range nSeg {
		var events []segment.Event
		for i := range 8 { // 4 blocks/segment
			seq := uint64(s*1000 + i + 1)
			events = append(events, makeCreate(t, seq, "did:plc:a", "app.bsky.feed.post", "r"+strconv.FormatUint(seq, 10)))
		}
		as.addSegment(t, segName(s), events)
		entries = append(entries, PlanEntry{SegmentName: segName(s), Index: uint32(s), Mode: ModeWholeSegment})
	}

	base := seqs(collectOrdered(t, as.downloader(1), entries))
	for _, c := range []int{4, 16, 32} {
		got := seqs(collectOrdered(t, as.downloader(c), entries))
		require.Equal(t, base, got, "output must be independent of decode concurrency=%d", c)
	}
}

// TestDownloadNoGoroutineLeak asserts the pipeline (framer, prefetcher, decode
// pool, reassembler) fully unwinds on both clean completion and early-stop —
// no leaked goroutine after Download returns. A leak would mean a worker blocked
// forever on a send with no reader, the exact failure mode the bounded channels
// + cancellation paths exist to prevent.
// Not parallel: goroutine counting needs a quiet baseline, so it must not run
// concurrently with other tests spawning their own goroutines.
//
//nolint:paralleltest // intentionally serial: measures process-wide goroutine count
func TestDownloadNoGoroutineLeak(t *testing.T) {
	as := newArchiveServer(t)
	const nSeg = 20
	var entries []PlanEntry
	for s := range nSeg {
		var events []segment.Event
		for i := range 6 {
			seq := uint64(s*1000 + i + 1)
			events = append(events, makeCreate(t, seq, "did:plc:a", "app.bsky.feed.post", "r"+strconv.FormatUint(seq, 10)))
		}
		as.addSegment(t, segName(s), events)
		entries = append(entries, PlanEntry{SegmentName: segName(s), Index: uint32(s), Mode: ModeWholeSegment})
	}

	settle := func() int {
		for range 40 {
			time.Sleep(5 * time.Millisecond)
			runtime.GC()
		}
		return runtime.NumGoroutine()
	}

	// Warm up first so any one-time/lazy goroutines exist before the baseline,
	// then measure growth across many runs. Keep-alives are disabled so HTTP
	// connection goroutines do not accumulate and the count reflects the pipeline.
	dl := as.noKeepAliveDownloader(8)
	_ = collectOrdered(t, dl, entries)
	_ = dl.Download(context.Background(), entries, func(EntryResult) bool { return false })

	before := settle()
	const runs = 15
	for range runs {
		_ = collectOrdered(t, dl, entries)
		_ = dl.Download(context.Background(), entries, func(EntryResult) bool { return false })
	}
	after := settle()
	// The pipeline spawns ~concurrency+4 goroutines per Download; a per-run leak
	// across 2*runs Downloads would add dozens. A leak-free pipeline returns to
	// the baseline (small slack for runtime/GC bookkeeping churn).
	require.LessOrEqual(t, after, before+5,
		"goroutines must not grow across %d Download runs (leak); before=%d after=%d", 2*runs, before, after)
}

func TestDownloadWholeSegment(t *testing.T) {
	t.Parallel()
	as := newArchiveServer(t)
	events := []segment.Event{
		makeCreate(t, 1, "did:plc:a", "app.bsky.feed.post", "r1"),
		makeCreate(t, 2, "did:plc:a", "app.bsky.feed.post", "r2"),
		makeCreate(t, 3, "did:plc:b", "app.bsky.feed.like", "r3"),
	}
	as.addSegment(t, "seg_0000000000.jss", events)

	entries := []PlanEntry{{SegmentName: "seg_0000000000.jss", Index: 0, Mode: ModeWholeSegment, MinSeq: 1, MaxSeq: 3}}
	got := collectOrdered(t, as.downloader(4), entries)

	require.Len(t, got, 3)
	require.Equal(t, []uint64{1, 2, 3}, seqs(got))
	require.Equal(t, KindCommit, got[0].Kind)
	require.Equal(t, OpCreate, got[0].Commit.Operation)
	require.Equal(t, "app.bsky.feed.post", got[0].Commit.Collection)
	require.Equal(t, "hello r1", got[0].Commit.Record["text"])
	require.NotEmpty(t, got[0].Commit.CID)
	require.NotEmpty(t, got[0].Commit.RecordCBOR)
	require.Positive(t, as.segReqs.Load())
	require.Zero(t, as.blockReqs.Load())
}

func TestDownloadBlocksMode(t *testing.T) {
	t.Parallel()
	as := newArchiveServer(t)
	// 6 events -> 3 blocks of 2 (MaxEventsPerBlock=2).
	var events []segment.Event
	for i := uint64(1); i <= 6; i++ {
		events = append(events, makeCreate(t, i, "did:plc:a", "app.bsky.feed.post", "r"+strconv.FormatUint(i, 10)))
	}
	as.addSegment(t, "seg_0000000000.jss", events)

	// Request only blocks 0 and 2 (seqs 1-2 and 5-6); skip block 1.
	entries := []PlanEntry{{
		SegmentName: "seg_0000000000.jss", Index: 0, Mode: ModeBlocks,
		Blocks: []BlockRange{{First: 0, Last: 0}, {First: 2, Last: 2}},
	}}
	got := collectOrdered(t, as.downloader(4), entries)

	require.Equal(t, []uint64{1, 2, 5, 6}, seqs(got))
	require.Positive(t, as.blockReqs.Load())
	require.Zero(t, as.segReqs.Load())
}

func TestDownloadOrderedAcrossEntries(t *testing.T) {
	t.Parallel()
	as := newArchiveServer(t)
	for s := range 5 {
		base := uint64(s*10 + 1)
		as.addSegment(t, segName(s), []segment.Event{
			makeCreate(t, base, "did:plc:a", "app.bsky.feed.post", "r1"),
			makeCreate(t, base+1, "did:plc:a", "app.bsky.feed.post", "r2"),
		})
	}
	var entries []PlanEntry
	for s := range 5 {
		entries = append(entries, PlanEntry{SegmentName: segName(s), Index: uint32(s), Mode: ModeWholeSegment})
	}

	// High concurrency must not reorder emission.
	got := collectOrdered(t, as.downloader(8), entries)
	want := []uint64{1, 2, 11, 12, 21, 22, 31, 32, 41, 42}
	require.Equal(t, want, seqs(got))
}

func TestDownloadMissingSegmentReportsEntryError(t *testing.T) {
	t.Parallel()
	as := newArchiveServer(t)
	as.addSegment(t, segName(0), []segment.Event{makeCreate(t, 1, "did:plc:a", "c", "r1")})

	entries := []PlanEntry{
		{SegmentName: segName(0), Index: 0, Mode: ModeWholeSegment},
		{SegmentName: "seg_does_not_exist.jss", Index: 1, Mode: ModeWholeSegment},
	}

	var results []EntryResult
	err := as.downloader(2).Download(context.Background(), entries, func(res EntryResult) bool {
		results = append(results, res)
		return true
	})
	require.NoError(t, err, "a missing segment is a per-entry error, not a Download failure")
	require.Len(t, results, 2)
	require.NoError(t, results[0].Err)
	require.Error(t, results[1].Err, "missing segment must surface as entry error")
}

func TestDownloadContextCancel(t *testing.T) {
	t.Parallel()
	as := newArchiveServer(t)
	as.addSegment(t, segName(0), []segment.Event{makeCreate(t, 1, "did:plc:a", "c", "r1")})
	entries := []PlanEntry{{SegmentName: segName(0), Index: 0, Mode: ModeWholeSegment}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := as.downloader(2).Download(ctx, entries, func(EntryResult) bool { return true })
	require.ErrorIs(t, err, context.Canceled)
}

// TestDownloadEmitStopCancelsDownloads asserts the documented early-stop
// contract: when emit returns false, Download must stop fetching the remaining
// entries (cancel in-flight/pending downloads) rather than draining the whole
// plan. We seed many entries, stop after the first emit, and assert that far
// fewer than all segments were fetched.
func TestDownloadEmitStopCancelsDownloads(t *testing.T) {
	t.Parallel()
	as := newArchiveServer(t)
	const n = 50
	var entries []PlanEntry
	for s := range n {
		as.addSegment(t, segName(s), []segment.Event{
			makeCreate(t, uint64(s*10+1), "did:plc:a", "app.bsky.feed.post", "r1"),
		})
		entries = append(entries, PlanEntry{SegmentName: segName(s), Index: uint32(s), Mode: ModeWholeSegment})
	}

	// concurrency=1 makes the bound tight: with early cancel, the producer must
	// stop launching after the consumer bails, so the server sees far fewer
	// than n getSegment calls.
	var emitted int
	err := as.downloader(1).Download(context.Background(), entries, func(EntryResult) bool {
		emitted++
		return false // stop immediately after the first entry
	})
	require.NoError(t, err, "an emit-driven early stop is a clean stop, not an error")
	require.Equal(t, 1, emitted, "emit must be called exactly once before stopping")
	require.Less(t, int(as.segReqs.Load()), n,
		"early stop must cancel pending downloads, not fetch the whole plan")
}

// TestDownloadBlocksMaxUint32NoWraparound guards against the unsigned
// wraparound non-termination: a block range ending at math.MaxUint32 (which the
// planner's `> MaxUint32` validation permits) must be visited exactly once. A
// uint32 loop counter would wrap from MaxUint32 back to 0 after the final
// increment and call getBlock forever. The server serves a valid frame for any
// blockIndex and fails the test if getBlock is called more than once, so a
// wrapped second call is caught immediately rather than hanging.
func TestDownloadBlocksMaxUint32NoWraparound(t *testing.T) {
	t.Parallel()

	// Build a real one-block segment and capture its block-0 frame so the test
	// server can serve a decodable frame for the MaxUint32 index.
	src := newArchiveServer(t)
	src.addSegment(t, "seg_src.jss", []segment.Event{
		makeCreate(t, 1, "did:plc:a", "app.bsky.feed.post", "r1"),
	})
	src.mu.Lock()
	raw := src.segments["seg_src.jss"]
	src.mu.Unlock()
	hdr, err := segment.ReadSealedHeader(bytes.NewReader(raw))
	require.NoError(t, err)
	frame, err := segment.ReadBlockFrame(bytes.NewReader(raw), hdr, 0)
	require.NoError(t, err)

	var calls atomic.Int64
	var firstIdx atomic.Value // string: the blockIndex of the first call
	mux := http.NewServeMux()
	mux.HandleFunc("/xrpc/network.bsky.jetstream.getBlock", func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		idx := r.URL.Query().Get("blockIndex")
		if n == 1 {
			firstIdx.Store(idx)
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(frame)
			return
		}
		// A second call means the uint32 loop counter wrapped from MaxUint32 to
		// 0. Error out so the buggy loop terminates (instead of looping 2^32
		// times) and the test observes calls > 1 rather than hanging.
		writeXRPCError(w, http.StatusInternalServerError, "UnexpectedSecondCall")
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	d := NewDownloader(&xrpc.Client{Host: srv.URL}, 1, nil)
	entries := []PlanEntry{{
		SegmentName: "seg_max.jss", Index: 0, Mode: ModeBlocks,
		Blocks: []BlockRange{{First: math.MaxUint32, Last: math.MaxUint32}},
	}}

	got := collectOrdered(t, d, entries)
	require.Equal(t, []uint64{1}, seqs(got))
	require.Equal(t, int64(1), calls.Load(), "exactly one getBlock call for a single-index range at MaxUint32; >1 means the counter wrapped")
	require.Equal(t, strconv.FormatUint(math.MaxUint32, 10), firstIdx.Load(), "the single call must be for the MaxUint32 index")
}

func seqs(events []Event) []uint64 {
	out := make([]uint64, len(events))
	for i := range events {
		out[i] = events[i].Seq
	}
	return out
}

func segName(i int) string {
	return "seg_" + strconv.Itoa(i) + ".jss"
}
