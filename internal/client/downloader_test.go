package client

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/stretchr/testify/require"
)

// archiveServer serves getSegment (whole sealed file) and getBlock (raw frame)
// from real on-disk sealed segments, mirroring the production XRPC contract
// closely enough to exercise the downloader's decode path end to end.
type archiveServer struct {
	srv       *httptest.Server
	mu        sync.Mutex
	segments  map[string][]byte // name -> whole sealed file bytes
	blockReqs atomic.Int64
	segReqs   atomic.Int64
}

func newArchiveServer(t *testing.T) *archiveServer {
	t.Helper()
	as := &archiveServer{segments: map[string][]byte{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/xrpc/network.bsky.jetstream.getSegment", func(w http.ResponseWriter, r *http.Request) {
		as.segReqs.Add(1)
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

func (as *archiveServer) downloaderWith(concurrency int, sel RowSelector) *Downloader {
	return NewDownloader(&xrpc.Client{Host: as.srv.URL}, concurrency, sel)
}

// makeCreate builds a create-commit segment row carrying a minimal CBOR record.
func makeCreate(t *testing.T, seq uint64, did, collection, rkey string) segment.Event {
	t.Helper()
	rec := map[string]any{"$type": collection, "text": "hello " + rkey}
	payload, err := cbor.Marshal(rec)
	require.NoError(t, err)
	return segment.Event{
		Seq:        seq,
		IndexedAt:  int64(1_730_000_000_000_000 + seq*1_000),
		Kind:       segment.KindCreate,
		DID:        did,
		Collection: collection,
		Rkey:       rkey,
		Rev:        "rev" + strconv.FormatUint(seq, 10),
		Payload:    payload,
	}
}

func collectOrdered(t *testing.T, d *Downloader, entries []PlanEntry) []Event {
	t.Helper()
	var all []Event
	var lastIdx = -1
	err := d.Download(context.Background(), entries, func(res EntryResult) bool {
		require.NoError(t, res.Err, "entry %d (%s)", res.Index, res.Entry.SegmentName)
		require.Equal(t, lastIdx+1, res.Index, "entries must emit in plan order")
		lastIdx = res.Index
		all = append(all, res.Events...)
		return true
	})
	require.NoError(t, err)
	return all
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
