package follower_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	jetstream "github.com/bluesky-social/jetstream"
	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/follower"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/bluesky-social/jetstream/internal/status"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/bluesky-social/jetstream/segment"
)

// buildArchive writes three sealed segments, an active segment with one
// folded block, and two unfolded hot batches, mixing inline and pointer
// batches: seqs 1-36 sealed, 37-41 active, 42-46 hot.
func (r *rig) buildArchive() {
	r.t.Helper()
	for range 3 {
		r.commitHot(4, false)
		r.commitHot(3, true)
		r.fold(2)
		r.commitHot(5, false)
		r.fold(1)
		r.seal()
	}
	r.commitHot(3, true)
	r.commitHot(2, false)
	r.fold(2)
	r.commitHot(3, false)
	r.commitHot(2, true)
}

const sealedTip = 36

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// hitCounter counts GET requests by NSID.
type hitCounter struct {
	mu sync.Mutex
	n  map[string]int
}

func (h *hitCounter) get(nsid string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n[nsid]
}

// archiveServer serves f's archive endpoints the way a disaggregated pod
// does (design §11.5), counting GET requests in hits when it is non-nil.
func archiveServer(t *testing.T, f *follower.Follower, m *manifest.Manifest, hits *hitCounter) *httptest.Server {
	t.Helper()
	s := xrpcapi.New(xrpcapi.Config{
		Src:                 m,
		Opener:              xrpcapi.ObjectOpener{Gens: f, Objects: f.Objects()},
		Logger:              quietLogger(),
		Ready:               f,
		CompactionDeadline:  f,
		Sync:                f,
		MaxResponseDuration: time.Hour,
		// A threshold of 1 serves a segment whole only when every block
		// matches, so a DID filter exercises getBlock too.
		Plan: xrpcapi.PlanConfig{MaxDIDs: 10, MaxCollections: 10, WholeSegmentThreshold: 1},
	})
	mux := http.NewServeMux()
	for _, nsid := range []string{"network.bsky.jetstream.getSegment", "network.bsky.jetstream.getBlock"} {
		mux.Handle("HEAD /xrpc/"+nsid, s.HeadHandler(nsid))
	}
	mux.Handle("/xrpc/", s.Handler())
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if hits != nil && req.Method == http.MethodGet {
			hits.mu.Lock()
			hits.n[strings.TrimPrefix(req.URL.Path, "/xrpc/network.bsky.jetstream.")]++
			hits.mu.Unlock()
		}
		mux.ServeHTTP(w, req)
	}))
	t.Cleanup(ts.Close)
	return ts
}

func readyFollower(t *testing.T, r *rig) (*follower.Follower, *manifest.Manifest) {
	t.Helper()
	f, _, m := r.follower(followerOpts{manifest: true})
	require.NoError(t, f.Refresh(t.Context()))
	require.NoError(t, f.Ready(t.Context()))
	return f, m
}

// The real Go client downloads and verifies the sealed archive from a pod
// that holds no segment files: whole segments through the virtual
// getSegment file, and DID-filtered blocks through getBlock.
func TestServe_ClientDownloadsSealedArchive(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.setPhase(lifecycle.PhaseSteadyState)
	r.buildArchive()
	f, m := readyFollower(t, r)

	for _, tc := range []struct {
		name string
		dids []string
	}{
		{name: "whole"},
		{name: "did", dids: []string{"did:plc:0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hits := &hitCounter{n: map[string]int{}}
			ts := archiveServer(t, f, m, hits)
			opts := []jetstream.Option{
				jetstream.WithHTTPClient(ts.Client()),
				jetstream.WithAfterSeq(0),
				jetstream.WithSnapshotOnly(),
				jetstream.WithRawRecords(),
				jetstream.WithSegmentStripes(3),
			}
			if tc.dids != nil {
				opts = append(opts, jetstream.WithDIDs(tc.dids))
			}
			c, err := jetstream.Subscribe(ts.URL, opts...)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			var got []jetstream.Event
			for b, err := range c.Events(ctx) {
				require.NoError(t, err)
				got = append(got, b.Events()...)
			}

			var want []segment.Event
			for _, ev := range r.events[:sealedTip] {
				if tc.dids == nil || ev.DID == tc.dids[0] {
					want = append(want, ev)
				}
			}
			require.Len(t, got, len(want))
			for i, ev := range want {
				require.Equal(t, ev.Seq, got[i].Seq)
				require.Equal(t, ev.DID, got[i].DID, "seq %d", ev.Seq)
				require.NotNil(t, got[i].Commit, "seq %d", ev.Seq)
				require.Equal(t, ev.Rkey, got[i].Commit.Rkey, "seq %d", ev.Seq)
				require.Equal(t, ev.Payload, got[i].Commit.RecordCBOR, "seq %d", ev.Seq)
			}
			require.Positive(t, hits.get("getSegment"), "whole segments")
			if tc.dids != nil {
				require.Positive(t, hits.get("getBlock"), "the filter planned single blocks")
			}
		})
	}
}

// A getSegment for a segment sealed after the pod's last tick is served:
// the unknown index runs a synchronous tick instead of a 404.
func TestServe_GetSegmentPastMirrorSyncs(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.setPhase(lifecycle.PhaseSteadyState)
	r.buildArchive()
	f, m := readyFollower(t, r)
	ts := archiveServer(t, f, m, nil)

	r.fold(2)
	r.seal() // segment 3, which f has not seen
	code, body := get(t, ts, "/xrpc/network.bsky.jetstream.getSegment?name="+ingest.SegmentFilename(3))
	require.Equal(t, http.StatusOK, code)
	hdr, err := segment.ReadSealedHeader(bytes.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, uint64(sealedTip+1), hdr.MinSeq)
	require.Equal(t, uint64(46), hdr.MaxSeq)
	code, _ = get(t, ts, "/xrpc/network.bsky.jetstream.getSegment?name="+ingest.SegmentFilename(4))
	require.Equal(t, http.StatusNotFound, code)
}

func get(t *testing.T, ts *httptest.Server, path string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+path, nil)
	require.NoError(t, err)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode, body
}

// A subscriber replays the archive below the follower's log floor through
// the cold reader, crosses into the readable log, and follows it live; a
// cursor the pod has not yet seen runs a synchronous tick rather than going
// live from a stale tip (design §11.6).
func TestServe_SubscribeReplaysSyncsAndTails(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.setPhase(lifecycle.PhaseSteadyState)
	r.buildArchive()
	f, m := readyFollower(t, r)
	floor, ok := f.LogFloor()
	require.True(t, ok)
	require.Equal(t, uint64(len(r.events))+1, floor, "a follower starting on a non-empty archive logs from its start tip")

	cold := subscribe.NewColdReader(subscribe.ColdReaderConfig{
		Catalog: f, Fetcher: f, Ready: f.Ready, Floor: f.LogFloor, Keyer: f,
		BlockCacheBytes: 1 << 20,
	})
	tail, err := subscribe.New(subscribe.Config{Logger: quietLogger()}, cold.Read, f.NextSeq)
	require.NoError(t, err)
	tail.SetReadLogSource(f.Log)
	ts := httptest.NewServer(subscribe.NewHandler(subscribe.Subscription{
		Tail: tail, Ready: f, Manifest: m, Catalog: f, Fetcher: f, Seqs: f,
		Logger:   quietLogger(),
		Metrics:  subscribe.NewMetrics(prometheus.NewRegistry()),
		Lookback: 36 * time.Hour,
	}))
	t.Cleanup(ts.Close)

	r.commitHot(4, false) // 47-50, in the log after a tick
	require.NoError(t, f.Refresh(t.Context()))
	all := dial(t, ts, 1)
	readSeqs(t, all, 1, uint64(len(r.events)))

	r.commitHot(3, true) // 51-53, unseen by f
	next := uint64(len(r.events)) - 2
	require.Less(t, f.NextSeq(), next+1)
	fresh := dial(t, ts, next)
	require.Equal(t, uint64(len(r.events))+1, f.NextSeq(), "the cursor ran a tick")
	readSeqs(t, fresh, next, uint64(len(r.events)))
	readSeqs(t, all, 51, uint64(len(r.events)))

	r.commitHot(2, false) // 54-55, live
	r.fold(3)
	require.NoError(t, f.Refresh(t.Context()))
	readSeqs(t, fresh, next+3, uint64(len(r.events)))
	readSeqs(t, all, 54, uint64(len(r.events)))
}

func dial(t *testing.T, ts *httptest.Server, cursor uint64) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/?cursor=" + strconv.FormatUint(cursor, 10)
	conn, resp, err := websocket.Dial(ctx, url, nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func readSeqs(t *testing.T, conn *websocket.Conn, from, to uint64) {
	t.Helper()
	for want := from; want <= to; want++ {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		_, body, err := conn.Read(ctx)
		cancel()
		require.NoError(t, err, "reading seq %d", want)
		require.Contains(t, string(body), `"cursor":`+strconv.FormatUint(want, 10)+",", "seq %d", want)
	}
}

// Decoded blocks are shared across pods and generations only where the key
// names immutable bytes: a sealed block by its object's SHA-256 and an
// inline batch by first seq and frame hash. Active blocks and pointer hot
// batches are not cached, since a fold or seal retires them.
func TestServe_BlockCacheKeys(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.setPhase(lifecycle.PhaseSteadyState)
	r.buildArchive()
	f1, _ := readyFollower(t, r)
	f2, _ := readyFollower(t, r)

	counts := map[string]int{}
	var refs2 []catalog.BlockRef
	for ref := range f2.Snapshot().RefsFrom(catalog.Main, 1) {
		refs2 = append(refs2, ref)
	}
	i := 0
	for ref := range f1.Snapshot().RefsFrom(catalog.Main, 1) {
		k1, ok1 := f1.BlockCacheKey(ref)
		k2, ok2 := f2.BlockCacheKey(refs2[i])
		i++
		require.Equal(t, ok1, ok2)
		require.Equal(t, k1, k2, "pods agree on block %d-%d's key", ref.MinSeq, ref.MaxSeq)
		kind := "active"
		switch loc := ref.Loc.(type) {
		case catalog.InlineBlock:
			kind = "inline"
		case catalog.ObjectBlock:
			if ref.Generation != 0 {
				kind = "sealed"
			} else if ref.MinSeq > 41 {
				kind = "pointer"
			}
			_ = loc
		}
		counts[kind]++
		require.Equal(t, kind == "sealed" || kind == "inline", ok1, "%s block %d-%d", kind, ref.MinSeq, ref.MaxSeq)
	}
	require.Len(t, refs2, i)
	require.Equal(t, map[string]int{"sealed": 6, "active": 1, "inline": 1, "pointer": 1}, counts)
}

// /status on a disaggregated pod reads Main's sealed segments from the
// manifest and its active segment through the follower's view, and never
// walks a segments directory or the repo keyspace.
func TestServe_Status(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.setPhase(lifecycle.PhaseSteadyState)
	r.buildArchive()
	f, m := readyFollower(t, r)

	st := memstore.New()
	require.NoError(t, lifecycle.WritePhase(t.Context(), st, lifecycle.PhaseSteadyState, time.Now()))
	c, err := status.New(status.Options{Store: st, Archive: f, ArchiveReady: f.Ready, Manifest: m})
	require.NoError(t, err)
	snap, err := c.Snapshot(t.Context())
	require.NoError(t, err)

	main := snap.SegmentAggregate.Trees[0]
	require.Equal(t, 3, main.SealedCount)
	require.Equal(t, 1, main.ActiveCount)
	require.Equal(t, uint64(46), main.EventCount, "the active segment counts its folded blocks and hot batches")
	require.Empty(t, snap.SegmentAggregate.Warnings)
	require.Equal(t, uint64(46), snap.SegmentAggregate.Network.Events)
	_, hasRepoCount := snap.Pebble.KeyspaceCounts["repo/"]
	require.False(t, hasRepoCount)
	require.Len(t, snap.SegmentAggregate.Collections, 1)
	require.Equal(t, "app.bsky.feed.post", snap.SegmentAggregate.Collections[0].NSID)
	require.Equal(t, uint64(46), snap.SegmentAggregate.Collections[0].EventCount)
}
