package xrpcapi

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// memObjects is an in-memory objstore.Store that counts reads and records
// whether each read's context carried a deadline.
type memObjects struct {
	mu        sync.Mutex
	objs      map[uint64][]byte
	gets      int
	ranges    int
	deadlines int
}

var _ objstore.Store = (*memObjects)(nil)

func newMemObjects() *memObjects { return &memObjects{objs: make(map[uint64][]byte)} }

func (m *memObjects) read(ctx context.Context, id uint64, whole bool) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if whole {
		m.gets++
	} else {
		m.ranges++
	}
	if _, ok := ctx.Deadline(); ok {
		m.deadlines++
	}
	b, ok := m.objs[id]
	if !ok {
		return nil, objstore.ErrGone
	}
	return b, nil
}

func (m *memObjects) Get(ctx context.Context, id uint64) ([]byte, error) {
	return m.read(ctx, id, true)
}

func (m *memObjects) GetRange(ctx context.Context, id uint64, off, n int64) ([]byte, error) {
	b, err := m.read(ctx, id, false)
	if err != nil {
		return nil, err
	}
	if off < 0 || n <= 0 || off+n > int64(len(b)) {
		return nil, objstore.ErrInvalidRange
	}
	return b[off : off+n], nil
}

func (m *memObjects) reads() (gets, ranges int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gets, m.ranges
}

// split cuts a sealed segment file into the objects a disaggregated archive
// stores: the header row, one object per block frame (without its length
// prefix), and the footer. Object IDs start at firstID.
func (m *memObjects) split(t *testing.T, raw []byte, gen, firstID uint64) (catalog.GenerationParts, [][]byte) {
	t.Helper()
	hdr, err := segment.ReadSealedHeader(bytesReaderAt(raw))
	require.NoError(t, err)
	m.mu.Lock()
	defer m.mu.Unlock()
	id := firstID
	p := catalog.GenerationParts{
		Generation: gen,
		Header:     raw[:segment.ReservedHeaderBytes],
		CreatedAt:  time.Date(2026, 9, 25, 12, 0, int(gen), 0, time.UTC),
	}
	var frames [][]byte
	for off := uint64(segment.ReservedHeaderBytes); off < hdr.FooterOffset; {
		n := binary.LittleEndian.Uint64(raw[off:])
		frame := raw[off+8 : off+8+n]
		frames = append(frames, frame)
		m.objs[id] = frame
		p.Blocks = append(p.Blocks, catalog.ObjectPart{ID: id, Length: int64(n)})
		id++
		off += 8 + n
	}
	m.objs[id] = raw[hdr.FooterOffset:]
	p.Footer = catalog.ObjectPart{ID: id, Length: int64(len(raw)) - int64(hdr.FooterOffset)}
	return p, frames
}

type bytesReaderAt []byte

func (b bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(b)) {
		return 0, io.EOF
	}
	n := copy(p, b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// fakeGens is a swappable GenerationSource over one segment index.
type fakeGens struct {
	idx   uint64
	parts atomic.Pointer[catalog.GenerationParts]
	err   error
}

func (g *fakeGens) GenerationParts(_ context.Context, idx uint64) (catalog.GenerationParts, bool, error) {
	if g.err != nil {
		return catalog.GenerationParts{}, false, g.err
	}
	p := g.parts.Load()
	if idx != g.idx || p == nil {
		return catalog.GenerationParts{}, false, nil
	}
	return *p, true, nil
}

func (g *fakeGens) set(p catalog.GenerationParts) { g.parts.Store(&p) }

func objectServer(t *testing.T, cfg Config) *httptest.Server {
	t.Helper()
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	ts := httptest.NewServer(archiveMux(New(cfg)))
	t.Cleanup(ts.Close)
	return ts
}

// The virtual file an ObjectOpener serves is the sealed file byte for byte:
// whole, in random ranges, and in ranges straddling every part boundary; its
// validators are the header checksum and the generation's created_at; each
// getBlock body is the block object; and HEAD reads no object at all.
func TestObjectOpener_ServesSealedFile(t *testing.T) {
	t.Parallel()
	for seed := range uint64(6) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewPCG(seed, 0x0b1ec7))
			const idx = 3
			raw := rawFile(t, writeRandomSegment(t, rng, t.TempDir(), idx))
			objs := newMemObjects()
			parts, frames := objs.split(t, raw, 1, 100)
			gens := &fakeGens{idx: idx}
			gens.set(parts)
			ts := objectServer(t, Config{Opener: ObjectOpener{Gens: gens, Objects: objs}})
			hdr, err := segment.ReadSealedHeader(bytesReaderAt(raw))
			require.NoError(t, err)
			etag := fmt.Sprintf("%q", checksumHex(hdr.Checksum))
			url := getSegURL(ts.URL, fmt.Sprintf("seg_%010d.jss", idx))

			resp := doGet(t, url)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, etag, resp.Header.Get("ETag"))
			require.Equal(t, parts.CreatedAt.Format(http.TimeFormat), resp.Header.Get("Last-Modified"))
			require.Equal(t, raw, responseBody(t, resp))

			// Every part boundary, straddled from both sides, then random ranges.
			var ranges [][2]int
			off := segment.ReservedHeaderBytes
			for _, f := range frames {
				ranges = append(ranges, [2]int{off - 3, off + 2}, [2]int{off + 7, off + 9}, [2]int{off, off + 8 + len(f)})
				off += 8 + len(f)
			}
			ranges = append(ranges, [2]int{off - 1, off}, [2]int{0, len(raw) - 1})
			for range 16 {
				a := rng.IntN(len(raw))
				ranges = append(ranges, [2]int{a, a + rng.IntN(len(raw)-a)})
			}
			for _, r := range ranges {
				resp := doGetWith(t, url, func(req *http.Request) {
					req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", r[0], r[1]))
				})
				require.Equal(t, http.StatusPartialContent, resp.StatusCode, "range %v", r)
				require.Equal(t, etag, resp.Header.Get("ETag"))
				require.Equal(t, raw[r[0]:r[1]+1], responseBody(t, resp), "range %v", r)
			}

			for i, frame := range frames {
				resp := doGet(t, blockURL(ts.URL, fmt.Sprintf("seg_%010d.jss", idx), i))
				require.Equal(t, http.StatusOK, resp.StatusCode)
				require.Equal(t, fmt.Sprintf("%q", checksumHex(hdr.Checksum)+":"+fmt.Sprint(i)), resp.Header.Get("ETag"))
				require.Equal(t, frame, responseBody(t, resp), "block %d", i)
			}

			gets, ranged := objs.reads()
			resp = doHead(t, url)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, int64(len(raw)), resp.ContentLength)
			require.Equal(t, etag, resp.Header.Get("ETag"))
			_ = responseBody(t, resp)
			resp = doHeadWith(t, url, func(req *http.Request) { req.Header.Set("Range", "bytes=10-20") })
			require.Equal(t, http.StatusPartialContent, resp.StatusCode)
			_ = responseBody(t, resp)
			resp = doHead(t, blockURL(ts.URL, fmt.Sprintf("seg_%010d.jss", idx), len(frames)-1))
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, int64(len(frames[len(frames)-1])), resp.ContentLength)
			_ = responseBody(t, resp)
			g2, r2 := objs.reads()
			require.Equal(t, [2]int{gets, ranged}, [2]int{g2, r2}, "HEAD fetched objects")
		})
	}
}

// A compaction that swaps the generation between a client's requests changes
// the validators, so If-Range with the old ETag gets the whole new file,
// never a splice; a file opened before the swap keeps reading the old
// generation's objects.
func TestObjectOpener_GenerationSwap(t *testing.T) {
	t.Parallel()
	const idx = 3
	rng := rand.New(rand.NewPCG(1, 2))
	raw1 := rawFile(t, writeRandomSegment(t, rng, t.TempDir(), idx))
	raw2 := rawFile(t, writeRandomSegment(t, rng, t.TempDir(), idx))
	require.NotEqual(t, raw1, raw2)
	objs := newMemObjects()
	p1, _ := objs.split(t, raw1, 1, 100)
	p2, _ := objs.split(t, raw2, 2, 200)
	gens := &fakeGens{idx: idx}
	gens.set(p1)
	opener := ObjectOpener{Gens: gens, Objects: objs}
	ts := objectServer(t, Config{Opener: opener})
	url := getSegURL(ts.URL, fmt.Sprintf("seg_%010d.jss", idx))

	pinned, err := opener.OpenSegment(t.Context(), idx)
	require.NoError(t, err)

	resp := doGetWith(t, url, func(req *http.Request) { req.Header.Set("Range", "bytes=0-99") })
	require.Equal(t, http.StatusPartialContent, resp.StatusCode)
	etag1 := resp.Header.Get("ETag")
	require.Equal(t, raw1[:100], responseBody(t, resp))

	gens.set(p2)
	resp = doGetWith(t, url, func(req *http.Request) {
		req.Header.Set("Range", "bytes=100-199")
		req.Header.Set("If-Range", etag1)
	})
	require.Equal(t, http.StatusOK, resp.StatusCode, "a stale If-Range must get the whole new file")
	require.NotEqual(t, etag1, resp.Header.Get("ETag"))
	require.Equal(t, raw2, responseBody(t, resp))

	got, err := io.ReadAll(io.NewSectionReader(pinned, 0, pinned.Size()))
	require.NoError(t, err)
	require.Equal(t, raw1, got, "an open file reads its own generation")
}

// Parts that do not add up to their header, and a failing lookup, are server
// faults; an index the source does not know is SegmentNotFound.
func TestObjectOpener_Errors(t *testing.T) {
	t.Parallel()
	const idx = 3
	raw := rawFile(t, writeSealedSegmentBlocks(t, t.TempDir(), idx, 1, 2, 3))
	objs := newMemObjects()
	good, _ := objs.split(t, raw, 1, 100)

	cases := map[string]struct {
		mutate func(*catalog.GenerationParts)
		err    error
		status int
	}{
		"missing block":    {mutate: func(p *catalog.GenerationParts) { p.Blocks = p.Blocks[1:] }, status: http.StatusInternalServerError},
		"wrong length":     {mutate: func(p *catalog.GenerationParts) { p.Blocks[0].Length++ }, status: http.StatusInternalServerError},
		"empty footer":     {mutate: func(p *catalog.GenerationParts) { p.Footer.Length = 0 }, status: http.StatusInternalServerError},
		"short header":     {mutate: func(p *catalog.GenerationParts) { p.Header = p.Header[:100] }, status: http.StatusInternalServerError},
		"corrupt header":   {mutate: func(p *catalog.GenerationParts) { p.Header = append([]byte("XXXX"), p.Header[4:]...) }, status: http.StatusInternalServerError},
		"lookup failure":   {err: errors.New("catalog down"), status: http.StatusInternalServerError},
		"unknown segment":  {mutate: func(*catalog.GenerationParts) {}, status: http.StatusNotFound},
		"consistent parts": {mutate: func(*catalog.GenerationParts) {}, status: http.StatusOK},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := good
			p.Blocks = append([]catalog.ObjectPart(nil), good.Blocks...)
			gens := &fakeGens{idx: idx, err: tc.err}
			if tc.mutate != nil {
				tc.mutate(&p)
			}
			gens.set(p)
			ts := objectServer(t, Config{Opener: ObjectOpener{Gens: gens, Objects: objs}})
			seg := idx
			if name == "unknown segment" {
				seg = idx + 1
			}
			segName := fmt.Sprintf("seg_%010d.jss", seg)
			require.Equal(t, tc.status, getStatus(t, getSegURL(ts.URL, segName)))
			require.Equal(t, tc.status, getStatus(t, blockURL(ts.URL, segName, 0)))
		})
	}
}

// Every object read of an archive response runs under the response cutoff,
// and a response past its cutoff stops reading instead of finishing.
func TestArchiveResponseCutoff(t *testing.T) {
	t.Parallel()
	const idx = 3
	raw := rawFile(t, writeSealedSegmentBlocks(t, t.TempDir(), idx, 1, 2, 3))
	segName := fmt.Sprintf("seg_%010d.jss", idx)

	objs := newMemObjects()
	parts, _ := objs.split(t, raw, 1, 100)
	gens := &fakeGens{idx: idx}
	gens.set(parts)
	ts := objectServer(t, Config{Opener: ObjectOpener{Gens: gens, Objects: objs}, MaxResponseDuration: DefaultMaxArchiveResponseDuration})
	segResp := doGet(t, getSegURL(ts.URL, segName))
	defer func() { _ = segResp.Body.Close() }()
	require.Equal(t, raw, responseBody(t, segResp))
	blockResp := doGet(t, blockURL(ts.URL, segName, 1))
	defer func() { _ = blockResp.Body.Close() }()
	require.Equal(t, http.StatusOK, blockResp.StatusCode)
	_ = responseBody(t, blockResp)
	gets, ranged := objs.reads()
	require.Positive(t, gets)
	require.Positive(t, ranged)
	require.Equal(t, gets+ranged, objs.deadlines, "every read carries the cutoff")

	expired := newMemObjects()
	parts, _ = expired.split(t, raw, 1, 100)
	gens.set(parts)
	ts = objectServer(t, Config{Opener: ObjectOpener{Gens: gens, Objects: expired}, MaxResponseDuration: time.Nanosecond})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, getSegURL(ts.URL, segName), nil)
	require.NoError(t, err)
	// The expired write deadline may cut the response before its headers.
	if resp, err := http.DefaultClient.Do(req); err == nil {
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.True(t, err != nil || len(body) < len(raw), "a response past its cutoff must not complete")
	}
	gets, ranged = expired.reads()
	require.Zero(t, gets+ranged, "no object read after the cutoff")
}

type recordingSyncer struct {
	mu   sync.Mutex
	seqs []uint64
	err  error
}

func (s *recordingSyncer) SyncSeq(_ context.Context, seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seqs = append(s.seqs, seq)
	return s.err
}

// planSnapshot syncs to the highest seq a request names before planning, and
// a failed sync is a retryable 503 rather than a short plan.
func TestPlanSnapshot_SyncsNamedSeq(t *testing.T) {
	t.Parallel()
	cases := []struct {
		body map[string]any
		want []uint64
	}{
		{body: map[string]any{}, want: nil},
		{body: map[string]any{"afterSeq": 5}, want: []uint64{5}},
		{body: map[string]any{"beforeSeq": 10}, want: []uint64{9}},
		{body: map[string]any{"afterSeq": 3, "beforeSeq": 10}, want: []uint64{9}},
		{body: map[string]any{"beforeSeq": 0}, want: nil},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.body), func(t *testing.T) {
			t.Parallel()
			syncer := &recordingSyncer{}
			ts := objectServer(t, Config{Src: emptyPlanSource(t), Plan: defaultPlanTestConfig(), Sync: syncer})
			status, _ := postPlan(t, ts, tc.body)
			require.Equal(t, http.StatusOK, status)
			require.Equal(t, tc.want, syncer.seqs)
		})
	}

	syncer := &recordingSyncer{err: errors.New("catalog down")}
	ts := objectServer(t, Config{Src: emptyPlanSource(t), Plan: defaultPlanTestConfig(), Sync: syncer})
	status, _ := postPlan(t, ts, map[string]any{"afterSeq": 5})
	require.Equal(t, http.StatusServiceUnavailable, status)
}

func emptyPlanSource(t *testing.T) SegmentSource {
	t.Helper()
	m, err := manifest.Open(manifest.Options{SegmentsDir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	return m
}

func TestCheckGCDelay(t *testing.T) {
	t.Parallel()
	const viewAge, resp = 30 * time.Second, time.Hour
	require.NoError(t, CheckGCDelay(6*time.Hour, viewAge, resp))
	require.NoError(t, CheckGCDelay(viewAge+resp+10*time.Minute+time.Second, viewAge, resp))
	require.Error(t, CheckGCDelay(viewAge+resp+10*time.Minute, viewAge, resp), "the bound is strict")
	require.ErrorContains(t, CheckGCDelay(time.Hour, viewAge, resp), "GC delay 1h0m0s")
}
