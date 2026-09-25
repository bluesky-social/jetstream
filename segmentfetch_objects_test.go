package jetstream

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/bluesky-social/jetstream/segment"
)

// objectMap is an in-memory objstore.Store.
type objectMap struct {
	mu   sync.Mutex
	objs map[uint64][]byte
	next uint64
}

func (m *objectMap) put(b []byte) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.next++
	m.objs[m.next] = b
	return m.next
}

func (m *objectMap) Get(ctx context.Context, id uint64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[id]
	if !ok {
		return nil, objstore.ErrGone
	}
	return b, nil
}

func (m *objectMap) GetRange(ctx context.Context, id uint64, off, n int64) ([]byte, error) {
	b, err := m.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if off < 0 || n <= 0 || off+n > int64(len(b)) {
		return nil, objstore.ErrInvalidRange
	}
	return b[off : off+n], nil
}

// swappableGens serves one segment whose current generation a test can
// replace, as a compaction does.
type swappableGens struct {
	cur atomic.Pointer[catalog.GenerationParts]
}

func (g *swappableGens) GenerationParts(_ context.Context, idx uint64) (catalog.GenerationParts, bool, error) {
	if idx != 0 {
		return catalog.GenerationParts{}, false, nil
	}
	return *g.cur.Load(), true, nil
}

// sealedObjects writes events as a sealed segment and stores it the way a
// disaggregated archive does: one object per block frame and one for the
// footer. It returns the file's bytes and the generation's parts.
func sealedObjects(t *testing.T, objs *objectMap, gen uint64, events []segment.Event) ([]byte, catalog.GenerationParts) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "seg.jss")
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 16})
	require.NoError(t, err)
	for i := range events {
		_, err := w.Append(events[i])
		require.NoError(t, err)
		if (i+1)%16 == 0 && i != len(events)-1 {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	hdr, err := segment.ReadSealedHeader(bytes.NewReader(raw))
	require.NoError(t, err)
	p := catalog.GenerationParts{
		Generation: gen,
		Header:     raw[:segment.ReservedHeaderBytes],
		CreatedAt:  time.Unix(1_730_000_000+int64(gen), 0),
	}
	off := int64(segment.ReservedHeaderBytes)
	for range hdr.BlockCount {
		n := int64(binary.LittleEndian.Uint64(raw[off:]))
		frame := raw[off+8 : off+8+n]
		p.Blocks = append(p.Blocks, catalog.ObjectPart{ID: objs.put(frame), Length: n})
		off += 8 + n
	}
	require.Equal(t, hdr.FooterOffset, uint64(off))
	p.Footer = catalog.ObjectPart{ID: objs.put(raw[off:]), Length: int64(len(raw)) - off}
	return raw, p
}

func objectEvents(rng *rand.Rand, n int) []segment.Event {
	events := make([]segment.Event, n)
	for i := range events {
		// Random payloads keep the blocks from compressing to a few bytes,
		// so the file spans many download parts.
		payload := make([]byte, 64)
		for j := range payload {
			payload[j] = byte(rng.Uint32())
		}
		events[i] = segment.Event{
			Seq: uint64(i + 1), Kind: segment.KindCreate, DID: fmt.Sprintf("did:plc:%d", i%5),
			Collection: "app.bsky.feed.post", Rkey: fmt.Sprint(i), Rev: "r", Payload: payload,
		}
	}
	return events
}

// A compaction that replaces a segment's generation while the client is
// downloading its object-assembled file: the parts after the probe carry
// If-Range with the old generation's ETag, the pod assembles the new
// generation, so the ETag no longer matches and the client restarts on the
// new file instead of splicing the two (design §11.5).
func TestFetchSegmentFromObjectsGenerationSwap(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(1, 2))
	objs := &objectMap{objs: map[uint64][]byte{}}
	rawA, genA := sealedObjects(t, objs, 1, objectEvents(rng, 300))
	rawB, genB := sealedObjects(t, objs, 2, objectEvents(rng, 280))
	require.NotEqual(t, rawA, rawB)

	gens := &swappableGens{}
	gens.cur.Store(&genA)
	s := xrpcapi.New(xrpcapi.Config{
		Opener: xrpcapi.ObjectOpener{Gens: gens, Objects: objs},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	var swapped atomic.Bool
	h := s.Handler()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-Range") != "" && !swapped.Swap(true) {
			gens.cur.Store(&genB)
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	const partSize = 4 << 10
	require.Greater(t, len(rawA), 4*partSize, "the file must span several parts")
	d := stripedDownloader(srv.URL, 4, partSize)
	got, err := d.fetchSegment(t.Context(), ingest.SegmentFilename(0))
	require.NoError(t, err)
	require.True(t, swapped.Load())
	require.Equal(t, rawB, got, "the client restarts on the new generation, intact")
}
