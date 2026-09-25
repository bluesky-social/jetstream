package xrpcapi

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// writeRandomSegment seals a segment at idx with a random block layout and
// random payload sizes, so offsets and frame lengths vary across seeds.
func writeRandomSegment(t *testing.T, rng *rand.Rand, dir string, idx uint64) string {
	t.Helper()
	path := filepath.Join(dir, ingest.SegmentFilename(idx))
	perBlock := 1 + rng.IntN(8)
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: perBlock})
	require.NoError(t, err)
	seq := uint64(1 + rng.IntN(100))
	pending := 0
	for range 1 + rng.IntN(40) {
		// The writer refuses an append past MaxEventsPerBlock, so flush a
		// full block, and sometimes a partial one.
		if pending == perBlock || (pending > 0 && rng.IntN(6) == 0) {
			require.NoError(t, w.Flush())
			pending = 0
		}
		payload := make([]byte, rng.IntN(300))
		for i := range payload {
			payload[i] = byte(rng.Uint32())
		}
		_, err = w.Append(segment.Event{
			Seq: seq, WitnessedAt: int64(1_730_000_000_000_000 + seq*1_000),
			Kind: segment.KindCreate, DID: fmt.Sprintf("did:plc:%d", rng.IntN(5)),
			Collection: "app.bsky.feed.post", Rkey: fmt.Sprint(rng.Uint32()), Rev: "rev",
			Payload: payload,
		})
		require.NoError(t, err)
		pending++
		seq += 1 + uint64(rng.IntN(3))
	}
	_, err = w.Seal()
	require.NoError(t, err)
	return path
}

// TestFileOpener_ByteIdentity checks the SegmentFile the archive endpoints
// serve is the sealed file byte for byte: ReadAt, Content, and the getSegment
// body (whole and ranged) all equal the on-disk bytes, the ETags come from
// the header checksum at bytes [4:12], and each getBlock body is the frame
// at its 52-byte index entry's offset+8, read straight from the raw bytes.
func TestFileOpener_ByteIdentity(t *testing.T) {
	t.Parallel()

	for seed := range uint64(8) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewPCG(seed, 0x0be4e7))
			dir := t.TempDir()
			const idx = 3
			path := writeRandomSegment(t, rng, dir, idx)
			raw := rawFile(t, path)
			m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
			require.NoError(t, err)

			opener := FileOpener{Src: m}
			f, err := opener.OpenSegment(t.Context(), idx)
			require.NoError(t, err)
			defer func() { _ = f.Close() }()
			require.Equal(t, int64(len(raw)), f.Size())
			wantHdr, err := segment.ReadSealedHeader(f)
			require.NoError(t, err)
			require.Equal(t, wantHdr, f.Header())
			checksum := binary.LittleEndian.Uint64(raw[4:12])
			require.Equal(t, checksum, f.Header().Checksum)

			got := make([]byte, len(raw))
			n, err := f.ReadAt(got, 0)
			require.NoError(t, err)
			require.Equal(t, len(raw), n)
			require.Equal(t, raw, got, "ReadAt")
			off := rng.IntN(len(raw))
			part := make([]byte, len(raw)-off)
			_, err = f.ReadAt(part, int64(off))
			require.NoError(t, err)
			require.Equal(t, raw[off:], part, "ReadAt from %d", off)
			content, err := io.ReadAll(f.Content())
			require.NoError(t, err)
			require.Equal(t, raw, content, "Content")

			_, err = opener.OpenSegment(t.Context(), idx+1)
			require.ErrorIs(t, err, ErrSegmentNotFound)

			ts := httptest.NewServer(New(Config{Src: m, Opener: opener, Logger: slog.Default()}).Handler())
			defer ts.Close()
			name := ingest.SegmentFilename(idx)

			resp := doGet(t, getSegURL(ts.URL, name))
			body, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, raw, body, "getSegment body")
			require.Equal(t, fmt.Sprintf("%q", fmt.Sprintf("%016x", checksum)), resp.Header.Get("ETag"))

			lo := rng.IntN(len(raw))
			hi := lo + rng.IntN(len(raw)-lo)
			resp = doGetWith(t, getSegURL(ts.URL, name), func(r *http.Request) {
				r.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", lo, hi))
			})
			body, err = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			require.NoError(t, err)
			require.Equal(t, http.StatusPartialContent, resp.StatusCode)
			require.Equal(t, fmt.Sprintf("bytes %d-%d/%d", lo, hi, len(raw)), resp.Header.Get("Content-Range"))
			require.Equal(t, raw[lo:hi+1], body, "getSegment range")

			le := binary.LittleEndian
			indexAt := le.Uint64(raw[90:98])
			blocks := int(le.Uint32(raw[14:18]))
			require.Equal(t, int(f.Header().BlockCount), blocks)
			for b := range blocks {
				entry := raw[indexAt+uint64(b)*52:]
				frameAt := le.Uint64(entry[0:8]) + 8
				want := raw[frameAt : frameAt+uint64(le.Uint32(entry[8:12]))]

				resp := doGet(t, blockURL(ts.URL, name, b))
				body, err := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
				require.Equal(t, want, body, "getBlock %d", b)
				require.Equal(t, fmt.Sprintf("%q", fmt.Sprintf("%016x:%d", checksum, b)), resp.Header.Get("ETag"))
			}
		})
	}
}
