// Package corpus holds the real-data corpus tier of the oracle test
// system (specs/oracle.md "Real-Data Corpus Tier", issue #32).
//
// Every fixture under testdata/ was captured from the real atproto
// network and independently verified with bluesky-social/indigo at
// capture time — never derived with github.com/jcalabro/atmos. That
// independence is the point: the simulator, the oracle, and Jetstream
// all share atmos for protocol handling, so a symmetric protocol bug
// can pass every other tier. Real network bytes with expectations
// pinned by a foreign implementation (production Jetstream v1 for the
// firehose window, indigo/goat for the getRepo CAR) break that closed
// loop.
//
// All tests here are offline and fast; see testdata/README.md for
// capture provenance and the re-capture procedure.
package corpus

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

var updateGolden = flag.Bool("update", false, "regenerate golden testdata")

// manifest mirrors the capture tool's manifest.json. The counts are
// used as anti-vacuity checks: a test that silently processed zero
// deletes (or zero anything) must fail, not pass.
type manifest struct {
	CapturedAt   string   `json:"captured_at"`
	RelayURL     string   `json:"relay_url"`
	JetstreamURL string   `json:"jetstream_url"`
	SeqFirst     int64    `json:"seq_first"`
	SeqLast      int64    `json:"seq_last"`
	Frames       int      `json:"frames"`
	CommitFrames int      `json:"commit_frames"`
	V1Events     int      `json:"v1_events"`
	Creates      int      `json:"creates"`
	Updates      int      `json:"updates"`
	Deletes      int      `json:"deletes"`
	Identity     int      `json:"identity"`
	Account      int      `json:"account"`
	CommitDIDs   []string `json:"commit_dids"`
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	require.NoError(t, err)
	return string(b)
}

func loadManifest(t *testing.T) manifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "manifest.json"))
	require.NoError(t, err)
	var m manifest
	require.NoError(t, json.Unmarshal(raw, &m))

	// The fixture must carry real diversity or the whole tier is
	// vacuous. If a re-capture produced a thinner window, fail here
	// rather than letting every downstream assertion trivially pass.
	require.NotZero(t, m.Frames)
	require.NotZero(t, m.CommitFrames)
	require.NotZero(t, m.Creates)
	require.NotZero(t, m.Updates)
	require.NotZero(t, m.Deletes)
	require.NotZero(t, m.Identity)
	require.NotZero(t, m.Account)
	require.Equal(t, m.SeqLast-m.SeqFirst+1, int64(m.Frames),
		"manifest seq range must match the frame count (contiguous window)")
	return m
}

// loadFrames returns the raw captured relay websocket frames in
// capture order: zstd([u32 LE length][frame bytes])...
func loadFrames(t *testing.T) [][]byte {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", "frames.bin.zst"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	zr, err := zstd.NewReader(f)
	require.NoError(t, err)
	t.Cleanup(zr.Close)

	var frames [][]byte
	var lenBuf [4]byte
	for {
		_, err := io.ReadFull(zr, lenBuf[:])
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		frame := make([]byte, binary.LittleEndian.Uint32(lenBuf[:]))
		_, err = io.ReadFull(zr, frame)
		require.NoError(t, err)
		frames = append(frames, frame)
	}
	require.NotEmpty(t, frames)
	return frames
}

// loadExpectedV1 returns the production Jetstream v1 JSON lines, one
// per v1-visible event, in relay (frame, op) order.
func loadExpectedV1(t *testing.T) []map[string]any {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", "expected_v1.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	var out []map[string]any
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var m map[string]any
		require.NoError(t, json.Unmarshal(line, &m))
		out = append(out, m)
	}
	require.NoError(t, sc.Err())
	require.NotEmpty(t, out)
	return out
}

// loadDIDDocs returns the verbatim DID documents captured for every
// commit DID in the window, keyed by DID.
func loadDIDDocs(t *testing.T) map[string][]byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "did_docs.json.zst"))
	require.NoError(t, err)
	zr, err := zstd.NewReader(bytes.NewReader(raw))
	require.NoError(t, err)
	defer zr.Close()
	decompressed, err := io.ReadAll(zr)
	require.NoError(t, err)

	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(decompressed, &m))
	require.NotEmpty(t, m)

	docs := make(map[string][]byte, len(m))
	for did, doc := range m {
		docs[did] = doc
	}
	return docs
}
