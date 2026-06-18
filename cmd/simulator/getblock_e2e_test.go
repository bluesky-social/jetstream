package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"log/slog"
	"math/rand/v2"
	"net/http/httptest"

	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

func getForTest(t *testing.T, ctx context.Context, url string) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)
	return http.DefaultClient.Do(req)
}

// TestEndToEnd_GetBlockMatchesOracle is the headline correctness test for the
// getBlock endpoint. It boots the simulator, spawns jetstream as a subprocess
// pointed at it, lets ingest drain to steady-state and seal at least one
// segment, then for every sealed segment (enumerated via listSegments) and
// every block index in it, verifies the served getBlock response against an
// independent ("oracle") decode of the on-disk segment file.
//
// For each block it asserts:
//  1. getBlock returns 200 with Content-Type application/octet-stream.
//  2. The served body is byte-identical to segment.ReadBlockFrame on an
//     independently-opened fd + segment.ReadSealedHeader.
//  3. The ETag equals "%q" of fmt.Sprintf("%016x:%d", hdr.Checksum, idx).
//  4. A second request carrying If-None-Match: <ETag> returns 304.
//
// Plus negatives: blockIndex == blockCount -> 404; an unknown but well-formed
// segment name -> 404.
//
// Decode-equivalence (brief sub-check (d)) is intentionally NOT duplicated
// here: the segment package's frame decoder (decodeBlockCompressedSized) is
// unexported and there is no exported frame-decode entrypoint usable from an
// external test, so per the brief we rely on assertion (2)'s byte-identity plus
// Task 1's blockframe_test.go, which already proves a ReadBlockFrame frame
// decodes identically to Reader.DecodeBlock.
//
// Heavy test (subprocess + backfill drain + seal): skipped under -short.
func TestEndToEnd_GetBlockMatchesOracle(t *testing.T) {
	if testing.Short() {
		t.Skip("heavy e2e test: spawns jetstream subprocess and waits for a sealed segment")
	}
	t.Parallel()

	// --- Build the simulator world directly (mirrors e2e_test.go). ---
	cfg := world.DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Accounts = 25
	cfg.InitialRecords = 1
	// 50 cps × 25 accounts ≈ 2 cps/DID, well under atmos's per-DID FIFO
	// queue capacity. See TestEndToEnd_JetstreamConsumesSimulator for the
	// full rationale on why we keep the per-DID rate low.
	cfg.CommitsPerSec = 50
	w, err := world.New(context.Background(), cfg)
	require.NoError(t, err)
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(99, 100)), fanout.New(64)))

	simSrv := httptest.NewServer(nil)
	simSrv.Config.Handler = simhttp.NewHandler(w, simSrv.URL)
	defer simSrv.Close()

	// Run live traffic concurrently. Cancel before closing the world to
	// avoid "pebble: closed" panics.
	trafficCtx, trafficCancel := context.WithCancel(context.Background())
	trafficDone := make(chan struct{})
	defer func() {
		trafficCancel()
		<-trafficDone
		_ = w.Close()
	}()
	go func() {
		_ = w.RunTraffic(trafficCtx, slog.Default())
		close(trafficDone)
	}()

	// --- Spawn jetstream pointed at the simulator. ---
	binPath := buildJetstreamForTest(t)

	jetDir := filepath.Join(t.TempDir(), "jetstream-data")

	// Generous deadline: we must wait long enough for live ingest to fill and
	// SEAL at least one segment, not just reach steady-state.
	jetCtx, jetCancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer jetCancel()

	jetAddr := freePortAddr(t)
	jetDebug := freePortAddr(t)

	cmd := newJetstreamCmd(jetCtx, binPath, []string{
		"serve",
		"--addr", jetAddr,
		"--debug-addr", jetDebug,
		"--data-dir", jetDir,
		"--relay-url", simSrv.URL,
		"--plc-url", simSrv.URL,
		"--shutdown-timeout=5s",
	})
	stderr := &lockedBuffer{}
	cmd.Stdout = stderr
	cmd.Stderr = stderr
	require.NoError(t, cmd.Start())
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	// Wait for jetstream's /subscribe to start serving (backfill drained →
	// steady-state). The getBlock/listSegments routes share the same readiness
	// gate, so a successful websocket dial also means those routes are live.
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		conn, resp, err := websocket.Dial(ctx, "ws://"+jetAddr+"/subscribe", nil)
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		if err != nil {
			return false
		}
		_ = conn.Close(websocket.StatusNormalClosure, "probe")
		return true
	}, 45*time.Second, 250*time.Millisecond,
		"jetstream did not become ready; logs:\n%s", stderr.String())

	segDir := filepath.Join(jetDir, "segments")
	listURL := "http://" + jetAddr + "/xrpc/network.bsky.jetstream.listSegments?limit=1000"

	// Steady-state pins to the live consumer, but segments only appear in
	// listSegments once they are SEALED. Live ingest seals a segment when it
	// fills, so poll listSegments until at least one sealed segment exists
	// rather than asserting immediately (which would race the first seal).
	var list struct {
		Segments []struct {
			Name  string `json:"name"`
			Index int64  `json:"index"`
		} `json:"segments"`
	}
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		resp, err := getForTest(t, ctx, listURL)
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return false
		}
		var decoded struct {
			Segments []struct {
				Name  string `json:"name"`
				Index int64  `json:"index"`
			} `json:"segments"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
			return false
		}
		if len(decoded.Segments) == 0 {
			return false
		}
		list = decoded
		return true
	}, 90*time.Second, time.Second,
		"no sealed segment appeared in listSegments; logs:\n%s", stderr.String())

	require.NotEmpty(t, list.Segments, "expected at least one sealed segment")

	for _, seg := range list.Segments {
		segPath := filepath.Join(segDir, seg.Name)

		// Oracle side: open the file independently. Reader.Blocks() gives the
		// authoritative block count; an independent fd + ReadSealedHeader feed
		// ReadBlockFrame for the byte-identity check.
		r, err := segment.Open(segment.ReaderConfig{Path: segPath})
		require.NoErrorf(t, err, "oracle open %s", seg.Name)
		blockCount := len(r.Blocks())
		require.Positivef(t, blockCount, "%s should have at least one block", seg.Name)

		f, err := os.Open(segPath)
		require.NoErrorf(t, err, "open %s", seg.Name)
		hdr, err := segment.ReadSealedHeader(f)
		require.NoErrorf(t, err, "read sealed header %s", seg.Name)
		require.EqualValuesf(t, blockCount, hdr.BlockCount,
			"%s: Reader.Blocks()=%d vs header.BlockCount=%d", seg.Name, blockCount, hdr.BlockCount)

		for idx := range blockCount {
			url := fmt.Sprintf("http://%s/xrpc/network.bsky.jetstream.getBlock?segment=%s&blockIndex=%d",
				jetAddr, seg.Name, idx)

			resp, err := getForTest(t, t.Context(), url)
			require.NoErrorf(t, err, "%s block %d GET", seg.Name, idx)
			require.Equalf(t, http.StatusOK, resp.StatusCode, "%s block %d status", seg.Name, idx)

			// (1) Content-Type.
			require.Equalf(t, "application/octet-stream", resp.Header.Get("Content-Type"),
				"%s block %d content-type", seg.Name, idx)

			body, err := io.ReadAll(resp.Body)
			require.NoErrorf(t, err, "%s block %d read body", seg.Name, idx)
			etag := resp.Header.Get("ETag")
			_ = resp.Body.Close()

			// (2) served bytes == raw stored frame (oracle).
			wantFrame, err := segment.ReadBlockFrame(f, hdr, idx)
			require.NoErrorf(t, err, "%s block %d ReadBlockFrame", seg.Name, idx)
			require.Equalf(t, wantFrame, body, "%s block %d frame bytes", seg.Name, idx)

			// (3) ETag == "%q" of "%016x:%d".
			require.Equalf(t,
				fmt.Sprintf("%q", fmt.Sprintf("%016x:%d", hdr.Checksum, idx)),
				etag, "%s block %d etag", seg.Name, idx)

			// (4) second request with If-None-Match -> 304.
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
			require.NoError(t, err)
			req.Header.Set("If-None-Match", etag)
			resp2, err := http.DefaultClient.Do(req)
			require.NoErrorf(t, err, "%s block %d revalidate", seg.Name, idx)
			require.Equalf(t, http.StatusNotModified, resp2.StatusCode,
				"%s block %d revalidate status", seg.Name, idx)
			_ = resp2.Body.Close()
		}

		// Negative: blockIndex == blockCount -> 404 BlockNotFound.
		nf, err := getForTest(t, t.Context(), fmt.Sprintf(
			"http://%s/xrpc/network.bsky.jetstream.getBlock?segment=%s&blockIndex=%d",
			jetAddr, seg.Name, blockCount))
		require.NoError(t, err)
		require.Equalf(t, http.StatusNotFound, nf.StatusCode,
			"%s out-of-range blockIndex %d", seg.Name, blockCount)
		_ = nf.Body.Close()

		_ = f.Close()
		_ = r.Close()
	}

	// Negative: a well-formed but absent segment name -> 404 SegmentNotFound.
	// seg_9999999999.jss parses as a valid segment index (so it passes the
	// ParseSegmentIndex bad-request gate) but no such segment exists.
	unknown, err := getForTest(t, t.Context(), "http://"+jetAddr+
		"/xrpc/network.bsky.jetstream.getBlock?segment=seg_9999999999.jss&blockIndex=0")
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, unknown.StatusCode)
	_ = unknown.Body.Close()
}
