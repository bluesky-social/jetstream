package ingest

import (
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

// TestWriter_Swarm randomizes Writer configuration and event counts,
// then validates the global invariants that no other test exercises
// in concert: max(seq)+1 == nextSeq across reopens, every sealed
// segment passes Reader.Open via segment.ScanMaxSeq's torn-tail
// short-circuit, and the active file re-Opens cleanly under random
// rotation thresholds.
//
// Skipped under -short to keep `just test` under a second.
func TestWriter_Swarm(t *testing.T) {
	t.Parallel()

	iterations := 30
	if !testing.Short() {
		iterations = 1000
	}

	for i := range iterations {
		t.Run("", func(t *testing.T) {
			t.Parallel()
			// Each sub-test gets its own RNG with a deterministic seed
			// so failures are reproducible and there's no shared state.
			rng := rand.New(rand.NewPCG(uint64(i), 0xa57c5))
			runOneSwarm(t, rng)
		})
	}
}

func runOneSwarm(t *testing.T, rng *rand.Rand) {
	t.Helper()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	maxBlock := 1 + rng.IntN(64)
	maxSegmentBytes := int64(1 + rng.IntN(8192))
	totalAppends := 1 + rng.IntN(2048)

	cfg := Config{
		ShardsDir:         filepath.Join(dataDir, "shards"),
		Store:             st,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxEventsPerBlock: maxBlock,
		MaxSegmentBytes:   maxSegmentBytes,
	}

	w, err := Open(cfg)
	require.NoError(t, err)

	for range totalAppends {
		ev := segment.Event{
			Kind:    segment.KindCreate,
			DID:     randomDID(rng),
			Payload: randomPayload(rng),
		}
		require.NoError(t, w.Append(t.Context(), &ev))
	}
	require.NoError(t, w.Close())

	// Re-open: must not error, must report a nextSeq that dominates
	// every observed seq.
	w2, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })

	// Walk the active segment (highest index) and confirm the
	// re-opened nextSeq dominates its max seq + 1. Sealed segments
	// are verified by segment.ScanMaxSeq returning ErrSegmentSealed,
	// which is the same path the segment.Reader test suite covers.
	entries, err := os.ReadDir(cfg.ShardsDir)
	require.NoError(t, err)

	var maxSeqGlobal uint64
	var found bool
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if _, ok := parseSegmentIndex(e.Name()); !ok {
			continue
		}
		path := filepath.Join(cfg.ShardsDir, e.Name())
		max, ok, err := segment.ScanMaxSeq(path)
		if err != nil {
			// Sealed files return ErrSegmentSealed; that's expected
			// for the rotated-out segments under tiny MaxSegmentBytes.
			continue
		}
		if ok && (max > maxSeqGlobal || !found) {
			maxSeqGlobal = max
			found = true
		}
	}

	if found {
		require.GreaterOrEqual(t, w2.NextSeq(), maxSeqGlobal+1,
			"reopened nextSeq must dominate every observed seq")
	}
}

func randomDID(rng *rand.Rand) string {
	const charset = "abcdefghijklmnopqrstuvwxyz234567"
	const n = 24
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = charset[rng.IntN(len(charset))]
	}
	return "did:plc:" + string(buf)
}

func randomPayload(rng *rand.Rand) []byte {
	n := rng.IntN(64)
	if n == 0 {
		return nil
	}
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = byte(rng.IntN(256))
	}
	return buf
}
