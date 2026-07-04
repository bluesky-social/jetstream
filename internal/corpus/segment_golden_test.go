package corpus

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/cbor"
	atmosrepo "github.com/jcalabro/atmos/repo"
	"github.com/stretchr/testify/require"
)

// corpusGoldenEvents derives a deterministic event stream from the
// real repo.car fixture: one KindCreate per record in MST key order,
// with pinned seqs and timestamps. Real network payload bytes, fully
// reproducible input.
func corpusGoldenEvents(t *testing.T) []segment.Event {
	t.Helper()

	f, err := os.Open(filepath.Join("testdata", "repo.car"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	rp, commit, err := atmosrepo.LoadCompleteFromCAR(f)
	require.NoError(t, err)

	var events []segment.Event
	seq := uint64(1)
	require.NoError(t, rp.Tree.Walk(func(key string, cid cbor.CID) error {
		payload, err := rp.Store.GetBlock(cid)
		require.NoError(t, err)
		collection, rkey, _ := strings.Cut(key, "/")
		events = append(events, segment.Event{
			Seq:         seq,
			WitnessedAt: 1_750_000_000_000_000 + int64(seq), // pinned, unix µs
			Kind:        segment.KindCreate,
			DID:         commit.DID,
			Collection:  collection,
			Rkey:        rkey,
			Rev:         commit.Rev,
			Payload:     payload,
		})
		seq++
		return nil
	}))
	require.NotEmpty(t, events)
	return events
}

// TestCorpusSegmentGolden pins the byte-exact sealed segment produced
// from real corpus payloads, and independently re-opens the committed
// golden file through the full reader path.
//
// This is the anti-symmetric-bug check for the segment codec (the
// mutation catalog's m009 class): the committed file's checksum and
// layout are facts produced by a past, known-good build. A bug that
// shifts both the writer and the reader identically — e.g. an
// off-by-one in the shared checksum range — passes every write-then-
// read-back test in the tree, but cannot reproduce the committed bytes
// (write side) and cannot validate the committed checksum (read side).
//
// Regenerate after an INTENTIONAL format change with:
//
//	go test ./internal/corpus -run TestCorpusSegmentGolden -update
//
// and justify the diff in the commit message; docs/README.md is the
// source of truth for the on-disk format.
func TestCorpusSegmentGolden(t *testing.T) {
	t.Parallel()

	events := corpusGoldenEvents(t)
	goldenPath := filepath.Join("testdata", "golden_corpus_segment.jss")

	// Write side: real payloads through the production writer.
	path := filepath.Join(t.TempDir(), "seg.jss")
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 8})
	require.NoError(t, err)
	for _, ev := range events {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
	got, err := os.ReadFile(path)
	require.NoError(t, err)

	if *updateGolden {
		require.NoError(t, os.WriteFile(goldenPath, got, 0o644))
		t.Logf("updated %s (%d bytes)", goldenPath, len(got))
		return
	}

	want, err := os.ReadFile(goldenPath)
	require.NoError(t, err, "missing golden segment; run with -update")
	require.Equal(t, want, got,
		"sealed segment bytes drifted from the committed golden (intentional format change? regenerate with -update and document it)")

	// Read side: the COMMITTED file (not the just-written one) must
	// open cleanly — checksum verified against stored bytes — and
	// decode to exactly the derived events.
	r, err := segment.Open(segment.ReaderConfig{Path: goldenPath})
	require.NoError(t, err, "committed golden segment failed to open (checksum/footer verification)")
	t.Cleanup(func() { _ = r.Close() })

	var decoded []segment.Event
	for i := range r.Blocks() {
		block, err := r.DecodeBlock(i)
		require.NoError(t, err)
		decoded = append(decoded, block...)
	}
	require.Equal(t, events, decoded, "committed golden decoded differently from the derived events")
}
