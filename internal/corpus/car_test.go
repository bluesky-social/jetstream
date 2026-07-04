package corpus

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	atmosrepo "github.com/jcalabro/atmos/repo"
	"github.com/stretchr/testify/require"
)

// expectedRecord is one row of the goat-derived repo listing:
// "collection/rkey\tCID" per line. goat (bluesky-social/indigo) walked
// the MST and derived each record CID independently of atmos.
type expectedRecord struct {
	collection string
	rkey       string
	cid        string
}

func loadCARExpectations(t *testing.T) (records []expectedRecord, did, rev string) {
	t.Helper()

	f, err := os.Open(filepath.Join("testdata", "car_expected_records.tsv"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		path, cid, ok := strings.Cut(line, "\t")
		require.True(t, ok, "malformed listing line %q", line)
		collection, rkey, ok := strings.Cut(path, "/")
		require.True(t, ok, "malformed record path %q", path)
		records = append(records, expectedRecord{collection: collection, rkey: rkey, cid: cid})
	}
	require.NoError(t, sc.Err())
	require.NotEmpty(t, records)

	commitTxt, err := os.ReadFile(filepath.Join("testdata", "car_expected_commit.txt"))
	require.NoError(t, err)
	for line := range strings.Lines(string(commitTxt)) {
		if v, ok := strings.CutPrefix(line, "DID: "); ok {
			did = strings.TrimSpace(v)
		}
		if v, ok := strings.CutPrefix(line, "Revision: "); ok {
			rev = strings.TrimSpace(v)
		}
	}
	require.NotEmpty(t, did)
	require.NotEmpty(t, rev)
	return records, did, rev
}

// TestCorpusCARBackfill feeds a real production getRepo CAR through the
// production backfill path (atmos CAR/MST parse → SegmentHandler →
// ingest.Writer → segment files) and compares the archived rows to a
// record listing pinned by goat (bluesky-social/indigo) at capture
// time. Record CIDs are recomputed from the archived payload bytes, so
// agreement proves atmos's CAR block extraction, MST walk, and CID
// derivation against indigo's on the same real repo.
func TestCorpusCARBackfill(t *testing.T) {
	t.Parallel()

	expected, did, rev := loadCARExpectations(t)

	carFile, err := os.Open(filepath.Join("testdata", "repo.car"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = carFile.Close() })

	rp, commit, err := atmosrepo.LoadCompleteFromCAR(carFile)
	require.NoError(t, err)
	require.Equal(t, did, commit.DID, "CAR commit DID vs goat inspect")
	require.Equal(t, rev, commit.Rev, "CAR commit rev vs goat inspect")

	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	dir := filepath.Join(t.TempDir(), "segments")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	w, err := ingest.Open(ingest.Config{
		SegmentsDir: dir,
		Store:       st,
		Logger:      logger,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	h := backfill.NewSegmentHandler(w, logger, nil)
	require.NoError(t, h.HandleRepo(t.Context(), atmos.DID(did), rp, commit))
	require.NoError(t, w.Close())

	events := readAllSegmentEvents(t, dir)
	require.Len(t, events, len(expected), "archived row count vs goat listing")

	got := make(map[string]segment.Event, len(events))
	for _, ev := range events {
		require.Equal(t, segment.KindCreate, segment.Kind(ev.Kind))
		require.Equal(t, did, ev.DID)
		require.Equal(t, rev, ev.Rev, "backfill rows carry the commit rev")
		key := ev.Collection + "/" + ev.Rkey
		_, dup := got[key]
		require.False(t, dup, "duplicate archived record %q", key)
		got[key] = ev
	}

	for _, want := range expected {
		ev, ok := got[want.collection+"/"+want.rkey]
		require.True(t, ok, "missing archived record %s/%s", want.collection, want.rkey)
		gotCID := cbor.ComputeCID(cbor.CodecDagCBOR, ev.Payload).String()
		require.Equal(t, want.cid, gotCID,
			"record CID for %s/%s: goat-pinned vs recomputed from archived payload", want.collection, want.rkey)
	}
}

// TestCorpusCARTruncated derives a malformed variant from the real CAR
// — the bytes cut mid-stream — and requires the backfill parse path to
// reject it with an error rather than succeed with partial data. A
// production PDS closing the connection mid-getRepo must never
// materialize as a silently half-archived repo.
func TestCorpusCARTruncated(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(filepath.Join("testdata", "repo.car"))
	require.NoError(t, err)

	for _, frac := range []int{2, 4} {
		cut := len(raw) / frac
		_, _, err := atmosrepo.LoadCompleteFromCAR(strings.NewReader(string(raw[:cut])))
		require.Errorf(t, err, "truncated CAR (%d/%d bytes) must not parse as complete", cut, len(raw))
	}
}

// TestCorpusCARCorrupted flips one byte inside a record block of the
// real CAR. Either the parse fails (CID mismatch detected by block
// hashing) or — if the corruption lands in framing the parser
// tolerates — the archived records must not silently disagree with the
// goat listing. Fail-loud over corrupt.
func TestCorpusCARCorrupted(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(filepath.Join("testdata", "repo.car"))
	require.NoError(t, err)

	// Flip a byte two-thirds into the file: past the header and root
	// block, inside record data.
	pos := len(raw) * 2 / 3
	mutated := append([]byte(nil), raw...)
	mutated[pos] ^= 0xff

	rp, commit, err := atmosrepo.LoadCompleteFromCAR(strings.NewReader(string(mutated)))
	if err != nil {
		return // rejected at parse: the strong outcome
	}

	// Parse survived; the MST walk must then fail or every archived
	// record must still match its goat-pinned CID.
	expected, _, _ := loadCARExpectations(t)
	pinned := make(map[string]string, len(expected))
	for _, e := range expected {
		pinned[e.collection+"/"+e.rkey] = e.cid
	}
	err = rp.Tree.Walk(func(key string, cid cbor.CID) error {
		payload, gerr := rp.Store.GetBlock(cid)
		if gerr != nil {
			return gerr
		}
		want, ok := pinned[key]
		if !ok {
			return fmt.Errorf("unexpected record %q in corrupted CAR", key)
		}
		if got := cbor.ComputeCID(cbor.CodecDagCBOR, payload).String(); got != want {
			return fmt.Errorf("record %q CID drifted under corruption: %s != %s", key, got, want)
		}
		return nil
	})
	require.Error(t, err,
		"corrupted CAR parsed cleanly AND matched every pinned CID; corruption was silently absorbed (commit rev=%s)", commit.Rev)
}
