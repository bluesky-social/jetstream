package repoexport

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
	"github.com/stretchr/testify/require"
)

const testAuthoritativeRev = "2222222222222"

func TestVerify_MatchingRoots(t *testing.T) {
	t.Parallel()

	events := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
		createEvent(testDID, "app.bsky.feed.post", "r2", "rev2", payload("2")),
	}
	carBytes, authoritativeRoot := authoritativeCAR(t, testDID, testAuthoritativeRev, events)
	relay := newGetRepoServer(t, testDID, carBytes)

	dataDir, st := newTestDataDir(t)
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), events)

	report, err := Verify(t.Context(), VerifyConfig{
		DataDir:  dataDir,
		DID:      testDID,
		RelayURL: relay.URL,
		Selector: openSelector(t, dataDir),
	})
	require.NoError(t, err)
	require.Equal(t, VerifyReport{
		DID:               testDID,
		Match:             true,
		AuthoritativeRev:  testAuthoritativeRev,
		AuthoritativeRoot: authoritativeRoot.String(),
		LocalLatestRev:    "rev2",
		LocalRoot:         authoritativeRoot.String(),
		LocalRecordCount:  2,
	}, report)
}

func TestVerify_PendingEventsMatchAuthoritativeRoot(t *testing.T) {
	t.Parallel()

	// The authoritative repo already reflects both records.
	authoritativeEvents := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
		createEvent(testDID, "app.bsky.feed.like", "r2", "rev2", payload("2")),
	}
	carBytes, authoritativeRoot := authoritativeCAR(t, testDID, testAuthoritativeRev, authoritativeEvents)
	relay := newGetRepoServer(t, testDID, carBytes)

	// Locally, only the first record made it to disk; the second (the
	// just-created like) is still in the live writer's pending block.
	// Without folding pending events in, Verify reports a root mismatch
	// even though the local state is actually current.
	dataDir, st := newTestDataDir(t)
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), authoritativeEvents[:1])
	pending := authoritativeEvents[1:]

	report, err := Verify(t.Context(), VerifyConfig{
		DataDir:       dataDir,
		DID:           testDID,
		RelayURL:      relay.URL,
		Selector:      openSelector(t, dataDir),
		PendingEvents: pending,
	})
	require.NoError(t, err)
	require.True(t, report.Match, "expected pending like to close the gap; message=%q", report.Message)
	require.Equal(t, authoritativeRoot.String(), report.LocalRoot)
	require.Equal(t, "rev2", report.LocalLatestRev)
	require.Equal(t, 2, report.LocalRecordCount)
}

func TestVerify_MismatchingRootsReturnsReport(t *testing.T) {
	t.Parallel()

	authoritativeEvents := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
		createEvent(testDID, "app.bsky.feed.post", "r2", "rev2", payload("2")),
	}
	carBytes, authoritativeRoot := authoritativeCAR(t, testDID, testAuthoritativeRev, authoritativeEvents)
	relay := newGetRepoServer(t, testDID, carBytes)

	localEvents := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
	}
	localRoot, localCount := expectedRoot(t, localEvents)
	dataDir, st := newTestDataDir(t)
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), localEvents)

	report, err := Verify(t.Context(), VerifyConfig{
		DataDir:  dataDir,
		DID:      testDID,
		RelayURL: relay.URL,
		Selector: openSelector(t, dataDir),
	})
	require.NoError(t, err)
	require.Equal(t, VerifyReport{
		DID:               testDID,
		Match:             false,
		AuthoritativeRev:  testAuthoritativeRev,
		AuthoritativeRoot: authoritativeRoot.String(),
		LocalLatestRev:    "rev1",
		LocalRoot:         localRoot.String(),
		LocalRecordCount:  localCount,
		Message:           "local reconstructed MST root does not match authoritative repo root",
	}, report)
}

func TestVerify_MissingLocalRepoReturnsMismatchReport(t *testing.T) {
	t.Parallel()

	events := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
	}
	carBytes, authoritativeRoot := authoritativeCAR(t, testDID, testAuthoritativeRev, events)
	relay := newGetRepoServer(t, testDID, carBytes)

	emptyDir := t.TempDir()
	report, err := Verify(t.Context(), VerifyConfig{
		DataDir:  emptyDir,
		DID:      testDID,
		RelayURL: relay.URL,
		Selector: openSelector(t, emptyDir),
	})
	require.NoError(t, err)
	require.Equal(t, testDID, report.DID)
	require.False(t, report.Match)
	require.Equal(t, testAuthoritativeRev, report.AuthoritativeRev)
	require.Equal(t, authoritativeRoot.String(), report.AuthoritativeRoot)
	require.Empty(t, report.LocalLatestRev)
	require.Empty(t, report.LocalRoot)
	require.Zero(t, report.LocalRecordCount)
	require.Contains(t, report.Message, "no local commit events")
}

func TestVerify_MalformedAuthoritativeCARReturnsError(t *testing.T) {
	t.Parallel()

	relay := newGetRepoServer(t, testDID, []byte("not a car"))

	_, err := Verify(t.Context(), VerifyConfig{
		DataDir:  t.TempDir(),
		DID:      testDID,
		RelayURL: relay.URL,
	})
	require.Error(t, err)
}

func TestVerify_AuthoritativeDIDMismatchReturnsError(t *testing.T) {
	t.Parallel()

	authoritativeDID := "did:plc:otherrepo"
	events := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
	}
	carBytes, _ := authoritativeCAR(t, authoritativeDID, testAuthoritativeRev, events)
	relay := newGetRepoServer(t, testDID, carBytes)

	_, err := Verify(t.Context(), VerifyConfig{
		DataDir:  t.TempDir(),
		DID:      testDID,
		RelayURL: relay.URL,
	})
	require.Error(t, err)
	require.ErrorContains(t, err, testDID)
	require.ErrorContains(t, err, authoritativeDID)
}

func TestVerify_HTTPFailureReturnsError(t *testing.T) {
	t.Parallel()

	relay := newGetRepoErrorServer(t, http.StatusInternalServerError)

	_, err := Verify(t.Context(), VerifyConfig{
		DataDir:  t.TempDir(),
		DID:      testDID,
		RelayURL: relay.URL,
	})
	require.Error(t, err)
}

func TestVerify_ValidatesConfig(t *testing.T) {
	t.Parallel()

	_, err := Verify(t.Context(), VerifyConfig{DID: testDID})
	require.ErrorContains(t, err, "DataDir is required")

	_, err = Verify(t.Context(), VerifyConfig{DataDir: t.TempDir()})
	require.ErrorContains(t, err, "DID is required")
}

func authoritativeCAR(t *testing.T, did, rev string, events []segment.Event) ([]byte, cbor.CID) {
	t.Helper()

	blocks := mst.NewMemBlockStore()
	tree := mst.NewTree(blocks)
	for _, ev := range events {
		key := ev.Collection + "/" + ev.Rkey
		switch ev.Kind {
		case segment.KindCreate, segment.KindUpdate, segment.KindCreateResync:
			cid := cbor.ComputeCID(cbor.CodecDagCBOR, ev.Payload)
			require.NoError(t, blocks.PutBlock(cid, append([]byte(nil), ev.Payload...)))
			require.NoError(t, tree.Insert(key, cid))
		case segment.KindDelete:
			require.NoError(t, tree.Remove(key))
		}
	}
	root, err := tree.WriteBlocks(blocks)
	require.NoError(t, err)

	commit := &repo.Commit{
		DID:     did,
		Version: 3,
		Data:    root,
		Rev:     rev,
		Sig:     []byte{1, 2, 3},
	}
	commitBytes, err := commit.EncodeCBOR()
	require.NoError(t, err)
	commitCID := cbor.ComputeCID(cbor.CodecDagCBOR, commitBytes)
	require.NoError(t, blocks.PutBlock(commitCID, commitBytes))

	carBlocks := make([]car.Block, 0)
	for cid, data := range blocks.All() {
		carBlocks = append(carBlocks, car.Block{
			CID:  cid,
			Data: append([]byte(nil), data...),
		})
	}
	sort.Slice(carBlocks, func(i, j int) bool {
		return carBlocks[i].CID.String() < carBlocks[j].CID.String()
	})

	var buf bytes.Buffer
	require.NoError(t, car.WriteAll(&buf, []cbor.CID{commitCID}, carBlocks))
	return buf.Bytes(), root
}

func newGetRepoServer(t *testing.T, did string, carBytes []byte) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != "/xrpc/com.atproto.sync.getRepo" {
			http.NotFound(rw, r)
			return
		}
		if got := r.URL.Query().Get("did"); got != did {
			http.Error(rw, "unexpected did", http.StatusBadRequest)
			return
		}

		rw.Header().Set("Content-Type", "application/vnd.ipld.car")
		_, _ = rw.Write(carBytes)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newGetRepoErrorServer(t *testing.T, status int) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		http.Error(rw, "upstream error", status)
	}))
	t.Cleanup(srv.Close)
	return srv
}
