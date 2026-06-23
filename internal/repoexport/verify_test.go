package repoexport

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/identity"
	"github.com/stretchr/testify/require"
)

const testAuthoritativeRev = "2222222222222"

func TestVerify_MatchingRoots(t *testing.T) {
	t.Parallel()

	events := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
		createEvent(testDID, "app.bsky.feed.post", "r2", "rev2", payload("2")),
	}
	authoritativeRoot, _ := expectedRoot(t, events)
	relay := newAuthoritativeCommitServer(t, testDID, testAuthoritativeRev, authoritativeRoot)

	dataDir, st := newTestDataDir(t)
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), events)

	report, err := Verify(t.Context(), VerifyConfig{
		DataDir:          dataDir,
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
		Selector:         openSelector(t, dataDir),
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
	authoritativeRoot, _ := expectedRoot(t, authoritativeEvents)
	relay := newAuthoritativeCommitServer(t, testDID, testAuthoritativeRev, authoritativeRoot)

	// Locally, only the first record made it to disk; the second (the
	// just-created like) is still in the live writer's pending block.
	// Without folding pending events in, Verify reports a root mismatch
	// even though the local state is actually current.
	dataDir, st := newTestDataDir(t)
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), authoritativeEvents[:1])
	pending := authoritativeEvents[1:]

	report, err := Verify(t.Context(), VerifyConfig{
		DataDir:          dataDir,
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
		Selector:         openSelector(t, dataDir),
		PendingEvents:    pending,
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
	authoritativeRoot, _ := expectedRoot(t, authoritativeEvents)
	relay := newAuthoritativeCommitServer(t, testDID, testAuthoritativeRev, authoritativeRoot)

	localEvents := []segment.Event{
		createEvent(testDID, "app.bsky.feed.post", "r1", "rev1", payload("1")),
	}
	localRoot, localCount := expectedRoot(t, localEvents)
	dataDir, st := newTestDataDir(t)
	writeSegmentTree(t, st, filepath.Join(dataDir, "segments"), localEvents)

	report, err := Verify(t.Context(), VerifyConfig{
		DataDir:          dataDir,
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
		Selector:         openSelector(t, dataDir),
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
	authoritativeRoot, _ := expectedRoot(t, events)
	relay := newAuthoritativeCommitServer(t, testDID, testAuthoritativeRev, authoritativeRoot)

	emptyDir := t.TempDir()
	report, err := Verify(t.Context(), VerifyConfig{
		DataDir:          emptyDir,
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
		Selector:         openSelector(t, emptyDir),
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

func TestVerify_MalformedLatestCommitCIDReturnsError(t *testing.T) {
	t.Parallel()

	relay := newMalformedLatestCommitServer(t, testDID)

	_, err := Verify(t.Context(), VerifyConfig{
		DataDir:          t.TempDir(),
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
	})
	require.Error(t, err)
}

func TestVerify_MissingAuthoritativeCommitBlockReturnsError(t *testing.T) {
	t.Parallel()

	root := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root"))
	commitCID, _ := authoritativeCommitCAR(t, testDID, testAuthoritativeRev, root)
	otherCID, otherCAR := authoritativeCommitCAR(t, testDID, testAuthoritativeRev, cbor.ComputeCID(cbor.CodecDagCBOR, []byte("other-root")))
	require.False(t, otherCID.Equal(commitCID))

	relay := newCommitBlockServer(t, testDID, testAuthoritativeRev, commitCID, otherCAR)
	_, err := Verify(t.Context(), VerifyConfig{
		DataDir:          t.TempDir(),
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
	})
	require.ErrorContains(t, err, "not found in getBlocks response")
}

func TestVerify_AuthoritativeCommitDIDMismatchReturnsError(t *testing.T) {
	t.Parallel()

	root := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root"))
	commitCID, commitCAR := authoritativeCommitCAR(t, "did:plc:other", testAuthoritativeRev, root)
	relay := newCommitBlockServer(t, testDID, testAuthoritativeRev, commitCID, commitCAR)

	_, err := Verify(t.Context(), VerifyConfig{
		DataDir:          t.TempDir(),
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
	})
	require.ErrorContains(t, err, "authoritative commit DID mismatch")
}

func TestVerify_AuthoritativeCommitRevMismatchReturnsError(t *testing.T) {
	t.Parallel()

	root := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root"))
	commitCID, commitCAR := authoritativeCommitCAR(t, testDID, "2222222222223", root)
	relay := newCommitBlockServer(t, testDID, testAuthoritativeRev, commitCID, commitCAR)

	_, err := Verify(t.Context(), VerifyConfig{
		DataDir:          t.TempDir(),
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
	})
	require.ErrorContains(t, err, "authoritative commit rev mismatch")
}

func TestVerify_HTTPFailureReturnsError(t *testing.T) {
	t.Parallel()

	relay := newGetLatestCommitErrorServer(t, http.StatusInternalServerError)

	_, err := Verify(t.Context(), VerifyConfig{
		DataDir:          t.TempDir(),
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
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

func newAuthoritativeCommitServer(t *testing.T, did string, rev string, root cbor.CID) *httptest.Server {
	t.Helper()

	commitCID, commitCAR := authoritativeCommitCAR(t, did, rev, root)
	return newCommitBlockServer(t, did, rev, commitCID, commitCAR)
}

type testIdentityResolver struct {
	did    string
	pdsURL string
}

func newTestIdentityResolver(did, pdsURL string) *testIdentityResolver {
	return &testIdentityResolver{did: did, pdsURL: pdsURL}
}

func (r *testIdentityResolver) ResolveDID(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	if string(did) != r.did {
		return nil, identity.ErrDIDNotFound
	}
	return &identity.DIDDocument{
		ID: r.did,
		Service: []identity.Service{
			{
				ID:              r.did + "#atproto_pds",
				Type:            "AtprotoPersonalDataServer",
				ServiceEndpoint: r.pdsURL,
			},
		},
	}, nil
}

func (r *testIdentityResolver) ResolveHandle(_ context.Context, _ atmos.Handle) (atmos.DID, error) {
	return "", identity.ErrHandleNotFound
}

func newCommitBlockServer(t *testing.T, did string, rev string, commitCID cbor.CID, commitCAR []byte) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		switch r.URL.Path {
		case "/xrpc/com.atproto.sync.getLatestCommit":
			if got := r.URL.Query().Get("did"); got != did {
				http.Error(rw, "unexpected did", http.StatusBadRequest)
				return
			}

			rw.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(rw).Encode(map[string]string{
				"rev": rev,
				"cid": commitCID.String(),
			})
		case "/xrpc/com.atproto.sync.getBlocks":
			if got := r.URL.Query().Get("did"); got != did {
				http.Error(rw, "unexpected did", http.StatusBadRequest)
				return
			}
			if got := r.URL.Query()["cids"]; len(got) != 1 || got[0] != commitCID.String() {
				http.Error(rw, "unexpected cids", http.StatusBadRequest)
				return
			}

			rw.Header().Set("Content-Type", "application/vnd.ipld.car")
			_, _ = rw.Write(commitCAR)
		default:
			http.NotFound(rw, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func authoritativeCommitCAR(t *testing.T, did string, rev string, root cbor.CID) (cbor.CID, []byte) {
	t.Helper()

	commitData := cbor.AppendMapHeader(nil, 6)
	commitData = cbor.AppendText(commitData, "did")
	commitData = cbor.AppendText(commitData, did)
	commitData = cbor.AppendText(commitData, "rev")
	commitData = cbor.AppendText(commitData, rev)
	commitData = cbor.AppendText(commitData, "sig")
	commitData = cbor.AppendBytes(commitData, []byte{1, 2, 3})
	commitData = cbor.AppendText(commitData, "data")
	commitData = cbor.AppendCIDLink(commitData, &root)
	commitData = cbor.AppendText(commitData, "prev")
	commitData = cbor.AppendNull(commitData)
	commitData = cbor.AppendText(commitData, "version")
	commitData = cbor.AppendUint(commitData, 3)

	commitCID := cbor.ComputeCID(cbor.CodecDagCBOR, commitData)
	var buf bytes.Buffer
	writeCARAllowEmptyRoots(t, &buf, nil, []car.Block{{CID: commitCID, Data: commitData}})
	return commitCID, buf.Bytes()
}

func writeCARAllowEmptyRoots(t *testing.T, buf *bytes.Buffer, roots []cbor.CID, blocks []car.Block) {
	t.Helper()

	header := cbor.AppendMapHeader(nil, 2)
	header = cbor.AppendText(header, "roots")
	header = cbor.AppendArrayHeader(header, uint64(len(roots)))
	for i := range roots {
		header = cbor.AppendCIDLink(header, &roots[i])
	}
	header = cbor.AppendText(header, "version")
	header = cbor.AppendUint(header, 1)

	_, err := buf.Write(cbor.AppendUvarint(nil, uint64(len(header))))
	require.NoError(t, err)
	_, err = buf.Write(header)
	require.NoError(t, err)

	for _, block := range blocks {
		blockLen := uint64(cbor.CIDByteLen(&block.CID) + len(block.Data))
		_, err = buf.Write(cbor.AppendUvarint(nil, blockLen))
		require.NoError(t, err)
		_, err = buf.Write(block.CID.AppendBytes(nil))
		require.NoError(t, err)
		_, err = buf.Write(block.Data)
		require.NoError(t, err)
	}
}

func newMalformedLatestCommitServer(t *testing.T, did string) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != "/xrpc/com.atproto.sync.getLatestCommit" {
			http.NotFound(rw, r)
			return
		}
		if got := r.URL.Query().Get("did"); got != did {
			http.Error(rw, "unexpected did", http.StatusBadRequest)
			return
		}

		rw.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(rw).Encode(map[string]string{
			"rev": testAuthoritativeRev,
			"cid": "not-a-cid",
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newGetLatestCommitErrorServer(t *testing.T, status int) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		http.Error(rw, "upstream error", status)
	}))
	t.Cleanup(srv.Close)
	return srv
}
