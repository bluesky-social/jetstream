package repoexport

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

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
		Archive:          openArchive(t, dataDir),
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
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
		Archive:          openArchive(t, dataDir),
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
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
		Archive:          openArchive(t, dataDir),
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
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
		Archive:          openArchive(t, emptyDir),
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
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
		Archive:          openArchive(t, t.TempDir()),
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
		Archive:          openArchive(t, t.TempDir()),
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
		Archive:          openArchive(t, t.TempDir()),
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
		Archive:          openArchive(t, t.TempDir()),
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
	})
	require.ErrorContains(t, err, "authoritative commit rev mismatch")
}

func TestVerify_HTTPFailureReturnsError(t *testing.T) {
	t.Parallel()

	relay := newGetLatestCommitErrorServer(t, http.StatusInternalServerError)

	_, err := Verify(t.Context(), VerifyConfig{
		Archive:          openArchive(t, t.TempDir()),
		DID:              testDID,
		IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
	})
	require.Error(t, err)
}

func TestVerify_CancelsBlockedAuthoritativeCARRead(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
	}{
		{name: "caller cancellation interrupts a partial CAR block"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root"))
			commitCID, commitCAR := authoritativeCommitCAR(t, testDID, testAuthoritativeRev, root)
			headerLen, headerPrefixLen := binary.Uvarint(commitCAR)
			require.Greater(t, headerPrefixLen, 0)
			blockLengthOffset := headerPrefixLen + int(headerLen)
			_, blockLengthPrefixLen := binary.Uvarint(commitCAR[blockLengthOffset:])
			require.Greater(t, blockLengthPrefixLen, 0)
			partialEnd := blockLengthOffset + blockLengthPrefixLen + 1
			require.Less(t, partialEnd, len(commitCAR))

			blockReadStarted := make(chan struct{})
			relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/xrpc/com.atproto.sync.getLatestCommit":
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]string{"rev": testAuthoritativeRev, "cid": commitCID.String()})
				case "/xrpc/com.atproto.sync.getBlocks":
					w.Header().Set("Content-Type", "application/vnd.ipld.car")
					_, _ = w.Write(commitCAR[:partialEnd])
					if err := http.NewResponseController(w).Flush(); err != nil {
						http.Error(w, "flush failed", http.StatusInternalServerError)
						return
					}
					close(blockReadStarted)
					<-r.Context().Done()
				default:
					http.NotFound(w, r)
				}
			}))
			defer relay.Close()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			archive := openArchive(t, t.TempDir())
			result := make(chan error, 1)
			go func() {
				_, err := Verify(ctx, VerifyConfig{
					Archive:          archive,
					DID:              testDID,
					IdentityResolver: newTestIdentityResolver(testDID, relay.URL),
				})
				result <- err
			}()

			select {
			case <-blockReadStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("verification did not reach the blocked CAR response")
			}
			cancel()
			select {
			case err := <-result:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("verification did not stop after request cancellation")
			}
		})
	}
}

func FuzzReadAuthoritativeCommitCAR(f *testing.F) {
	cid, validCAR := fuzzAuthoritativeCARSeed()
	f.Add(validCAR)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 8<<20 {
			t.Skip()
		}
		_, _ = readAuthoritativeCommitBlock(t.Context(), bytes.NewReader(data), cid)
	})
}

func fuzzAuthoritativeCARSeed() (cbor.CID, []byte) {
	data := []byte{0xa0}
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	header := cbor.AppendMapHeader(nil, 2)
	header = cbor.AppendText(header, "roots")
	header = cbor.AppendArrayHeader(header, 0)
	header = cbor.AppendText(header, "version")
	header = cbor.AppendUint(header, 1)

	carData := cbor.AppendUvarint(nil, uint64(len(header)))
	carData = append(carData, header...)
	block := cid.AppendBytes(nil)
	block = append(block, data...)
	carData = append(carData, cbor.AppendUvarint(nil, uint64(len(block)))...)
	carData = append(carData, block...)
	return cid, carData
}

func TestVerify_ValidatesConfig(t *testing.T) {
	t.Parallel()

	_, err := Verify(t.Context(), VerifyConfig{DID: testDID})
	require.ErrorContains(t, err, "Catalog is required")

	_, err = Verify(t.Context(), VerifyConfig{Archive: openArchive(t, t.TempDir())})
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
