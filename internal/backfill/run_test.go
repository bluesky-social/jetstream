package backfill

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	"github.com/stretchr/testify/require"
)

// TestRun_RejectsInvalidConfig pins the contract for cmd/jetstream:
// pass the wrong Config and you get a clear error before any network
// I/O happens.
func TestRun_RejectsInvalidConfig(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tests := []struct {
		name    string
		cfg     Config
		errPart string
	}{
		{"missing Store", Config{RelayURL: "x", Logger: logger}, "Config.Store"},
		{"missing RelayURL", Config{Store: &store.Store{}, Logger: logger}, "Config.RelayURL"},
		{"missing Logger", Config{Store: &store.Store{}, RelayURL: "x"}, "Config.Logger"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Run(context.Background(), tc.cfg)
			require.ErrorContains(t, err, tc.errPart)
		})
	}
}

// stubResolver is a fixed-document Resolver. It maps DID -> document
// and never hits the network, mirroring atmos's own test pattern in
// backfill_test.go. We need it because the production resolver talks
// to plc.directory; the test environment has no PLC.
type stubResolver struct {
	docs map[atmos.DID]*identity.DIDDocument
}

func (r *stubResolver) ResolveDID(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	doc, ok := r.docs[did]
	if !ok {
		return nil, identity.ErrDIDNotFound
	}
	return doc, nil
}

func (r *stubResolver) ResolveHandle(_ context.Context, _ atmos.Handle) (atmos.DID, error) {
	return "", identity.ErrHandleNotFound
}

// repoFixture is one DID + its CAR + its public-key multibase. We
// build the CAR via atmos/repo so signature verification (which the
// engine performs because we set Directory) actually succeeds.
type repoFixture struct {
	did       atmos.DID
	car       []byte
	multibase string
}

// buildRepoFixture constructs a single-record repo for did, signs it
// with a fresh P-256 key, and returns the CAR + the multibase that
// will go in the DID document.
func buildRepoFixture(t *testing.T, did atmos.DID) repoFixture {
	t.Helper()

	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	mstore := mst.NewMemBlockStore()
	r := &atmosrepo.Repo{
		DID:   did,
		Clock: atmos.NewTIDClock(0),
		Store: mstore,
		Tree:  mst.NewTree(mstore),
	}
	require.NoError(t, r.Create("app.bsky.feed.post", "rec0", map[string]any{"text": "hi"}))

	var buf bytes.Buffer
	require.NoError(t, r.ExportCAR(&buf, key))

	pub, ok := key.PublicKey().(*crypto.P256PublicKey)
	require.True(t, ok)

	return repoFixture{
		did:       did,
		car:       buf.Bytes(),
		multibase: pub.DIDKey()[8:],
	}
}

// stubServer serves both the relay (listRepos) and PDS (getRepo) on
// one host. The engine is happy to talk to anything that speaks
// XRPC; collapsing both endpoints into a single httptest.Server
// keeps fixture construction simple.
type stubServer struct {
	srv          *httptest.Server
	fixtures     map[atmos.DID]repoFixture
	listReposHit atomic.Int64
	getRepoHit   atomic.Int64

	// failGetRepo, when set, makes getRepo return failGetRepoCode for
	// the listed DIDs.
	failGetRepo     map[atmos.DID]bool
	failGetRepoCode int
}

func newStubServer(t *testing.T, fixtures map[atmos.DID]repoFixture) *stubServer {
	t.Helper()
	s := &stubServer{fixtures: fixtures}
	s.srv = httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(s.srv.Close)
	return s
}

type listEntry struct {
	DID    string `json:"did"`
	Head   string `json:"head"`
	Rev    string `json:"rev"`
	Active bool   `json:"active"`
}
type listPage struct {
	Cursor string      `json:"cursor,omitempty"`
	Repos  []listEntry `json:"repos"`
}

func (s *stubServer) handle(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/xrpc/com.atproto.sync.listRepos":
		s.listReposHit.Add(1)
		// Stable order so tests that count fail-vs-not are deterministic.
		dids := make([]atmos.DID, 0, len(s.fixtures))
		for did := range s.fixtures {
			dids = append(dids, did)
		}
		slices.Sort(dids)
		page := listPage{}
		for _, d := range dids {
			page.Repos = append(page.Repos, listEntry{
				DID: string(d), Head: "bafytest", Rev: "rev1", Active: true,
			})
		}
		_ = json.NewEncoder(w).Encode(page)

	case "/xrpc/com.atproto.sync.getRepo":
		s.getRepoHit.Add(1)
		didStr := r.URL.Query().Get("did")
		did := atmos.DID(didStr)
		if s.failGetRepo[did] {
			w.WriteHeader(s.failGetRepoCode)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "TransientError"})
			return
		}
		f, ok := s.fixtures[did]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "RepoNotFound"})
			return
		}
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		_, _ = w.Write(f.car)

	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// runWithStub drives runWithDirectory with a Directory whose Resolver
// returns the stubServer's PDS document for each fixture DID. This is
// the integration entry point for our run_test.go.
func runWithStub(t *testing.T, ctx context.Context, srv *stubServer, db *store.Store) error {
	t.Helper()
	docs := make(map[atmos.DID]*identity.DIDDocument, len(srv.fixtures))
	for did, f := range srv.fixtures {
		docs[did] = &identity.DIDDocument{
			ID: string(did),
			VerificationMethod: []identity.VerificationMethod{
				{ID: "#atproto", Type: "Multikey", Controller: string(did), PublicKeyMultibase: f.multibase},
			},
			Service: []identity.Service{
				{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: srv.srv.URL},
			},
		}
	}
	dir := &identity.Directory{Resolver: &stubResolver{docs: docs}}

	cfg := Config{
		Store:    db,
		RelayURL: srv.srv.URL,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return runWithDirectory(ctx, cfg, &http.Client{Timeout: 5 * time.Second}, dir)
}

// TestRun_HappyPath_DownloadsAllRepos is the wiring smoke test: three
// DIDs in listRepos, each with a real signed CAR served by the stub
// PDS. After Run, every DID lands at StatusComplete in pebble.
func TestRun_HappyPath_DownloadsAllRepos(t *testing.T) {
	t.Parallel()

	dids := []atmos.DID{"did:plc:aaa", "did:plc:bbb", "did:plc:ccc"}
	fixtures := make(map[atmos.DID]repoFixture, len(dids))
	for _, d := range dids {
		fixtures[d] = buildRepoFixture(t, d)
	}
	srv := newStubServer(t, fixtures)

	db, err := store.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, runWithStub(t, t.Context(), srv, db))

	bf := NewStore(db, nil)
	for _, did := range dids {
		got, err := bf.Lookup(context.Background(), did)
		require.NoError(t, err)
		require.Equal(t, atmosbackfill.StateComplete, got.State, "%s should be Complete", did)
	}
}

// TestRun_Resume_NoOpAfterCompletion exercises restart-after-
// completion: the second Run call should drain immediately without
// hitting getRepo, because every Lookup returns StateComplete.
func TestRun_Resume_NoOpAfterCompletion(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:done")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)

	db, err := store.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, runWithStub(t, t.Context(), srv, db))
	firstGetRepo := srv.getRepoHit.Load()
	require.Equal(t, int64(1), firstGetRepo)

	// Second pass: same data dir, same DID. Engine still walks
	// listRepos but skips download.
	require.NoError(t, runWithStub(t, t.Context(), srv, db))
	require.Equal(t, firstGetRepo, srv.getRepoHit.Load(), "second Run must not re-download Complete DIDs")
}

// TestRun_FailedRepoIsRetriable: a DID whose getRepo always 500s
// exhausts the engine's retry budget and lands at StatusFailed. A
// subsequent Run with the failure cleared re-attempts and succeeds.
//
// The atmos engine's defaults are 5 retries with 1s base / 30s max
// delay, so worst-case the failing path sits about a minute. We gate
// with testing.Short() so `just test` (which runs -short) skips this;
// `just test-long` exercises it.
func TestRun_FailedRepoIsRetriable(t *testing.T) {
	if testing.Short() {
		t.Skip("retry-budget test is slow under defaults; covered by test-long")
	}
	t.Parallel()

	did := atmos.DID("did:plc:flake")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)
	srv.failGetRepo = map[atmos.DID]bool{did: true}
	srv.failGetRepoCode = http.StatusInternalServerError

	db, err := store.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	err = runWithStub(t, t.Context(), srv, db)
	require.NoError(t, err, "Run drains successfully even when individual DIDs fail")

	bf := NewStore(db, nil)
	got, err := bf.Lookup(context.Background(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateFailed, got.State)

	// Clear the failure and re-run.
	srv.failGetRepo[did] = false

	require.NoError(t, runWithStub(t, t.Context(), srv, db))
	got, err = bf.Lookup(context.Background(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateComplete, got.State)
}
