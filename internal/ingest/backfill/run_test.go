package backfill

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
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

	// Helper for cases that need a non-nil Writer (real, opened).
	newWriter := func(t *testing.T) *ingest.Writer {
		t.Helper()
		dir := t.TempDir()
		st, err := store.Open(dir)
		require.NoError(t, err)
		t.Cleanup(func() { _ = st.Close() })
		w, err := ingest.Open(ingest.Config{
			SegmentsDir:       filepath.Join(dir, "segments"),
			Store:             st,
			Logger:            logger,
			MaxEventsPerBlock: 4,
			MaxSegmentBytes:   1 << 30,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = w.Close() })
		return w
	}

	httpClient := &http.Client{Timeout: 5 * time.Second}
	dir := &identity.Directory{Resolver: &stubResolver{}}

	tests := []struct {
		name    string
		build   func(t *testing.T) Config
		errPart string
	}{
		{
			name: "missing Store",
			build: func(t *testing.T) Config {
				return Config{Writer: newWriter(t), HTTPClient: httpClient, Directory: dir, RelayURL: "x", Logger: logger}
			},
			errPart: "Config.Store",
		},
		{
			name: "missing Writer",
			build: func(t *testing.T) Config {
				return Config{Store: &store.Store{}, HTTPClient: httpClient, Directory: dir, RelayURL: "x", Logger: logger}
			},
			errPart: "Config.Writer",
		},
		{
			name: "missing HTTPClient",
			build: func(t *testing.T) Config {
				return Config{Store: &store.Store{}, Writer: newWriter(t), Directory: dir, RelayURL: "x", Logger: logger}
			},
			errPart: "Config.HTTPClient",
		},
		{
			name: "missing Directory",
			build: func(t *testing.T) Config {
				return Config{Store: &store.Store{}, Writer: newWriter(t), HTTPClient: httpClient, RelayURL: "x", Logger: logger}
			},
			errPart: "Config.Directory",
		},
		{
			name: "missing RelayURL",
			build: func(t *testing.T) Config {
				return Config{Store: &store.Store{}, Writer: newWriter(t), HTTPClient: httpClient, Directory: dir, Logger: logger}
			},
			errPart: "Config.RelayURL",
		},
		{
			name: "missing Logger",
			build: func(t *testing.T) Config {
				return Config{Store: &store.Store{}, Writer: newWriter(t), HTTPClient: httpClient, Directory: dir, RelayURL: "x"}
			},
			errPart: "Config.Logger",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Run(context.Background(), tc.build(t))
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

	// firstListReposCursor records the cursor query param the relay
	// saw on its first listRepos request. Lets tests verify that a
	// pre-seeded resume cursor is passed through correctly.
	firstListReposCursor   string
	firstListReposCursorMu sync.Mutex
	firstListReposCursorOK bool
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
		s.firstListReposCursorMu.Lock()
		if !s.firstListReposCursorOK {
			s.firstListReposCursor = r.URL.Query().Get("cursor")
			s.firstListReposCursorOK = true
		}
		s.firstListReposCursorMu.Unlock()
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

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	segDir := filepath.Join(t.TempDir(), "segments")
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       segDir,
		Store:             db,
		Logger:            logger,
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	cfg := Config{
		Store:      db,
		Directory:  dir,
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Writer:     w,
		RelayURL:   srv.srv.URL,
		Logger:     logger,
	}
	return Run(ctx, cfg)
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

// TestRun_PersistsCursorAfterDrain confirms the post-drain cursor
// (empty string) is durably saved to pebble. Following the existing
// HappyPath: after Run returns, the cursor key exists in pebble with
// value "" — atmos fires OnPageComplete("") after the terminator
// page. Without this assertion, the cursor optimization could
// silently no-op.
func TestRun_PersistsCursorAfterDrain(t *testing.T) {
	t.Parallel()

	dids := []atmos.DID{"did:plc:aaa", "did:plc:bbb"}
	fixtures := make(map[atmos.DID]repoFixture, len(dids))
	for _, d := range dids {
		fixtures[d] = buildRepoFixture(t, d)
	}
	srv := newStubServer(t, fixtures)

	db, err := store.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, runWithStub(t, t.Context(), srv, db))

	// The cursor key must exist after a clean drain. Value is the
	// terminator-page cursor, which is empty for the stub (it returns
	// all DIDs in one page then no more).
	got, err := LoadListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "", got, "post-drain cursor is empty")
}

// TestRun_PassesSavedCursorToRelay confirms the resume path: a
// cursor pre-seeded into pebble is passed to the relay's listRepos
// as the startCursor on the first request of a new Run. Without
// this, the cursor optimization is dead weight on restart.
func TestRun_PassesSavedCursorToRelay(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:aaa")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)

	db, err := store.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Pre-seed a cursor as if a prior Run got partway through.
	require.NoError(t, SaveListReposCursor(db, "pretend-this-is-page-7"))

	require.NoError(t, runWithStub(t, t.Context(), srv, db))

	srv.firstListReposCursorMu.Lock()
	defer srv.firstListReposCursorMu.Unlock()
	require.True(t, srv.firstListReposCursorOK, "stub should have seen at least one listRepos request")
	require.Equal(t, "pretend-this-is-page-7", srv.firstListReposCursor,
		"first listRepos request must use the pre-seeded cursor as startCursor")
}

// TestRun_WritesSegmentFile confirms that backfilling a non-empty
// fixture leaves a real seg_*.jss on disk with at least one event.
func TestRun_WritesSegmentFile(t *testing.T) {
	t.Parallel()

	dids := []atmos.DID{"did:plc:aaa", "did:plc:bbb"}
	fixtures := make(map[atmos.DID]repoFixture, len(dids))
	for _, d := range dids {
		fixtures[d] = buildRepoFixture(t, d)
	}
	srv := newStubServer(t, fixtures)

	dataDir := t.TempDir()
	db, err := store.Open(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	segDir := filepath.Join(dataDir, "segments")
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       segDir,
		Store:             db,
		Logger:            logger,
		MaxEventsPerBlock: 2, // two records each, so each repo fills a block
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)

	docs := make(map[atmos.DID]*identity.DIDDocument, len(fixtures))
	for did, f := range fixtures {
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
		Store:      db,
		Directory:  dir,
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Writer:     w,
		RelayURL:   srv.srv.URL,
		Logger:     logger,
	}
	require.NoError(t, Run(t.Context(), cfg))
	require.NoError(t, w.Close())

	// At least one fully-flushed event per DID. Each fixture has 1
	// record, so we expect 2 events total. NextSeq advances even past
	// Close because Close does not seal.
	maxSeq, found, err := segment.ScanMaxSeq(filepath.Join(segDir, "seg_0000000000.jss"))
	require.NoError(t, err)
	require.True(t, found, "segment must contain at least one block")
	require.GreaterOrEqual(t, maxSeq, uint64(1),
		"two repos × 1 record each = 2 events; max seq must be at least 1")
}
