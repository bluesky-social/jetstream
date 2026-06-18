package backfill

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
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
		st, err := store.Open(dir, nil)
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

func TestConfig_CrashInjectorField(t *testing.T) {
	t.Parallel()

	cfg := Config{CrashInjector: stubCrashInjector{}}
	require.NotNil(t, cfg.CrashInjector)
}

type stubCrashInjector struct{}

func (stubCrashInjector) SimulateCrash(context.Context, crashpoint.Point) error {
	return nil
}

// stubResolver is a fixed-document Resolver. It maps DID -> document
// and never hits the network, mirroring atmos's own test pattern in
// backfill_test.go. We need it because the production resolver talks
// to plc.directory; the test environment has no PLC.
type stubResolver struct {
	docs         map[atmos.DID]*identity.DIDDocument
	onResolveDID func(context.Context, atmos.DID) error
}

func (r *stubResolver) ResolveDID(ctx context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	if r.onResolveDID != nil {
		if err := r.onResolveDID(ctx, did); err != nil {
			return nil, err
		}
	}
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

	// transientFailGetRepo maps a DID to the number of getRepo calls
	// that should still fail with transientFailGetRepoCode before the
	// real CAR is served. Each matching call decrements the counter, so
	// after the budget is exhausted the DID downloads normally. Guarded
	// by transientMu because the engine drives getRepo from many worker
	// goroutines concurrently.
	transientMu              sync.Mutex
	transientFailGetRepo     map[atmos.DID]int
	transientFailGetRepoCode int

	// transientTruncateGetRepo maps a DID to the number of successful-status
	// getRepo responses that should return an incomplete CAR before the
	// real CAR is served. Guarded by transientMu.
	transientTruncateGetRepo map[atmos.DID]int

	getRepoDelay     time.Duration
	getRepoActive    atomic.Int64
	getRepoMaxActive atomic.Int64

	eventsMu sync.Mutex
	events   []string

	// firstListReposCursor records the cursor query param the relay
	// saw on its first listRepos request. Lets tests verify that a
	// pre-seeded resume cursor is passed through correctly.
	firstListReposCursor   string
	firstListReposCursorMu sync.Mutex
	firstListReposCursorOK bool

	// listReposPageSize, when non-zero, makes listRepos return results
	// in pages of this size. Cursor is the first DID of the next page,
	// or "" when drained.
	listReposPageSize int

	// emptyListReposCursor, when non-empty, makes listRepos return an empty
	// terminal page for that cursor. This models resuming after the final DID.
	emptyListReposCursor string
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
		s.recordEvent("listRepos")
		cursor := r.URL.Query().Get("cursor")
		s.firstListReposCursorMu.Lock()
		if !s.firstListReposCursorOK {
			s.firstListReposCursor = cursor
			s.firstListReposCursorOK = true
		}
		s.firstListReposCursorMu.Unlock()
		if s.emptyListReposCursor != "" && cursor == s.emptyListReposCursor {
			_ = json.NewEncoder(w).Encode(listPage{})
			return
		}
		// Stable order so tests that count fail-vs-not are deterministic.
		dids := make([]atmos.DID, 0, len(s.fixtures))
		for did := range s.fixtures {
			dids = append(dids, did)
		}
		slices.Sort(dids)

		page := listPage{}
		if s.listReposPageSize > 0 {
			// Paginated mode: cursor is the first DID of this page.
			startIdx := 0
			if cursor != "" {
				// Find where this cursor starts in the sorted DID list.
				for i, d := range dids {
					if string(d) == cursor {
						startIdx = i
						break
					}
				}
			}
			endIdx := min(startIdx+s.listReposPageSize, len(dids))
			for _, d := range dids[startIdx:endIdx] {
				page.Repos = append(page.Repos, listEntry{
					DID: string(d), Head: "bafytest", Rev: "rev1", Active: true,
				})
			}
			// Set cursor to the first DID of the next page, or "" if drained.
			if endIdx < len(dids) {
				page.Cursor = string(dids[endIdx])
			}
		} else {
			// Non-paginated mode: return all DIDs in one page.
			for _, d := range dids {
				page.Repos = append(page.Repos, listEntry{
					DID: string(d), Head: "bafytest", Rev: "rev1", Active: true,
				})
			}
		}
		_ = json.NewEncoder(w).Encode(page)

	case "/xrpc/com.atproto.sync.getRepo":
		s.getRepoHit.Add(1)
		s.recordEvent("getRepo")
		active := s.getRepoActive.Add(1)
		for {
			prev := s.getRepoMaxActive.Load()
			if active <= prev || s.getRepoMaxActive.CompareAndSwap(prev, active) {
				break
			}
		}
		defer s.getRepoActive.Add(-1)
		if s.getRepoDelay > 0 {
			time.Sleep(s.getRepoDelay)
		}
		didStr := r.URL.Query().Get("did")
		did := atmos.DID(didStr)
		if s.failGetRepo[did] {
			w.WriteHeader(s.failGetRepoCode)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "TransientError"})
			return
		}
		s.transientMu.Lock()
		if remaining := s.transientFailGetRepo[did]; remaining > 0 {
			s.transientFailGetRepo[did] = remaining - 1
			s.transientMu.Unlock()
			w.WriteHeader(s.transientFailGetRepoCode)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "TransientError"})
			return
		}
		s.transientMu.Unlock()
		f, ok := s.fixtures[did]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "RepoNotFound"})
			return
		}
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		s.transientMu.Lock()
		if remaining := s.transientTruncateGetRepo[did]; remaining > 0 {
			s.transientTruncateGetRepo[did] = remaining - 1
			s.transientMu.Unlock()
			if len(f.car) > 0 {
				_, _ = w.Write(f.car[:max(1, len(f.car)/2)])
			}
			return
		}
		s.transientMu.Unlock()
		_, _ = w.Write(f.car)

	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *stubServer) recordEvent(event string) {
	s.eventsMu.Lock()
	defer s.eventsMu.Unlock()
	s.events = append(s.events, event)
}

func (s *stubServer) eventIndex(event string, n int) int {
	s.eventsMu.Lock()
	defer s.eventsMu.Unlock()
	seen := 0
	for i, got := range s.events {
		if got != event {
			continue
		}
		seen++
		if seen == n {
			return i
		}
	}
	return -1
}

// runWithStub drives runWithDirectory with a Directory whose Resolver
// returns the stubServer's PDS document for each fixture DID. This is
// the integration entry point for our run_test.go.
func runWithStub(t *testing.T, ctx context.Context, srv *stubServer, db *store.Store) error {
	t.Helper()
	return runWithStubResolverAndRepos(t, ctx, srv, db, nil, nil)
}

func runWithStubRepos(
	t *testing.T,
	ctx context.Context,
	srv *stubServer,
	db *store.Store,
	repos []atmos.DID,
) error {
	t.Helper()
	return runWithStubResolverAndRepos(t, ctx, srv, db, nil, repos)
}

func runWithStubResolverAndRepos(
	t *testing.T,
	ctx context.Context,
	srv *stubServer,
	db *store.Store,
	onResolveDID func(context.Context, atmos.DID) error,
	repos []atmos.DID,
) error {
	t.Helper()
	docs := make(map[atmos.DID]*identity.DIDDocument, len(srv.fixtures))
	for did, f := range srv.fixtures {
		docs[did] = &identity.DIDDocument{
			ID:          string(did),
			AlsoKnownAs: []string{"at://" + did.Identifier() + ".test"},
			VerificationMethod: []identity.VerificationMethod{
				{ID: "#atproto", Type: "Multikey", Controller: string(did), PublicKeyMultibase: f.multibase},
			},
			Service: []identity.Service{
				{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: srv.srv.URL},
			},
		}
	}
	dir := &identity.Directory{Resolver: &stubResolver{docs: docs, onResolveDID: onResolveDID}}

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
		Store:         db,
		Directory:     dir,
		HTTPClient:    &http.Client{Timeout: 5 * time.Second},
		Writer:        w,
		RelayURL:      srv.srv.URL,
		Logger:        logger,
		BackfillRepos: repos,
	}
	return Run(ctx, cfg)
}

// TestRun_TransientGetRepoFailureThenRecovers exercises the real
// engine retry path end-to-end: the stub PDS returns one retryable 503
// for a DID before serving its CAR, and Run must still drive that DID
// to StateComplete. Unlike the simulator-handler unit test (which calls
// GetRepoStream directly with retries disabled), this proves jetstream's
// configured retry/backoff loop recovers from a transient upstream
// failure. RetryBaseDelay is pinned tiny so the test does not pay
// atmos's 1s production backoff.
func TestRun_TransientGetRepoFailureThenRecovers(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:transient")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)
	srv.transientFailGetRepo = map[atmos.DID]int{did: 2}
	srv.transientFailGetRepoCode = http.StatusServiceUnavailable

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	docs := map[atmos.DID]*identity.DIDDocument{
		did: {
			ID: string(did),
			VerificationMethod: []identity.VerificationMethod{
				{ID: "#atproto", Type: "Multikey", Controller: string(did), PublicKeyMultibase: fixtures[did].multibase},
			},
			Service: []identity.Service{
				{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: srv.srv.URL},
			},
		},
	}
	dir := &identity.Directory{Resolver: &stubResolver{docs: docs}}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       filepath.Join(t.TempDir(), "segments"),
		Store:             db,
		Logger:            logger,
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.NoError(t, Run(t.Context(), Config{
		Store:          db,
		Directory:      dir,
		HTTPClient:     &http.Client{Timeout: 5 * time.Second},
		Writer:         w,
		RelayURL:       srv.srv.URL,
		Logger:         logger,
		RetryBaseDelay: time.Millisecond,
		RetryMaxDelay:  10 * time.Millisecond,
	}))

	// The DID must have completed despite the two transient 503s, and
	// the budget must be fully consumed (the engine actually retried).
	got, err := NewStore(db, nil).Lookup(t.Context(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateComplete, got.State, "transient 503s must be retried to completion")

	srv.transientMu.Lock()
	defer srv.transientMu.Unlock()
	require.Equal(t, 0, srv.transientFailGetRepo[did], "all scheduled transient failures must have fired")
}

// TestRun_TruncatedGetRepoCARThenRecovers exercises the real engine retry
// path for a successful HTTP response whose body fails while parsing as CAR.
// The first getRepo returns a 200 with only half the CAR body; the retry must
// fetch the full CAR and complete the DID without persisting a failed state.
func TestRun_TruncatedGetRepoCARThenRecovers(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:truncated")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)
	srv.transientTruncateGetRepo = map[atmos.DID]int{did: 1}

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	docs := map[atmos.DID]*identity.DIDDocument{
		did: {
			ID: string(did),
			VerificationMethod: []identity.VerificationMethod{
				{ID: "#atproto", Type: "Multikey", Controller: string(did), PublicKeyMultibase: fixtures[did].multibase},
			},
			Service: []identity.Service{
				{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: srv.srv.URL},
			},
		},
	}
	dir := &identity.Directory{Resolver: &stubResolver{docs: docs}}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       filepath.Join(t.TempDir(), "segments"),
		Store:             db,
		Logger:            logger,
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.NoError(t, Run(t.Context(), Config{
		Store:          db,
		Directory:      dir,
		HTTPClient:     &http.Client{Timeout: 5 * time.Second},
		Writer:         w,
		RelayURL:       srv.srv.URL,
		Logger:         logger,
		RetryBaseDelay: time.Millisecond,
		RetryMaxDelay:  10 * time.Millisecond,
	}))

	got, err := NewStore(db, nil).Lookup(t.Context(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateComplete, got.State, "truncated CAR must be retried to completion")

	srv.transientMu.Lock()
	defer srv.transientMu.Unlock()
	require.Equal(t, 0, srv.transientTruncateGetRepo[did], "all scheduled truncated CAR faults must have fired")
}

func TestRun_IdentityDiagnosticsPersistenceFailureAbortsRun(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:diagfail")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	host, ok := normalizeHostBucket(srv.srv.URL)
	require.True(t, ok)
	hostStatusKey, err := hostKey(host)
	require.NoError(t, err)
	require.NoError(t, db.Set(hostStatusKey, []byte("not json"), store.SyncWrites))

	err = runWithStub(t, t.Context(), srv, db)
	require.Error(t, err)
	require.ErrorContains(t, err, "identity diagnostics")

	rs, err := NewStore(db, nil).readRepoStatus(did)
	require.NoError(t, err)
	require.NotNil(t, rs)
	require.Equal(t, StatusNotStarted, rs.Backfill.Status)
	require.Empty(t, rs.Backfill.LastError)
	require.Empty(t, rs.Host)
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

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, runWithStub(t, t.Context(), srv, db))

	bf := NewStore(db, nil)
	wantHost, ok := normalizeHostBucket(srv.srv.URL)
	require.True(t, ok)
	for _, did := range dids {
		got, err := bf.Lookup(context.Background(), did)
		require.NoError(t, err)
		require.Equal(t, atmosbackfill.StateComplete, got.State, "%s should be Complete", did)

		rs, err := bf.readRepoStatus(did)
		require.NoError(t, err)
		require.NotNil(t, rs)
		require.Equal(t, srv.srv.URL, rs.PDS)
		require.Equal(t, wantHost, rs.Host)
	}

	hs, ok, err := loadHostStatus(db, wantHost)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(len(dids)), hs.Total)
	require.Equal(t, uint64(len(dids)), hs.Complete)
}

func TestRun_PassesBackfillBatchSizeToAtmos(t *testing.T) {
	t.Parallel()

	dids := []atmos.DID{"did:plc:aaa", "did:plc:bbb", "did:plc:ccc", "did:plc:ddd"}
	fixtures := make(map[atmos.DID]repoFixture, len(dids))
	for _, d := range dids {
		fixtures[d] = buildRepoFixture(t, d)
	}
	srv := newPaginatingStubServer(t, fixtures, 2)

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

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

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       filepath.Join(t.TempDir(), "segments"),
		Store:             db,
		Logger:            logger,
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.NoError(t, Run(t.Context(), Config{
		Store:             db,
		Directory:         &identity.Directory{Resolver: &stubResolver{docs: docs}},
		HTTPClient:        &http.Client{Timeout: 5 * time.Second},
		Writer:            w,
		RelayURL:          srv.srv.URL,
		Logger:            logger,
		BackfillBatchSize: 2,
	}))

	firstGetRepo := srv.eventIndex("getRepo", 1)
	secondListRepos := srv.eventIndex("listRepos", 2)
	require.NotEqual(t, -1, firstGetRepo)
	require.NotEqual(t, -1, secondListRepos)
	require.Less(t, firstGetRepo, secondListRepos,
		"BackfillBatchSize=2 should dispatch the first page before requesting the second listRepos page")
}

func TestRun_PassesBackfillWorkersToAtmos(t *testing.T) {
	t.Parallel()

	dids := []atmos.DID{"did:plc:aaa", "did:plc:bbb", "did:plc:ccc", "did:plc:ddd"}
	fixtures := make(map[atmos.DID]repoFixture, len(dids))
	for _, d := range dids {
		fixtures[d] = buildRepoFixture(t, d)
	}
	srv := newStubServer(t, fixtures)
	srv.getRepoDelay = 25 * time.Millisecond

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

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

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       filepath.Join(t.TempDir(), "segments"),
		Store:             db,
		Logger:            logger,
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.NoError(t, Run(t.Context(), Config{
		Store:           db,
		Directory:       &identity.Directory{Resolver: &stubResolver{docs: docs}},
		HTTPClient:      &http.Client{Timeout: 5 * time.Second},
		Writer:          w,
		RelayURL:        srv.srv.URL,
		Logger:          logger,
		BackfillWorkers: 1,
	}))

	require.Equal(t, int64(1), srv.getRepoMaxActive.Load(),
		"BackfillWorkers=1 should serialize getRepo downloads")
}

func TestRun_BackfillReposDownloadsSelectedDIDsWithoutListRepos(t *testing.T) {
	t.Parallel()

	selected := atmos.DID("did:plc:selected")
	other := atmos.DID("did:plc:other")
	fixtures := map[atmos.DID]repoFixture{
		selected: buildRepoFixture(t, selected),
		other:    buildRepoFixture(t, other),
	}
	srv := newStubServer(t, fixtures)

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, MaybeSaveBootstrapLastListReposCursor(db, "stale-cursor"))

	require.NoError(t, runWithStubRepos(t, t.Context(), srv, db, []atmos.DID{selected}))

	require.Equal(t, int64(0), srv.listReposHit.Load(), "selected backfill must not scan listRepos")
	require.Equal(t, int64(1), srv.getRepoHit.Load(), "selected backfill should download only requested repos")

	bf := NewStore(db, nil)
	got, err := bf.Lookup(t.Context(), selected)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateComplete, got.State)

	got, err = bf.Lookup(t.Context(), other)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateUnknown, got.State)

	cursor, err := LoadBootstrapLastListReposCursor(db)
	require.NoError(t, err)
	require.Empty(t, cursor, "selected backfill must clear stale merge discovery state")
}

func TestRun_BackfillReposRetriesTransientGetRepoFailure(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:selectedtransient")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)
	srv.transientFailGetRepo = map[atmos.DID]int{did: 2}
	srv.transientFailGetRepoCode = http.StatusServiceUnavailable

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	docs := map[atmos.DID]*identity.DIDDocument{
		did: {
			ID: string(did),
			VerificationMethod: []identity.VerificationMethod{
				{ID: "#atproto", Type: "Multikey", Controller: string(did), PublicKeyMultibase: fixtures[did].multibase},
			},
			Service: []identity.Service{
				{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: srv.srv.URL},
			},
		},
	}
	dir := &identity.Directory{Resolver: &stubResolver{docs: docs}}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       filepath.Join(t.TempDir(), "segments"),
		Store:             db,
		Logger:            logger,
		MaxEventsPerBlock: 4,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.NoError(t, Run(t.Context(), Config{
		Store:          db,
		Directory:      dir,
		HTTPClient:     &http.Client{Timeout: 5 * time.Second},
		Writer:         w,
		RelayURL:       srv.srv.URL,
		Logger:         logger,
		BackfillRepos:  []atmos.DID{did},
		RetryBaseDelay: time.Millisecond,
		RetryMaxDelay:  10 * time.Millisecond,
	}))

	got, err := NewStore(db, nil).Lookup(t.Context(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateComplete, got.State)
	require.Equal(t, int64(0), srv.listReposHit.Load())

	srv.transientMu.Lock()
	defer srv.transientMu.Unlock()
	require.Equal(t, 0, srv.transientFailGetRepo[did], "all scheduled transient failures must have fired")
}

// TestRun_Resume_NoOpAfterCompletion exercises restart-after-
// completion: the second Run call should drain immediately without
// hitting getRepo, because every Lookup returns StateComplete.
func TestRun_Resume_NoOpAfterCompletion(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:done")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)

	db, err := store.Open(t.TempDir(), nil)
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
// value "" — atmos fires OnBatchComplete("") after the terminator
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

	db, err := store.Open(t.TempDir(), nil)
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

// TestRun_MaxRepos_StopsEarly is the debug-flag smoke test.
// With MaxRepos=1 and several fixtures, Run must return nil
// (not a context.Canceled error) so the orchestrator can advance
// to the merge phase. We only assert that at least the limit was
// reached and Run returned nil; how many extra repos completed
// before the cancel propagated is implementation-dependent
// (atmos worker-pool scheduling and HTTP timing) and not the
// contract this test pins.
func TestRun_MaxRepos_StopsEarly(t *testing.T) {
	t.Parallel()

	dids := []atmos.DID{
		"did:plc:aaa", "did:plc:bbb", "did:plc:ccc",
		"did:plc:ddd", "did:plc:eee",
	}
	fixtures := make(map[atmos.DID]repoFixture, len(dids))
	for _, d := range dids {
		fixtures[d] = buildRepoFixture(t, d)
	}
	srv := newStubServer(t, fixtures)

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

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
		MaxRepos:   1,
	}
	require.NoError(t, Run(t.Context(), cfg))

	// At least one repo must have reached Complete; otherwise
	// OnProgress never fired and the limit logic is dead code.
	bf := NewStore(db, nil)
	completed := 0
	for _, did := range dids {
		got, err := bf.Lookup(context.Background(), did)
		require.NoError(t, err)
		if got.State == atmosbackfill.StateComplete {
			completed++
		}
	}
	require.GreaterOrEqual(t, completed, 1, "MaxRepos=1 should have completed at least one repo")
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

	db, err := store.Open(t.TempDir(), nil)
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

func TestRun_ClearsSavedCursorWhenResumeDrainsNoRepos(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:aaa")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)
	srv.emptyListReposCursor = "after-last-did"

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, SaveListReposCursor(db, "after-last-did"))

	require.NoError(t, runWithStub(t, t.Context(), srv, db))

	got, err := LoadListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "", got, "full clean drain must clear stale non-empty resume cursor")
	require.Equal(t, int64(0), srv.getRepoHit.Load(), "empty resumed listRepos must not download repos")
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
	db, err := store.Open(dataDir, nil)
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

func TestRun_WriterFlushErrorAbortsAfterDurableCompletion(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:flush-fails")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)

	dataDir := t.TempDir()
	db, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	errFlush := errors.New("flush failed")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       filepath.Join(dataDir, "segments"),
		Store:             db,
		Logger:            logger,
		MaxEventsPerBlock: 4096,
		MaxSegmentBytes:   1 << 30,
		OnAfterFlush: func(context.Context) error {
			return errFlush
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

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

	err = Run(t.Context(), Config{
		Store:      db,
		Directory:  &identity.Directory{Resolver: &stubResolver{docs: docs}},
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Writer:     w,
		RelayURL:   srv.srv.URL,
		Logger:     logger,
	})
	require.ErrorIs(t, err, errFlush)

	got, err := NewStore(db, nil).Lookup(t.Context(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateComplete, got.State,
		"completion is committed in the same durable batch before legacy OnAfterFlush runs")
}

func TestRun_RestartAfterQueuedCompletionErrorDoesNotRedownload(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:queued-complete-restart")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)

	dataDir := t.TempDir()
	db, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

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
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	errFlush := errors.New("flush failed after completion commit")
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       filepath.Join(dataDir, "segments"),
		Store:             db,
		Logger:            logger,
		MaxEventsPerBlock: 4096,
		MaxSegmentBytes:   1 << 30,
		OnAfterFlush: func(context.Context) error {
			return errFlush
		},
	})
	require.NoError(t, err)

	err = Run(t.Context(), Config{
		Store:      db,
		Directory:  dir,
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Writer:     w,
		RelayURL:   srv.srv.URL,
		Logger:     logger,
	})
	require.ErrorIs(t, err, errFlush)
	require.NoError(t, w.Close())
	firstGetRepo := srv.getRepoHit.Load()
	require.Equal(t, int64(1), firstGetRepo)

	w, err = ingest.Open(ingest.Config{
		SegmentsDir:       filepath.Join(dataDir, "segments"),
		Store:             db,
		Logger:            logger,
		MaxEventsPerBlock: 4096,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.NoError(t, Run(t.Context(), Config{
		Store:      db,
		Directory:  dir,
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Writer:     w,
		RelayURL:   srv.srv.URL,
		Logger:     logger,
	}))
	require.Equal(t, firstGetRepo, srv.getRepoHit.Load(), "durable queued completion must prevent restart redownload")
}

func TestRun_AfterRepoCompleteErrorAbortsRun(t *testing.T) {
	t.Parallel()

	dids := []atmos.DID{"did:plc:after-complete-fails", "did:plc:after-complete-next"}
	fixtures := make(map[atmos.DID]repoFixture, len(dids))
	for _, did := range dids {
		fixtures[did] = buildRepoFixture(t, did)
	}
	srv := newPaginatingStubServer(t, fixtures, 1)

	dataDir := t.TempDir()
	db, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       filepath.Join(dataDir, "segments"),
		Store:             db,
		Logger:            logger,
		MaxEventsPerBlock: 4096,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

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

	errHook := errors.New("after complete hook failed")
	err = Run(t.Context(), Config{
		Store:      db,
		Directory:  &identity.Directory{Resolver: &stubResolver{docs: docs}},
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Writer:     w,
		RelayURL:   srv.srv.URL,
		Logger:     logger,
		AfterRepoComplete: func(context.Context, atmos.DID) error {
			return errHook
		},
	})
	require.ErrorIs(t, err, errHook)

	got, err := NewStore(db, nil).Lookup(t.Context(), dids[0])
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateComplete, got.State,
		"completion row is durable before the hook failure is surfaced")

	_, closer, err := db.Get([]byte(listReposCursorKey))
	if closer != nil {
		require.NoError(t, closer.Close())
	}
	require.ErrorIs(t, err, store.ErrNotFound,
		"listRepos cursor must not advance past a failed durable completion hook")

	_, closer, err = db.Get([]byte(bootstrapLastListReposCursorKey))
	if closer != nil {
		require.NoError(t, closer.Close())
	}
	require.ErrorIs(t, err, store.ErrNotFound,
		"bootstrap-last cursor must not advance past a failed durable completion hook")
}

// TestRun_PersistsBootstrapLastListReposCursor confirms the bootstrap-
// last cursor is written on every non-empty page. The merge phase needs
// the last non-empty cursor to resume listRepos for new-DID discovery.
func TestRun_PersistsBootstrapLastListReposCursor(t *testing.T) {
	t.Parallel()

	// Build three DIDs that will be returned across two listRepos pages.
	dids := []atmos.DID{"did:plc:aaa", "did:plc:bbb", "did:plc:ccc"}
	fixtures := make(map[atmos.DID]repoFixture, len(dids))
	for _, d := range dids {
		fixtures[d] = buildRepoFixture(t, d)
	}

	// Create a stub server that paginates: page 1 returns did:plc:aaa
	// and did:plc:bbb with NextCursor=did:plc:ccc; page 2 returns did:plc:ccc
	// with NextCursor="" (the drain sentinel).
	srv := newPaginatingStubServer(t, fixtures, 2)

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, runWithStub(t, t.Context(), srv, db))

	// After drain, relay/list_repos_cursor must be empty.
	got, err := LoadListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "", got, "post-drain cursor must be empty")

	// Bootstrap-last cursor must be the last NON-EMPTY cursor seen,
	// so the merge phase can resume listRepos for new-DID discovery.
	last, err := LoadBootstrapLastListReposCursor(db)
	require.NoError(t, err)
	require.Equal(t, "did:plc:ccc", last, "bootstrap-last cursor must be the last non-empty cursor (did:plc:ccc)")
}

// newPaginatingStubServer builds a stubServer that returns repos
// across multiple pages of pageSize DIDs each.
func newPaginatingStubServer(t *testing.T, fixtures map[atmos.DID]repoFixture, pageSize int) *stubServer {
	t.Helper()
	s := newStubServer(t, fixtures)
	s.listReposPageSize = pageSize
	return s
}
