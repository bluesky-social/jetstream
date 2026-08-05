package backfill

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/crypto"
	atmosidentity "github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
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

	tests := []struct {
		name    string
		build   func(t *testing.T) Config
		errPart string
	}{
		{
			name: "missing Store",
			build: func(t *testing.T) Config {
				return Config{Writer: newWriter(t), HTTPClient: httpClient, RelayURL: "x", Logger: logger}
			},
			errPart: "Config.Store",
		},
		{
			name: "missing Writer",
			build: func(t *testing.T) Config {
				return Config{Store: &store.Store{}, HTTPClient: httpClient, RelayURL: "x", Logger: logger}
			},
			errPart: "Config.Writer",
		},
		{
			name: "missing HTTPClient",
			build: func(t *testing.T) Config {
				return Config{Store: &store.Store{}, Writer: newWriter(t), RelayURL: "x", Logger: logger}
			},
			errPart: "Config.HTTPClient",
		},
		{
			name: "missing RelayURL",
			build: func(t *testing.T) Config {
				return Config{Store: &store.Store{}, Writer: newWriter(t), HTTPClient: httpClient, Logger: logger}
			},
			errPart: "Config.RelayURL",
		},
		{
			name: "missing Logger",
			build: func(t *testing.T) Config {
				return Config{Store: &store.Store{}, Writer: newWriter(t), HTTPClient: httpClient, RelayURL: "x"}
			},
			errPart: "Config.Logger",
		},
		{
			name: "missing IdentityResolver for selected repos",
			build: func(t *testing.T) Config {
				return Config{
					Store:         &store.Store{},
					Writer:        newWriter(t),
					HTTPClient:    httpClient,
					RelayURL:      "x",
					Logger:        logger,
					BackfillRepos: []atmos.DID{"did:plc:selected"},
				}
			},
			errPart: "Config.IdentityResolver",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Run(context.Background(), tc.build(t))
			require.ErrorContains(t, err, tc.errPart)
		})
	}
}

// repoFixture is one DID + its signed CAR. The backfill download path
// neither resolves identity nor verifies signatures, so the CAR is all a
// fixture needs.
type repoFixture struct {
	did atmos.DID
	car []byte
}

type stubIdentityResolver struct {
	docs map[atmos.DID]*atmosidentity.DIDDocument
	err  error
}

func (r *stubIdentityResolver) ResolveDID(_ context.Context, did atmos.DID) (*atmosidentity.DIDDocument, error) {
	if r.err != nil {
		return nil, r.err
	}
	doc, ok := r.docs[did]
	if !ok {
		return nil, errors.New("identity: DID not found")
	}
	return doc, nil
}

func (r *stubIdentityResolver) ResolveHandle(_ context.Context, _ atmos.Handle) (atmos.DID, error) {
	return "", errors.New("stubIdentityResolver: ResolveHandle not implemented")
}

func didDocumentForTest(did atmos.DID, handle string, pds string) *atmosidentity.DIDDocument {
	return &atmosidentity.DIDDocument{
		ID:          string(did),
		AlsoKnownAs: []string{"at://" + handle},
		VerificationMethod: []atmosidentity.VerificationMethod{{
			ID:                 string(did) + "#atproto",
			Type:               "Multikey",
			Controller:         string(did),
			PublicKeyMultibase: "zQ3shQo7n7VdGV9XEvjyXEFy3sCvi5R8VC2sXkqMfV3oRUDoY",
		}},
		Service: []atmosidentity.Service{{
			ID:              "#atproto_pds",
			Type:            "AtprotoPersonalDataServer",
			ServiceEndpoint: pds,
		}},
	}
}

func selectedResolverForFixtures(fixtures map[atmos.DID]repoFixture, pds string) *stubIdentityResolver {
	dids := make([]atmos.DID, 0, len(fixtures))
	for did := range fixtures {
		dids = append(dids, did)
	}
	slices.Sort(dids)
	docs := make(map[atmos.DID]*atmosidentity.DIDDocument, len(dids))
	for i, did := range dids {
		docs[did] = didDocumentForTest(did, "selected-"+string(rune('a'+i))+".test", pds)
	}
	return &stubIdentityResolver{docs: docs}
}

// buildRepoFixture constructs a single-record repo for did, signs it
// with a fresh P-256 key, and returns the CAR.
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

	return repoFixture{
		did: did,
		car: buf.Bytes(),
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
	firstListReposLimit    string
	firstListReposCursorMu sync.Mutex
	firstListReposCursorOK bool

	// listReposPageSize, when non-zero, makes listRepos return results
	// in pages of this size. Cursor is the first DID of the next page,
	// or "" when drained.
	listReposPageSize int

	// emptyListReposCursor, when non-empty, makes listRepos return an empty
	// terminal page for that cursor. This models resuming after the final DID.
	emptyListReposCursor string

	// transientFailListRepos is the number of listRepos requests that return
	// 503 before normal pagination resumes.
	transientFailListRepos int

	// blockListReposCursor pauses a page request until listReposRelease closes.
	// Tests use this to hold the host manager open after a completed batch.
	blockListReposCursor string
	listReposBlocked     chan struct{}
	listReposRelease     chan struct{}
	listReposBlockOnce   sync.Once
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
	case "/xrpc/com.atproto.sync.listHosts":
		_ = json.NewEncoder(w).Encode(map[string]any{"hosts": []map[string]any{{
			"hostname": "pds.stub.test", "status": "active", "accountCount": len(s.fixtures),
		}}})
	case "/xrpc/com.atproto.sync.listRepos":
		s.listReposHit.Add(1)
		s.recordEvent("listRepos")
		cursor := r.URL.Query().Get("cursor")
		s.transientMu.Lock()
		if s.transientFailListRepos > 0 {
			s.transientFailListRepos--
			s.transientMu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "TransientError"})
			return
		}
		s.transientMu.Unlock()
		if s.blockListReposCursor != "" && cursor == s.blockListReposCursor {
			s.listReposBlockOnce.Do(func() { close(s.listReposBlocked) })
			select {
			case <-r.Context().Done():
				return
			case <-s.listReposRelease:
			}
		}
		s.firstListReposCursorMu.Lock()
		if !s.firstListReposCursorOK {
			s.firstListReposCursor = cursor
			s.firstListReposLimit = r.URL.Query().Get("limit")
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

func stubHostClient(srv *stubServer) func(string) (*atmossync.Client, error) {
	return func(hostname string) (*atmossync.Client, error) {
		if hostname != "pds.stub.test" {
			return nil, fmt.Errorf("unexpected stub hostname %q", hostname)
		}
		return atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{
			Host: srv.srv.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
		}}), nil
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
	return runWithStubResolverAndRepos(t, ctx, srv, db, repos, selectedResolverForFixtures(srv.fixtures, srv.srv.URL))
}

func runWithStubResolverAndRepos(
	t *testing.T,
	ctx context.Context,
	srv *stubServer,
	db *store.Store,
	repos []atmos.DID,
	resolver atmosidentity.Resolver,
) error {
	t.Helper()
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
		Store:            db,
		HTTPClient:       &http.Client{Timeout: 5 * time.Second},
		Writer:           w,
		RelayURL:         srv.srv.URL,
		NewHostClient:    stubHostClient(srv),
		Logger:           logger,
		BackfillRepos:    repos,
		IdentityResolver: resolver,
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
	srv.transientFailGetRepo = map[atmos.DID]int{did: 1}
	srv.transientFailGetRepoCode = http.StatusServiceUnavailable

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

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
		HTTPClient:     &http.Client{Timeout: 5 * time.Second},
		Writer:         w,
		RelayURL:       srv.srv.URL,
		NewHostClient:  stubHostClient(srv),
		Logger:         logger,
		RetryBaseDelay: time.Millisecond,
		RetryMaxDelay:  10 * time.Millisecond,
	}))

	// The DID must have completed despite the transient 503, and
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
		HTTPClient:     &http.Client{Timeout: 5 * time.Second},
		Writer:         w,
		RelayURL:       srv.srv.URL,
		NewHostClient:  stubHostClient(srv),
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

func TestRun_PeriodicallyDrainsQueuedCompletions(t *testing.T) {
	t.Parallel()

	firstDID := atmos.DID("did:plc:periodic-drain-a")
	secondDID := atmos.DID("did:plc:periodic-drain-b")
	fixtures := map[atmos.DID]repoFixture{
		firstDID:  buildRepoFixture(t, firstDID),
		secondDID: buildRepoFixture(t, secondDID),
	}
	srv := newStubServer(t, fixtures)
	srv.listReposPageSize = 1
	srv.blockListReposCursor = string(secondDID)
	srv.listReposBlocked = make(chan struct{})
	srv.listReposRelease = make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(srv.listReposRelease) }) }
	t.Cleanup(release)

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       filepath.Join(t.TempDir(), "segments"),
		Store:             db,
		Logger:            logger,
		MaxEventsPerBlock: 4096,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{
			Store:                   db,
			HTTPClient:              &http.Client{Timeout: 5 * time.Second},
			Writer:                  w,
			RelayURL:                srv.srv.URL,
			NewHostClient:           stubHostClient(srv),
			Logger:                  logger,
			BackfillBatchSize:       1,
			durabilityDrainInterval: 5 * time.Millisecond,
		})
	}()

	select {
	case <-srv.listReposBlocked:
	case err := <-done:
		require.FailNow(t, "Run returned before the second page blocked", "err=%v", err)
	case <-time.After(time.Second):
		require.FailNow(t, "second listRepos page did not block")
	}

	statusStore := NewStore(db, nil)
	require.Eventually(t, func() bool {
		rs, err := statusStore.readRepoStatus(firstDID)
		return err == nil && rs != nil && rs.Backfill.Status == StatusComplete
	}, time.Second, 5*time.Millisecond,
		"a sub-block completion must become durable while the host manager is still running")

	select {
	case err := <-done:
		require.FailNow(t, "Run returned before the blocked host was released", "err=%v", err)
	default:
	}

	release()
	require.NoError(t, <-done)
}

func TestRun_HostEnumerationAllowsOnlyOneRetry(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:host-retry-budget")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)
	srv.transientFailListRepos = 10

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

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
		HTTPClient:     &http.Client{Timeout: 5 * time.Second},
		Writer:         w,
		RelayURL:       srv.srv.URL,
		NewHostClient:  stubHostClient(srv),
		Logger:         logger,
		RetryBaseDelay: time.Millisecond,
		RetryMaxDelay:  time.Millisecond,
	}))

	require.EqualValues(t, 2, srv.listReposHit.Load(), "one host retry means two total attempts")
	hosts, err := ListPDSHosts(db)
	require.NoError(t, err)
	require.Len(t, hosts, 1)
	require.Equal(t, string(atmosbackfill.HostStateExhausted), hosts[0].State)
	require.Equal(t, 2, hosts[0].Attempts)
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
	wantHost := "pds.stub.test"
	for _, did := range dids {
		got, err := bf.Lookup(context.Background(), did)
		require.NoError(t, err)
		require.Equal(t, atmosbackfill.StateComplete, got.State, "%s should be Complete", did)

		rs, err := bf.readRepoStatus(did)
		require.NoError(t, err)
		require.NotNil(t, rs)
		// Discovery records the validated roster hostname before download.
		require.Equal(t, wantHost, rs.Host)
		require.Equal(t, wantHost, rs.PDS)
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
		HTTPClient:        &http.Client{Timeout: 5 * time.Second},
		Writer:            w,
		RelayURL:          srv.srv.URL,
		NewHostClient:     stubHostClient(srv),
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
		HTTPClient:      &http.Client{Timeout: 5 * time.Second},
		Writer:          w,
		RelayURL:        srv.srv.URL,
		NewHostClient:   stubHostClient(srv),
		Logger:          logger,
		GlobalDownloads: 1,
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

}

func TestRun_BackfillReposIndexesDeclaredHandle(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:selectedhandle")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)
	resolver := &stubIdentityResolver{docs: map[atmos.DID]*atmosidentity.DIDDocument{
		did: didDocumentForTest(did, "Alice.Example.COM", "https://pds.example.com"),
	}}

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, runWithStubResolverAndRepos(t, t.Context(), srv, db, []atmos.DID{did}, resolver))

	got, ok, err := lookupDIDByHandle(db, "alice.example.com")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, did, got)

	rs, ok, err := LoadRepoStatus(db, did)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "alice.example.com", rs.Handle)
	require.Equal(t, "https://pds.example.com", rs.PDS)
}

func TestRun_BackfillReposRepairsCompletedDeclaredHandleMetadata(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:selectedrepair")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)
	resolver := &stubIdentityResolver{docs: map[atmos.DID]*atmosidentity.DIDDocument{
		did: didDocumentForTest(did, "repair.test", "https://repair-pds.example.com"),
	}}

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	bf := NewStore(db, nil)
	require.NoError(t, bf.putRepoStatus(did, &RepoStatus{
		Backfill: RepoBackfillStatus{Status: StatusComplete, Rev: "rev-complete"},
		Active:   true,
		Host:     "old-pds.example.com",
	}))

	require.NoError(t, runWithStubResolverAndRepos(t, t.Context(), srv, db, []atmos.DID{did}, resolver))
	require.Equal(t, int64(0), srv.getRepoHit.Load(), "complete selected DID must not be redownloaded while repairing metadata")

	got, ok, err := lookupDIDByHandle(db, "repair.test")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, did, got)

	rs, ok, err := LoadRepoStatus(db, did)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "repair.test", rs.Handle)
	require.Equal(t, "https://repair-pds.example.com", rs.PDS)
	require.Equal(t, "rev-complete", rs.Backfill.Rev)
}

func TestRun_BackfillReposRetriesTransientGetRepoFailure(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:selectedtransient")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)
	srv.transientFailGetRepo = map[atmos.DID]int{did: 1}
	srv.transientFailGetRepoCode = http.StatusServiceUnavailable

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

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
		Store:            db,
		HTTPClient:       &http.Client{Timeout: 5 * time.Second},
		Writer:           w,
		RelayURL:         srv.srv.URL,
		Logger:           logger,
		BackfillRepos:    []atmos.DID{did},
		IdentityResolver: selectedResolverForFixtures(fixtures, srv.srv.URL),
		RetryBaseDelay:   time.Millisecond,
		RetryMaxDelay:    10 * time.Millisecond,
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

// TestRun_PersistsCursorAfterDrain confirms the per-host roster row is
// durably marked drained after segment durability catches up.
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

	host, ok, err := NewStore(db, nil).loadPDSHost("pds.stub.test")
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, host.Enumerated)
	require.Equal(t, string(atmosbackfill.HostStateDrained), host.State)
	require.Empty(t, host.ListReposCursor)
}

// TestRun_MaxRepos_StopsEarly is the debug-flag smoke test.
// With MaxRepos=1 and several fixtures, Run must request only one
// listRepos entry, download exactly one repo, and return nil so the
// orchestrator can advance to the merge phase.
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
		HTTPClient:    &http.Client{Timeout: 5 * time.Second},
		Writer:        w,
		RelayURL:      srv.srv.URL,
		NewHostClient: stubHostClient(srv),
		Logger:        logger,
		MaxRepos:      1,
	}
	require.NoError(t, Run(t.Context(), cfg))

	// Count on-disk StatusComplete rows, not Lookup projections: Lookup
	// deliberately reports interrupted not_started rows as StateComplete
	// (#262 crash-recovery), which would count never-downloaded repos here.
	bf := NewStore(db, nil)
	completed := 0
	for _, did := range dids {
		rs, err := bf.readRepoStatus(did)
		require.NoError(t, err)
		if rs != nil && rs.Backfill.Status == StatusComplete {
			completed++
		}
	}
	require.GreaterOrEqual(t, completed, 1)
	// Fleet-level cancellation is intentionally imprecise (in-flight repos
	// finish), but it must observably stop early: with MaxRepos=1 and five
	// fixtures, completing the whole roster means the knob was ignored.
	require.Less(t, completed, len(dids), "MaxRepos must stop the fleet before the full roster completes")
	require.Less(t, srv.getRepoHit.Load(), int64(len(dids)), "MaxRepos must suppress at least one download")
}

func TestRun_RejectsRetiredRelayCursor(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:aaa")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Pre-seed a cursor as if a prior Run got partway through.
	require.NoError(t, db.Set([]byte(listReposCursorKey), []byte("pretend-this-is-page-7"), store.SyncWrites))

	err = runWithStub(t, t.Context(), srv, db)
	require.ErrorContains(t, err, "old-scheme bootstrap in progress")
}

func TestRun_RejectsRetiredBootstrapLastCursor(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:aaa")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)

	db, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, db.Set([]byte(bootstrapLastListReposCursorKey), []byte("after-last-did"), store.SyncWrites))
	err = runWithStub(t, t.Context(), srv, db)
	require.ErrorContains(t, err, "old-scheme bootstrap in progress")
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

	cfg := Config{
		Store:         db,
		HTTPClient:    &http.Client{Timeout: 5 * time.Second},
		Writer:        w,
		RelayURL:      srv.srv.URL,
		NewHostClient: stubHostClient(srv),
		Logger:        logger,
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

func TestRun_AfterCompleteErrorAbortsAfterDurableCompletion(t *testing.T) {
	t.Parallel()

	did := atmos.DID("did:plc:flush-fails")
	fixtures := map[atmos.DID]repoFixture{did: buildRepoFixture(t, did)}
	srv := newStubServer(t, fixtures)

	dataDir := t.TempDir()
	db, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	errComplete := errors.New("after complete failed")
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

	err = Run(t.Context(), Config{
		Store:         db,
		HTTPClient:    &http.Client{Timeout: 5 * time.Second},
		Writer:        w,
		RelayURL:      srv.srv.URL,
		NewHostClient: stubHostClient(srv),
		Logger:        logger,
		AfterRepoComplete: func(context.Context, atmos.DID) error {
			return errComplete
		},
	})
	require.ErrorIs(t, err, errComplete)

	got, err := NewStore(db, nil).Lookup(t.Context(), did)
	require.NoError(t, err)
	require.Equal(t, atmosbackfill.StateComplete, got.State,
		"completion is committed before post-completion hook failure is surfaced")
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

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	errComplete := errors.New("after complete failed after completion commit")
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       filepath.Join(dataDir, "segments"),
		Store:             db,
		Logger:            logger,
		MaxEventsPerBlock: 4096,
		MaxSegmentBytes:   1 << 30,
	})
	require.NoError(t, err)

	err = Run(t.Context(), Config{
		Store:         db,
		HTTPClient:    &http.Client{Timeout: 5 * time.Second},
		Writer:        w,
		RelayURL:      srv.srv.URL,
		NewHostClient: stubHostClient(srv),
		Logger:        logger,
		AfterRepoComplete: func(context.Context, atmos.DID) error {
			return errComplete
		},
	})
	require.ErrorIs(t, err, errComplete)
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
		Store:         db,
		HTTPClient:    &http.Client{Timeout: 5 * time.Second},
		Writer:        w,
		RelayURL:      srv.srv.URL,
		NewHostClient: stubHostClient(srv),
		Logger:        logger,
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

	errHook := errors.New("after complete hook failed")
	err = Run(t.Context(), Config{
		Store:         db,
		HTTPClient:    &http.Client{Timeout: 5 * time.Second},
		Writer:        w,
		RelayURL:      srv.srv.URL,
		NewHostClient: stubHostClient(srv),
		Logger:        logger,
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

func TestRun_PersistsPerHostLastNonEmptyCursor(t *testing.T) {
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

	host, ok, err := NewStore(db, nil).loadPDSHost("pds.stub.test")
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, host.Enumerated)
	require.Equal(t, "did:plc:ccc", host.LastNonEmptyCursor)
}

// newPaginatingStubServer builds a stubServer that returns repos
// across multiple pages of pageSize DIDs each.
func newPaginatingStubServer(t *testing.T, fixtures map[atmos.DID]repoFixture, pageSize int) *stubServer {
	t.Helper()
	s := newStubServer(t, fixtures)
	s.listReposPageSize = pageSize
	return s
}
