package orchestrator

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/identity"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// fakeRelay is a minimal stub combining the listRepos endpoint
// (drains in one page) and the subscribeRepos WebSocket (accepts
// the connection and holds it open).
//
// Subscribed is closed the first time the relay accepts a
// subscribeRepos WebSocket. Tests use this as a deterministic
// "live consumer is up" signal so they can cancel without waiting
// on a timeout.
type fakeRelay struct {
	t          *testing.T
	repos      []listReposEntry
	pages      map[string]listReposPage // optional; cursor → page
	srv        *httptest.Server
	Subscribed chan struct{}
	subOnce    sync.Once
}

type listReposEntry struct {
	DID    string `json:"did"`
	Head   string `json:"head"`
	Rev    string `json:"rev"`
	Active bool   `json:"active"`
}

type listReposPage struct {
	Cursor string           `json:"cursor,omitempty"`
	Repos  []listReposEntry `json:"repos"`
}

func newFakeRelay(t *testing.T, repos []listReposEntry) *fakeRelay {
	t.Helper()
	f := &fakeRelay{t: t, repos: repos, Subscribed: make(chan struct{})}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeRelay) URL() string { return f.srv.URL }

func (f *fakeRelay) handle(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasSuffix(r.URL.Path, "/com.atproto.sync.subscribeRepos"):
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()
		f.subOnce.Do(func() { close(f.Subscribed) })
		<-r.Context().Done()
	case strings.HasSuffix(r.URL.Path, "/com.atproto.sync.listRepos"):
		cursor := r.URL.Query().Get("cursor")
		if page, ok := f.pages[cursor]; ok {
			_ = json.NewEncoder(w).Encode(page)
			return
		}
		_ = json.NewEncoder(w).Encode(listReposPage{Repos: f.repos})
	default:
		// Surface unexpected paths in test output so a future
		// consumer-side test bug doesn't silently 404 forever.
		f.t.Logf("fakeRelay: unexpected path %q", r.URL.Path)
		http.NotFound(w, r)
	}
}

// readSegFiles returns paths to all seg_*.jss files in dir, sorted.
// Used by tests that need to inspect the on-disk segment tree after
// orchestrator runs.
func readSegFiles(dir string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "seg_*.jss"))
	if err != nil {
		return nil, err
	}
	return matches, nil
}

// isSealed returns true if the segment file at path has been sealed
// per docs/README.md §3.1.1. Delegates to segment.Inspect so the
// "what does sealed mean on disk" knowledge stays in one place.
func isSealed(t *testing.T, path string) bool {
	t.Helper()
	ins, err := segment.Inspect(path)
	require.NoError(t, err)
	return ins.Sealed
}

// newTestVerifier builds a real Sync 1.1 verifier against an
// in-memory identity directory + an in-memory state store. The
// in-memory directory is critical: a DefaultResolver here would
// reach the live PLC directory if any frame ever triggered a
// lookup, which would make these tests network-dependent and
// flaky.
func newTestVerifier(t *testing.T, relayURL string) *atmossync.Verifier {
	t.Helper()
	xc := &xrpc.Client{Host: relayURL}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})
	v, err := atmossync.NewVerifier(atmossync.VerifierOptions{
		Directory:  identity.NewInMemoryDirectory(),
		StateStore: atmossync.NewMemStateStore(),
		SyncClient: gt.Some(sc),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })
	return v
}

// testIdentityDirectory mirrors newTestVerifier's choice of an
// in-memory directory so a stray DID resolution in a test cannot
// reach the network.
func testIdentityDirectory() *identity.Directory {
	return identity.NewInMemoryDirectory()
}

func newOrchestratorTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func mustEncodeStatus(t *testing.T, rs *backfill.RepoStatus) []byte {
	t.Helper()
	b, err := backfill.EncodeRepoStatus(rs)
	require.NoError(t, err)
	return b
}

// mergeFixture builds a data dir with backfill/live_segments/ populated
// from the supplied event slices (one slice per source segment) and
// repo/<did> rows from the supplied per-DID backfill revs. Returns the
// data dir, the open store (cleanup wired via t.Cleanup), and the
// orchestrator Config wired to a fakeRelay.
type mergeFixture struct {
	dataDir string
	store   *store.Store
	cfg     Config
	relay   *fakeRelay
}

// newMergeFixture builds the data tree. sources is a slice of slices —
// one outer entry per source segment. Each segment is sealed before
// returning so the merge sees fully-sealed source files. repoRevs is
// the pre-merge per-DID Backfill.Rev (also pre-populates the top-level
// Rev to that same value, mirroring what the real OnComplete callback
// does). Both arguments may be nil/empty.
func newMergeFixture(t *testing.T, sources [][]segment.Event, repoRevs map[string]string, storeOpts ...store.Option) *mergeFixture {
	t.Helper()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil, storeOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	for did, rev := range repoRevs {
		rs := &backfill.RepoStatus{
			Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: rev},
			Rev:      rev,
		}
		require.NoError(t, st.Set(backfill.RepoKey(did), mustEncodeStatus(t, rs), store.SyncWrites))
	}

	liveDir := filepath.Join(dataDir, "backfill", "live_segments")
	require.NoError(t, os.MkdirAll(liveDir, 0o755))
	for _, evs := range sources {
		w, err := ingest.Open(ingest.Config{
			SegmentsDir: liveDir,
			Store:       st,
			SeqKey:      live.BootstrapSeqKey,
			Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		require.NoError(t, err)
		for i := range evs {
			require.NoError(t, w.Append(t.Context(), &evs[i]))
		}
		require.NoError(t, w.SealActiveAndClose())
	}

	relay := newFakeRelay(t, nil)
	cfg := Config{
		DataDir:    dataDir,
		Store:      st,
		RelayURL:   relay.URL(),
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
		Directory:  testIdentityDirectory(),
		Verifier:   newTestVerifier(t, relay.URL()),
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return &mergeFixture{dataDir: dataDir, store: st, cfg: cfg, relay: relay}
}

// seedBootstrapLastCursor pre-populates bootstrap/last_listrepos_cursor.
// Used by tests that exercise the post-merge discovery step.
func (f *mergeFixture) seedBootstrapLastCursor(t *testing.T, cursor string) {
	t.Helper()
	require.NoError(t, backfill.MaybeSaveBootstrapLastListReposCursor(f.store, cursor))
}
