package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
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
type fakeRelay struct {
	t     *testing.T
	repos []listReposEntry
	srv   *httptest.Server
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
	f := &fakeRelay{t: t, repos: repos}
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
		<-r.Context().Done()
	case strings.HasSuffix(r.URL.Path, "/com.atproto.sync.listRepos"):
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
// per DESIGN.md §3.1.1. Delegates to segment.Inspect so the
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
