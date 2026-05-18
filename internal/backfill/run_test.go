package backfill

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeRelay is a tiny stub of com.atproto.sync.listRepos that
// exists to drive the bootstrap orchestrator end-to-end. It tracks
// how many listRepos pagination calls landed so tests can assert
// that the second Run() invocation is genuinely a no-op.
type fakeRelay struct {
	server *httptest.Server
	calls  atomic.Int32
	dids   []string
}

type fakeRelayRepo struct {
	DID    string `json:"did"`
	Head   string `json:"head"`
	Rev    string `json:"rev"`
	Active bool   `json:"active"`
}

type fakeRelayPage struct {
	Cursor string          `json:"cursor,omitempty"`
	Repos  []fakeRelayRepo `json:"repos"`
}

func newFakeRelay(t *testing.T, dids []string) *fakeRelay {
	t.Helper()
	fr := &fakeRelay{dids: dids}
	fr.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/xrpc/com.atproto.sync.listRepos", r.URL.Path)
		idx := int(fr.calls.Add(1)) - 1
		if idx == 0 {
			repos := make([]fakeRelayRepo, len(fr.dids))
			for i, d := range fr.dids {
				repos[i] = fakeRelayRepo{
					DID: d, Head: "bafy" + d, Rev: "rev" + d, Active: true,
				}
			}
			_ = json.NewEncoder(w).Encode(fakeRelayPage{Repos: repos})
			return
		}
		// Subsequent calls return an empty page to signal end of pagination.
		_ = json.NewEncoder(w).Encode(fakeRelayPage{})
	}))
	t.Cleanup(fr.server.Close)
	return fr
}

// TestRun_FirstBootRunsSeedAndCompletes is the happy-path test:
// PhaseUnset → seed → PhaseComplete, with the expected number of
// repo/<did> rows landing in the store.
func TestRun_FirstBootRunsSeedAndCompletes(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	fr := newFakeRelay(t, []string{"did:plc:aaa", "did:plc:bbb"})

	err := Run(t.Context(), Config{
		Store:    s,
		RelayURL: fr.server.URL,
		Metrics:  NewSeedMetrics(nil),
	})
	require.NoError(t, err)

	st, err := GetBootstrapState(s)
	require.NoError(t, err)
	require.Equal(t, PhaseComplete, st.Phase)
	require.False(t, st.StartedAt.IsZero())
	require.False(t, st.CompletedAt.IsZero())

	count, err := CountRepos(s)
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
}

// TestRun_AlreadyCompleteIsNoop pins the documented contract:
// re-running Run on a PhaseComplete data directory must not touch
// the relay. We assert the call count on the fake relay is exactly
// zero after the second invocation.
func TestRun_AlreadyCompleteIsNoop(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	// Pre-seed the store as already complete. The PutBootstrapState
	// call below also covers the cross-process case where another
	// instance finished bootstrap and crashed before we started.
	require.NoError(t, PutBootstrapState(s, BootstrapState{Phase: PhaseComplete}))

	fr := newFakeRelay(t, []string{"did:plc:aaa"})

	err := Run(t.Context(), Config{
		Store:    s,
		RelayURL: fr.server.URL,
		Metrics:  NewSeedMetrics(nil),
	})
	require.NoError(t, err)

	require.Equal(t, int32(0), fr.calls.Load(), "Run should not touch the relay when already complete")
	count, err := CountRepos(s)
	require.NoError(t, err)
	require.Equal(t, int64(0), count, "no DIDs should have been seeded")
}

// TestRun_ResumesInterruptedSeed simulates a process that died
// mid-seed: bootstrap/state is PhaseSeed, some repo/<did> rows
// already exist. Run should re-enter the seed phase, pick up new
// DIDs, and advance to PhaseComplete without clobbering the
// already-seeded rows.
func TestRun_ResumesInterruptedSeed(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	// Simulate a partial seed: one DID already present, state is
	// PhaseSeed (orchestrator wrote the marker before crashing).
	require.NoError(t, PutRepoStatus(s, "did:plc:aaa", RepoStatus{
		Backfill: RepoBackfillStatus{Status: StatusNotStarted},
	}))
	require.NoError(t, PutBootstrapState(s, BootstrapState{Phase: PhaseSeed}))

	fr := newFakeRelay(t, []string{"did:plc:aaa", "did:plc:bbb", "did:plc:ccc"})

	err := Run(t.Context(), Config{
		Store:    s,
		RelayURL: fr.server.URL,
		Metrics:  NewSeedMetrics(nil),
	})
	require.NoError(t, err)

	st, err := GetBootstrapState(s)
	require.NoError(t, err)
	require.Equal(t, PhaseComplete, st.Phase)

	count, err := CountRepos(s)
	require.NoError(t, err)
	require.Equal(t, int64(3), count, "all three DIDs should now be present")
}

// TestRun_RequiresStoreAndRelay pins the input validation. Both
// missing inputs are programmer errors and we want loud failures.
func TestRun_RequiresStoreAndRelay(t *testing.T) {
	t.Parallel()

	t.Run("missing store", func(t *testing.T) {
		t.Parallel()
		err := Run(context.Background(), Config{
			RelayURL: "https://relay.example.com",
			Metrics:  NewSeedMetrics(nil),
		})
		require.ErrorContains(t, err, "Store is required")
	})
	t.Run("missing relay", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)
		err := Run(context.Background(), Config{
			Store:   s,
			Metrics: NewSeedMetrics(nil),
		})
		require.ErrorContains(t, err, "RelayURL is required")
	})
}
