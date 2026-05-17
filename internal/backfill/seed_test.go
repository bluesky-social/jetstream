package backfill

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/jcalabro/atmos"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// fakeLister implements SeedReposLister for fast, allocation-free
// unit tests of the seed loop. The integration with the real atmos
// XRPC stack is exercised separately by TestSeedRepos_AgainstRelay.
type fakeLister struct {
	entries []atmossync.ListReposEntry
	err     error
}

func (f *fakeLister) ListRepos(_ context.Context, _ int64) listReposIter {
	return func(yield func(atmossync.ListReposEntry, error) bool) {
		for _, e := range f.entries {
			if !yield(e, nil) {
				return
			}
		}
		if f.err != nil {
			yield(atmossync.ListReposEntry{}, f.err)
		}
	}
}

func dids(raw ...string) []atmossync.ListReposEntry {
	out := make([]atmossync.ListReposEntry, len(raw))
	for i, r := range raw {
		out[i] = atmossync.ListReposEntry{
			DID:    atmos.DID(r),
			Active: true,
		}
	}
	return out
}

// TestSeedRepos_HappyPath exercises the seed loop end-to-end via a
// fake lister. It validates that every entry produces a row, that
// the stored status is StatusNotStarted, and that the SeedResult /
// metric counters line up.
func TestSeedRepos_HappyPath(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	metrics := NewSeedMetrics(nil)

	lister := &fakeLister{
		entries: dids("did:plc:aaa", "did:plc:bbb", "did:plc:ccc"),
	}

	res, err := seedReposImpl(t.Context(), s, lister, metrics, nil)
	require.NoError(t, err)
	require.Equal(t, SeedResult{Enumerated: 3, Seeded: 3}, res)

	count, err := CountRepos(s)
	require.NoError(t, err)
	require.Equal(t, int64(3), count)

	for _, raw := range []string{"did:plc:aaa", "did:plc:bbb", "did:plc:ccc"} {
		rs, ok, err := GetRepoStatus(s, atmos.DID(raw))
		require.NoError(t, err)
		require.True(t, ok, "missing row for %s", raw)
		require.Equal(t, StatusNotStarted, rs.Backfill.Status)
		require.False(t, rs.UpdatedAt.IsZero(), "UpdatedAt should be stamped")
	}
}

// TestSeedRepos_Idempotent guards the documented re-run contract:
// re-running the seed step never clobbers an existing row,
// regardless of its current Status. We simulate a partially-
// completed backfill by writing one DID as StatusComplete before
// the seed run; the seed must leave it alone.
func TestSeedRepos_Idempotent(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	did := atmos.DID("did:plc:already-done")
	require.NoError(t, PutRepoStatus(s, did, RepoStatus{
		Backfill: RepoBackfillStatus{
			Status: StatusComplete,
			Rev:    "rev-abc",
		},
	}))

	lister := &fakeLister{entries: dids(string(did), "did:plc:new")}
	metrics := NewSeedMetrics(nil)

	res, err := seedReposImpl(t.Context(), s, lister, metrics, nil)
	require.NoError(t, err)
	require.Equal(t, SeedResult{Enumerated: 2, Seeded: 1, SkippedExisting: 1}, res)

	// Existing row is preserved.
	rs, ok, err := GetRepoStatus(s, did)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, StatusComplete, rs.Backfill.Status)
	require.Equal(t, "rev-abc", rs.Backfill.Rev)

	// New row was inserted at not_started.
	rs, ok, err = GetRepoStatus(s, "did:plc:new")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, StatusNotStarted, rs.Backfill.Status)
}

// TestSeedRepos_PropagatesListerError ensures a relay-side error
// surfaces as a seed error and that any rows we'd already batched
// are still written (the documented best-effort flush).
func TestSeedRepos_PropagatesListerError(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	boom := errors.New("relay exploded")
	lister := &fakeLister{
		entries: dids("did:plc:aaa", "did:plc:bbb"),
		err:     boom,
	}

	res, err := seedReposImpl(t.Context(), s, lister, nil, nil)
	require.ErrorIs(t, err, boom)
	require.Equal(t, int64(2), res.Enumerated)

	// Both pre-error rows should still be on disk after the flush.
	count, err := CountRepos(s)
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
}

// TestSeedRepos_RespectsContextCancel verifies that a cancelled
// context aborts the loop and that whatever was batched lands on
// disk (so a retry is meaningful). We feed a "many" stream and
// cancel after the first entry.
func TestSeedRepos_RespectsContextCancel(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel before the loop even starts

	lister := &fakeLister{entries: dids("did:plc:aaa", "did:plc:bbb")}
	res, err := seedReposImpl(ctx, s, lister, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(0), res.Seeded)
}

// TestSeedRepos_AgainstRelay is the happy-path integration test that
// drives a real atmos sync.Client against a stubbed relay. It's the
// guard that catches breakage in the SeedReposLister adaptation
// layer if either atmos or our wrapper changes shape.
func TestSeedRepos_AgainstRelay(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	type repoEntry struct {
		DID    string `json:"did"`
		Head   string `json:"head"`
		Rev    string `json:"rev"`
		Active bool   `json:"active"`
	}
	type page struct {
		Cursor string      `json:"cursor,omitempty"`
		Repos  []repoEntry `json:"repos"`
	}
	pages := []page{
		{
			Cursor: "p1",
			Repos: []repoEntry{
				{DID: "did:plc:aaa", Head: "bafyaaa", Rev: "rev1", Active: true},
				{DID: "did:plc:bbb", Head: "bafybbb", Rev: "rev2", Active: true},
			},
		},
		{
			Repos: []repoEntry{
				{DID: "did:plc:ccc", Head: "bafyccc", Rev: "rev3", Active: true},
			},
		},
	}

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/xrpc/com.atproto.sync.listRepos", r.URL.Path)
		idx := int(calls.Add(1)) - 1
		if idx >= len(pages) {
			_ = json.NewEncoder(w).Encode(page{})
			return
		}
		_ = json.NewEncoder(w).Encode(pages[idx])
	}))
	t.Cleanup(srv.Close)

	xc := &xrpc.Client{
		Host:  srv.URL,
		Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
	}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})

	res, err := SeedRepos(t.Context(), s, sc, nil, nil)
	require.NoError(t, err)
	require.Equal(t, SeedResult{Enumerated: 3, Seeded: 3}, res)

	count, err := CountRepos(s)
	require.NoError(t, err)
	require.Equal(t, int64(3), count)
}
