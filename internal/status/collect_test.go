package status_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/status"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/stretchr/testify/require"
)

type fakeIdentityResolver struct {
	handles map[atmos.Handle]atmos.DID
	docs    map[atmos.DID]*identity.DIDDocument
	err     error
}

func (r *fakeIdentityResolver) ResolveDID(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	if r.err != nil {
		return nil, r.err
	}
	doc, ok := r.docs[did]
	if !ok {
		return nil, errors.New("did not found")
	}
	return doc, nil
}

func (r *fakeIdentityResolver) ResolveHandle(_ context.Context, handle atmos.Handle) (atmos.DID, error) {
	if r.err != nil {
		return "", r.err
	}
	did, ok := r.handles[handle]
	if !ok {
		return "", errors.New("handle not found")
	}
	return did, nil
}

func TestCollect_FreshDataDir(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	c, err := status.New(status.Options{
		Store:   st,
		DataDir: dataDir,
		Now:     func() time.Time { return time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC) },
	})
	require.NoError(t, err)

	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)
	require.NotNil(t, snap)

	// Empty data dir: no segments, no phase, no cursors.
	require.Equal(t, "", string(snap.Phase.Phase))
	require.True(t, snap.Phase.PhaseEnteredAt.IsZero())
	require.Equal(t, status.BackfillStats{}, snap.Backfill)
	require.Equal(t, status.LiveStats{}, snap.Live)
	require.NotNil(t, snap.SegmentAggregate)
	require.Len(t, snap.SegmentAggregate.Trees, 2)
	require.Equal(t, filepath.Join(dataDir, "segments"), snap.SegmentAggregate.Trees[0].Dir)
	require.Equal(t, filepath.Join(dataDir, "backfill", "live_segments"), snap.SegmentAggregate.Trees[1].Dir)
	require.Equal(t, 0, snap.SegmentAggregate.Trees[0].SealedCount+snap.SegmentAggregate.Trees[0].ActiveCount)
	require.Equal(t, 0, snap.SegmentAggregate.Trees[1].SealedCount+snap.SegmentAggregate.Trees[1].ActiveCount)
}

func TestCollect_BuildsFreshSnapshotEachCall(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	var mu sync.Mutex
	now := time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC)
	c, err := status.New(status.Options{
		Store:   st,
		DataDir: dataDir,
		Now: func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			out := now
			now = now.Add(time.Second)
			return out
		},
	})
	require.NoError(t, err)

	a, err := c.Snapshot(context.Background())
	require.NoError(t, err)
	b, err := c.Snapshot(context.Background())
	require.NoError(t, err)
	require.NotSame(t, a, b, "snapshot pointer should change without a snapshot cache")
	require.NotEqual(t, a.GeneratedAt, b.GeneratedAt)
}

func TestCollect_LiveLastSeenUpstreamEvent(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	now := time.Date(2026, 7, 8, 12, 0, 30, 0, time.UTC)
	seen := now.Add(-17 * time.Second)
	c, err := status.New(status.Options{
		Store:   st,
		DataDir: dataDir,
		Now:     func() time.Time { return now },
		LastSeenUpstreamEvent: func() time.Time {
			return seen
		},
	})
	require.NoError(t, err)

	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)
	require.True(t, snap.Live.LastSeenUpstreamEventAt.Equal(seen))
	require.Equal(t, 17*time.Second, snap.Live.LastSeenUpstreamEventAge)
}

func TestCollect_PhaseAndEnteredAt(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	enteredAt := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, lifecycle.WritePhase(st, lifecycle.PhaseSteadyState, enteredAt))

	c, err := status.New(status.Options{
		Store:   st,
		DataDir: dataDir,
		Now:     func() time.Time { return enteredAt.Add(24 * time.Hour) },
	})
	require.NoError(t, err)
	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	require.Equal(t, lifecycle.PhaseSteadyState, snap.Phase.Phase)
	require.True(t, snap.Phase.PhaseEnteredAt.Equal(enteredAt))
}

func TestCollect_BackfillTiming(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	startedAt := time.Date(2026, 5, 1, 1, 0, 0, 0, time.UTC)
	completedAt := startedAt.Add(3*24*time.Hour + 7*time.Hour)
	require.NoError(t, lifecycle.WriteBackfillTiming(st, startedAt, completedAt))
	require.NoError(t, backfill.SaveCounts(st, backfill.Counts{Total: 10, Discovered: 10, Complete: 10}))

	c, err := status.New(status.Options{Store: st, DataDir: dataDir})
	require.NoError(t, err)
	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	require.True(t, snap.Backfill.StartedAt.Equal(startedAt))
	require.True(t, snap.Backfill.CompletedAt.Equal(completedAt))
	require.Equal(t, completedAt.Sub(startedAt), snap.Backfill.Duration)
}

func TestCollect_BackfillCounts(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	bs := backfill.NewStore(st, nil)
	ctx := context.Background()

	for i := range 5 {
		did := atmos.DID("did:plc:disc" + string(rune('a'+i)))
		require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
	}
	for i := range 3 {
		did := atmos.DID("did:plc:done" + string(rune('a'+i)))
		require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
		require.NoError(t, bs.OnComplete(ctx, did, "", &repo.Commit{Rev: "abcdef"}))
	}
	for i := range 2 {
		did := atmos.DID("did:plc:gone" + string(rune('a'+i)))
		require.NoError(t, bs.OnDiscover(ctx, atmossync.ListReposEntry{DID: did, Active: true}))
		require.NoError(t, bs.OnFail(ctx, did, "", &xrpc.Error{
			StatusCode: 400, Name: "RepoDeactivated", Message: "Repo has been deactivated",
		}, 1))
	}

	c, err := status.New(status.Options{Store: st, DataDir: dataDir})
	require.NoError(t, err)
	snap, err := c.Snapshot(ctx)
	require.NoError(t, err)

	require.Equal(t, uint64(10), snap.Backfill.TotalDIDs)
	require.Equal(t, uint64(5), snap.Backfill.Discovered)
	require.Equal(t, uint64(3), snap.Backfill.Complete)
	require.Equal(t, uint64(0), snap.Backfill.Failed)
	require.Equal(t, uint64(2), snap.Backfill.Unavailable)
	require.InDelta(t, 30.0, snap.Backfill.PercentComplete, 0.001)
}

func TestCollect_HostDiagnosticsFromAggregates(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	hs := &backfill.HostStatus{
		Host:             "pds.example.com",
		Total:            10,
		Active:           9,
		NotStarted:       1,
		Complete:         7,
		Failed:           2,
		Unavailable:      1,
		LastAttemptedAt:  now,
		LatestError:      "xrpc: HTTP 503",
		LatestErrorClass: backfill.ErrorClassHTTP5xx,
		ErrorClassCounts: map[backfill.ErrorClass]uint64{backfill.ErrorClassHTTP5xx: 2},
		RecentErrors: []backfill.HostErrorSample{
			{DID: atmos.DID("did:plc:alice"), AttemptedAt: now, Class: backfill.ErrorClassHTTP5xx, Error: "xrpc: HTTP 503"},
		},
	}
	enc, err := backfill.EncodeHostStatus(hs)
	require.NoError(t, err)
	require.NoError(t, st.Set([]byte("host/pds.example.com"), enc, store.SyncWrites))

	c, err := status.New(status.Options{Store: st, DataDir: dataDir})
	require.NoError(t, err)
	snap, err := c.SnapshotForRequest(t.Context(), status.Request{Tab: "hosts"})
	require.NoError(t, err)
	require.Equal(t, "hosts", snap.Request.Tab)
	require.Len(t, snap.Hosts.Rows, 1)
	require.Equal(t, "pds.example.com", snap.Hosts.Rows[0].Host)
	require.Equal(t, uint64(10), snap.Hosts.Rows[0].Total)
	require.Equal(t, uint64(2), snap.Hosts.Rows[0].Failed)
	require.Equal(t, uint64(1), snap.Hosts.Rows[0].Unavailable)
	require.Equal(t, "http_5xx", snap.Hosts.Rows[0].LatestErrorClass)
	require.Equal(t, uint64(2), snap.Hosts.Rows[0].ErrorClassCounts["http_5xx"])
	require.Len(t, snap.Hosts.Rows[0].RecentErrors, 1)
	require.Equal(t, "did:plc:alice", snap.Hosts.Rows[0].RecentErrors[0].DID)
	require.Len(t, snap.Hosts.TopFailing, 1)
	require.Equal(t, "pds.example.com", snap.Hosts.TopFailing[0].Host)
}

func TestCollect_HostDiagnosticsTopFailingIsBounded(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	for i := range 12 {
		host := "pds" + string(rune('a'+i)) + ".example.com"
		enc, err := backfill.EncodeHostStatus(&backfill.HostStatus{
			Host: host, Total: 20, Failed: uint64(12 - i),
		})
		require.NoError(t, err)
		require.NoError(t, st.Set([]byte("host/"+host), enc, store.SyncWrites))
	}
	healthy, err := backfill.EncodeHostStatus(&backfill.HostStatus{
		Host: "healthy.example.com", Total: 100, Complete: 100,
	})
	require.NoError(t, err)
	require.NoError(t, st.Set([]byte("host/healthy.example.com"), healthy, store.SyncWrites))

	c, err := status.New(status.Options{Store: st, DataDir: dataDir})
	require.NoError(t, err)
	snap, err := c.SnapshotForRequest(t.Context(), status.Request{Tab: "hosts", HostSort: "failing"})
	require.NoError(t, err)

	require.Equal(t, "failing", snap.Request.HostSort)
	require.Len(t, snap.Hosts.Rows, 13)
	require.Len(t, snap.Hosts.TopFailing, 10)
	require.Equal(t, uint64(12), snap.Hosts.Rows[0].Failed)
	require.Equal(t, uint64(12), snap.Hosts.TopFailing[0].Failed)
	require.Equal(t, uint64(3), snap.Hosts.TopFailing[9].Failed)
	for _, row := range snap.Hosts.TopFailing {
		require.Positive(t, row.Failed)
		require.NotEqual(t, "healthy.example.com", row.Host)
	}
}

func TestCollect_HostDiagnosticsDefaultsToLargestHosts(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	for _, hs := range []*backfill.HostStatus{
		{Host: "small-failing.example.com", Total: 10, Failed: 9},
		{Host: "large-healthy.example.com", Total: 100, Complete: 100},
		{Host: "medium-failing.example.com", Total: 50, Failed: 8},
	} {
		enc, err := backfill.EncodeHostStatus(hs)
		require.NoError(t, err)
		require.NoError(t, st.Set([]byte("host/"+hs.Host), enc, store.SyncWrites))
	}

	c, err := status.New(status.Options{Store: st, DataDir: dataDir})
	require.NoError(t, err)
	snap, err := c.SnapshotForRequest(t.Context(), status.Request{Tab: "hosts"})
	require.NoError(t, err)

	require.Equal(t, "largest", snap.Request.HostSort)
	require.Equal(t, "large-healthy.example.com", snap.Hosts.Rows[0].Host)
	require.Equal(t, "medium-failing.example.com", snap.Hosts.Rows[1].Host)
	require.Equal(t, "small-failing.example.com", snap.Hosts.Rows[2].Host)
	require.Equal(t, "small-failing.example.com", snap.Hosts.TopFailing[0].Host)
}

func TestCollect_AccountLookupByHandle(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	did := atmos.DID("did:plc:alice")
	attemptedAt := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	rs := &backfill.RepoStatus{
		Backfill: backfill.RepoBackfillStatus{
			Status:    backfill.StatusFailed,
			Attempts:  3,
			LastError: "boom",
			Rev:       "backfill-rev",
		},
		Handle:          "alice.test",
		Host:            "pds.example.com",
		PDS:             "https://pds.example.com",
		Rev:             "latest-rev",
		LastAttemptedAt: attemptedAt,
		Active:          true,
		RecordCount:     42,
		TotalBytes:      1024,
	}
	enc, err := backfill.EncodeRepoStatus(rs)
	require.NoError(t, err)
	require.NoError(t, st.Set(backfill.RepoKey(string(did)), enc, store.SyncWrites))
	require.NoError(t, st.Set([]byte("handle/alice.test"), []byte(did), store.SyncWrites))

	hostEnc, err := backfill.EncodeHostStatus(&backfill.HostStatus{
		Host: "pds.example.com", Total: 5, Active: 4, Complete: 3, Failed: 2,
	})
	require.NoError(t, err)
	require.NoError(t, st.Set([]byte("host/pds.example.com"), hostEnc, store.SyncWrites))

	c, err := status.New(status.Options{Store: st, DataDir: dataDir})
	require.NoError(t, err)
	snap, err := c.SnapshotForRequest(t.Context(), status.Request{Tab: "accounts", Handle: "Alice.Test"})
	require.NoError(t, err)
	require.Equal(t, "accounts", snap.Request.Tab)
	require.True(t, snap.Account.Found)
	require.Equal(t, string(did), snap.Account.DID)
	require.Equal(t, "alice.test", snap.Account.Query)
	require.Equal(t, "handle", snap.Account.QueryKind)
	require.Equal(t, "alice.test", snap.Account.Handle)
	require.Equal(t, "pds.example.com", snap.Account.Host)
	require.Equal(t, "https://pds.example.com", snap.Account.PDS)
	require.True(t, snap.Account.Active)
	require.Equal(t, "failed", snap.Account.Backfill)
	require.Equal(t, 3, snap.Account.Attempts)
	require.Equal(t, "boom", snap.Account.LastError)
	require.Equal(t, "backfill-rev", snap.Account.BackfillRev)
	require.Equal(t, "latest-rev", snap.Account.LatestRev)
	require.Equal(t, int64(42), snap.Account.RecordCount)
	require.Equal(t, int64(1024), snap.Account.TotalBytes)
}

func TestCollect_AccountLookupByHandleResolverFallback(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	did := atmos.DID("did:plc:resolved")
	enc, err := backfill.EncodeRepoStatus(&backfill.RepoStatus{
		Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete},
		Host:     "pds.example.com",
		Active:   true,
	})
	require.NoError(t, err)
	require.NoError(t, st.Set(backfill.RepoKey(string(did)), enc, store.SyncWrites))

	c, err := status.New(status.Options{
		Store:   st,
		DataDir: dataDir,
		IdentityResolver: &fakeIdentityResolver{handles: map[atmos.Handle]atmos.DID{
			atmos.Handle("calabro.io"): did,
		}},
	})
	require.NoError(t, err)
	snap, err := c.SnapshotForRequest(t.Context(), status.Request{Tab: "accounts", Account: "Calabro.IO"})
	require.NoError(t, err)

	require.True(t, snap.Account.Found)
	require.Equal(t, string(did), snap.Account.DID)
	require.Equal(t, "calabro.io", snap.Account.Query)
	require.Equal(t, "handle", snap.Account.QueryKind)
	require.Equal(t, "calabro.io", snap.Account.Handle)
	require.Equal(t, "complete", snap.Account.Backfill)
}

func TestCollect_AccountLookupDIDHydratesMissingHandleFromResolver(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	did := atmos.DID("did:plc:resolved")
	enc, err := backfill.EncodeRepoStatus(&backfill.RepoStatus{
		Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete},
		Host:     "pds.example.com",
		Active:   true,
	})
	require.NoError(t, err)
	require.NoError(t, st.Set(backfill.RepoKey(string(did)), enc, store.SyncWrites))

	c, err := status.New(status.Options{
		Store:   st,
		DataDir: dataDir,
		IdentityResolver: &fakeIdentityResolver{docs: map[atmos.DID]*identity.DIDDocument{
			did: {
				ID:          string(did),
				AlsoKnownAs: []string{"at://resolved.test"},
				Service: []identity.Service{{
					ID:              "#atproto_pds",
					Type:            "AtprotoPersonalDataServer",
					ServiceEndpoint: "https://pds.example.com",
				}},
			},
		}},
	})
	require.NoError(t, err)
	snap, err := c.SnapshotForRequest(t.Context(), status.Request{Tab: "accounts", Account: string(did)})
	require.NoError(t, err)

	require.True(t, snap.Account.Found)
	require.Equal(t, string(did), snap.Account.DID)
	require.Equal(t, "resolved.test", snap.Account.Handle)
	require.Equal(t, "https://pds.example.com", snap.Account.PDS)
}

func TestCollect_AccountLookupByHandlePrefersResolverOverLocalIndex(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	staleDID := atmos.DID("did:plc:stale")
	freshDID := atmos.DID("did:plc:fresh")
	for _, did := range []atmos.DID{staleDID, freshDID} {
		enc, err := backfill.EncodeRepoStatus(&backfill.RepoStatus{
			Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete},
			Active:   true,
		})
		require.NoError(t, err)
		require.NoError(t, st.Set(backfill.RepoKey(string(did)), enc, store.SyncWrites))
	}
	require.NoError(t, st.Set([]byte("handle/alice.test"), []byte(staleDID), store.SyncWrites))

	c, err := status.New(status.Options{
		Store:   st,
		DataDir: dataDir,
		IdentityResolver: &fakeIdentityResolver{handles: map[atmos.Handle]atmos.DID{
			atmos.Handle("alice.test"): freshDID,
		}},
	})
	require.NoError(t, err)
	snap, err := c.SnapshotForRequest(t.Context(), status.Request{Tab: "accounts", Account: "alice.test"})
	require.NoError(t, err)

	require.True(t, snap.Account.Found)
	require.Equal(t, string(freshDID), snap.Account.DID)
}

func TestCollect_AccountLookupResolvedHandleWithMissingLocalMetadata(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	did := atmos.DID("did:plc:missinglocal")
	c, err := status.New(status.Options{
		Store:   st,
		DataDir: dataDir,
		IdentityResolver: &fakeIdentityResolver{handles: map[atmos.Handle]atmos.DID{
			atmos.Handle("missing.test"): did,
		}},
	})
	require.NoError(t, err)
	snap, err := c.SnapshotForRequest(t.Context(), status.Request{Tab: "accounts", Account: "missing.test"})
	require.NoError(t, err)

	require.False(t, snap.Account.Found)
	require.Equal(t, string(did), snap.Account.DID)
	require.Equal(t, "missing.test", snap.Account.Query)
	require.Equal(t, "handle", snap.Account.QueryKind)
}

func TestCollect_LiveCursors(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, live.SaveUpstreamCursor(st, live.CursorKey, 1234567))

	var seqBuf [8]byte
	binary.LittleEndian.PutUint64(seqBuf[:], 4242)
	require.NoError(t, st.Set([]byte(live.SteadySeqKey), seqBuf[:], store.SyncWrites))
	binary.LittleEndian.PutUint64(seqBuf[:], 1111)
	require.NoError(t, st.Set([]byte(live.BootstrapSeqKey), seqBuf[:], store.SyncWrites))

	c, err := status.New(status.Options{Store: st, DataDir: dataDir})
	require.NoError(t, err)
	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	require.Equal(t, int64(1234567), snap.Live.UpstreamCursor)
	require.Equal(t, uint64(4242), snap.Live.NextSeq)
	require.Equal(t, uint64(1111), snap.Live.BootstrapSeq)
}

func TestCollect_PebbleKeyspaces(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, st.Set([]byte("repo/a"), []byte("x"), store.SyncWrites))
	require.NoError(t, st.Set([]byte("repo/b"), []byte("x"), store.SyncWrites))
	require.NoError(t, st.Set([]byte("sync/chain/a"), []byte("x"), store.SyncWrites))
	require.NoError(t, st.Set([]byte("sync/host/a"), []byte("x"), store.SyncWrites))
	require.NoError(t, st.Set([]byte("relay/other"), []byte("x"), store.SyncWrites))
	require.NoError(t, st.Set([]byte("sync/identity/a"), []byte("x"), store.SyncWrites))

	c, err := status.New(status.Options{Store: st, DataDir: dataDir})
	require.NoError(t, err)
	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	require.Equal(t, uint64(2), snap.Pebble.KeyspaceCounts["repo/"])
	require.Equal(t, uint64(1), snap.Pebble.KeyspaceCounts["sync/chain/"])
	require.Equal(t, uint64(1), snap.Pebble.KeyspaceCounts["sync/host/"])
	require.Equal(t, uint64(1), snap.Pebble.KeyspaceCounts["relay/"])
	_, hasIdentity := snap.Pebble.KeyspaceCounts["sync/identity/"]
	require.False(t, hasIdentity, "sync/identity/ must not be exposed")
}

func TestCollect_WithManifestSkipsRepoScan(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	segmentsDir := filepath.Join(dataDir, "segments")
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
	require.NoError(t, makeSealedStatusSegment(filepath.Join(segmentsDir, "seg_0000000000.jss")))

	mft, err := manifest.Open(manifest.Options{
		SegmentsDir: segmentsDir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	require.NoError(t, st.Set([]byte("repo/did:plc:corrupt"), []byte("not json"), store.SyncWrites))
	require.NoError(t, backfill.SaveCounts(st, backfill.Counts{
		Total: 10, Discovered: 3, Complete: 6, Failed: 1,
	}))

	c, err := status.New(status.Options{Store: st, DataDir: dataDir, Manifest: mft})
	require.NoError(t, err)
	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	require.Equal(t, uint64(10), snap.Backfill.TotalDIDs)
	require.Equal(t, uint64(6), snap.Backfill.Complete)
	require.NotNil(t, snap.SegmentAggregate)
	require.Len(t, snap.SegmentAggregate.Trees, 2)
	require.Equal(t, 1, snap.SegmentAggregate.Trees[0].SealedCount)
	require.Equal(t, uint64(2), snap.SegmentAggregate.Trees[0].LatestSegment.EventCount)
	require.Equal(t, uint64(2), snap.SegmentAggregate.Network.Events)
	require.Greater(t, snap.SegmentAggregate.Trees[0].CompressedBytes, int64(0))
	_, hasRepoCount := snap.Pebble.KeyspaceCounts["repo/"]
	require.False(t, hasRepoCount, "manifest-backed status must not count Pebble keyspaces")
}

func TestCollect_DefaultSnapshotDoesNotScanHostAggregates(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	require.NoError(t, st.Set([]byte("host/corrupt.example.com"), []byte("not json"), store.SyncWrites))

	c, err := status.New(status.Options{Store: st, DataDir: dataDir})
	require.NoError(t, err)
	snap, err := c.Snapshot(t.Context())
	require.NoError(t, err)
	require.Empty(t, snap.Hosts.Rows)

	_, err = c.SnapshotForRequest(t.Context(), status.Request{Tab: "hosts"})
	require.Error(t, err)
	require.ErrorContains(t, err, "decode HostStatus")
}

func TestCollect_SnapshotForRequestSingleflightKeyDoesNotCollide(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	didA := atmos.DID("did:plc:a")
	didAB := atmos.DID("did:plc:a:b")
	for _, did := range []atmos.DID{didA, didAB} {
		enc, err := backfill.EncodeRepoStatus(&backfill.RepoStatus{
			Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete},
			Handle:   string(did),
		})
		require.NoError(t, err)
		require.NoError(t, st.Set(backfill.RepoKey(string(did)), enc, store.SyncWrites))
	}

	c, err := status.New(status.Options{Store: st, DataDir: dataDir})
	require.NoError(t, err)
	first, err := c.SnapshotForRequest(t.Context(), status.Request{Tab: "accounts", DID: string(didA), Handle: "b:c"})
	require.NoError(t, err)
	second, err := c.SnapshotForRequest(t.Context(), status.Request{Tab: "accounts", DID: string(didAB), Handle: "c"})
	require.NoError(t, err)

	require.Equal(t, string(didA), first.Account.DID)
	require.Equal(t, string(didAB), second.Account.DID)
}

func TestCollect_WithManifestIncludesWritableTails(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	segmentsDir := filepath.Join(dataDir, "segments")
	liveDir := filepath.Join(dataDir, "backfill", "live_segments")
	require.NoError(t, os.MkdirAll(segmentsDir, 0o755))
	require.NoError(t, os.MkdirAll(liveDir, 0o755))
	require.NoError(t, makeSealedStatusSegment(filepath.Join(segmentsDir, "seg_0000000000.jss")))

	mft, err := manifest.Open(manifest.Options{
		SegmentsDir: segmentsDir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	writeActiveSegment(t, segmentsDir, 1, []segment.Event{
		{Seq: 3, WitnessedAt: 1_700_000_000_000_003, Kind: segment.KindCreate, DID: "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb", Collection: "app.bsky.feed.like", Rkey: "r", Rev: "v", Payload: []byte("p")},
	})
	writeActiveSegment(t, liveDir, 0, []segment.Event{
		{Seq: 4, WitnessedAt: 1_700_000_000_000_004, Kind: segment.KindCreate, DID: "did:plc:cccccccccccccccccccccccc", Collection: "app.bsky.graph.follow", Rkey: "r", Rev: "v", Payload: []byte("p")},
	})

	require.NoError(t, backfill.SaveCounts(st, backfill.Counts{Total: 10, Discovered: 10, Complete: 8}))

	c, err := status.New(status.Options{Store: st, DataDir: dataDir, Manifest: mft})
	require.NoError(t, err)
	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	require.Len(t, snap.SegmentAggregate.Trees, 2)
	require.Equal(t, 1, snap.SegmentAggregate.Trees[0].SealedCount)
	require.Equal(t, 1, snap.SegmentAggregate.Trees[0].ActiveCount)
	require.Equal(t, uint64(3), snap.SegmentAggregate.Trees[0].EventCount)
	require.NotNil(t, snap.SegmentAggregate.Trees[0].LatestSegment)
	require.False(t, snap.SegmentAggregate.Trees[0].LatestSegment.Sealed)
	require.Equal(t, uint64(1), snap.SegmentAggregate.Trees[0].LatestSegment.Index)

	require.Equal(t, 0, snap.SegmentAggregate.Trees[1].SealedCount)
	require.Equal(t, 1, snap.SegmentAggregate.Trees[1].ActiveCount)
	require.Equal(t, uint64(1), snap.SegmentAggregate.Trees[1].EventCount)

	require.Equal(t, 3, snap.SegmentAggregate.Network.Segments)
	require.Equal(t, 1, snap.SegmentAggregate.Network.SealedSegments)
	require.Equal(t, 2, snap.SegmentAggregate.Network.ActiveSegments)
	require.Equal(t, uint64(4), snap.SegmentAggregate.Network.Events)
	require.Equal(t, 3, snap.SegmentAggregate.Network.Collections)
}

func TestCollect_CursorLookback_NoManifest(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	c, err := status.New(status.Options{
		Store:          st,
		DataDir:        dataDir,
		CursorLookback: 24 * time.Hour,
		Manifest:       nil, // No manifest wired in
	})
	require.NoError(t, err)
	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	// Should still report the configured lookback, but other fields are zero.
	require.Equal(t, 24*time.Hour, snap.CursorLookback.ConfiguredLookback)
	require.Equal(t, 0, snap.CursorLookback.ManifestSegmentCount)
	require.Equal(t, uint64(0), snap.CursorLookback.OldestRetainedSeq)
	require.True(t, snap.CursorLookback.OldestRetainedAt.IsZero())
}

func makeSealedStatusSegment(path string) error {
	w, err := segment.New(segment.Config{
		Path:              path,
		MaxEventsPerBlock: 2,
	})
	if err != nil {
		return err
	}
	for i := range 2 {
		if _, err := w.Append(segment.Event{
			Seq:         uint64(i + 1),
			Kind:        segment.KindCreate,
			DID:         "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
			WitnessedAt: 1_700_000_000_000_000 + int64(i),
			Collection:  "app.bsky.feed.post",
			Payload:     []byte("hello"),
		}); err != nil {
			return err
		}
	}
	_, err = w.Seal()
	return err
}

func TestCollect_CursorLookback_Disabled(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	c, err := status.New(status.Options{
		Store:          st,
		DataDir:        dataDir,
		CursorLookback: 0, // Disabled
	})
	require.NoError(t, err)
	snap, err := c.Snapshot(context.Background())
	require.NoError(t, err)

	// All fields should be zero when disabled.
	require.Equal(t, time.Duration(0), snap.CursorLookback.ConfiguredLookback)
	require.Equal(t, 0, snap.CursorLookback.ManifestSegmentCount)
	require.Equal(t, uint64(0), snap.CursorLookback.OldestRetainedSeq)
	require.True(t, snap.CursorLookback.OldestRetainedAt.IsZero())
}
