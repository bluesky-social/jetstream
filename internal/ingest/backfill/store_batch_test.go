package backfill

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/bluesky-social/jetstream/internal/metastore/storetest"
	"github.com/jcalabro/atmos"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

// reconcilePage is one listRepos page as the engine reconciles it.
type reconcilePage struct {
	host    string
	entries []atmossync.ListReposEntry
}

// reconcileOne drives a page through the Store one entry at a time: batches
// of one, which is how atmos's engine behaved before its callbacks took pages.
func reconcileOne(t *testing.T, st atmosbackfill.Store, p reconcilePage) []atmosbackfill.StoreEntry {
	t.Helper()
	ctx := t.Context()
	recs := make([]atmosbackfill.StoreEntry, 0, len(p.entries))
	for _, entry := range p.entries {
		got, err := st.Lookup(ctx, []atmos.DID{entry.DID})
		require.NoError(t, err)
		require.Len(t, got, 1)
		switch {
		case got[0].State == atmosbackfill.StateUnknown:
			require.NoError(t, st.OnDiscover(ctx, p.host, []atmossync.ListReposEntry{entry}))
		case got[0].Active != entry.Active:
			require.NoError(t, st.OnUpdate(ctx, p.host, []atmossync.ListReposEntry{entry}))
		}
		recs = append(recs, got[0])
	}
	return recs
}

// reconcileBatched drives a page through the Store exactly as atmos's engine
// does: per run of distinct DIDs, one Lookup, then at most one OnDiscover and
// one OnUpdate.
func reconcileBatched(t *testing.T, st atmosbackfill.Store, p reconcilePage) []atmosbackfill.StoreEntry {
	t.Helper()
	ctx := t.Context()
	recs := make([]atmosbackfill.StoreEntry, 0, len(p.entries))
	entries := p.entries
	for len(entries) > 0 {
		n := len(entries)
		seen := map[atmos.DID]struct{}{}
		for i, entry := range entries {
			if _, dup := seen[entry.DID]; dup {
				n = i
				break
			}
			seen[entry.DID] = struct{}{}
		}
		chunk := entries[:n]
		dids := make([]atmos.DID, n)
		for i, entry := range chunk {
			dids[i] = entry.DID
		}
		got, err := st.Lookup(ctx, dids)
		require.NoError(t, err)
		require.Len(t, got, n)
		var discovered, updated []atmossync.ListReposEntry
		for i, entry := range chunk {
			switch {
			case got[i].State == atmosbackfill.StateUnknown:
				discovered = append(discovered, entry)
			case got[i].Active != entry.Active:
				updated = append(updated, entry)
			}
		}
		if len(discovered) > 0 {
			require.NoError(t, st.OnDiscover(ctx, p.host, discovered))
		}
		if len(updated) > 0 {
			require.NoError(t, st.OnUpdate(ctx, p.host, updated))
		}
		recs = append(recs, got...)
		entries = entries[n:]
	}
	return recs
}

func recordHostsOne(t *testing.T, st atmosbackfill.Store, hosts []atmosbackfill.HostInfo) []atmosbackfill.HostCursorState {
	t.Helper()
	out := make([]atmosbackfill.HostCursorState, 0, len(hosts))
	for _, info := range hosts {
		require.NoError(t, st.OnHost(t.Context(), []atmosbackfill.HostInfo{info}))
		got, err := st.HostCursor(t.Context(), []string{info.Hostname})
		require.NoError(t, err)
		require.Len(t, got, 1)
		out = append(out, got[0])
	}
	return out
}

func recordHostsBatched(t *testing.T, st atmosbackfill.Store, hosts []atmosbackfill.HostInfo) []atmosbackfill.HostCursorState {
	t.Helper()
	require.NoError(t, st.OnHost(t.Context(), hosts))
	names := make([]string, len(hosts))
	for i, info := range hosts {
		names[i] = info.Hostname
	}
	got, err := st.HostCursor(t.Context(), names)
	require.NoError(t, err)
	require.Len(t, got, len(hosts))
	return got
}

// normalizedKeyspace returns every metadata row with wall-clock timestamps
// blanked, so two runs that made the same writes at different instants
// compare equal.
func normalizedKeyspace(t *testing.T, db metastore.Store) map[string]any {
	t.Helper()
	out := map[string]any{}
	for _, kv := range storetest.Scan(t, db, nil, nil) {
		var v any
		require.NoError(t, json.Unmarshal(kv.Value, &v), "row %q", kv.Key)
		out[string(kv.Key)] = blankTimes(v)
	}
	return out
}

func blankTimes(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			x[k] = blankTimes(e)
		}
	case []any:
		for i, e := range x {
			x[i] = blankTimes(e)
		}
	case string:
		if _, err := time.Parse(time.RFC3339Nano, x); err == nil {
			return "<time>"
		}
	}
	return v
}

// batchScenario is a seeded prior state and a listing to reconcile against it.
type batchScenario struct {
	prior  map[atmos.DID]*RepoStatus
	roster []*PDSHost
	hosts  []atmosbackfill.HostInfo
	pages  []reconcilePage
}

var batchScenarioHosts = []string{"alpha.example.test", "beta.example.test:8443", "gamma.example.test"}

func newBatchScenario(rng *rand.Rand) batchScenario {
	var sc batchScenario
	universe := make([]atmos.DID, 60)
	for i := range universe {
		universe[i] = atmos.DID(fmt.Sprintf("did:plc:batch%03d", i))
	}
	buckets := []string{"", "alpha.example.test", "beta.example.test", "other.example.test"}
	statuses := []Status{StatusNotStarted, StatusPending, StatusComplete, StatusFailed, StatusUnavailable}
	sc.prior = map[atmos.DID]*RepoStatus{}
	for _, did := range universe {
		if rng.IntN(2) == 0 {
			continue
		}
		sc.prior[did] = &RepoStatus{
			Backfill: RepoBackfillStatus{Status: statuses[rng.IntN(len(statuses))], Attempts: rng.IntN(3)},
			PDS:      batchScenarioHosts[rng.IntN(len(batchScenarioHosts))],
			Host:     buckets[rng.IntN(len(buckets))],
			Active:   rng.IntN(3) != 0,
		}
	}
	for i, host := range batchScenarioHosts {
		sc.hosts = append(sc.hosts, atmosbackfill.HostInfo{Hostname: host, RelayStatus: "active", RelayAccounts: int64(100 * (i + 1)), Seq: int64(i)})
		if rng.IntN(2) == 0 {
			sc.roster = append(sc.roster, &PDSHost{
				Hostname: host, ListReposCursor: fmt.Sprintf("cursor-%d", i), LastNonEmptyCursor: "last",
				Enumerated: rng.IntN(2) == 0, State: string(atmosbackfill.HostStateRunning), ActualAccounts: uint64(rng.IntN(5)),
			})
		}
	}
	for _, host := range batchScenarioHosts {
		for range 1 + rng.IntN(3) {
			p := reconcilePage{host: host}
			for range 1 + rng.IntN(25) {
				// Draw from a small window so pages repeat DIDs within
				// themselves and across hosts.
				p.entries = append(p.entries, atmossync.ListReposEntry{DID: universe[rng.IntN(len(universe))], Active: rng.IntN(4) != 0})
			}
			sc.pages = append(sc.pages, p)
		}
	}
	rng.Shuffle(len(sc.pages), func(i, j int) { sc.pages[i], sc.pages[j] = sc.pages[j], sc.pages[i] })
	return sc
}

// seed writes the scenario's prior rows into a fresh store.
func (sc batchScenario) seed(t *testing.T) (*Store, metastore.Store) {
	t.Helper()
	db := memstore.New()
	for did, rs := range sc.prior {
		enc, err := encodeRepoStatus(rs)
		require.NoError(t, err)
		require.NoError(t, db.Set(t.Context(), repoKey(did), enc))
	}
	for _, host := range sc.roster {
		enc, err := encodePDSHost(host)
		require.NoError(t, err)
		require.NoError(t, db.Set(t.Context(), pdsHostKey(host.Hostname), enc))
	}
	// Host aggregates consistent with the rows, as production maintains
	// them. The aggregate decrements clamp at zero, so inconsistent seeds
	// would make legitimately reordered writes compare unequal.
	aggs := map[string]*HostStatus{}
	for _, rs := range sc.prior {
		if rs.Host == "" {
			continue
		}
		hs := aggs[rs.Host]
		if hs == nil {
			hs = newHostStatus(rs.Host)
			aggs[rs.Host] = hs
		}
		applyHostStatusTransition(hs, true, rs.Active, "", rs.Backfill.Status)
	}
	b := db.NewBatch()
	for _, hs := range aggs {
		require.NoError(t, stageHostStatus(b, hs))
	}
	require.NoError(t, b.Commit(t.Context()))
	return newSeededStore(t, db, nil), db
}

// TestStore_BatchMatchesPerEntry is the batching equivalence property: a
// seeded listing reconciled a page at a time must return the same lookups and
// leave the same keyspace as the same listing in batches of one — across prior rows in every status, interrupted not_started rows,
// host moves, Active flips, repeated DIDs, and pre-existing roster rows.
func TestStore_BatchMatchesPerEntry(t *testing.T) {
	t.Parallel()
	for seed := range uint64(40) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			sc := newBatchScenario(rand.New(rand.NewPCG(seed, 0xba7c4)))

			perEntry, perEntryDB := sc.seed(t)
			batched, batchedDB := sc.seed(t)
			pa, ba := perEntry.AtmosStore(), batched.AtmosStore()

			require.Equal(t, recordHostsOne(t, pa, sc.hosts), recordHostsBatched(t, ba, sc.hosts))

			for i, p := range sc.pages {
				want := reconcileOne(t, pa, p)
				got := reconcileBatched(t, ba, p)
				require.Equal(t, want, got, "page %d (%s) lookups", i, p.host)
			}
			require.Equal(t, normalizedKeyspace(t, perEntryDB), normalizedKeyspace(t, batchedDB))
		})
	}
}

// TestStore_BatchRoundTrips pins the metadata round trips each batched path
// makes, independent of how many entries it carries. A regression to per-key
// reads multiplies a leader's PostgreSQL latency by the page size.
func TestStore_BatchRoundTrips(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := &countingStore{Store: memstore.New()}
	const host = "pds.example.test"

	// Prior rows, written before SeedCounts tallies them: ten complete rows
	// attributed to ten different hosts, which their flips must decrement,
	// and ten interrupted not_started rows from an earlier run.
	dids := make([]atmos.DID, 1000)
	for i := range dids {
		dids[i] = atmos.DID(fmt.Sprintf("did:plc:rt%04d", i))
	}
	for i := range 10 {
		enc, err := encodeRepoStatus(&RepoStatus{Backfill: RepoBackfillStatus{Status: StatusComplete}, Host: fmt.Sprintf("old%d.example.test", i), Active: true})
		require.NoError(t, err)
		require.NoError(t, db.Set(ctx, repoKey(dids[i]), enc))
	}
	interrupted := make([]atmos.DID, 0, 10)
	for i := 10; i < 20; i++ {
		did := atmos.DID(fmt.Sprintf("did:plc:old%04d", i))
		interrupted = append(interrupted, did)
		enc, err := encodeRepoStatus(&RepoStatus{Backfill: RepoBackfillStatus{Status: StatusNotStarted}, Active: true})
		require.NoError(t, err)
		require.NoError(t, db.Set(ctx, repoKey(did), enc))
	}
	st := newSeededStore(t, db, nil)
	ba := st.AtmosStore()

	hosts := make([]atmosbackfill.HostInfo, 500)
	for i := range hosts {
		hosts[i] = atmosbackfill.HostInfo{Hostname: fmt.Sprintf("h%03d.example.test", i), RelayStatus: "active"}
	}
	names := make([]string, len(hosts))
	for i, info := range hosts {
		names[i] = info.Hostname
	}
	db.reset()
	require.NoError(t, ba.OnHost(ctx, hosts))
	require.Zero(t, db.gets.Load(), "OnHost: point reads")
	require.Equal(t, int64(1), db.getManys.Load(), "OnHost: multi-key reads")
	require.Equal(t, int64(1), db.commits.Load(), "OnHost: commits")

	db.reset()
	cursors, err := ba.HostCursor(ctx, names)
	require.NoError(t, err)
	require.Len(t, cursors, len(names))
	require.Zero(t, db.gets.Load(), "HostCursor: point reads")
	require.Equal(t, int64(1), db.getManys.Load(), "HostCursor: multi-key reads")
	require.Zero(t, db.commits.Load(), "HostCursor: commits")

	db.reset()
	recs, err := ba.Lookup(ctx, dids)
	require.NoError(t, err)
	require.Len(t, recs, len(dids))
	require.Zero(t, db.gets.Load(), "Lookup: point reads")
	require.Equal(t, int64(1), db.getManys.Load(), "Lookup: multi-key reads")
	require.Zero(t, db.commits.Load(), "Lookup: commits")

	var discovered, updated []atmossync.ListReposEntry
	for i, did := range dids {
		if i < 10 {
			updated = append(updated, atmossync.ListReposEntry{DID: did, Active: false})
			continue
		}
		discovered = append(discovered, atmossync.ListReposEntry{DID: did, Active: i%7 != 0})
	}
	db.reset()
	require.NoError(t, ba.OnDiscover(ctx, host, discovered))
	require.Zero(t, db.gets.Load(), "OnDiscover: point reads")
	require.LessOrEqual(t, db.getManys.Load(), int64(2), "OnDiscover: multi-key reads")
	require.Equal(t, int64(1), db.commits.Load(), "OnDiscover: commits")

	db.reset()
	require.NoError(t, ba.OnUpdate(ctx, host, updated))
	require.Zero(t, db.gets.Load(), "OnUpdate: point reads")
	require.LessOrEqual(t, db.getManys.Load(), int64(2), "OnUpdate: multi-key reads")
	require.Equal(t, int64(1), db.commits.Load(), "OnUpdate: commits")

	// Interrupted not_started rows from an earlier run defer in one write.
	db.reset()
	recs, err = ba.Lookup(ctx, interrupted)
	require.NoError(t, err)
	for _, rec := range recs {
		require.Equal(t, atmosbackfill.StateComplete, rec.State)
	}
	require.Zero(t, db.gets.Load(), "Lookup with deferrals: point reads")
	require.Equal(t, int64(1), db.commits.Load(), "Lookup with deferrals: commits")

	// Completion staging for a hundred repos reads in two round trips.
	cb := NewCompletionBatcher(st, nil)
	st.SetCompletionBatcher(cb)
	for i := 100; i < 200; i++ {
		cb.RecordWatermark(dids[i], uint64(i), true)
		require.NoError(t, cb.QueueComplete(ctx, dids[i], fmt.Sprintf("p%d.example.test", i%5), &repo.Commit{DID: string(dids[i]), Rev: "rev"}))
	}
	require.NoError(t, cb.QueueHostCursor(ctx, host, "next"))
	db.reset()
	b := db.NewBatch()
	afterCommit, afterDone, err := cb.StageDurable(ctx, b, 1_000, true, nil)
	require.NoError(t, err)
	require.Zero(t, db.gets.Load(), "StageDurable: point reads")
	require.LessOrEqual(t, db.getManys.Load(), int64(2), "StageDurable: multi-key reads")
	commitErr := b.Commit(ctx)
	afterDone(commitErr)
	require.NoError(t, commitErr)
	afterCommit()

	counts, ok, err := LoadCounts(db)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(len(dids)+len(interrupted)), counts.Total)
	require.Equal(t, uint64(100+10), counts.Complete, "100 staged completions plus the 10 prior complete rows")
	require.Equal(t, uint64(len(interrupted)), counts.Pending)
}

// TestStore_ConcurrentHostsGroupCommit reconciles many hosts' pages
// concurrently a page at a time — so their writes share group commits —
// and requires the same keyspace as reconciling them one entry at a time.
func TestStore_ConcurrentHostsGroupCommit(t *testing.T) {
	t.Parallel()
	const hosts, pages, perPage = 16, 4, 40
	plan := make([][]reconcilePage, hosts)
	infos := make([]atmosbackfill.HostInfo, hosts)
	for h := range hosts {
		name := fmt.Sprintf("host%02d.example.test", h)
		infos[h] = atmosbackfill.HostInfo{Hostname: name, RelayStatus: "active", RelayAccounts: pages * perPage}
		for p := range pages {
			page := reconcilePage{host: name}
			for i := range perPage {
				// Each host lists its own DIDs; a few repeat across its
				// pages with a flipped Active.
				n := p*perPage + i
				if i%10 == 9 && p > 0 {
					n = (p-1)*perPage + i
				}
				page.entries = append(page.entries, atmossync.ListReposEntry{
					DID: atmos.DID(fmt.Sprintf("did:plc:h%02d-%04d", h, n)), Active: (n+p)%3 != 0,
				})
			}
			plan[h] = append(plan[h], page)
		}
	}

	refDB := memstore.New()
	ref := newSeededStore(t, refDB, nil).AtmosStore()
	recordHostsOne(t, ref, infos)
	for h := range hosts {
		for _, page := range plan[h] {
			reconcileOne(t, ref, page)
		}
	}

	db := &countingStore{Store: memstore.New(), commitDelay: 500 * time.Microsecond}
	st := newSeededStore(t, db, nil).AtmosStore()
	db.reset()
	var wg sync.WaitGroup
	for h := range hosts {
		wg.Go(func() {
			require.NoError(t, st.OnHost(context.Background(), infos[h:h+1]))
			for _, page := range plan[h] {
				reconcileBatched(t, st, page)
			}
		})
	}
	wg.Wait()

	require.Equal(t, normalizedKeyspace(t, refDB), normalizedKeyspace(t, db.Store))
	writes := int64(hosts * (1 + pages))
	require.Less(t, db.commits.Load(), writes, "concurrent page writes must share commits")
}
