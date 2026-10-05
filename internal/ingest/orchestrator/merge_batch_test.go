package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// countingStore counts reads, so tests can pin the drain's round trips.
type countingStore struct {
	metastore.Store
	mu         sync.Mutex
	repoGets   int
	getMany    int
	getManyDID map[string]int
}

func (s *countingStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	if bytes.HasPrefix(key, backfill.RepoKey("")) {
		s.mu.Lock()
		s.repoGets++
		s.mu.Unlock()
	}
	return s.Store.Get(ctx, key)
}

func (s *countingStore) GetMany(ctx context.Context, keys [][]byte) ([][]byte, error) {
	s.mu.Lock()
	s.getMany++
	for _, k := range keys {
		s.getManyDID[strings.TrimPrefix(string(k), string(backfill.RepoKey("")))]++
	}
	s.mu.Unlock()
	return s.Store.GetMany(ctx, keys)
}

// drainFixture is a data dir whose bootstrap_live namespace holds small
// blocks, so each source segment spans many of them.
type drainFixture struct {
	dataDir string
	store   *countingStore
	o       *Orchestrator
	blocks  int
}

func newDrainFixture(t *testing.T, sources [][]segment.Event, repoRevs map[string]string, eventsPerBlock int) *drainFixture {
	t.Helper()
	fix := newMergeFixture(t, nil, nil)
	dataDir := t.TempDir()
	raw, err := pebblestore.Open(dataDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	st := &countingStore{Store: raw, getManyDID: map[string]int{}}
	for did, rev := range repoRevs {
		rs := &backfill.RepoStatus{
			Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: rev},
			Rev:      rev,
		}
		require.NoError(t, raw.Set(t.Context(), backfill.RepoKey(did), mustEncodeStatus(t, rs)))
	}
	liveDir := filepath.Join(dataDir, "backfill", "live_segments")
	require.NoError(t, os.MkdirAll(liveDir, 0o755))
	for _, evs := range sources {
		w, err := ingest.Open(ingest.Config{
			SegmentsDir:       liveDir,
			Store:             raw,
			SeqKey:            live.BootstrapSeqKey,
			MaxEventsPerBlock: eventsPerBlock,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		require.NoError(t, err)
		for i := range evs {
			require.NoError(t, w.Append(t.Context(), &evs[i]))
		}
		require.NoError(t, w.SealActiveAndClose())
	}
	cfg := fix.cfg
	cfg.DataDir = dataDir
	cfg.Store = st
	o, err := New(cfg)
	require.NoError(t, err)
	require.NoError(t, o.segments().Refresh(t.Context()))
	blocks := 0
	for _, v := range o.segments().Snapshot().Segments(catalog.BootstrapLive) {
		blocks += len(v.Blocks)
	}
	return &drainFixture{dataDir: dataDir, store: st, o: o, blocks: blocks}
}

// drain runs the merge drain alone with the given read-ahead and returns
// the destination events (WitnessedAt zeroed: merge re-stamps it with the
// wall clock) and every repo row afterwards.
func (f *drainFixture) drain(t *testing.T, readAhead int) ([]segment.Event, map[string]backfill.RepoStatus) {
	t.Helper()
	dst, err := ingest.Open(ingest.Config{
		SegmentsDir:                filepath.Join(f.dataDir, "segments"),
		DataDir:                    f.dataDir,
		Store:                      f.store,
		SeqKey:                     live.SteadySeqKey,
		ReserveClientVisibleSeqs:   true,
		UnreservedSeqsUnobservable: true,
		Logger:                     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Catalog:                    f.o.segments(),
		Namespace:                  catalog.Main,
	})
	require.NoError(t, err)
	runner := newMergeRunner(dst, f.store, localMergeSource{f.o.segments()}, f.o.cfg.Logger, f.o.cfg.Metrics, nil)
	runner.readAhead = readAhead
	require.NoError(t, runner.run(t.Context()))
	require.NoError(t, dst.SealActiveAndClose())

	got := readDestEvents(t, f.dataDir)
	for i := range got {
		got[i].WitnessedAt = 0
	}
	rows := map[string]backfill.RepoStatus{}
	prefix := backfill.RepoKey("")
	it, err := f.store.NewIter(t.Context(), prefix, metastore.PrefixUpperBound(prefix))
	require.NoError(t, err)
	for it.Next() {
		rs, err := backfill.DecodeRepoStatus(it.Value())
		require.NoError(t, err)
		rows[strings.TrimPrefix(string(it.Key()), string(prefix))] = *rs
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	return got, rows
}

// randomSources builds nSegs source segments of mixed commit and
// non-commit events over nDIDs DIDs, plus each backfilled DID's rev.
func randomSources(rng *rand.Rand, nSegs, perSeg, nDIDs int) ([][]segment.Event, map[string]string) {
	revs := map[string]string{}
	for d := range nDIDs {
		if rng.IntN(3) > 0 {
			revs[fmt.Sprintf("did:plc:d%03d", d)] = fmt.Sprintf("3l%03d", rng.IntN(500))
		}
	}
	kinds := []segment.Kind{segment.KindCreate, segment.KindUpdate, segment.KindDelete, segment.KindIdentity, segment.KindAccount}
	var out [][]segment.Event
	for range nSegs {
		var evs []segment.Event
		for range perSeg {
			did := fmt.Sprintf("did:plc:d%03d", rng.IntN(nDIDs))
			e := ev(did, fmt.Sprintf("3l%03d", rng.IntN(1000)), kinds[rng.IntN(len(kinds))], 1000)
			if !e.Kind.IsCommit() {
				e.Collection, e.Rkey, e.Rev = "", "", ""
			}
			evs = append(evs, e)
		}
		out = append(out, evs)
	}
	return out, revs
}

func TestMergeRunner_ReadAheadMatchesSerial(t *testing.T) {
	t.Parallel()
	for seed := range uint64(8) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			srcs, revs := randomSources(rand.New(rand.NewPCG(seed, seed)), 3, 200, 40)
			serial := newDrainFixture(t, srcs, revs, 7)
			wantEvents, wantRows := serial.drain(t, 0)
			ahead := newDrainFixture(t, srcs, revs, 7)
			gotEvents, gotRows := ahead.drain(t, mergeReadAhead)
			require.Greater(t, serial.blocks, 3*mergeReadAhead, "sources must span more blocks than the read-ahead")
			require.NotEmpty(t, wantEvents)
			require.Less(t, len(wantEvents), 600, "some commits must be dropped as covered")
			require.Equal(t, wantEvents, gotEvents)
			require.Equal(t, len(wantRows), len(gotRows))
			for did, want := range wantRows {
				got := gotRows[did]
				require.Equal(t, want.Rev, got.Rev, did)
				require.Equal(t, want.Backfill, got.Backfill, did)
			}
		})
	}
}

func TestMergeRunner_BatchesRepoLookups(t *testing.T) {
	t.Parallel()
	srcs, revs := randomSources(rand.New(rand.NewPCG(1, 2)), 2, 300, 50)
	// DIDs that appear only in non-commit events must never be looked up.
	srcs[0] = append(srcs[0],
		segment.Event{Kind: segment.KindIdentity, DID: "did:plc:identityonly", WitnessedAt: 1000},
		segment.Event{Kind: segment.KindAccount, DID: "did:plc:accountonly", WitnessedAt: 1000},
	)
	f := newDrainFixture(t, srcs, revs, 11)
	f.drain(t, mergeReadAhead)

	require.Zero(t, f.store.repoGets, "the drain reads repo rows only in batches")
	require.LessOrEqual(t, f.store.getMany, f.blocks, "at most one batched read per source block")
	require.NotContains(t, f.store.getManyDID, "did:plc:identityonly")
	require.NotContains(t, f.store.getManyDID, "did:plc:accountonly")
	for did, n := range f.store.getManyDID {
		require.Equal(t, 1, n, "%s read more than once", did)
	}
}

// failingSource fails the fetch of one block.
type failingSource struct {
	mergeSource
	failBlock int
}

func (s failingSource) fetcher() catalog.Fetcher {
	return failingFetcher{Fetcher: s.mergeSource.fetcher(), failBlock: s.failBlock}
}

type failingFetcher struct {
	catalog.Fetcher
	failBlock int
}

var errInjectedFetch = errors.New("injected fetch failure")

func (f failingFetcher) Fetch(ctx context.Context, ref catalog.BlockRef) ([]byte, error) {
	if ref.Block == f.failBlock {
		return nil, errInjectedFetch
	}
	return f.Fetcher.Fetch(ctx, ref)
}

func TestMergeRunner_ReadErrorStopsDrain(t *testing.T) {
	t.Parallel()
	for _, readAhead := range []int{0, mergeReadAhead} {
		t.Run(fmt.Sprint(readAhead), func(t *testing.T) {
			t.Parallel()
			srcs, revs := randomSources(rand.New(rand.NewPCG(3, 4)), 1, 200, 20)
			f := newDrainFixture(t, srcs, revs, 5)
			dst, err := ingest.Open(ingest.Config{
				SegmentsDir: filepath.Join(f.dataDir, "segments"),
				DataDir:     f.dataDir,
				Store:       f.store,
				SeqKey:      live.SteadySeqKey,
				Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
				Catalog:     f.o.segments(),
				Namespace:   catalog.Main,
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = dst.Close() })
			src := failingSource{mergeSource: localMergeSource{f.o.segments()}, failBlock: 13}
			runner := newMergeRunner(dst, f.store, src, f.o.cfg.Logger, f.o.cfg.Metrics, nil)
			runner.readAhead = readAhead
			err = runner.run(t.Context())
			require.ErrorIs(t, err, errInjectedFetch)
			cursor, err := loadMergeCursor(f.store)
			require.NoError(t, err)
			require.Zero(t, cursor, "a failed segment must not advance the cursor")
		})
	}
}
