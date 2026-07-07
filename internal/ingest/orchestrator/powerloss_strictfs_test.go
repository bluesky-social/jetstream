package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

var errStrictFSPowerLoss = errors.New("orchestrator: injected strict-FS power loss")

func TestRunDeleteCompaction_StrictMemPowerLossRewriteCrashpoints(t *testing.T) {
	t.Parallel()

	for _, point := range []crashpoint.Point{
		crashpoint.AfterSegmentRewriteTempWritten,
		crashpoint.AfterSegmentRewriteTempSynced,
		crashpoint.AfterSegmentRewriteRenamed,
		crashpoint.AfterSegmentRewriteDirSynced,
	} {
		t.Run(point.String(), func(t *testing.T) {
			t.Parallel()
			runStrictMemCompactionPowerLossCase(t, point)
		})
	}
}

func TestRunDeleteCompaction_StrictMemPowerLossCompactionCrashpoints(t *testing.T) {
	t.Parallel()

	for _, point := range []crashpoint.Point{
		crashpoint.AfterCompactionRewriteBeforeWatermark,
		crashpoint.AfterCompactionChunkWatermark,
	} {
		t.Run(point.String(), func(t *testing.T) {
			t.Parallel()
			runStrictMemCompactionPowerLossCase(t, point)
		})
	}
}

func runStrictMemCompactionPowerLossCase(t *testing.T, point crashpoint.Point) {
	t.Helper()

	fs := vfs.NewStrictMem()
	syncStrictMemDir(t, fs, "/")
	const dataDir = "/data"
	segmentsDir := fs.PathJoin(dataDir, "segments")
	require.NoError(t, ingest.MkdirAllSyncedFS(fs, segmentsDir, 0o755, "orchestrator-test"))

	const did = "did:plc:rewrite"
	sealed := []segment.Event{
		{Seq: 1, WitnessedAt: 10, Kind: segment.KindCreate, DID: did, Collection: "app.bsky.feed.post", Rkey: "r", Rev: "1", Payload: []byte("old")},
		{Seq: 2, WitnessedAt: 20, Kind: segment.KindDelete, DID: did, Collection: "app.bsky.feed.post", Rkey: "r", Rev: "2"},
	}
	segPath := writeStrictMemSegment(t, fs, segmentsDir, 0, sealed)
	fs.ResetToSyncedState()
	fs.SetIgnoreSyncs(false)

	liveSet := tombstone.New()
	for i := range sealed {
		require.NoError(t, liveSet.Observe(&sealed[i]))
	}

	st := openStrictMemStore(t, fs, dataDir)
	inj := &strictFSPowerLossPointInjector{point: point, fs: fs}
	o := newStrictMemCompactionOrchestrator(dataDir, fs, st, liveSet, inj)
	err := o.runDeleteCompaction(context.Background(), compactionMergeTail, nil)
	require.ErrorIsf(t, err, errStrictFSPowerLoss, "first compaction pass must fail at %s", point)
	require.Truef(t, inj.fired.Load(), "crashpoint %s did not fire", point)
	require.NoError(t, st.Close())

	fs.ResetToSyncedState()
	fs.SetIgnoreSyncs(false)

	st = openStrictMemStore(t, fs, dataDir)
	t.Cleanup(func() { _ = st.Close() })
	o = newStrictMemCompactionOrchestrator(dataDir, fs, st, liveSet, nil)
	require.NoError(t, o.runDeleteCompaction(context.Background(), compactionMergeTail, nil))

	watermark, _, err := loadCompactionWatermark(st)
	require.NoError(t, err)
	require.Equal(t, uint64(2), watermark, "recovered pass must advance the compaction watermark")
	requireNoStrictMemTmp(t, fs, segPath+".tmp")

	got := readStrictMemSegment(t, fs, segPath)
	require.Len(t, got, 1, "recovered compaction must converge to the compacted segment")
	require.Equal(t, segment.KindDelete, got[0].Kind)
	require.Equal(t, uint64(2), got[0].Seq)
}

func TestRunImport_StrictMemPowerLossPatchCrashpoints(t *testing.T) {
	t.Parallel()

	for _, point := range []crashpoint.Point{
		crashpoint.AfterSegmentPatchTempWritten,
		crashpoint.AfterSegmentPatchTempSynced,
		crashpoint.AfterSegmentPatchRenamed,
		crashpoint.AfterSegmentPatchDirSynced,
	} {
		t.Run(point.String(), func(t *testing.T) {
			t.Parallel()

			fs := vfs.NewStrictMem()
			syncStrictMemDir(t, fs, "/")
			const dataDir = "/data"
			segmentsDir := fs.PathJoin(dataDir, "segments")
			require.NoError(t, ingest.MkdirAllSyncedFS(fs, segmentsDir, 0o755, "orchestrator-test"))

			const importedTS = int64(1_640_000_000_000_000)
			segPath := writeStrictMemSegment(t, fs, segmentsDir, 0, []segment.Event{
				{Seq: 1, WitnessedAt: 1_000, Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "1", Payload: []byte("v1")},
				{Seq: 2, WitnessedAt: 2_000, Kind: segment.KindCreate, DID: "did:plc:bob", Collection: "app.bsky.feed.post", Rkey: "r2", Rev: "1", Payload: []byte("v2")},
			})
			fs.ResetToSyncedState()
			fs.SetIgnoreSyncs(false)

			csv := writeImportCSVFile(t, "uri,timestamp,scope,cid",
				"at://did:plc:alice/app.bsky.feed.post/r1,2021-12-20T11:33:20Z,all_versions,")
			inj := &strictFSPowerLossPointInjector{point: point, fs: fs}
			rig := newStrictMemImportRig(t, fs, dataDir, inj)
			_, err := rig.o.RunImport(context.Background(), ImportJob{
				CSVPath: csv,
				JobDir:  filepath.Join(t.TempDir(), "job1"),
			})
			require.ErrorIsf(t, err, errStrictFSPowerLoss, "first import must fail at %s", point)
			require.Truef(t, inj.fired.Load(), "crashpoint %s did not fire", point)

			fs.ResetToSyncedState()
			fs.SetIgnoreSyncs(false)

			rig = newStrictMemImportRig(t, fs, dataDir, nil)
			res, err := rig.o.RunImport(context.Background(), ImportJob{
				CSVPath: csv,
				JobDir:  filepath.Join(t.TempDir(), "job2"),
			})
			require.NoError(t, err, "fault-free import re-run must converge")
			require.EqualValues(t, 1, res.SegmentsExamined)
			requireNoStrictMemTmp(t, fs, segPath+".tmp")

			for _, ev := range readStrictMemSegment(t, fs, segPath) {
				switch ev.DID {
				case "did:plc:alice":
					require.Equal(t, importedTS, ev.IndexedAt, "re-run import must land the timestamp")
				case "did:plc:bob":
					require.EqualValues(t, 0, ev.IndexedAt, "untargeted row must remain unchanged")
				}
			}
		})
	}
}

// TestRunMerge_StrictMemPowerLossCleanupComplete pins the durability
// ordering between the backfill-subtree removal and the SyncWrites merge
// cursor deletes at the tail of merge cleanup. deleteMergeCursor commits with
// store.SyncWrites, so if the RemoveAll of data/backfill is not fsynced before
// those deletes, a power cut at AfterMergeCleanupComplete rolls back the dirent
// removal while keeping the cursor deletion durable. On restart the phase is
// still PhaseMerging, live_segments reappears, the restart-after-cleanup guard
// is skipped, and the drain re-runs from cursor 0 — appending the surviving
// events a second time into data/segments. The test asserts the survivor lands
// exactly once after recovery, which fails without the syncStorageDirFS call in
// runMerge's cleanup.
func TestRunMerge_StrictMemPowerLossCleanupComplete(t *testing.T) {
	t.Parallel()

	fs := vfs.NewStrictMem()
	syncStrictMemDir(t, fs, "/")
	const dataDir = "/data"
	liveSegmentsDir := fs.PathJoin(dataDir, "backfill", "live_segments")
	segmentsDir := fs.PathJoin(dataDir, "segments")
	require.NoError(t, ingest.MkdirAllSyncedFS(fs, liveSegmentsDir, 0o755, "orchestrator-test"))
	require.NoError(t, ingest.MkdirAllSyncedFS(fs, segmentsDir, 0o755, "orchestrator-test"))

	// One survivor: rev "3l9" > backfill watermark "3l5", so shouldKeep
	// promotes it into data/segments during the drain.
	const survivorDID = "did:plc:survivor"
	const survivorRev = "3l9"
	writeStrictMemSegment(t, fs, liveSegmentsDir, 0, []segment.Event{
		{Seq: 1, WitnessedAt: 1_000, Kind: segment.KindCreate, DID: survivorDID, Collection: "app.bsky.feed.post", Rkey: "r1", Rev: survivorRev, Payload: []byte("v1")},
	})

	st := openStrictMemStore(t, fs, dataDir)
	seedStrictMemBackfillRev(t, st, survivorDID, "3l5")
	fs.ResetToSyncedState()
	fs.SetIgnoreSyncs(false)

	inj := &strictFSPowerLossPointInjector{point: crashpoint.AfterMergeCleanupComplete, fs: fs}
	o := newStrictMemMergeOrchestrator(dataDir, fs, st, inj)
	err := o.runMerge(context.Background())
	require.ErrorIs(t, err, errStrictFSPowerLoss, "first merge must fire the cleanup-complete crashpoint")
	require.True(t, inj.fired.Load(), "cleanup-complete crashpoint did not fire")
	require.NoError(t, st.Close())

	// Power loss: discard unsynced dirents and reopen from the durable state.
	fs.ResetToSyncedState()
	fs.SetIgnoreSyncs(false)

	st = openStrictMemStore(t, fs, dataDir)
	t.Cleanup(func() { _ = st.Close() })
	o = newStrictMemMergeOrchestrator(dataDir, fs, st, nil)
	require.NoError(t, o.runMerge(context.Background()), "recovery merge must converge")

	// The survivor must be present exactly once — not duplicated by a
	// re-drain that saw a rolled-back backfill tree.
	var survivorCount int
	for _, ev := range readAllStrictMemSegments(t, fs, segmentsDir) {
		if ev.DID == survivorDID && ev.Rev == survivorRev {
			survivorCount++
		}
	}
	require.Equal(t, 1, survivorCount, "merge survivor must appear exactly once after power-loss recovery")

	// Cleanup completed: the backfill tree is gone and the cursor is clear.
	_, statErr := fs.Stat(fs.PathJoin(dataDir, "backfill"))
	require.Truef(t, oserror.IsNotExist(statErr), "backfill dir should be removed after recovery: %v", statErr)
	cur, err := loadMergeCursor(st)
	require.NoError(t, err)
	require.Equal(t, uint64(0), cur)
}

// TestRunMerge_StrictMemPowerLossCleanupGuard pins the same durability ordering
// on the restart-after-cleanup guard path. The guard fires when a prior process
// removed data/backfill and died before deleting the SyncWrites merge cursors
// (e.g. SIGKILL with the page cache intact, so live_segments reads as gone even
// though the dirent removal never reached stable storage). The guard must fsync
// the data dir before its durable cursor deletes; otherwise a power cut right
// after the guard leaves the cursors deleted while the backfill tree reappears,
// and the next boot skips the guard, re-drains from cursor 0, and duplicates the
// survivors that a prior drain already promoted into data/segments.
func TestRunMerge_StrictMemPowerLossCleanupGuard(t *testing.T) {
	t.Parallel()

	fs := vfs.NewStrictMem()
	syncStrictMemDir(t, fs, "/")
	const dataDir = "/data"
	liveSegmentsDir := fs.PathJoin(dataDir, "backfill", "live_segments")
	segmentsDir := fs.PathJoin(dataDir, "segments")
	require.NoError(t, ingest.MkdirAllSyncedFS(fs, liveSegmentsDir, 0o755, "orchestrator-test"))
	require.NoError(t, ingest.MkdirAllSyncedFS(fs, segmentsDir, 0o755, "orchestrator-test"))

	const survivorDID = "did:plc:survivor"
	const survivorRev = "3l9"
	survivor := segment.Event{Seq: 1, WitnessedAt: 1_000, Kind: segment.KindCreate, DID: survivorDID, Collection: "app.bsky.feed.post", Rkey: "r1", Rev: survivorRev, Payload: []byte("v1")}
	// A prior drain already promoted the survivor into data/segments...
	writeStrictMemSegment(t, fs, segmentsDir, 0, []segment.Event{survivor})
	// ...and the source is still on disk (its removal is what rolls back).
	writeStrictMemSegment(t, fs, liveSegmentsDir, 0, []segment.Event{survivor})

	st := openStrictMemStore(t, fs, dataDir)
	seedStrictMemBackfillRev(t, st, survivorDID, "3l5")
	// The prior drain advanced the cursor past the single source segment.
	require.NoError(t, st.SetVersionedUint64LE(mergeNextSourceIdxKey, mergeCursorV1, 1))
	fs.ResetToSyncedState()
	fs.SetIgnoreSyncs(false)

	// Prior process removed the backfill tree but its dirent removal never
	// reached stable storage (no data-dir fsync).
	require.NoError(t, removeAllStorageFS(fs, fs.PathJoin(dataDir, "backfill")))

	// Current process: runMerge observes live_segments gone and takes the
	// restart-after-cleanup guard, durably deleting the cursors.
	o := newStrictMemMergeOrchestrator(dataDir, fs, st, nil)
	require.NoError(t, o.runMerge(context.Background()))
	cur, err := loadMergeCursor(st)
	require.NoError(t, err)
	require.Equal(t, uint64(0), cur, "guard must delete the merge cursor")
	require.NoError(t, st.Close())

	// Power loss right after the guard.
	fs.SetIgnoreSyncs(true)
	fs.ResetToSyncedState()
	fs.SetIgnoreSyncs(false)

	st = openStrictMemStore(t, fs, dataDir)
	t.Cleanup(func() { _ = st.Close() })
	o = newStrictMemMergeOrchestrator(dataDir, fs, st, nil)
	require.NoError(t, o.runMerge(context.Background()), "recovery merge must converge")

	var survivorCount int
	for _, ev := range readAllStrictMemSegments(t, fs, segmentsDir) {
		if ev.DID == survivorDID && ev.Rev == survivorRev {
			survivorCount++
		}
	}
	require.Equal(t, 1, survivorCount, "survivor must not be re-drained after a guard-path power loss")
}

func newStrictMemMergeOrchestrator(
	dataDir string,
	fs *vfs.MemFS,
	st *store.Store,
	inj crashpoint.Injector,
) *Orchestrator {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &Orchestrator{
		cfg: Config{
			DataDir:            dataDir,
			FS:                 fs,
			Store:              st,
			RelayURL:           "http://127.0.0.1",
			HTTPClient:         &http.Client{Timeout: 5 * time.Second},
			Logger:             logger,
			CompactionInterval: time.Hour,
			SkipMergeDiscovery: true,
			CrashInjector:      inj,
		},
		logger: logger,
	}
}

func seedStrictMemBackfillRev(t *testing.T, st *store.Store, did, rev string) {
	t.Helper()
	rs := &backfill.RepoStatus{
		Backfill: backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: rev},
		Rev:      rev,
	}
	enc, err := backfill.EncodeRepoStatus(rs)
	require.NoError(t, err)
	require.NoError(t, st.Set(backfill.RepoKey(did), enc, store.SyncWrites))
}

func readAllStrictMemSegments(t *testing.T, fs *vfs.MemFS, segmentsDir string) []segment.Event {
	t.Helper()
	files, err := ingest.SegmentFilesFS(fs, segmentsDir)
	require.NoError(t, err)
	var out []segment.Event
	for _, f := range files {
		out = append(out, readStrictMemSegment(t, fs, f.Path)...)
	}
	return out
}

type strictFSPowerLossPointInjector struct {
	point crashpoint.Point
	fs    *vfs.MemFS
	fired atomic.Bool
}

func (i *strictFSPowerLossPointInjector) SimulateCrash(_ context.Context, point crashpoint.Point) error {
	if point != i.point {
		return nil
	}
	if !i.fired.CompareAndSwap(false, true) {
		return nil
	}
	i.fs.SetIgnoreSyncs(true)
	return fmt.Errorf("%w: %s", errStrictFSPowerLoss, point)
}

func newStrictMemCompactionOrchestrator(
	dataDir string,
	fs *vfs.MemFS,
	st *store.Store,
	liveSet *tombstone.Set,
	inj crashpoint.Injector,
) *Orchestrator {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &Orchestrator{
		cfg: Config{
			DataDir:                  dataDir,
			FS:                       fs,
			Store:                    st,
			Logger:                   logger,
			Tombstones:               liveSet,
			CompactionInterval:       time.Hour,
			CompactionRewriteWorkers: 1,
			CrashInjector:            inj,
		},
		logger: logger,
	}
}

type strictMemImportRig struct {
	o *Orchestrator
}

func newStrictMemImportRig(t *testing.T, fs *vfs.MemFS, dataDir string, inj crashpoint.Injector) *strictMemImportRig {
	t.Helper()

	segmentsDir := fs.PathJoin(dataDir, "segments")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mft, err := manifest.Open(manifest.Options{
		SegmentsDir: segmentsDir,
		FS:          fs,
		Logger:      logger,
	})
	require.NoError(t, err)

	rig := &strictMemImportRig{}
	rig.o = &Orchestrator{
		logger: logger,
		cfg: Config{
			DataDir:        dataDir,
			FS:             fs,
			Logger:         logger,
			ImportSelector: mft,
			OnSegmentCompacted: func(idx uint64, path string) error {
				return mft.OnSegmentCompacted(idx, path)
			},
			SegmentManifestChecksums: mft.SegmentChecksums,
			CrashInjector:            inj,
		},
	}
	return rig
}

func openStrictMemStore(t *testing.T, fs *vfs.MemFS, dataDir string) *store.Store {
	t.Helper()
	st, err := store.Open(dataDir, nil, store.WithFS(fs))
	require.NoError(t, err)
	return st
}

func writeStrictMemSegment(t *testing.T, fs *vfs.MemFS, dir string, idx uint64, events []segment.Event) string {
	t.Helper()
	path := fs.PathJoin(dir, ingest.SegmentFilename(idx))
	w, err := segment.New(segment.Config{Path: path, FS: fs, MaxEventsPerBlock: 2})
	require.NoError(t, err)
	for _, ev := range events {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if full {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
	return path
}

func readStrictMemSegment(t *testing.T, fs *vfs.MemFS, path string) []segment.Event {
	t.Helper()
	r, err := segment.Open(segment.ReaderConfig{Path: path, FS: fs})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	var out []segment.Event
	for i := range int(r.Header().BlockCount) {
		events, err := r.DecodeBlock(i)
		require.NoError(t, err)
		out = append(out, events...)
	}
	return out
}

func syncStrictMemDir(t *testing.T, fs *vfs.MemFS, dir string) {
	t.Helper()
	f, err := fs.OpenDir(dir)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
}

func requireNoStrictMemTmp(t *testing.T, fs *vfs.MemFS, path string) {
	t.Helper()
	_, err := fs.Stat(path)
	require.Truef(t, oserror.IsNotExist(err), "strict FS temp file %s should not exist after recovery: %v", path, err)
}
