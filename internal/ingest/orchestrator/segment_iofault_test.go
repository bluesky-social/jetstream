package orchestrator

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// segmentIOFault fails the ordinal-th occurrence of one segment I/O op,
// mirroring the helpers in segment/writer_test.go and ingest/writer_test.go.
type segmentIOFault struct {
	op      segment.IOOp
	ordinal int
	err     error
	seen    atomic.Int64
}

func (f *segmentIOFault) BeforeSegmentIO(_ string, op segment.IOOp) error {
	if op != f.op {
		return nil
	}
	if int(f.seen.Add(1)) == f.ordinal {
		return f.err
	}
	return nil
}

// requireDiskFullOperatorMessage pins the #201 fail-loud contract: an
// ENOSPC-rooted persistence error must carry the actionable operator message,
// on every segment persistence path uniformly.
func requireDiskFullOperatorMessage(t *testing.T, err error, dataDir string) {
	t.Helper()
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.ErrorContains(t, err, "fatal persistence error")
	require.ErrorContains(t, err, "disk full")
	require.ErrorContains(t, err, dataDir)
	require.ErrorContains(t, err, "restart jetstream")
}

func TestRunDeleteCompaction_ENOSPCRewriteReturnsFatalOperatorMessage(t *testing.T) {
	t.Parallel()

	const did, coll, rkey = "did:plc:a", "app.bsky.feed.post", "r"
	sealed := []segment.Event{
		{Seq: 1, WitnessedAt: 10, Kind: segment.KindCreate, DID: did, Collection: coll, Rkey: rkey, Rev: "1", Payload: []byte("old")},
		{Seq: 2, WitnessedAt: 20, Kind: segment.KindDelete, DID: did, Collection: coll, Rkey: rkey, Rev: "2"},
	}
	dataDir, st, _ := newCompactionDataDir(t, sealed)

	liveSet := tombstone.New()
	for i := range sealed {
		require.NoError(t, liveSet.Observe(&sealed[i]))
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	o := &Orchestrator{cfg: Config{
		DataDir:            dataDir,
		Store:              st,
		Logger:             logger,
		Tombstones:         liveSet,
		CompactionInterval: time.Hour,
		SegmentIOFaultInjector: &segmentIOFault{
			op: segment.IOOpWrite, ordinal: 1, err: syscall.ENOSPC,
		},
	}, logger: logger}

	err := o.runDeleteCompaction(t.Context(), compactionSteady, nil)
	requireDiskFullOperatorMessage(t, err, dataDir)
}

func TestRunImport_ENOSPCPatchReturnsFatalOperatorMessage(t *testing.T) {
	t.Parallel()

	events := []segment.Event{
		{Seq: 1, WitnessedAt: 1_000, Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "1", Payload: []byte("v1")},
	}
	rig := newImportTestRig(t, events)
	rig.o.cfg.SegmentIOFaultInjector = &segmentIOFault{
		op: segment.IOOpWrite, ordinal: 1, err: syscall.ENOSPC,
	}

	csv := writeImportCSVFile(t, "uri,timestamp,scope,cid",
		"at://did:plc:alice/app.bsky.feed.post/r1,2021-12-20T11:33:20Z,all_versions,")
	jobDir := filepath.Join(t.TempDir(), "job1")
	_, err := rig.o.RunImport(context.Background(), ImportJob{CSVPath: csv, JobDir: jobDir})
	requireDiskFullOperatorMessage(t, err, rig.dataDir)
}

// TestRunImport_SegmentIOFaultSweep_FailsThenRerunSucceeds is the import-patch
// half of the #200 fault sweep, kept at the orchestrator level by design: the
// oracle restart child never runs a timestamp import (operator-submitted via
// XRPC), and RunImport drives the identical segment.Patch seam and error
// path. Each case arms one fault (write/sync/rename), asserts the import
// fails loud with the segment untouched and no stray tmp, then re-runs the
// import fault-free from a fresh job dir and requires it to fully converge —
// the fail-then-recover contract, import edition.
func TestRunImport_SegmentIOFaultSweep_FailsThenRerunSucceeds(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		op      segment.IOOp
		ordinal int
		err     error
	}{
		{name: "write-shortwrite-frame", op: segment.IOOpWrite, ordinal: 2, err: io.ErrShortWrite},
		{name: "sync-eio-tmp-fsync", op: segment.IOOpSync, ordinal: 2, err: syscall.EIO},
		{name: "rename-eio-commit", op: segment.IOOpRename, ordinal: 1, err: syscall.EIO},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			events := []segment.Event{
				{Seq: 1, WitnessedAt: 1_000, Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "1", Payload: []byte("v1")},
				{Seq: 2, WitnessedAt: 2_000, Kind: segment.KindCreate, DID: "did:plc:bob", Collection: "app.bsky.feed.post", Rkey: "r2", Rev: "1", Payload: []byte("v2")},
			}
			rig := newImportTestRig(t, events)
			segPath := filepath.Join(rig.segmentsDir, "seg_0000000000.jss")
			before, err := os.ReadFile(segPath)
			require.NoError(t, err)

			rig.o.cfg.SegmentIOFaultInjector = &segmentIOFault{op: tc.op, ordinal: tc.ordinal, err: tc.err}
			csv := writeImportCSVFile(t, "uri,timestamp,scope,cid",
				"at://did:plc:alice/app.bsky.feed.post/r1,2021-12-20T11:33:20Z,all_versions,")

			_, err = rig.o.RunImport(context.Background(), ImportJob{CSVPath: csv, JobDir: filepath.Join(t.TempDir(), "job1")})
			require.ErrorIsf(t, err, tc.err, "import must fail loud on the injected %s fault", tc.op)
			require.NoFileExists(t, segPath+".tmp", "failed patch must clean up its tmp")
			after, err := os.ReadFile(segPath)
			require.NoError(t, err)
			require.Equal(t, before, after, "failed patch must leave the source segment byte-identical")

			// Fault disarmed: a fresh run of the same import converges.
			rig.o.cfg.SegmentIOFaultInjector = nil
			res, err := rig.o.RunImport(context.Background(), ImportJob{CSVPath: csv, JobDir: filepath.Join(t.TempDir(), "job2")})
			require.NoError(t, err, "fault-free re-run must succeed")
			require.EqualValues(t, 1, res.SegmentsPatched)
			require.EqualValues(t, 1, res.RowsMutated)

			const importedTS = int64(1_640_000_000_000_000)
			for _, ev := range rig.segmentEvents(t) {
				if ev.DID == "did:plc:alice" {
					require.Equal(t, importedTS, ev.IndexedAt, "re-run import must land the timestamp")
				} else {
					require.EqualValues(t, 0, ev.IndexedAt)
				}
			}
		})
	}
}
