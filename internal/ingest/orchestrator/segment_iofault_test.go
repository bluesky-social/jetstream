package orchestrator

import (
	"context"
	"io"
	"log/slog"
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
