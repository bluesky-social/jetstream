package segment

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// recordingIOFault counts every consult per op without ever failing, so a
// sweep can learn how many seam points a Rewrite crosses.
type recordingIOFault struct {
	counts map[IOOp]int
}

func (r *recordingIOFault) BeforeSegmentIO(_ string, op IOOp) error {
	if r.counts == nil {
		r.counts = map[IOOp]int{}
	}
	r.counts[op]++
	return nil
}

func ioFaultSweepFixtureEvents() []Event {
	return []Event{
		{Seq: 1, WitnessedAt: 100, Kind: KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "v1", Payload: []byte{0xa0}},
		{Seq: 2, WitnessedAt: 200, Kind: KindCreate, DID: "did:plc:b", Collection: "app.bsky.feed.like", Rkey: "r2", Rev: "v2", Payload: []byte{0xa1}},
		{Seq: 3, WitnessedAt: 300, Kind: KindIdentity, DID: "did:plc:a"},
		{Seq: 4, WitnessedAt: 400, Kind: KindUpdate, DID: "did:plc:c", Collection: "app.bsky.feed.post", Rkey: "r4", Rev: "v4", Payload: []byte{0xa2}},
		{Seq: 5, WitnessedAt: 500, Kind: KindDelete, DID: "did:plc:b", Collection: "app.bsky.feed.post", Rkey: "r5", Rev: "v5"},
	}
}

// ioFaultSweepFixture builds one sealed multi-block segment and returns its
// bytes, so every subtest rewrites byte-identical input.
func ioFaultSweepFixture(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir, ioFaultSweepFixtureEvents(), 2)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return raw
}

func writeFixtureCopy(t *testing.T, fixture []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "seg.jss")
	require.NoError(t, os.WriteFile(path, fixture, 0o644))
	return path
}

func rewriteForSweep(path string, faults IOFaultInjector) (RewriteResult, error) {
	return Rewrite(path, func(ev *Event) RowDecision {
		if ev.DID == "did:plc:b" {
			return RowDrop
		}
		return RowKeep
	}, RewriteOptions{IOFaultInjector: faults})
}

// runIOFaultSweep runs the target against a fresh fixture copy with a
// fault armed at (op, ordinal), then asserts the no-silent-corruption
// contract: the injected error propagates, no tmp file survives, and the
// on-disk file is EITHER byte-identical to the source (fault at or before the
// rename) or byte-identical to the committed output (fault after the rename,
// i.e. the final parent-dir fsync).
type ioFaultSweepTarget struct {
	name string
	run  func(path string, faults IOFaultInjector) error
}

func runIOFaultSweep(t *testing.T, target ioFaultSweepTarget, fixture, committed []byte, counts map[IOOp]int) {
	t.Helper()
	type errCase struct {
		label string
		err   error
	}
	opErrs := map[IOOp][]errCase{
		IOOpWrite:  {{"enospc", syscall.ENOSPC}, {"short-write", io.ErrShortWrite}},
		IOOpSync:   {{"eio", syscall.EIO}},
		IOOpRename: {{"eio", syscall.EIO}},
	}
	for op, total := range counts {
		require.Positivef(t, total, "%s: op %q never consulted — seam missing", target.name, op)
		for _, ec := range opErrs[op] {
			for ordinal := 1; ordinal <= total; ordinal++ {
				name := fmt.Sprintf("%s/%s/%s/ordinal-%d", target.name, op, ec.label, ordinal)
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					path := writeFixtureCopy(t, fixture)
					fault := &opOrdinalIOFault{op: op, ordinal: ordinal, err: ec.err}
					err := target.run(path, fault)
					require.ErrorIsf(t, err, ec.err,
						"fault at (%s, %d) must propagate", op, ordinal)
					require.NoFileExists(t, path+".tmp", "tmp must be cleaned up on failure")

					got, readErr := os.ReadFile(path)
					require.NoError(t, readErr)
					// The final parent-dir fsync is the only seam past the
					// commit rename: a fault there leaves the (fully valid)
					// committed file in place. Every earlier fault must leave
					// the source byte-for-byte untouched.
					if op == IOOpSync && ordinal == total {
						require.Equal(t, committed, got,
							"post-rename fault must leave the committed file intact")
					} else {
						require.Equal(t, fixture, got,
							"pre-rename fault must leave the original untouched")
					}
				})
			}
		}
	}
}

// TestNewRemovesEmptyFileWhenInitFails pins the torn-creation cleanup
// contract: when New creates a brand-new segment file and the reserved-header
// initialization fails (e.g. ENOSPC on the first write), the empty file must
// be unlinked before the error returns. Leaving it behind wedges restart
// recovery permanently — the manifest loader rejects a 0-byte segment as
// corrupt on every subsequent boot (found by the #200 oracle segment-fault
// tier, write-enospc-first-header case).
func TestNewRemovesEmptyFileWhenInitFails(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		fault *opOrdinalIOFault
	}{
		{name: "header-write-enospc", fault: &opOrdinalIOFault{op: IOOpWrite, ordinal: 1, err: syscall.ENOSPC}},
		{name: "header-fsync-eio", fault: &opOrdinalIOFault{op: IOOpSync, ordinal: 1, err: syscall.EIO}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "seg.jss")
			_, err := New(Config{Path: path, IOFaultInjector: tc.fault})
			require.ErrorIs(t, err, tc.fault.err)
			require.NoFileExists(t, path,
				"failed initialization must not leave an empty segment file (permanent recovery wedge)")
		})
	}
}

// TestNewKeepsPreexistingEmptyFileContractHealed: a 0-byte segment file left
// by a hard kill between create and header write (the dirent survives a
// process crash) must be healed by the next successful New, not rejected.
func TestNewHealsPreexistingEmptyFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "seg.jss")
	require.NoError(t, os.WriteFile(path, nil, 0o644))

	w, err := New(Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.EqualValues(t, ReservedHeaderBytes, info.Size())
}

func TestRewriteIOFaultSweep(t *testing.T) {
	t.Parallel()
	fixture := ioFaultSweepFixture(t)

	refPath := writeFixtureCopy(t, fixture)
	rec := &recordingIOFault{}
	res, err := rewriteForSweep(refPath, rec)
	require.NoError(t, err)
	require.True(t, res.Rewritten)
	committed, err := os.ReadFile(refPath)
	require.NoError(t, err)
	require.NotEqual(t, fixture, committed)

	require.Equal(t, 1, rec.counts[IOOpRename], "rewrite commits via exactly one rename")
	require.Equal(t, 3, rec.counts[IOOpSync], "init fsync + tmp fsync + parent-dir fsync")
	require.GreaterOrEqual(t, rec.counts[IOOpWrite], 5,
		"init header + per-block frames + footer + header")

	runIOFaultSweep(t, ioFaultSweepTarget{
		name: "rewrite",
		run: func(path string, faults IOFaultInjector) error {
			_, err := rewriteForSweep(path, faults)
			return err
		},
	}, fixture, committed, rec.counts)
}
