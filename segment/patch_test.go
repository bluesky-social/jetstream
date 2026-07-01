package segment

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// patchFixtureEvents is a multi-block, multi-DID, multi-collection set with a
// mix of commit and marker kinds, all carrying IndexedAt==0 (un-imported), so
// tests can assert the display column starts at the witnessed sentinel and
// moves only where a patch sets it.
func patchFixtureEvents() []Event {
	return []Event{
		{Seq: 1, WitnessedAt: 100, Kind: KindCreate, DID: "did:plc:a", Collection: "app.bsky.feed.post", Rkey: "r1", Rev: "v1", Payload: []byte{0xa0}},
		{Seq: 2, WitnessedAt: 200, Kind: KindCreate, DID: "did:plc:b", Collection: "app.bsky.feed.like", Rkey: "r2", Rev: "v2", Payload: []byte{0xa1}},
		{Seq: 3, WitnessedAt: 300, Kind: KindIdentity, DID: "did:plc:a"},
		{Seq: 4, WitnessedAt: 400, Kind: KindUpdate, DID: "did:plc:c", Collection: "app.bsky.feed.post", Rkey: "r4", Rev: "v4", Payload: []byte{0xa2}},
		{Seq: 5, WitnessedAt: 500, Kind: KindDelete, DID: "did:plc:b", Collection: "app.bsky.feed.post", Rkey: "r5", Rev: "v5"},
	}
}

// snapshotSegment reads every block's events plus the footer-derived indexes
// so a test can compare a whole segment before/after a patch, field by field.
type segmentSnapshot struct {
	header      Header
	blocks      []BlockInfo
	events      [][]Event
	collections []string
	colCounts   []uint32
	blockCols   [][]uint32
}

func snapshotSegment(t *testing.T, path string) segmentSnapshot {
	t.Helper()
	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	defer func() { _ = r.Close() }()

	snap := segmentSnapshot{
		header:      r.Header(),
		blocks:      r.Blocks(),
		collections: r.Collections(),
		colCounts:   r.CollectionEventCounts(),
	}
	for i := range int(r.Header().BlockCount) {
		evs, err := r.DecodeBlock(i)
		require.NoError(t, err)
		// Clone strings/payloads: DecodeBlock aliases a per-call buffer.
		cloned := make([]Event, len(evs))
		for j, ev := range evs {
			ev.DID = string([]byte(ev.DID))
			ev.Collection = string([]byte(ev.Collection))
			ev.Rkey = string([]byte(ev.Rkey))
			ev.Rev = string([]byte(ev.Rev))
			ev.Payload = append([]byte(nil), ev.Payload...)
			cloned[j] = ev
		}
		snap.events = append(snap.events, cloned)
		cols, err := r.BlockCollections(i)
		require.NoError(t, err)
		snap.blockCols = append(snap.blockCols, cols)
	}
	return snap
}

func rawBlockBlooms(t *testing.T, path string) [][]byte {
	t.Helper()
	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	out := make([][]byte, r.Header().BlockCount)
	for i := range int(r.Header().BlockCount) {
		bloom, err := r.BlockBloom(i)
		require.NoError(t, err)
		b, err := bloom.MarshalBinary()
		require.NoError(t, err)
		out[i] = b
	}
	return out
}

func TestPatchMutatesOnlyDisplayColumn(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir, patchFixtureEvents(), 2)

	before := snapshotSegment(t, path)
	beforeBlooms := rawBlockBlooms(t, path)

	// Import a display timestamp for did:plc:a's rows only.
	const importedTS = int64(1_600_000_000_000_000)
	res, err := Patch(path, func(ev *Event) bool {
		if ev.DID == "did:plc:a" {
			ev.IndexedAt = importedTS
			return true
		}
		return false
	}, PatchOptions{})
	require.NoError(t, err)
	require.True(t, res.Patched)
	require.EqualValues(t, 2, res.RowsMutated, "two did:plc:a rows (seq 1 and 3)")

	after := snapshotSegment(t, path)

	// Envelope, counts, topology, and the whole footer identity survive.
	require.Equal(t, before.header.BlockCount, after.header.BlockCount)
	require.Equal(t, before.header.EventCount, after.header.EventCount)
	require.Equal(t, before.header.UniqueDIDCount, after.header.UniqueDIDCount)
	require.Equal(t, before.header.MinSeq, after.header.MinSeq)
	require.Equal(t, before.header.MaxSeq, after.header.MaxSeq)
	require.Equal(t, before.header.MinWitnessedAt, after.header.MinWitnessedAt)
	require.Equal(t, before.header.MaxWitnessedAt, after.header.MaxWitnessedAt)
	require.Equal(t, before.collections, after.collections, "collection string table unchanged")
	require.Equal(t, before.colCounts, after.colCounts, "per-collection counts unchanged")
	require.Equal(t, before.blockCols, after.blockCols, "per-block collection bitmasks unchanged")
	require.Equal(t, beforeBlooms, rawBlockBlooms(t, path), "per-block DID blooms byte-identical")

	// Per-row: every column but IndexedAt is identical; IndexedAt moved
	// exactly on the targeted DID.
	require.Len(t, after.events, len(before.events))
	for bi := range before.events {
		require.Len(t, after.events[bi], len(before.events[bi]))
		for ri := range before.events[bi] {
			b := before.events[bi][ri]
			a := after.events[bi][ri]
			require.Equal(t, b.Seq, a.Seq)
			require.Equal(t, b.WitnessedAt, a.WitnessedAt)
			require.Equal(t, b.Kind, a.Kind)
			require.Equal(t, b.DID, a.DID)
			require.Equal(t, b.Collection, a.Collection)
			require.Equal(t, b.Rkey, a.Rkey)
			require.Equal(t, b.Rev, a.Rev)
			require.Equal(t, b.Payload, a.Payload)
			if a.DID == "did:plc:a" {
				require.Equal(t, importedTS, a.IndexedAt, "targeted row got the imported display value")
				require.Equal(t, importedTS, a.DisplayTimeUS())
			} else {
				require.EqualValues(t, 0, a.IndexedAt, "untargeted row keeps the sentinel")
				require.Equal(t, a.WitnessedAt, a.DisplayTimeUS())
			}
		}
	}
}

func TestPatchZeroMutationLeavesFileUntouched(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir, patchFixtureEvents(), 2)

	before, err := os.ReadFile(path)
	require.NoError(t, err)
	beforeStat, err := os.Stat(path)
	require.NoError(t, err)

	res, err := Patch(path, func(ev *Event) bool {
		// Leave IndexedAt at its current value: the mutate makes no change,
		// so Patch must skip the rename entirely.
		return false
	}, PatchOptions{})
	require.NoError(t, err)
	require.False(t, res.Patched)
	require.EqualValues(t, 0, res.RowsMutated)

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after, "zero-mutation patch must be byte-identical")

	afterStat, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, beforeStat.ModTime(), afterStat.ModTime(), "no-op patch must not touch mtime")
	beforeSys, ok := beforeStat.Sys().(*syscall.Stat_t)
	require.True(t, ok)
	afterSys, ok := afterStat.Sys().(*syscall.Stat_t)
	require.True(t, ok)
	require.Equal(t, beforeSys.Ino, afterSys.Ino, "no-op patch must not replace the inode")
}

func TestPatchIsIdempotentForFixedMutate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir, patchFixtureEvents(), 2)

	mutate := func(ev *Event) bool {
		if ev.DID == "did:plc:b" {
			ev.IndexedAt = 1_600_000_000_000_000
			return true
		}
		return false
	}

	res1, err := Patch(path, mutate, PatchOptions{})
	require.NoError(t, err)
	require.True(t, res1.Patched)
	require.EqualValues(t, 2, res1.RowsMutated)

	afterFirst, err := os.ReadFile(path)
	require.NoError(t, err)

	// Re-running the same mutate finds every target already at its value, so
	// nothing changes and the file is not rewritten.
	res2, err := Patch(path, mutate, PatchOptions{})
	require.NoError(t, err)
	require.False(t, res2.Patched, "re-applying the same import is a no-op")
	require.EqualValues(t, 0, res2.RowsMutated)

	afterSecond, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, afterFirst, afterSecond)
}

func TestPatchReopensWithValidChecksum(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir, patchFixtureEvents(), 2)

	res, err := Patch(path, func(ev *Event) bool {
		ev.IndexedAt = ev.WitnessedAt + 1_000
		return true
	}, PatchOptions{})
	require.NoError(t, err)
	require.True(t, res.Patched)
	require.EqualValues(t, 5, res.RowsMutated)

	// Open with checksum verification on (the default) — a bad footer/header
	// rebuild would trip ErrChecksumMismatch here.
	r, err := Open(ReaderConfig{Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	require.Equal(t, res.NewChecksum, r.Header().Checksum)
	for bi := range int(r.Header().BlockCount) {
		evs, err := r.DecodeBlock(bi)
		require.NoError(t, err)
		for _, ev := range evs {
			require.Equal(t, ev.WitnessedAt+1_000, ev.IndexedAt)
		}
	}
}

func TestPatchRejectsMutationOfImmutableField(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		mutate func(*Event)
	}{
		{"DID", func(ev *Event) { ev.DID = "did:plc:evil" }},
		{"Seq", func(ev *Event) { ev.Seq = ev.Seq + 1 }},
		{"WitnessedAt", func(ev *Event) { ev.WitnessedAt = 42 }},
		{"Kind", func(ev *Event) { ev.Kind = KindDelete }},
		{"Collection", func(ev *Event) { ev.Collection = "app.bsky.graph.follow" }},
		{"Rkey", func(ev *Event) { ev.Rkey = "hacked" }},
		{"Rev", func(ev *Event) { ev.Rev = "hacked" }},
		{"Payload", func(ev *Event) { ev.Payload = append(ev.Payload, 0xff) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := sealedSegmentForReader(t, dir, patchFixtureEvents(), 2)
			before, err := os.ReadFile(path)
			require.NoError(t, err)

			_, err = Patch(path, func(ev *Event) bool {
				tc.mutate(ev)
				return true
			}, PatchOptions{})
			require.ErrorIs(t, err, ErrInvalidConfig, "mutating a non-display field must be rejected")

			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, before, after, "a rejected patch must leave the source untouched")
		})
	}
}

func TestPatchCandidateDIDsSkipDisjointSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := sealedSegmentForReader(t, dir, patchFixtureEvents(), 2)
	before, err := os.ReadFile(path)
	require.NoError(t, err)

	res, err := Patch(path, func(*Event) bool {
		t.Fatal("mutate must not run when candidate DIDs miss the segment bloom")
		return true
	}, PatchOptions{CandidateDIDs: []string{"did:plc:not-present"}})
	require.NoError(t, err)
	require.False(t, res.Patched)

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestPatchCrashSeams(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name           string
		point          string
		wantPatchedYet bool
	}{
		{name: "temp written", point: CrashPointPatchTempWritten, wantPatchedYet: false},
		{name: "temp synced", point: CrashPointPatchTempSynced, wantPatchedYet: false},
		{name: "renamed", point: CrashPointPatchRenamed, wantPatchedYet: true},
		{name: "dir synced", point: CrashPointPatchDirSynced, wantPatchedYet: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := sealedSegmentForReader(t, dir, patchFixtureEvents(), 2)

			const importedTS = int64(1_600_000_000_000_000)
			crashErr := errors.New("simulated segment patch crash")
			_, err := Patch(path, func(ev *Event) bool {
				ev.IndexedAt = importedTS
				return true
			}, PatchOptions{CrashInjector: patchPointInjector{point: tc.point, err: crashErr}})
			require.ErrorIs(t, err, crashErr)

			// The file must always reopen cleanly: either the pristine
			// original (crash before rename) or the fully-patched file
			// (crash at/after rename). No torn intermediate state.
			r, err := Open(ReaderConfig{Path: path})
			require.NoError(t, err)
			t.Cleanup(func() { _ = r.Close() })
			require.EqualValues(t, len(patchFixtureEvents()), r.Header().EventCount)

			var sawImported bool
			for bi := range int(r.Header().BlockCount) {
				evs, err := r.DecodeBlock(bi)
				require.NoError(t, err)
				for _, ev := range evs {
					if ev.IndexedAt == importedTS {
						sawImported = true
					}
				}
			}
			require.Equal(t, tc.wantPatchedYet, sawImported,
				"display column is imported iff the crash fired at/after the durable rename")
		})
	}
}

type patchPointInjector struct {
	point string
	err   error
}

func (i patchPointInjector) SimulateCrash(_ context.Context, point string) error {
	if point == i.point {
		return i.err
	}
	return nil
}

// FuzzPatch feeds arbitrary bytes to Patch: it must error (never panic) on
// corrupt input and leave the source file untouched. A successful patch of a
// valid seed must reopen cleanly.
func FuzzPatch(f *testing.F) {
	dir := f.TempDir()
	seedPath := sealedSegmentForReader(f, dir, patchFixtureEvents(), 2)
	seed, err := os.ReadFile(seedPath)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add(seed[:len(seed)/2])
	f.Add([]byte{})
	corrupted := append([]byte(nil), seed...)
	corrupted[300] ^= 0xff
	f.Add(corrupted)

	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "seg.jss")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		res, err := Patch(path, func(ev *Event) bool {
			ev.IndexedAt = 1_600_000_000_000_000
			return true
		}, PatchOptions{})
		after, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatalf("source file unreadable after patch attempt: %v", readErr)
		}
		if err != nil {
			if !bytes.Equal(data, after) {
				t.Fatalf("failed patch mutated the source file")
			}
			return
		}
		if res.Patched {
			r, openErr := Open(ReaderConfig{Path: path})
			if openErr != nil {
				t.Fatalf("patched file does not reopen: %v", openErr)
			}
			_ = r.Close()
		}
	})
}
