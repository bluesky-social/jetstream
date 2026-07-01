package timestamp_test

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/timestamp"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

// payloadA/payloadB are two distinct dag-cbor payloads; cidA/cidB are their CIDs.
var (
	payloadA = []byte{0xa1, 0x61, 0x78, 0x01} // {"x":1}
	payloadB = []byte{0xa1, 0x61, 0x78, 0x02} // {"x":2}
)

func cidString(t *testing.T, payload []byte) string {
	t.Helper()
	return cbor.ComputeCID(cbor.CodecDagCBOR, payload).String()
}

// writeImportCSV writes a plain import CSV and returns its path. rows are raw
// CSV data lines (without the header or trailing newline).
func writeImportCSV(t *testing.T, header string, rows ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "import.csv")
	var b strings.Builder
	b.WriteString(header)
	b.WriteByte('\n')
	for _, r := range rows {
		b.WriteString(r)
		b.WriteByte('\n')
	}
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o644))
	return path
}

// buildOffsetFile parses csvPath via Phase A+B into a single segment's offset
// file (routing every DID to one segment idx), and returns the offset file path
// plus the open RowReader. It is the realistic Phase A→B→C bridge.
func buildOffsetFile(t *testing.T, csvPath string, segIdx uint64) (string, *timestamp.RowReader) {
	t.Helper()
	f, err := os.Open(csvPath)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	jobDir := t.TempDir()
	// A selector that routes every DID to the single segIdx.
	sel := &oneSegmentSelector{idx: segIdx}
	b, err := timestamp.NewBucketer(timestamp.BucketerConfig{Selector: sel, JobDir: jobDir})
	require.NoError(t, err)
	_, err = timestamp.Parse(f, timestamp.Options{OnRow: b.Route})
	require.NoError(t, err)
	require.NoError(t, b.Close())

	rr, err := timestamp.OpenRowReader(csvPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rr.Close() })
	return filepath.Join(jobDir, timestamp.OffsetFileName(segIdx)), rr
}

type oneSegmentSelector struct{ idx uint64 }

func (s *oneSegmentSelector) Generation() uint64 { return 1 }
func (s *oneSegmentSelector) SelectBlocksForDID(string) ([]manifest.SegmentBlockSelection, error) {
	return []manifest.SegmentBlockSelection{{Idx: s.idx, Path: "/seg", Blocks: []int{0}}}, nil
}

func TestRowReader_ReadsRowByOffset(t *testing.T) {
	t.Parallel()
	rowA := "at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,,"
	rowB := "at://did:plc:bob/app.bsky.feed.like/r2,2023-06-07T08:09:10Z,,"
	path := writeImportCSV(t, "uri,timestamp,scope,cid", rowA, rowB)

	// Offsets: header line length + newline is the start of rowA.
	headerLen := int64(len("uri,timestamp,scope,cid") + 1)
	rowALen := int64(len(rowA) + 1)

	rr, err := timestamp.OpenRowReader(path)
	require.NoError(t, err)
	defer func() { _ = rr.Close() }()

	got, err := rr.ReadRow(headerLen)
	require.NoError(t, err)
	require.Equal(t, "did:plc:alice", got.DID)
	require.Equal(t, "app.bsky.feed.post", got.Collection)
	require.Equal(t, "r1", got.Rkey)

	got, err = rr.ReadRow(headerLen + rowALen)
	require.NoError(t, err)
	require.Equal(t, "did:plc:bob", got.DID)
}

func TestRowReader_CorruptOffsetRejected(t *testing.T) {
	t.Parallel()
	path := writeImportCSV(t, "uri,timestamp,scope,cid",
		"at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,,")
	rr, err := timestamp.OpenRowReader(path)
	require.NoError(t, err)
	defer func() { _ = rr.Close() }()

	// Offset in the middle of the header/row -> not a valid row start.
	_, err = rr.ReadRow(5)
	require.ErrorIs(t, err, timestamp.ErrCorruptOffset)

	// Offset past EOF.
	_, err = rr.ReadRow(1 << 30)
	require.ErrorIs(t, err, timestamp.ErrCorruptOffset)

	// Offset 0 is the header, never a data row.
	_, err = rr.ReadRow(0)
	require.ErrorIs(t, err, timestamp.ErrCorruptOffset)
}

// TestRowReader_MidRecordOffsetRejectedEvenIfSuffixParses pins the row-boundary
// check: an offset whose preceding byte is not a newline is corrupt (Phase B
// only ever records record starts) and must be rejected EVEN when the bytes
// from that offset happen to decode as a valid row. Field re-validation alone
// cannot catch this: here the uri field carries leading spaces (trimmed by
// validation), so the suffix starting two bytes in parses to the same valid
// row — a stale offset into a swapped file could otherwise apply an
// instruction Phase A never accepted.
func TestRowReader_MidRecordOffsetRejectedEvenIfSuffixParses(t *testing.T) {
	t.Parallel()
	path := writeImportCSV(t, "uri,timestamp,scope,cid",
		"  at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,,")
	rr, err := timestamp.OpenRowReader(path)
	require.NoError(t, err)
	defer func() { _ = rr.Close() }()

	headerLen := int64(len("uri,timestamp,scope,cid") + 1)

	// The true record start (leading spaces included) reads fine.
	got, err := rr.ReadRow(headerLen)
	require.NoError(t, err)
	require.Equal(t, "did:plc:alice", got.DID)

	// Two bytes in, the suffix alone would still validate ("at://..." after
	// TrimSpace) — but it is not a record boundary, so it must be rejected.
	_, err = rr.ReadRow(headerLen + 2)
	require.ErrorIs(t, err, timestamp.ErrCorruptOffset)
}

func TestRowReader_HonorsHeaderColumnOrder(t *testing.T) {
	t.Parallel()
	// Non-canonical column order: the reader must map columns from the header,
	// not by position, when reading a row mid-file.
	row := "2022-01-02T03:04:05Z,at://did:plc:alice/app.bsky.feed.post/r1," + cidString(t, payloadA) + ",specific_version"
	path := writeImportCSV(t, "timestamp,uri,cid,scope", row)
	rr, err := timestamp.OpenRowReader(path)
	require.NoError(t, err)
	defer func() { _ = rr.Close() }()

	headerLen := int64(len("timestamp,uri,cid,scope") + 1)
	got, err := rr.ReadRow(headerLen)
	require.NoError(t, err)
	require.Equal(t, "did:plc:alice", got.DID)
	require.Equal(t, timestamp.ScopeSpecificVersion, got.Scope)
	require.True(t, got.CID.Defined())
}

func TestBuildPatchPlan_FoldsRows(t *testing.T) {
	t.Parallel()
	const ts1 = "2022-01-02T03:04:05Z"
	const ts2 = "2023-01-02T03:04:05Z"
	path := writeImportCSV(t, "uri,timestamp,scope,cid",
		"at://did:plc:alice/app.bsky.feed.post/r1,"+ts1+",,",
		// last-write-wins: same path, later value overrides.
		"at://did:plc:alice/app.bsky.feed.post/r1,"+ts2+",all_versions,",
		"at://did:plc:bob/app.bsky.feed.post/r2,"+ts1+",specific_version,"+cidString(t, payloadA),
	)
	offsetPath, rr := buildOffsetFile(t, path, 5)
	plan, err := timestamp.BuildPatchPlan(offsetPath, rr)
	require.NoError(t, err)

	st := plan.Stats()
	require.EqualValues(t, 3, st.RowsPlanned)
	require.EqualValues(t, 0, st.RowsCorrupt)
	require.Equal(t, 2, st.DistinctPaths)
	require.Equal(t, 1, st.PathsAllVersions)
	require.Equal(t, 1, st.PathsSpecific)
}

// TestMutate_AllVersionsPatchesEveryMatchingRow: an all_versions target stamps
// every materialization row sharing the path, and leaves non-matching rows and
// non-materialization rows untouched.
func TestMutate_AllVersionsPatchesEveryMatchingRow(t *testing.T) {
	t.Parallel()
	const wantTS = int64(1_641_092_645_000_000) // 2022-01-02T03:04:05Z
	path := writeImportCSV(t, "uri,timestamp,scope,cid",
		"at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,all_versions,")
	offsetPath, rr := buildOffsetFile(t, path, 0)
	plan, err := timestamp.BuildPatchPlan(offsetPath, rr)
	require.NoError(t, err)
	m := plan.BuildMutate()
	fn := m.Fn()

	// Two versions of the same path (create + update) both get patched.
	create := &segment.Event{Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", WitnessedAt: 9_000, Payload: payloadA}
	update := &segment.Event{Kind: segment.KindUpdate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", WitnessedAt: 9_500, Payload: payloadB}
	require.True(t, fn(create))
	require.True(t, fn(update))
	require.Equal(t, wantTS, create.IndexedAt)
	require.Equal(t, wantTS, update.IndexedAt)

	// A delete row (no payload) for the same path is not a materialization -> skip.
	del := &segment.Event{Kind: segment.KindDelete, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", WitnessedAt: 9_600}
	require.False(t, fn(del))
	require.EqualValues(t, 0, del.IndexedAt)

	// A different path is untouched.
	other := &segment.Event{Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "rX", Payload: payloadA}
	require.False(t, fn(other))

	require.EqualValues(t, 2, m.Stats().RowsMatchedAllVersions)
}

// TestMutate_Idempotent: a row already at the target value is a no-op (false),
// so a re-run produces zero mutations and segment.Patch skips the rename.
func TestMutate_Idempotent(t *testing.T) {
	t.Parallel()
	path := writeImportCSV(t, "uri,timestamp,scope,cid",
		"at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,,")
	offsetPath, rr := buildOffsetFile(t, path, 0)
	plan, err := timestamp.BuildPatchPlan(offsetPath, rr)
	require.NoError(t, err)
	fn := plan.BuildMutate().Fn()

	ev := &segment.Event{Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Payload: payloadA}
	require.True(t, fn(ev), "first application changes the value")
	require.False(t, fn(ev), "second application is a no-op (already at target)")
}

// TestMutate_SpecificVersionMatchesCIDOnly: specific_version patches only the
// row whose payload CID matches, patches ALL rows with that CID (duplicate-CID
// rule), and counts unmatched CIDs.
func TestMutate_SpecificVersionMatchesCIDOnly(t *testing.T) {
	t.Parallel()
	const wantTS = int64(1_641_092_645_000_000)
	path := writeImportCSV(t, "uri,timestamp,scope,cid",
		"at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,specific_version,"+cidString(t, payloadA),
		// A specific_version CID that no row in the segment will carry.
		"at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,specific_version,"+cidString(t, []byte{0xa1, 0x61, 0x7a, 0x09}),
	)
	offsetPath, rr := buildOffsetFile(t, path, 0)
	plan, err := timestamp.BuildPatchPlan(offsetPath, rr)
	require.NoError(t, err)
	m := plan.BuildMutate()
	fn := m.Fn()

	// Row with payloadA (CID matches first target) -> patched.
	matchA := &segment.Event{Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Payload: payloadA}
	require.True(t, fn(matchA))
	require.Equal(t, wantTS, matchA.IndexedAt)

	// A second row with the SAME payloadA (duplicate CID, e.g. re-created) is
	// also patched.
	matchA2 := &segment.Event{Kind: segment.KindCreateResync, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Payload: payloadA}
	require.True(t, fn(matchA2))
	require.Equal(t, wantTS, matchA2.IndexedAt)

	// A row with payloadB (CID does not match any target) -> untouched.
	noMatch := &segment.Event{Kind: segment.KindUpdate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Payload: payloadB}
	require.False(t, fn(noMatch))
	require.EqualValues(t, 0, noMatch.IndexedAt)

	st := m.Stats()
	require.EqualValues(t, 2, st.RowsMatchedSpecific, "both payloadA rows patched")
	require.EqualValues(t, 0, st.RowsMatchedAllVersions)
	require.EqualValues(t, 1, st.SpecificCIDsUnmatched, "the bogus-CID target matched nothing")
}

// TestMutate_SpecificVersionWinsOverAllVersions: when both scopes target the
// same path and the row's CID matches the specific target, the specific
// timestamp is applied (operator's more precise instruction).
func TestMutate_SpecificVersionWinsOverAllVersions(t *testing.T) {
	t.Parallel()
	const allTS = "2020-01-01T00:00:00Z"
	const specTS = "2022-01-02T03:04:05Z"
	const wantSpecific = int64(1_641_092_645_000_000)
	path := writeImportCSV(t, "uri,timestamp,scope,cid",
		"at://did:plc:alice/app.bsky.feed.post/r1,"+allTS+",all_versions,",
		"at://did:plc:alice/app.bsky.feed.post/r1,"+specTS+",specific_version,"+cidString(t, payloadA),
	)
	offsetPath, rr := buildOffsetFile(t, path, 0)
	plan, err := timestamp.BuildPatchPlan(offsetPath, rr)
	require.NoError(t, err)
	fn := plan.BuildMutate().Fn()

	// Row whose CID matches the specific target: specific wins.
	matchA := &segment.Event{Kind: segment.KindCreate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Payload: payloadA}
	require.True(t, fn(matchA))
	require.Equal(t, wantSpecific, matchA.IndexedAt)

	// Row whose CID does NOT match the specific target: falls back to
	// all_versions.
	other := &segment.Event{Kind: segment.KindUpdate, DID: "did:plc:alice", Collection: "app.bsky.feed.post", Rkey: "r1", Payload: payloadB}
	require.True(t, fn(other))
	require.EqualValues(t, 1_577_836_800_000_000, other.IndexedAt) // 2020-01-01
}

func TestBuildPatchPlan_CorruptOffsetCounted(t *testing.T) {
	t.Parallel()
	path := writeImportCSV(t, "uri,timestamp,scope,cid",
		"at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,,")
	rr, err := timestamp.OpenRowReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rr.Close() })

	// Hand-write an offset file with one good offset and one corrupt (mid-line).
	offsetPath := filepath.Join(t.TempDir(), timestamp.OffsetFileName(0))
	headerLen := int64(len("uri,timestamp,scope,cid") + 1)
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(headerLen)) // valid row start
	binary.LittleEndian.PutUint64(buf[8:16], 3)                // corrupt: mid-header
	require.NoError(t, os.WriteFile(offsetPath, buf, 0o644))

	plan, err := timestamp.BuildPatchPlan(offsetPath, rr)
	require.NoError(t, err)
	st := plan.Stats()
	require.EqualValues(t, 1, st.RowsPlanned)
	require.EqualValues(t, 1, st.RowsCorrupt)
}

// TestBuildPatchPlan_TornOffsetFileRejected: a partial trailing record (a
// crash mid-append) is surfaced, not silently ignored.
func TestBuildPatchPlan_TornOffsetFileRejected(t *testing.T) {
	t.Parallel()
	path := writeImportCSV(t, "uri,timestamp,scope,cid",
		"at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,,")
	rr, err := timestamp.OpenRowReader(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rr.Close() })

	offsetPath := filepath.Join(t.TempDir(), timestamp.OffsetFileName(0))
	// 12 bytes = one full uint64 + a 4-byte partial (torn).
	require.NoError(t, os.WriteFile(offsetPath, make([]byte, 12), 0o644))
	_, err = timestamp.BuildPatchPlan(offsetPath, rr)
	require.ErrorIs(t, err, timestamp.ErrCorruptOffset)
}
