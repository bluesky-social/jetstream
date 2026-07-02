package timestamp_test

import (
	"io"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream/internal/timestamp"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

// validCID is a real dag-cbor CID string (round-trips through ParseCIDString).
const validCID = "bafyreigbtj4x7ip5legnfznufuopl4sg4knzc2cof6duas4b3q2fy6swua"

// collectRows runs Parse over src and returns the valid rows plus the stats.
func collectRows(t *testing.T, src string, opts timestamp.Options) ([]timestamp.Row, timestamp.Stats) {
	t.Helper()
	var rows []timestamp.Row
	userOnRow := opts.OnRow
	opts.OnRow = func(r timestamp.Row) error {
		rows = append(rows, r)
		if userOnRow != nil {
			return userOnRow(r)
		}
		return nil
	}
	stats, err := timestamp.Parse(strings.NewReader(src), opts)
	require.NoError(t, err)
	return rows, stats
}

func TestParse_ValidRows(t *testing.T) {
	t.Parallel()
	src := "uri,timestamp,scope,cid\n" +
		"at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,,\n" +
		"at://did:plc:bob/app.bsky.feed.like/r2,2023-06-07T08:09:10Z,all_versions,\n"

	rows, stats := collectRows(t, src, timestamp.Options{})
	require.EqualValues(t, 2, stats.RowsTotal)
	require.EqualValues(t, 2, stats.RowsValid)
	require.EqualValues(t, 0, stats.RowsRejected)
	require.Len(t, rows, 2)

	require.Equal(t, "did:plc:alice", rows[0].DID)
	require.Equal(t, "app.bsky.feed.post", rows[0].Collection)
	require.Equal(t, "r1", rows[0].Rkey)
	require.Equal(t, timestamp.ScopeAllVersions, rows[0].Scope)
	require.False(t, rows[0].CID.Defined(), "all_versions row carries no CID")
	// 2022-01-02T03:04:05Z in unix micros.
	require.EqualValues(t, 1_641_092_645_000_000, rows[0].TimestampMicros)

	require.Equal(t, "did:plc:bob", rows[1].DID)
	require.Equal(t, timestamp.ScopeAllVersions, rows[1].Scope)
}

// TestParse_ScopeDefaultsWhenColumnAbsent proves scope defaults to all_versions
// when the optional column is not even present in the header.
func TestParse_ScopeDefaultsWhenColumnAbsent(t *testing.T) {
	t.Parallel()
	src := "uri,timestamp\n" +
		"at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z\n"
	rows, stats := collectRows(t, src, timestamp.Options{})
	require.EqualValues(t, 1, stats.RowsValid)
	require.Equal(t, timestamp.ScopeAllVersions, rows[0].Scope)
}

func TestParse_SpecificVersionRequiresParseableCID(t *testing.T) {
	t.Parallel()
	src := "uri,timestamp,scope,cid\n" +
		// valid specific_version with a good CID
		"at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,specific_version," + validCID + "\n" +
		// specific_version, missing CID -> reject
		"at://did:plc:bob/app.bsky.feed.post/r2,2022-01-02T03:04:05Z,specific_version,\n" +
		// specific_version, unparseable CID -> reject
		"at://did:plc:carol/app.bsky.feed.post/r3,2022-01-02T03:04:05Z,specific_version,not-a-cid\n"

	rows, stats := collectRows(t, src, timestamp.Options{})
	require.EqualValues(t, 3, stats.RowsTotal)
	require.EqualValues(t, 1, stats.RowsValid)
	require.EqualValues(t, 2, stats.RowsRejected)
	require.Len(t, rows, 1)

	require.Equal(t, timestamp.ScopeSpecificVersion, rows[0].Scope)
	require.True(t, rows[0].CID.Defined())
	want, err := cbor.ParseCIDString(validCID)
	require.NoError(t, err)
	require.True(t, rows[0].CID.Equal(want))

	require.EqualValues(t, 1, stats.RejectsByReason[timestamp.ReasonMissingCID])
	require.EqualValues(t, 1, stats.RejectsByReason[timestamp.ReasonBadCID])
}

// TestParse_CIDIgnoredForAllVersions: a cid supplied with all_versions is
// ignored, not stored (design §4 D).
func TestParse_CIDIgnoredForAllVersions(t *testing.T) {
	t.Parallel()
	src := "uri,timestamp,scope,cid\n" +
		"at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,all_versions," + validCID + "\n"
	rows, stats := collectRows(t, src, timestamp.Options{})
	require.EqualValues(t, 1, stats.RowsValid)
	require.False(t, rows[0].CID.Defined(), "cid must be ignored for all_versions")
}

func TestParse_MalformedRowsSkippedAndCounted(t *testing.T) {
	t.Parallel()
	src := "uri,timestamp,scope,cid\n" +
		"at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,,\n" + // valid
		"not-a-uri,2022-01-02T03:04:05Z,,\n" + // bad uri
		"at://alice.bsky.social/app.bsky.feed.post/r,2022-01-02T03:04:05Z,,\n" + // handle authority, not DID
		"at://did:plc:bob,2022-01-02T03:04:05Z,,\n" + // uri missing collection/rkey
		"at://did:plc:carol/app.bsky.feed.post/r4,not-a-time,,\n" + // bad timestamp
		"at://did:plc:dave/app.bsky.feed.post/r5,1969-01-01T00:00:00Z,,\n" + // negative micros
		"at://did:plc:erin/app.bsky.feed.post/r6,2022-01-02T03:04:05Z,bogus_scope,\n" + // unknown scope
		",2022-01-02T03:04:05Z,,\n" + // missing uri
		"at://did:plc:frank/app.bsky.feed.post/r8,,,\n" // missing timestamp

	rows, stats := collectRows(t, src, timestamp.Options{})
	require.EqualValues(t, 9, stats.RowsTotal)
	require.EqualValues(t, 1, stats.RowsValid)
	require.EqualValues(t, 8, stats.RowsRejected)
	require.Len(t, rows, 1)
	require.Equal(t, "did:plc:alice", rows[0].DID)

	require.EqualValues(t, 1, stats.RejectsByReason[timestamp.ReasonBadURI])
	require.EqualValues(t, 1, stats.RejectsByReason[timestamp.ReasonURINotDID])
	require.EqualValues(t, 1, stats.RejectsByReason[timestamp.ReasonURIIncomplete])
	require.EqualValues(t, 1, stats.RejectsByReason[timestamp.ReasonBadTimestamp])
	require.EqualValues(t, 1, stats.RejectsByReason[timestamp.ReasonNonPositiveTime])
	require.EqualValues(t, 1, stats.RejectsByReason[timestamp.ReasonUnknownScope])
	// missing uri and missing timestamp both fold into ReasonMissingField.
	require.EqualValues(t, 2, stats.RejectsByReason[timestamp.ReasonMissingField])
}

// TestParse_OneBadRowDoesNotAbortFile is the Q-REJECT anchor: a bad row in the
// middle must not stop the rows after it from being processed.
func TestParse_OneBadRowDoesNotAbortFile(t *testing.T) {
	t.Parallel()
	src := "uri,timestamp\n" +
		"at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z\n" +
		"garbage-line-with-no-valid-uri,also-bad-time\n" +
		"at://did:plc:bob/app.bsky.feed.post/r2,2023-01-02T03:04:05Z\n"
	rows, stats := collectRows(t, src, timestamp.Options{})
	require.EqualValues(t, 2, stats.RowsValid)
	require.EqualValues(t, 1, stats.RowsRejected)
	require.Len(t, rows, 2)
	require.Equal(t, "did:plc:alice", rows[0].DID)
	require.Equal(t, "did:plc:bob", rows[1].DID)
}

// TestParse_WrongFieldCountRowRejected: a data row with too many/few fields is
// a recoverable csv.ErrFieldCount, reported not fatal.
func TestParse_WrongFieldCountRowRejected(t *testing.T) {
	t.Parallel()
	src := "uri,timestamp,scope,cid\n" +
		"at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,,\n" +
		"at://did:plc:bob/app.bsky.feed.post/r2,2022-01-02T03:04:05Z\n" + // 2 fields, header has 4
		"at://did:plc:carol/app.bsky.feed.post/r3,2022-01-02T03:04:05Z,,\n"
	rows, stats := collectRows(t, src, timestamp.Options{})
	require.EqualValues(t, 2, stats.RowsValid)
	require.EqualValues(t, 1, stats.RowsRejected)
	require.EqualValues(t, 1, stats.RejectsByReason[timestamp.ReasonMissingField])
	require.Len(t, rows, 2)
}

// TestParse_OverlongRowRejected pins the Phase A row-length bound: a row whose
// on-disk footprint reaches MaxRowBytes must be rejected at parse time, because
// Phase C re-reads rows through a MaxRowBytes window and a longer row would be
// silently truncated there — a truncated suffix can still validate and patch
// the WRONG record (e.g. rkey "rkey12345" truncated to "rkey").
func TestParse_OverlongRowRejected(t *testing.T) {
	t.Parallel()
	pad := strings.Repeat(" ", timestamp.MaxRowBytes) // spaces are trimmed by validation, so the row is otherwise valid
	src := "timestamp,uri\n" +
		"2022-01-02T03:04:05Z," + pad + "at://did:plc:alice/app.bsky.feed.post/rkey12345\n" +
		"2023-06-07T08:09:10Z,at://did:plc:bob/app.bsky.feed.like/r2\n"

	rows, stats := collectRows(t, src, timestamp.Options{})
	require.EqualValues(t, 1, stats.RowsValid, "the overlong row must not be accepted")
	require.EqualValues(t, 1, stats.RejectsByReason[timestamp.ReasonRowTooLong])
	require.Len(t, rows, 1)
	require.Equal(t, "did:plc:bob", rows[0].DID)
}

// TestParse_BareQuoteFailsFileLoudly: an unclosed quote makes encoding/csv
// consume input to EOF hunting for the closing quote, so everything after it
// is unparseable. Treating it as one skipped row would silently drop the whole
// rest of the file — it must fail the file loudly instead (like ErrHeader).
func TestParse_BareQuoteFailsFileLoudly(t *testing.T) {
	t.Parallel()
	src := "uri,timestamp\n" +
		"at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z\n" +
		"at://did:plc:bob/app.bsky.feed.post/\"r2,2022-01-02T03:04:05Z\n" + // stray quote
		"at://did:plc:carol/app.bsky.feed.post/r3,2022-01-02T03:04:05Z\n" +
		"at://did:plc:dave/app.bsky.feed.post/r4,2022-01-02T03:04:05Z\n"

	var rows []timestamp.Row
	_, err := timestamp.Parse(strings.NewReader(src), timestamp.Options{
		OnRow: func(r timestamp.Row) error { rows = append(rows, r); return nil },
	})
	require.Error(t, err, "a quote error swallows the rest of the file; it must abort, not skip")
	require.Len(t, rows, 1, "rows before the quote error were processed")
}

func TestParse_OffsetSeeksBackToRow(t *testing.T) {
	t.Parallel()
	// Two valid rows; capture their offsets and prove a Seek to each offset
	// lands exactly on that row's bytes. This is the Phase C contract.
	row1 := "at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,,\n"
	row2 := "at://did:plc:bob/app.bsky.feed.like/r2,2023-06-07T08:09:10Z,,\n"
	src := "uri,timestamp,scope,cid\n" + row1 + row2

	rows, stats := collectRows(t, src, timestamp.Options{})
	require.EqualValues(t, 2, stats.RowsValid)
	require.Len(t, rows, 2)

	require.True(t, strings.HasPrefix(src[rows[0].Offset:], row1),
		"offset of row 0 must land on its first byte")
	require.True(t, strings.HasPrefix(src[rows[1].Offset:], row2),
		"offset of row 1 must land on its first byte")
}

// TestParse_CRLFLineEndings: a Windows-authored CSV (CRLF row terminators)
// parses identically, and the recorded offsets still point at row starts —
// the byte before each offset is the previous row's '\n', which is exactly
// the boundary check Phase C's ReadRow applies.
func TestParse_CRLFLineEndings(t *testing.T) {
	t.Parallel()
	src := "uri,timestamp,scope,cid\r\n" +
		"at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,,\r\n" +
		"at://did:plc:bob/app.bsky.feed.like/r2,2023-06-07T08:09:10Z,all_versions,\r\n"

	rows, stats := collectRows(t, src, timestamp.Options{})
	require.EqualValues(t, 2, stats.RowsValid)
	require.EqualValues(t, 0, stats.RowsRejected)
	require.Equal(t, "did:plc:alice", rows[0].DID)
	require.Equal(t, "did:plc:bob", rows[1].DID)
	for _, r := range rows {
		require.Equal(t, byte('\n'), src[r.Offset-1], "offset must sit just past a newline")
		require.True(t, strings.HasPrefix(src[r.Offset:], "at://"+r.DID+"/"),
			"offset lands on the row's own bytes")
	}
}

func TestParse_RejectSampleIsBounded(t *testing.T) {
	t.Parallel()
	var b strings.Builder
	b.WriteString("uri,timestamp\n")
	const badRows = 250
	for range badRows {
		b.WriteString("not-a-uri,not-a-time\n")
	}
	_, stats := collectRows(t, b.String(), timestamp.Options{RejectSampleLimit: 100})
	require.EqualValues(t, badRows, stats.RowsRejected)
	require.EqualValues(t, badRows, stats.RejectsByReason[timestamp.ReasonBadURI])
	require.Len(t, stats.RejectSample, 100, "sample capped at limit")
}

// TestParse_OnRejectFiresForEveryReject: the callback (M6's durable artifact
// writer) sees every reject even when the in-memory sample is capped.
func TestParse_OnRejectFiresForEveryReject(t *testing.T) {
	t.Parallel()
	var b strings.Builder
	b.WriteString("uri,timestamp\n")
	const badRows = 150
	for range badRows {
		b.WriteString("bad,bad\n")
	}
	var seen int
	opts := timestamp.Options{
		RejectSampleLimit: 10,
		OnReject:          func(timestamp.Reject) { seen++ },
	}
	_, stats := collectRows(t, b.String(), opts)
	require.EqualValues(t, badRows, seen, "OnReject fires for every reject")
	require.Len(t, stats.RejectSample, 10)
}

func TestParse_HeaderErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		src  string
	}{
		{"empty file", ""},
		{"missing uri column", "timestamp,scope\n"},
		{"missing timestamp column", "uri,scope\n"},
		{"duplicate column", "uri,uri,timestamp\n"},
		{"unrecognized column", "uri,timestamp,timestmp\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := timestamp.Parse(strings.NewReader(tc.src), timestamp.Options{
				OnRow: func(timestamp.Row) error { return nil },
			})
			require.ErrorIs(t, err, timestamp.ErrHeader)
		})
	}
}

// TestParse_HeaderCaseAndWhitespaceInsensitive: column names are matched
// case-insensitively and trimmed, so a hand-edited header still works.
func TestParse_HeaderColumnOrderIndependent(t *testing.T) {
	t.Parallel()
	// Columns in a non-canonical order with mixed case / spaces.
	src := " Timestamp , URI , CID , Scope \n" +
		"2022-01-02T03:04:05Z,at://did:plc:alice/app.bsky.feed.post/r1," + validCID + ",specific_version\n"
	rows, stats := collectRows(t, src, timestamp.Options{})
	require.EqualValues(t, 1, stats.RowsValid)
	require.Equal(t, "did:plc:alice", rows[0].DID)
	require.Equal(t, timestamp.ScopeSpecificVersion, rows[0].Scope)
	require.True(t, rows[0].CID.Defined())
}

func TestParse_RequiresOnRow(t *testing.T) {
	t.Parallel()
	_, err := timestamp.Parse(strings.NewReader("uri,timestamp\n"), timestamp.Options{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "OnRow")
}

// TestParse_OnRowErrorAborts: a handler error stops the stream and surfaces.
func TestParse_OnRowErrorAborts(t *testing.T) {
	t.Parallel()
	src := "uri,timestamp\n" +
		"at://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z\n" +
		"at://did:plc:bob/app.bsky.feed.post/r2,2022-01-02T03:04:05Z\n"
	var count int
	_, err := timestamp.Parse(strings.NewReader(src), timestamp.Options{
		OnRow: func(timestamp.Row) error {
			count++
			return io.ErrClosedPipe
		},
	})
	require.ErrorIs(t, err, io.ErrClosedPipe)
	require.Equal(t, 1, count, "parse stops at the first OnRow error")
}
