package timestamp_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream/internal/timestamp"
	"github.com/stretchr/testify/require"
)

// FuzzParseRoundTrip feeds arbitrary bytes through Parse and asserts it never
// panics, its stats stay internally consistent, and — the load-bearing
// invariant — every row Parse accepts reads back IDENTICALLY through
// RowReader.ReadRow at its recorded offset. That round-trip is the whole
// Phase A ↔ Phase C contract: if any input (quoted newlines, CRLF, blank
// lines, adversarial garbage) can produce an accepted row whose offset does
// not re-read to the same row, Phase C would patch from a different
// instruction than Phase A validated.
func FuzzParseRoundTrip(f *testing.F) {
	f.Add([]byte("uri,timestamp,scope,cid\nat://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,,\n"))
	f.Add([]byte("uri,timestamp\r\nat://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z\r\n"))
	f.Add([]byte("uri,timestamp\n\"\nat://did:plc:dave/app.bsky.feed.post/r4\",2022-01-02T03:04:05Z\n"))
	f.Add([]byte("uri,timestamp\n\n\nat://did:plc:alice/app.bsky.feed.post/r1,2022-01-02T03:04:05Z\n"))
	f.Add([]byte("uri,timestamp,scope,cid\nat://did:plc:a/app.bsky.feed.post/r1,2022-01-02T03:04:05Z,specific_version," + validCID + "\n"))
	f.Add([]byte("uri,timestamp\nnot-a-uri,not-a-time\nat://did:plc:b/app.bsky.feed.post/r2,2023-01-02T03:04:05Z\n"))
	f.Add([]byte("uri,timestamp\nat://did:plc:a/app.bsky.feed.post/\"r1,2022-01-02T03:04:05Z\n"))
	f.Add([]byte("uri,uri,timestamp\n"))
	f.Add([]byte("uri,timestamp\n"))
	f.Add([]byte(""))
	f.Add([]byte("\xff\xfe garbage"))

	f.Fuzz(func(t *testing.T, data []byte) {
		var rows []timestamp.Row
		var rejectsSeen uint64
		stats, parseErr := timestamp.Parse(bytes.NewReader(data), timestamp.Options{
			OnRow:    func(r timestamp.Row) error { rows = append(rows, r); return nil },
			OnReject: func(timestamp.Reject) { rejectsSeen++ },
		})

		// Stats invariants hold even when Parse aborts partway.
		require.Equal(t, stats.RowsTotal, stats.RowsValid+stats.RowsRejected)
		var byReason uint64
		for _, n := range stats.RejectsByReason {
			byReason += n
		}
		require.Equal(t, stats.RowsRejected, byReason)
		require.Equal(t, stats.RowsRejected, rejectsSeen, "OnReject fires exactly once per reject")
		require.LessOrEqual(t, len(stats.RejectSample), timestamp.DefaultRejectSampleLimit)
		require.EqualValues(t, len(rows), stats.RowsValid)

		for _, r := range rows {
			require.NotEmpty(t, r.DID)
			require.True(t, strings.HasPrefix(r.DID, "did:"))
			require.NotEmpty(t, r.Collection)
			require.NotEmpty(t, r.Rkey)
			require.Positive(t, r.TimestampMicros)
			require.Equal(t, r.Scope == timestamp.ScopeSpecificVersion, r.CID.Defined())
			require.Greater(t, r.Offset, int64(0), "a data row can never start at 0 (the header lives there)")
			require.Less(t, r.Offset, int64(len(data)))
		}

		if len(rows) == 0 {
			return
		}
		_ = parseErr // accepted-row offsets stay valid even if a later row aborted the parse

		path := filepath.Join(t.TempDir(), "fuzz.csv")
		require.NoError(t, os.WriteFile(path, data, 0o644))
		rr, err := timestamp.OpenRowReader(path)
		require.NoError(t, err, "Parse got past this header, so OpenRowReader must too")
		defer func() { _ = rr.Close() }()

		for _, want := range rows {
			got, err := rr.ReadRow(want.Offset)
			require.NoError(t, err, "accepted row at offset %d must re-read", want.Offset)
			require.Equal(t, want.DID, got.DID)
			require.Equal(t, want.Collection, got.Collection)
			require.Equal(t, want.Rkey, got.Rkey)
			require.Equal(t, want.Scope, got.Scope)
			require.Equal(t, want.TimestampMicros, got.TimestampMicros)
			require.True(t, want.CID.Equal(got.CID))
			require.Equal(t, want.Offset, got.Offset)
		}
	})
}
