package oracle

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTraceRecordIndexesAreMonotonic(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	trace := NewTrace(&buf)

	require.NoError(t, trace.Record("first", map[string]any{"value": "a"}))
	require.NoError(t, trace.Record("second", map[string]any{"value": "b"}))
	require.NoError(t, trace.Record("third", nil))

	records := decodeTraceRecords(t, buf.String())
	require.Len(t, records, 3)
	require.Equal(t, uint64(1), records[0].Index)
	require.Equal(t, uint64(2), records[1].Index)
	require.Equal(t, uint64(3), records[2].Index)
	require.Equal(t, "first", records[0].Kind)
	require.Equal(t, "second", records[1].Kind)
	require.Equal(t, "third", records[2].Kind)
}

func TestTraceRecordsAreJSONL(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	trace := NewTrace(&buf)

	require.NoError(t, trace.Record("alpha", map[string]any{"n": 1}))
	require.NoError(t, trace.Record("beta", map[string]any{"n": 2}))

	output := buf.String()
	require.Equal(t, "{\"index\":1,\"kind\":\"alpha\",\"data\":{\"n\":1}}\n{\"index\":2,\"kind\":\"beta\",\"data\":{\"n\":2}}\n", output)
	require.True(t, strings.HasSuffix(output, "\n"))
	require.NotContains(t, strings.TrimSuffix(output, "\n"), "\n\n")

	scanner := bufio.NewScanner(strings.NewReader(output))
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	require.NoError(t, scanner.Err())
	require.Len(t, lines, 2)

	for _, line := range lines {
		require.NotEmpty(t, line)
		require.JSONEq(t, line, line)
	}
}

func TestTraceRecordIndexesAreConcurrentSafe(t *testing.T) {
	t.Parallel()

	const (
		goroutines = 16
		perWorker  = 32
		wantCount  = goroutines * perWorker
	)
	var buf bytes.Buffer
	trace := NewTrace(&buf)
	errs := make(chan error, wantCount)
	var wg sync.WaitGroup

	for worker := range goroutines {
		wg.Go(func() {
			for i := range perWorker {
				errs <- trace.Record("event", map[string]any{
					"worker": worker,
					"i":      i,
				})
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	records := decodeTraceRecords(t, buf.String())
	require.Len(t, records, wantCount)
	seen := make([]bool, wantCount+1)
	for _, record := range records {
		require.GreaterOrEqual(t, record.Index, uint64(1))
		require.LessOrEqual(t, record.Index, uint64(wantCount))
		require.False(t, seen[record.Index], "duplicate trace index %d", record.Index)
		seen[record.Index] = true
	}
	for i := 1; i <= wantCount; i++ {
		require.True(t, seen[i], "missing trace index %d", i)
	}
}

func TestTraceRecordRejectsShortWrite(t *testing.T) {
	t.Parallel()

	trace := NewTrace(shortTraceWriter{})

	require.ErrorIs(t, trace.Record("short", nil), io.ErrShortWrite)
}

func TestTracePayloadIsStableAndCompact(t *testing.T) {
	t.Parallel()

	payload := []byte("full payload bytes must not appear in helper output")

	got := tracePayload(payload)
	require.Equal(t, got, tracePayload(append([]byte(nil), payload...)))
	require.Equal(t, len(payload), got["len"])
	require.Equal(t, "cd4ed99709c89a07", got["sha256_64"])
	require.Len(t, got["sha256_64"], 16)

	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), string(payload))
	require.NotContains(t, string(encoded), "full payload bytes")
	require.Contains(t, string(encoded), "sha256_64")
	require.Less(t, len(encoded), len(payload)+32)
}

func TestTraceNilRecorderIsSafe(t *testing.T) {
	t.Parallel()

	require.NoError(t, recordTrace(nil, "missing", map[string]any{"value": "ignored"}))

	var trace *Trace
	require.NoError(t, trace.Record("missing", nil))
}

func TestNewOracleTraceHonorsTraceDir(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(envOracleTraceDir, dir)

	trace, tracePath, closeTrace := newOracleTrace(t, "custom-trace.jsonl")
	require.NoError(t, trace.Record("event", map[string]any{"n": 1}))
	closeTrace()

	require.Equal(t, filepath.Join(dir, "custom-trace.jsonl"), tracePath)
	body, err := os.ReadFile(tracePath)
	require.NoError(t, err)
	require.Equal(t, "{\"index\":1,\"kind\":\"event\",\"data\":{\"n\":1}}\n", string(body))
}

func decodeTraceRecords(t *testing.T, jsonl string) []TraceRecord {
	t.Helper()

	scanner := bufio.NewScanner(strings.NewReader(jsonl))
	var records []TraceRecord
	for scanner.Scan() {
		var record TraceRecord
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &record))
		records = append(records, record)
	}
	require.NoError(t, scanner.Err())
	return records
}

type shortTraceWriter struct{}

func (shortTraceWriter) Write(p []byte) (int, error) {
	return len(p) - 1, nil
}
