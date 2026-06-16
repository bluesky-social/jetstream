package oracle

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const envOracleTraceDir = "JETSTREAM_ORACLE_TRACE_DIR"

func newOracleTrace(t *testing.T, name string) (*Trace, string, func()) {
	t.Helper()

	tracePath, traceFile, closeTrace := newOracleArtifactFile(t, name)
	digest := sha256.New()
	t.Logf("oracle trace: %s", tracePath)
	if os.Getenv(envOracleTraceDir) == "" {
		t.Logf("oracle trace uses testing.T.ArtifactDir; pass -artifacts or set %s to keep it after test cleanup", envOracleTraceDir)
	}

	return NewTrace(io.MultiWriter(traceFile, digest)), tracePath, func() {
		t.Helper()
		closeTrace()
		sum := digest.Sum(nil)
		if t.Failed() {
			t.Logf("oracle trace failure artifact: path=%s sha256_64=%s", tracePath, hex.EncodeToString(sum[:8]))
			return
		}
		t.Logf("oracle trace sha256_64: %s", hex.EncodeToString(sum[:8]))
	}
}

func newOracleArtifactFile(t *testing.T, name string) (string, *os.File, func()) {
	t.Helper()

	path := filepath.Join(oracleArtifactDir(t), name)
	file, err := os.Create(path)
	require.NoError(t, err)

	return path, file, func() {
		t.Helper()
		require.NoError(t, file.Close())
	}
}

func oracleArtifactDir(t *testing.T) string {
	t.Helper()

	traceDir := os.Getenv(envOracleTraceDir)
	if traceDir == "" {
		return t.ArtifactDir()
	}
	require.NoError(t, os.MkdirAll(traceDir, 0o755))
	return traceDir
}

func assertTraceContainsKinds(t *testing.T, tracePath string, requiredKinds ...string) {
	t.Helper()

	file, err := os.Open(tracePath)
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()

	seen := make(map[string]bool)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record TraceRecord
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &record))
		seen[record.Kind] = true
	}
	require.NoError(t, scanner.Err())

	for _, kind := range requiredKinds {
		require.Truef(t, seen[kind], "oracle trace %s missing required kind %q", tracePath, kind)
	}
}
