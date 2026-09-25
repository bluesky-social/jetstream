package oracle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	localcatalog "github.com/bluesky-social/jetstream/internal/catalog/local"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/internal/seqspace"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const (
	envSeqLeaseChild  = "JETSTREAM_ORACLE_SEQ_LEASE_CHILD"
	envSeqLeaseDir    = "JETSTREAM_ORACLE_SEQ_LEASE_DIR"
	envSeqLeaseMarker = "JETSTREAM_ORACLE_SEQ_LEASE_MARKER"
)

type observedLeaseEvent struct {
	Seq uint64 `json:"seq"`
	DID string `json:"did"`
}

// TestOracle_ObservedSeqIsNotReusedAfterSIGKILL is the process-boundary proof
// for issue #345. A real websocket subscriber observes an event that exists
// only in the writer's pending block; the parent SIGKILLs that process, opens
// the same data dir, and proves a different event cannot receive the observed
// seq. Without the write-ahead lease this deterministically reuses seq 1.
func TestOracle_ObservedSeqIsNotReusedAfterSIGKILL(t *testing.T) {
	t.Parallel()
	if os.Getenv(envSeqLeaseChild) == "1" {
		runSeqLeaseSubscriberChild()
		return
	}
	if testing.Short() {
		t.Skip("skipping real-process seq-lease oracle under -short")
	}

	dataDir := t.TempDir()
	marker := filepath.Join(t.TempDir(), "observed.json")
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestOracle_ObservedSeqIsNotReusedAfterSIGKILL$")
	cmd.Env = append(os.Environ(),
		envSeqLeaseChild+"=1",
		envSeqLeaseDir+"="+dataDir,
		envSeqLeaseMarker+"="+marker,
	)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		if cmd.ProcessState == nil || !cmd.ProcessState.Exited() {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	})

	deadline := time.Now().Add(15 * time.Second)
	var observed observedLeaseEvent
	for {
		data, err := os.ReadFile(marker)
		if err == nil {
			require.NoError(t, json.Unmarshal(data, &observed))
			break
		}
		if !os.IsNotExist(err) {
			require.NoError(t, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for child subscriber observation\n%s", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, observedLeaseEvent{Seq: 1, DID: "did:plc:observed-before-crash"}, observed)

	require.NoError(t, cmd.Process.Signal(syscall.SIGKILL))
	err := cmd.Wait()
	require.True(t, wasSIGKILL(err), "child should die by SIGKILL: %v\n%s", err, output.String())

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := pebblestore.Open(dataDir, pebblestore.NewMetrics(prometheus.NewRegistry()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:              filepath.Join(dataDir, "segments"),
		Store:                    st,
		Logger:                   logger,
		MaxEventsPerBlock:        4,
		ReserveClientVisibleSeqs: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.Equal(t, uint64(5), w.NextSeq())
	require.Equal(t, []seqspace.Gap{{Start: 1, End: 5}}, w.SeqGaps().Ranges())

	replacement := leaseOracleEvent("did:plc:different-after-restart")
	require.NoError(t, w.Append(t.Context(), &replacement))
	require.Equal(t, uint64(5), replacement.Seq)
	require.NotEqual(t, observed.Seq, replacement.Seq, "a client-observed seq must never be reassigned")
}

func runSeqLeaseSubscriberChild() {
	dataDir := os.Getenv(envSeqLeaseDir)
	marker := os.Getenv(envSeqLeaseMarker)
	fail := func(err error) {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := pebblestore.Open(dataDir, pebblestore.NewMetrics(prometheus.NewRegistry()))
	if err != nil {
		fail(err)
	}
	segDir := filepath.Join(dataDir, "segments")
	cat, err := localcatalog.New(localcatalog.Config{Dirs: map[catalog.Namespace]string{catalog.Main: segDir}})
	if err != nil {
		fail(err)
	}
	if err := cat.Refresh(context.Background()); err != nil {
		fail(err)
	}
	w, err := ingest.Open(ingest.Config{
		SegmentsDir:              segDir,
		Store:                    st,
		Logger:                   logger,
		MaxEventsPerBlock:        4,
		ReserveClientVisibleSeqs: true,
		Catalog:                  cat,
	})
	if err != nil {
		fail(err)
	}
	if err := lifecycle.WritePhase(context.Background(), st, lifecycle.PhaseSteadyState, time.Now().UTC()); err != nil {
		fail(err)
	}

	var writerRef atomic.Pointer[ingest.Writer]
	writerRef.Store(w)
	metrics := subscribe.NewMetrics(prometheus.NewRegistry())
	cold := subscribe.NewColdReader(subscribe.ColdReaderConfig{
		Catalog: cat, Fetcher: cat.Fetcher(), WriterRef: &writerRef, Metrics: metrics,
	})
	tail, err := subscribe.New(subscribe.Config{Logger: logger, Metrics: metrics}, cold.Read, w.NextSeq)
	if err != nil {
		fail(err)
	}
	tail.SetReadLogSource(func() *ingest.ReadableLog { return w.ReadLog() })
	handler := subscribe.NewHandler(subscribe.Subscription{
		Tail: tail, Store: st, Writer: w, Logger: logger, Metrics: metrics, V2: true,
	})
	srv := httptest.NewServer(http.HandlerFunc(handler.ServeHTTP))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, "ws"+srv.URL[len("http"):], nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		fail(err)
	}
	ev := leaseOracleEvent("did:plc:observed-before-crash")
	if err := w.Append(ctx, &ev); err != nil {
		fail(err)
	}
	_, payload, err := conn.Read(ctx)
	if err != nil {
		fail(err)
	}
	var frame struct {
		Payload observedLeaseEvent `json:"payload"`
	}
	if err := json.Unmarshal(payload, &frame); err != nil {
		fail(err)
	}
	observed := frame.Payload
	tmp := marker + ".tmp"
	data, err := json.Marshal(observed)
	if err != nil {
		fail(err)
	}
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		fail(err)
	}
	if err := os.Rename(tmp, marker); err != nil {
		fail(err)
	}
	select {}
}

func leaseOracleEvent(did string) segment.Event {
	return segment.Event{
		WitnessedAt: time.Now().UnixMicro(),
		Kind:        segment.KindCreate,
		DID:         did,
		Collection:  "app.bsky.feed.post",
		Rkey:        "issue-345",
		Rev:         "3lease",
		Payload:     []byte{0xa0},
	}
}
