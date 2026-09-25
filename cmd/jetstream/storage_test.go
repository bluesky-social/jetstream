package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/xrpcapi"
	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

// pgSecret is the password planted in every connection string below. Ground
// rule 7: it must never reach a log line, an error, /status, /metrics, or
// --help.
const pgSecret = "hunter2-pg-secret"

const secretPGURL = "postgres://jetstream:" + pgSecret + "@127.0.0.1:15432/jetstream?sslmode=disable"

// lockedBuffer is a log sink shared with the runtime's goroutines.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureServeOptions swaps serve's action for one that only resolves
// options.
func captureServeOptions(app *cli.Command, opts *jetstreamd.Options) {
	for _, cmd := range app.Commands {
		if cmd.Name == "serve" {
			cmd.Action = func(_ context.Context, cmd *cli.Command) error {
				var err error
				*opts, err = serveOptionsFromCommand(cmd)
				return err
			}
		}
	}
}

// disaggregatedArgs is a complete, valid disaggregated serve command line.
func disaggregatedArgs(extra ...string) []string {
	return append([]string{
		"jetstream", "--log-level=debug", "serve",
		"--storage=disaggregated",
		"--pg-url=" + secretPGURL,
		"--s3-region=us-east-1",
		"--s3-bucket=jetstream",
		"--compaction-interval=0",
	}, extra...)
}

// Not parallel: it sets process environment (t.Setenv in a loop, which the
// linter does not recognise).
//
// nolint:paralleltest
func TestServeOptionsFromCLI_StorageEnv(t *testing.T) {
	withClearedEnv(t)

	app := newTestApp()
	var opts jetstreamd.Options
	captureServeOptions(app, &opts)

	for k, v := range map[string]string{
		"JETSTREAM_STORAGE":                       "disaggregated",
		"JETSTREAM_PG_URL":                        secretPGURL,
		"JETSTREAM_PG_MAX_CONNS":                  "7",
		"JETSTREAM_S3_ENDPOINT":                   "http://127.0.0.1:18333",
		"JETSTREAM_S3_REGION":                     "us-east-1",
		"JETSTREAM_S3_BUCKET":                     "jetstream",
		"JETSTREAM_S3_PREFIX":                     "pfx/",
		"JETSTREAM_S3_PATH_STYLE":                 "true",
		"JETSTREAM_S3_UPLOAD_CONCURRENCY":         "3",
		"JETSTREAM_S3_READ_CONCURRENCY":           "5",
		"JETSTREAM_S3_RETRY_TIMEOUT":              "11s",
		"JETSTREAM_LEADER_LEASE":                  "4s",
		"JETSTREAM_LEADER_RENEW_INTERVAL":         "2s",
		"JETSTREAM_LEADER_ACQUIRE_INTERVAL":       "300ms",
		"JETSTREAM_HOT_BATCH_MAX_AGE":             "20ms",
		"JETSTREAM_BLOCK_MAX_AGE":                 "40s",
		"JETSTREAM_HOT_INLINE_BYTES_PER_SEC":      "1000",
		"JETSTREAM_HOT_BULK_PENDING_BYTES":        "2000",
		"JETSTREAM_HOT_PENDING_BYTES":             "3000",
		"JETSTREAM_HOT_MAX_UNFOLDED_EVENTS":       "4000",
		"JETSTREAM_CATALOG_POLL_INTERVAL":         "100ms",
		"JETSTREAM_MAX_VIEW_AGE":                  "20s",
		"JETSTREAM_MAX_ARCHIVE_RESPONSE_DURATION": "2h",
		"JETSTREAM_GC_INTERVAL":                   "5m",
		"JETSTREAM_GC_DELAY":                      "7h",
		"JETSTREAM_GC_ORPHAN_AGE":                 "90m",
		"JETSTREAM_OBJECT_CACHE_BYTES":            "5000",
		"JETSTREAM_COMPACTION_MEMORY_BYTES":       "6000",
		"JETSTREAM_COMPACTION_INTERVAL":           "0",
	} {
		t.Setenv(k, v)
	}

	require.NoError(t, app.Run(t.Context(), []string{"jetstream", "serve"}))
	require.Empty(t, opts.DataDir, "disaggregated mode must not inherit --data-dir's default")
	require.Equal(t, jetstreamd.StorageConfig{
		Mode: jetstreamd.StorageDisaggregated,
		PG:   jetstreamd.PGConfig{URL: secretPGURL, MaxConns: 7},
		S3: jetstreamd.S3Config{
			Endpoint: "http://127.0.0.1:18333", Region: "us-east-1", Bucket: "jetstream", Prefix: "pfx/",
			PathStyle: true, UploadConcurrency: 3, ReadConcurrency: 5, RetryTimeout: 11 * time.Second,
		},
		Leader: jetstreamd.LeaderConfig{Lease: 4 * time.Second, RenewInterval: 2 * time.Second, AcquireInterval: 300 * time.Millisecond},
		Hot: jetstreamd.HotConfig{
			BatchMaxAge: 20 * time.Millisecond, InlineBytesPerSec: 1000, BulkPendingBytes: 2000,
			PendingBytes: 3000, MaxUnfoldedEvents: 4000,
		},
		BlockMaxAge:                40 * time.Second,
		CatalogPollInterval:        100 * time.Millisecond,
		MaxViewAge:                 20 * time.Second,
		MaxArchiveResponseDuration: 2 * time.Hour,
		GC:                         jetstreamd.GCConfig{Interval: 5 * time.Minute, Delay: 7 * time.Hour, OrphanAge: 90 * time.Minute},
		ObjectCacheBytes:           5000,
		CompactionMemoryBytes:      6000,
	}, opts.Storage)
	require.NoError(t, opts.Storage.Validate(opts))
}

// Finding 9: --data-dir has a default, so the refusal keys on "explicitly
// set" (flag or env), not on the value.
func TestServeOptionsFromCLI_DisaggregatedRefusesExplicitDataDir(t *testing.T) {
	t.Parallel()

	app := newTestApp()
	var opts jetstreamd.Options
	captureServeOptions(app, &opts)
	err := app.Run(t.Context(), disaggregatedArgs("--data-dir=./data"))
	require.ErrorContains(t, err, "JETSTREAM_DATA_DIR")
	require.NotContains(t, err.Error(), pgSecret)
}

func TestServeOptionsFromCLI_DisaggregatedRefusesDataDirEnv(t *testing.T) {
	withClearedEnv(t)
	t.Setenv("JETSTREAM_DATA_DIR", "./data-sim")

	app := newTestApp()
	var opts jetstreamd.Options
	captureServeOptions(app, &opts)
	err := app.Run(t.Context(), disaggregatedArgs())
	require.ErrorContains(t, err, "JETSTREAM_DATA_DIR")
}

// Not parallel: `just` exports .env (JETSTREAM_DATA_DIR among others), which
// the flags would read. The subtests run in parallel under the cleared env.
//
// nolint:paralleltest
func TestServe_StorageValidation(t *testing.T) {
	withClearedEnv(t)

	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"compaction on (D5)": {
			args: []string{"jetstream", "serve", "--storage=disaggregated", "--pg-url=" + secretPGURL, "--s3-region=r", "--s3-bucket=b"},
			want: "JETSTREAM_COMPACTION_INTERVAL=0",
		},
		"unknown mode":    {args: []string{"jetstream", "serve", "--storage=cloud"}, want: "JETSTREAM_STORAGE"},
		"missing pg url":  {args: []string{"jetstream", "serve", "--storage=disaggregated", "--compaction-interval=0"}, want: "JETSTREAM_PG_URL is required"},
		"missing bucket":  {args: disaggregatedArgs("--s3-bucket="), want: "JETSTREAM_S3_BUCKET is required"},
		"missing region":  {args: disaggregatedArgs("--s3-region="), want: "JETSTREAM_S3_REGION is required"},
		"renew >= lease":  {args: disaggregatedArgs("--leader-renew-interval=3s"), want: "JETSTREAM_LEADER_RENEW_INTERVAL"},
		"poll >= view":    {args: disaggregatedArgs("--catalog-poll-interval=30s"), want: "JETSTREAM_CATALOG_POLL_INTERVAL"},
		"zero budget":     {args: disaggregatedArgs("--object-cache-bytes=0"), want: "JETSTREAM_OBJECT_CACHE_BYTES must be > 0"},
		"zero gc delay":   {args: disaggregatedArgs("--gc-delay=0"), want: "JETSTREAM_GC_DELAY must be > 0"},
		"zero pool":       {args: disaggregatedArgs("--pg-max-conns=0"), want: "JETSTREAM_PG_MAX_CONNS must be > 0"},
		"not wired yet":   {args: disaggregatedArgs(), want: "not available"},
		"negative hot":    {args: disaggregatedArgs("--hot-pending-bytes=-1"), want: "JETSTREAM_HOT_PENDING_BYTES must be > 0"},
		"zero block age":  {args: disaggregatedArgs("--block-max-age=0"), want: "JETSTREAM_BLOCK_MAX_AGE must be > 0"},
		"zero view age":   {args: disaggregatedArgs("--max-view-age=0"), want: "JETSTREAM_MAX_VIEW_AGE must be > 0"},
		"zero s3 retries": {args: disaggregatedArgs("--s3-retry-timeout=0"), want: "JETSTREAM_S3_RETRY_TIMEOUT must be > 0"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			app := newTestApp()
			var logs lockedBuffer
			app.ErrWriter = &logs
			err := app.Run(t.Context(), tc.args)
			require.ErrorContains(t, err, tc.want)
			require.NotContains(t, err.Error(), pgSecret)
			require.NotContains(t, logs.String(), pgSecret)
		})
	}
}

// Ground rule 7, disaggregated path: the startup logs carry the whole
// storage config (so the operator can see what the pod will use), with the
// password redacted.
// Not parallel: `just` exports .env (JETSTREAM_DATA_DIR among others), which
// the flags would read. The subtests run in parallel under the cleared env.
//
// nolint:paralleltest
func TestServe_DisaggregatedStartupLogsRedactPGPassword(t *testing.T) {
	withClearedEnv(t)

	for _, format := range []string{"json", "text"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			app := newTestApp()
			var logs lockedBuffer
			app.ErrWriter = &logs
			args := disaggregatedArgs()
			args = append(args[:1], append([]string{"--log-format=" + format}, args[1:]...)...)
			err := app.Run(t.Context(), args)
			require.ErrorContains(t, err, "not available")
			require.NotContains(t, err.Error(), pgSecret)

			out := logs.String()
			require.Contains(t, out, "storage config")
			require.Contains(t, out, "127.0.0.1:15432", "the redacted URL keeps the host")
			require.Contains(t, out, "xxxxx")
			require.NotContains(t, out, pgSecret)
		})
	}
}

// Ground rule 7, --help: an env-provided URL never reaches the help text.
func TestServe_HelpOmitsPGPassword(t *testing.T) {
	withClearedEnv(t)
	t.Setenv("JETSTREAM_PG_URL", secretPGURL)

	for _, args := range [][]string{
		{"jetstream", "serve", "--help"},
		{"jetstream", "serve", "--pg-url=" + secretPGURL, "--help"},
		{"jetstream", "--help"},
	} {
		app := newTestApp()
		var out, errOut lockedBuffer
		app.Writer, app.ErrWriter = &out, &errOut
		require.NoError(t, app.Run(t.Context(), args))
		require.Contains(t, out.String()+errOut.String(), "JETSTREAM_", "help was printed")
		require.NotContains(t, out.String(), pgSecret)
		require.NotContains(t, errOut.String(), pgSecret)
	}
}

// Ground rule 7, runtime surfaces: a pod with a PG URL configured never
// exposes the password on its startup logs, /status, or /metrics. The pod
// runs local mode here because disaggregated mode is not wired yet (S2.16
// should switch this test to disaggregated mode); local mode still logs
// the storage config, in the warning that its settings are ignored.
func TestServe_PGPasswordAbsentFromStatusAndLogs(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/com.atproto.sync.subscribeRepos") {
			conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
			if err != nil {
				return
			}
			defer func() { _ = conn.CloseNow() }()
			<-r.Context().Done()
			return
		}
		_ = json.NewEncoder(w).Encode(struct {
			Repos []any `json:"repos"`
		}{})
	}))
	t.Cleanup(relay.Close)

	debugLn, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	storage := jetstreamd.DefaultStorageConfig()
	storage.PG.URL = secretPGURL
	storage.S3.Bucket = "jetstream"
	var logs lockedBuffer
	rt, err := jetstreamd.Build(ctx, jetstreamd.Options{
		PublicAddr:                     "127.0.0.1:0",
		DebugListener:                  debugLn,
		DataDir:                        t.TempDir(),
		Storage:                        storage,
		RelayURL:                       relay.URL,
		OTelServiceName:                "jetstream-test",
		LogLevel:                       "debug",
		LogFormat:                      "json",
		LogOutput:                      &logs,
		ShutdownTimeout:                5 * time.Second,
		ClientDrainTimeout:             time.Second,
		CursorLookback:                 36 * time.Hour,
		PlanMaxDIDs:                    xrpcapi.DefaultPlanMaxDIDs,
		PlanMaxCollections:             xrpcapi.DefaultPlanMaxCollections,
		PlanMaxEntries:                 xrpcapi.DefaultPlanMaxEntries,
		PlanWholeSegmentThreshold:      xrpcapi.DefaultPlanWholeSegmentThreshold,
		SubscribeReadLogRetentionBytes: 1 << 20,
		SubscribeBlockCacheBytes:       1 << 20,
		SubscribeReadBatch:             128,
		SubscribeSlowWindow:            time.Second,
		SubscribeSlowMinRate:           5,
		CursorBlockIndexCacheSize:      32,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		require.NoError(t, rt.Close(closeCtx))
	})
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	publicAddr := waitRuntimePublicAddr(t, rt, done)
	client := &http.Client{Timeout: 2 * time.Second}
	get := func(url string) string {
		deadline := time.Now().Add(5 * time.Second)
		for {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			require.NoError(t, err)
			resp, err := client.Do(req)
			if err == nil {
				body, readErr := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if readErr == nil && resp.StatusCode == http.StatusOK {
					return string(body)
				}
			}
			require.True(t, time.Now().Before(deadline), "%s never returned 200", url)
			select {
			case err := <-done:
				t.Fatalf("serve exited early: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
		}
	}

	status := get("http://" + publicAddr + "/status")
	require.Contains(t, status, "Phase")
	require.NotContains(t, status, pgSecret)
	require.NotContains(t, get("http://"+debugLn.Addr().String()+"/metrics"), pgSecret)

	out := logs.String()
	require.Contains(t, out, "disaggregated storage settings are ignored", "the storage config reached the log")
	require.Contains(t, out, "xxxxx")
	require.NotContains(t, out, pgSecret)

	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("serve exited with unexpected error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not exit")
	}
	require.NotContains(t, logs.String(), pgSecret)
}
