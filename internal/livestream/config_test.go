package livestream

import (
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/streaming"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// newTestVerifier returns a non-nil *sync.Verifier suitable only
// for satisfying validate() and exercising consumer wiring with
// non-commit events. Built against MemStateStore + an in-memory
// identity directory, so #identity / #account events flow through
// without triggering plc.directory lookups. NOT suitable for
// commit-event tests; those need a stub resolver.
func newTestVerifier(t *testing.T) *atmossync.Verifier {
	t.Helper()
	v, err := atmossync.NewVerifier(atmossync.VerifierOptions{
		Directory:  identity.NewInMemoryDirectory(),
		StateStore: atmossync.NewMemStateStore(),
		SyncClient: gt.Some(atmossync.NewClient(atmossync.Options{
			Client: &xrpc.Client{Host: "http://example.invalid"},
		})),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })
	return v
}

func TestConfig_Validate(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Ensure store is imported
	_ = (*store.Store)(nil)

	st := newTestStore(t)
	good := Config{
		SegmentsDir: t.TempDir(),
		Store:       st,
		SeqKey:      "live_segments/seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    "https://bsky.network",
		Logger:      logger,
		Verifier:    newTestVerifier(t),
	}

	t.Run("happy", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, good.validate())
	})

	cases := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"missing SegmentsDir", func(c *Config) { c.SegmentsDir = "" }, "SegmentsDir"},
		{"missing Store", func(c *Config) { c.Store = nil }, "Store"},
		{"missing SeqKey", func(c *Config) { c.SeqKey = "" }, "SeqKey"},
		{"missing CursorKey", func(c *Config) { c.CursorKey = "" }, "CursorKey"},
		{"missing RelayURL", func(c *Config) { c.RelayURL = "" }, "RelayURL"},
		{"missing Logger", func(c *Config) { c.Logger = nil }, "Logger"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := good
			c.Store = st // share, since *store.Store is fine across tests
			tc.mutate(&c)
			err := c.validate()
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrInvalidConfig))
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestOpen_RejectsBadConfig(t *testing.T) {
	t.Parallel()
	_, err := Open(Config{})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidConfig))
}

// TestOpen_UsesConfiguredSeqKey verifies that Open actually wires
// cfg.SeqKey down to the underlying ingest.Writer. We do that by
// choosing a deliberately unusual SeqKey, opening, appending one
// event, closing, and confirming the seq counter was persisted under
// THAT key (not the default "seq/next"). A regression that ignores
// SeqKey would persist under the wrong key and this test would fail.
func TestOpen_UsesConfiguredSeqKey(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	dir := filepath.Join(t.TempDir(), "live_segments")
	const customKey = "test/livestream_only/seq"

	c, err := Open(Config{
		SegmentsDir: dir,
		Store:       st,
		SeqKey:      customKey,
		CursorKey:   "relay/cursor",
		RelayURL:    "https://bsky.network",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:    newTestVerifier(t),
	})
	require.NoError(t, err)

	// Force the writer to allocate at least one seq so the persisted
	// counter is non-zero.
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{
		{Seq: 1, Identity: &comatproto.SyncSubscribeRepos_Identity{
			DID: "did:plc:aaa", Time: "2026-05-21T00:00:00Z",
		}},
	}))
	require.NoError(t, c.Close())

	// Custom key persisted; default key NOT touched.
	val, closer, err := st.Get([]byte(customKey))
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()
	require.Equal(t, uint64(1), binary.LittleEndian.Uint64(val),
		"custom SeqKey must hold the writer's persisted nextSeq")

	_, _, err = st.Get([]byte("seq/next"))
	require.ErrorIs(t, err, pebble.ErrNotFound,
		"default SeqKey must NOT be written when a custom one is configured")

	// Sanity: segment dir actually got a segment file.
	matches, err := filepath.Glob(filepath.Join(dir, "seg_*.jss"))
	require.NoError(t, err)
	require.Len(t, matches, 1)
}

// TestClose_Idempotent_AndPersistsCursor pins both:
//   - calling Close twice is safe (idempotent)
//   - the second-to-last lastUpstream value is durably persisted
//     to relay/cursor on Close even when no block flush ever fired
//     (e.g. a low-volume restart that buffers <1 block of events).
//
// Without the explicit cursor save on Close, a clean shutdown could
// regress relay/cursor to whatever value the OnAfterFlush hook last
// wrote — which on a slow stream might be 0 (never flushed) and
// would replay the entire bootstrap window on next start.
func TestClose_Idempotent_AndPersistsCursor(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	c, err := Open(Config{
		SegmentsDir: filepath.Join(t.TempDir(), "live_segments"),
		Store:       st,
		SeqKey:      "live_segments/seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    "https://bsky.network",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:    newTestVerifier(t),
		// Big enough that no block ever flushes during the test.
		MaxEventsPerBlock: 1024,
	})
	require.NoError(t, err)

	// Buffer one event but DON'T cross a block boundary — so the
	// only path that can persist the cursor is Close.
	require.NoError(t, c.processBatch(t.Context(), []streaming.Event{
		{Seq: 42, Identity: &comatproto.SyncSubscribeRepos_Identity{
			DID: "did:plc:aaa", Time: "2026-05-21T00:00:00Z",
		}},
	}))

	// Pre-Close, cursor must NOT yet be persisted (no flush happened).
	_, _, err = st.Get([]byte("relay/cursor"))
	require.ErrorIs(t, err, pebble.ErrNotFound,
		"no flush has fired yet, so OnAfterFlush should not have run")

	require.NoError(t, c.Close())
	require.NoError(t, c.Close(), "second Close must be a no-op")

	// Close persisted lastUpstream to relay/cursor.
	persisted, err := LoadUpstreamCursor(st, "relay/cursor")
	require.NoError(t, err)
	require.Equal(t, int64(42), persisted,
		"Close must durably write lastUpstream even when no block flush fired")
}

// TestConfig_Validate_RequiresVerifier pins that a livestream.Config
// with no Verifier is rejected. The package's purpose is now Sync 1.1.
func TestConfig_Validate_RequiresVerifier(t *testing.T) {
	t.Parallel()
	cfg := Config{
		SegmentsDir: t.TempDir(),
		Store:       newTestStore(t),
		SeqKey:      "live_segments/seq/next",
		CursorKey:   "relay/cursor",
		RelayURL:    "https://bsky.network",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		// Verifier deliberately unset
	}
	err := cfg.validate()
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "Verifier")
}
