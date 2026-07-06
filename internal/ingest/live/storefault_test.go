package live

import (
	"errors"
	"testing"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

// TestConsumer_SaveCursorFailsLoudOnStoreFault pins the fail-loud contract for
// the relay-cursor commit boundary (issue #30 fault point: "relay cursor
// writes"). The production writer durable-batch path stages the same cursor
// write with verifier sync state under one Sync commit. A failed commit must
// surface as an error so the consumer Run loop tears the process down, rather
// than reporting a durable cursor advance that never reached disk — which
// across a restart would resume the firehose from a seq that was never
// persisted, silently skipping events.
//
// The fault targets the relay/cursor batch commit; the assertion is that
// saveCursorAndSyncState exercises the same staging helper and must propagate
// the injected error AND leave the cursor non-durable (no silent advance).
func TestConsumer_SaveCursorFailsLoudOnStoreFault(t *testing.T) {
	t.Parallel()

	const cursorKey = "relay/cursor"
	injected := errors.New("injected: relay cursor commit failed")
	fault := &store.KeyPrefixFault{
		Prefix:  []byte(cursorKey),
		Op:      store.WriteOpBatchCommit,
		Ordinal: 1,
		Err:     injected,
	}
	st, err := store.Open(t.TempDir(), nil, store.WithFaultInjector(fault))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	// Minimal consumer: saveCursorAndSyncState only needs Store + CursorKey
	// (SyncStateStore nil → cursor-only batch, exactly the relay/cursor write).
	c := &Consumer{cfg: Config{Store: st, CursorKey: cursorKey}}

	require.ErrorIs(t, c.saveCursorAndSyncState(42), injected,
		"relay cursor commit failure must surface loud, not be swallowed")

	// No silent advance: the cursor was never durably written.
	got, err := LoadUpstreamCursor(st, cursorKey)
	require.NoError(t, err)
	require.Equal(t, int64(0), got, "relay cursor must not advance when its commit failed")
}
