package subscribe

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// TestHandlerV2_BootIDHeader pins the failover contract: clients compare the
// boot ID across reconnects to decide whether a seq cursor is still valid, so
// it must be on the upgrade AND on pre-upgrade rejections (a CursorTooOld from
// a different process must not trigger a re-backfill).
func TestHandlerV2_BootIDHeader(t *testing.T) {
	t.Parallel()
	boot := NewBootID()
	require.Len(t, boot, 32)
	require.NotEqual(t, boot, NewBootID(), "boot IDs are random per process")

	st := newSteadyStateStore(t)
	b, _ := newReadLogTail(t, 1<<20, noCold)
	srv := httptest.NewServer(NewHandler(Subscription{
		Tail:   b,
		Store:  st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		V2:     true,
		BootID: boot,
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := dialV2(t, ctx, srv, "", websocket.CompressionDisabled)
	require.NoError(t, err)
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
	_ = conn.Close(websocket.StatusNormalClosure, "test done")
	require.Equal(t, boot, resp.Header.Get(BootIDHeader), "101 must carry the boot ID")

	_, resp, err = dialV2(t, ctx, srv, "?zstdDictionary=999999", websocket.CompressionDisabled)
	require.Error(t, err)
	require.NotNil(t, resp)
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, boot, resp.Header.Get(BootIDHeader), "pre-upgrade 400 must carry the boot ID")
}
