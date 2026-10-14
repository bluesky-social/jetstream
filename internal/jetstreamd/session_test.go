package jetstreamd

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// A repo that keeps failing verification must not draw a full resync per
// failure: the second resync of one DID within resyncInterval is refused
// before it reaches the network.
func TestSessionVerifier_ResyncBudget(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		unlimited bool
		fetches   int32
	}{
		{name: "default", unlimited: false, fetches: 1},
		{name: "unlimited", unlimited: true, fetches: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var fetches atomic.Int32
			pds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fetches.Add(1)
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
			}))
			t.Cleanup(pds.Close)

			xc := &xrpc.Client{Host: pds.URL, Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)})}
			f := &sessionFactory{
				directory:        &identity.Directory{SkipHandleVerification: true},
				syncClient:       atmossync.NewClient(atmossync.Options{Client: xc}),
				logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
				unlimitedResyncs: tc.unlimited,
			}
			v, err := f.newVerifier(atmossync.NewMemStateStore())
			require.NoError(t, err)
			t.Cleanup(func() { _ = v.Close() })

			did := atmos.DID("did:plc:r4qm2f33x7rwpnvio7ltljtv")
			_, err = v.Resync(context.Background(), did)
			_, failed := errors.AsType[*atmossync.ResyncFailedError](err)
			require.True(t, failed, "first resync reaches the PDS: %v", err)

			_, err = v.Resync(context.Background(), did)
			_, throttled := errors.AsType[*atmossync.ResyncRateLimitedError](err)
			require.Equal(t, !tc.unlimited, throttled, "second resync: %v", err)
			require.Equal(t, tc.fetches, fetches.Load())
		})
	}
}
