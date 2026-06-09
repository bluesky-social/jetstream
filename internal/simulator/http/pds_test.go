package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	simhttp "github.com/bluesky-social/jetstream-v2/internal/simulator/http"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/jcalabro/jttp"
	"github.com/stretchr/testify/require"
)

func TestPDS_GetRepoRoundTrips(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 2)
	srv := httptest.NewServer(simhttp.NewHandler(w, "http://example.test"))
	defer srv.Close()

	a, err := w.LoadAccount(0)
	require.NoError(t, err)

	xc := &xrpc.Client{
		Host:       srv.URL,
		HTTPClient: gt.Some(jttp.New()),
	}
	sc := sync.NewClient(sync.Options{Client: xc})

	body, err := sc.GetRepoStream(context.Background(), a.DID, "")
	require.NoError(t, err)
	defer func() { _ = body.Close() }()

	// LoadFromCAR validates the CAR's structural integrity and the
	// commit decodes correctly. Signature validation against the
	// PLC-published key is exercised in Task 14's listRepos test.
	rp, commit, err := loadFromCAR(body)
	require.NoError(t, err)
	require.Equal(t, a.DID, rp.DID)
	require.NotEmpty(t, commit.Sig)
}

func TestPDS_GetRepoServesPersistedHead(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 2)
	srv := httptest.NewServer(simhttp.NewHandler(w, "http://example.test"))
	defer srv.Close()

	a, err := w.LoadAccount(0)
	require.NoError(t, err)
	page, _, err := w.ListReposPage(0, 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, a.DID, page[0].DID)

	xc := &xrpc.Client{
		Host:       srv.URL,
		HTTPClient: gt.Some(jttp.New()),
	}
	sc := sync.NewClient(sync.Options{Client: xc})

	body, err := sc.GetRepoStream(context.Background(), a.DID, "")
	require.NoError(t, err)
	defer func() { _ = body.Close() }()

	_, commit, err := loadFromCAR(body)
	require.NoError(t, err)
	commitData, err := commit.EncodeCBOR()
	require.NoError(t, err)

	require.Equal(t, page[0].Rev, commit.Rev)
	require.Equal(t, page[0].Head, cbor.ComputeCID(cbor.CodecDagCBOR, commitData).String())
}

// TestPDS_GetRepoFaultHandlerServesTransient503ThenCAR pins the
// SIMULATOR HANDLER's fault-injection mechanic: a scheduled getRepo fault
// returns the configured 503 (and increments the fired counter) on the
// first request, then serves the real CAR once the budget is exhausted.
// It deliberately disables client retries (MaxAttempts=1) and drives
// GetRepoStream by hand so it isolates the handler contract — it does NOT
// exercise jetstream's backfill retry loop. That end-to-end recovery is
// covered by backfill.TestRun_TransientGetRepoFailureThenRecovers and by
// the swarm-mode oracle lifecycle test.
func TestPDS_GetRepoFaultHandlerServesTransient503ThenCAR(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 2)
	a, err := w.LoadAccount(0)
	require.NoError(t, err)

	faults := simhttp.NewFaultPlan()
	faults.AddGetRepoHTTPFailures(string(a.DID), http.StatusServiceUnavailable, 1)

	srv := httptest.NewServer(simhttp.NewHandlerWithOptions(w, "http://example.test", simhttp.HandlerOptions{
		Faults: faults,
	}))
	defer srv.Close()

	xc := &xrpc.Client{
		Host:       srv.URL,
		HTTPClient: gt.Some(http.DefaultClient),
		Retry:      gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
	}
	sc := sync.NewClient(sync.Options{Client: xc})

	body, err := sc.GetRepoStream(context.Background(), a.DID, "")
	require.Error(t, err)
	require.Nil(t, body)
	require.Equal(t, 1, faults.GetRepoHTTPFailuresFired(string(a.DID)))

	body, err = sc.GetRepoStream(context.Background(), a.DID, "")
	require.NoError(t, err)
	defer func() { _ = body.Close() }()

	rp, commit, err := loadFromCAR(body)
	require.NoError(t, err)
	require.Equal(t, a.DID, rp.DID)
	require.NotEmpty(t, commit.Sig)
	require.Equal(t, 1, faults.GetRepoHTTPFailuresFired(string(a.DID)))
}
