package http_test

import (
	"context"
	"net/http/httptest"
	"testing"

	simhttp "github.com/bluesky-social/jetstream-v2/internal/simulator/http"
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
