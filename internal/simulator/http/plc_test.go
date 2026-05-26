package http_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	simhttp "github.com/bluesky-social/jetstream-v2/internal/simulator/http"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

func TestPLC_ResolvesAccount(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)
	srv := httptest.NewServer(simhttp.NewHandler(w, "http://example.test"))
	defer srv.Close()

	a, err := w.LoadAccount(0)
	require.NoError(t, err)

	// Use a plain http.Client for tests; jttp's SSRF protection
	// blocks localhost by design.
	resolver := &identity.DefaultResolver{
		PLCURL:     gt.Some(srv.URL),
		HTTPClient: gt.Some(http.DefaultClient),
	}
	doc, err := resolver.ResolveDID(context.Background(), a.DID)
	require.NoError(t, err)
	require.Equal(t, string(a.DID), doc.ID)
	id, err := identity.IdentityFromDocument(doc)
	require.NoError(t, err)
	require.Equal(t, "http://example.test", id.PDSEndpoint())
}

func TestPLC_404OnUnknown(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 1, 0)
	srv := httptest.NewServer(simhttp.NewHandler(w, "http://example.test"))
	defer srv.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		srv.URL+"/did:plc:doesnotexist000000000a", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	_, _ = io.Copy(io.Discard, resp.Body)
}
