package http_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
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

func TestPLCFaults_ModelDIDDocWeirdness(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t, 5, 1)
	a, err := w.LoadAccount(0)
	require.NoError(t, err)

	for _, tc := range []struct {
		name       string
		mode       simhttp.PLCFaultMode
		assertDoc  func(*testing.T, rawDIDDoc)
		wantStatus int
	}{
		{
			name: "missing_pds",
			mode: simhttp.PLCFaultMissingPDSEndpoint,
			assertDoc: func(t *testing.T, doc rawDIDDoc) {
				t.Helper()
				require.Empty(t, doc.Service)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "malformed_handle",
			mode: simhttp.PLCFaultMalformedHandle,
			assertDoc: func(t *testing.T, doc rawDIDDoc) {
				t.Helper()
				require.Equal(t, []string{"not an at-uri"}, doc.AlsoKnownAs)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "malformed_pds",
			mode: simhttp.PLCFaultMalformedPDSEndpoint,
			assertDoc: func(t *testing.T, doc rawDIDDoc) {
				t.Helper()
				require.Len(t, doc.Service, 1)
				require.Equal(t, "://not-a-url", doc.Service[0].ServiceEndpoint)
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "resolution_failure",
			mode:       simhttp.PLCFaultResolutionFailure,
			wantStatus: http.StatusServiceUnavailable,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			faults := simhttp.NewFaultPlan()
			faults.AddPLCFault(string(a.DID), tc.mode, 1)
			srv := httptest.NewServer(simhttp.NewHandlerWithOptions(w, "http://example.test", simhttp.HandlerOptions{
				Faults: faults,
			}))
			defer srv.Close()

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/"+string(a.DID), nil)
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			require.Equal(t, tc.wantStatus, resp.StatusCode)
			if tc.assertDoc != nil {
				var doc rawDIDDoc
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&doc))
				tc.assertDoc(t, doc)
			} else {
				_, _ = io.Copy(io.Discard, resp.Body)
			}
			require.Equal(t, 1, faults.PLCFaultsFired(string(a.DID)))
		})
	}
}

type rawDIDDoc struct {
	ID          string   `json:"id"`
	AlsoKnownAs []string `json:"alsoKnownAs"`
	Service     []struct {
		ID              string `json:"id"`
		Type            string `json:"type"`
		ServiceEndpoint string `json:"serviceEndpoint"`
	} `json:"service"`
}
