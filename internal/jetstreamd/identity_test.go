package jetstreamd

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/jttp"
	"github.com/stretchr/testify/require"
)

type identityRoundTripFunc func(*http.Request) (*http.Response, error)

func (f identityRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestNewIdentityResolver_ProductionClientsSeparateTrustDomains(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	plc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = fmt.Fprint(w, `{"id":"did:plc:alice","alsoKnownAs":[],"verificationMethod":[],"service":[]}`)
	}))
	defer plc.Close()

	resolver := newIdentityResolver(Options{PLCURL: plc.URL})
	require.NotSame(t, resolver.HTTPClient.Val(), resolver.PLCHTTPClient.Val())

	doc, err := resolver.ResolveDID(t.Context(), "did:plc:alice")
	require.NoError(t, err)
	require.Equal(t, "did:plc:alice", doc.ID)

	authority := strings.TrimPrefix(plc.URL, "http://")
	webDID := atmos.DID("did:web:" + strings.ReplaceAll(authority, ":", "%3A"))
	_, err = resolver.ResolveDID(t.Context(), webDID)
	require.ErrorIs(t, err, jttp.ErrBlockedByIPPolicy)
	require.EqualValues(t, 1, requests.Load(), "blocked did:web request reached the trusted PLC server")
}

func TestNewIdentityResolver_InjectedTransportRoutesBothClients(t *testing.T) {
	t.Parallel()

	var plcRequests atomic.Int32
	var webRequests atomic.Int32
	transport := identityRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		var body string
		switch req.URL.Host {
		case "plc.directory":
			plcRequests.Add(1)
			body = `{"id":"did:plc:alice","alsoKnownAs":[],"verificationMethod":[],"service":[]}`
		case "alice.test":
			webRequests.Add(1)
			body = `{"id":"did:web:alice.test","alsoKnownAs":[],"verificationMethod":[],"service":[]}`
		default:
			return nil, fmt.Errorf("unexpected identity host %q", req.URL.Host)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    req,
		}, nil
	})

	resolver := newIdentityResolver(Options{HTTPTransport: transport})
	require.NotSame(t, resolver.HTTPClient.Val(), resolver.PLCHTTPClient.Val())

	_, err := resolver.ResolveDID(t.Context(), "did:plc:alice")
	require.NoError(t, err)
	_, err = resolver.ResolveDID(t.Context(), "did:web:alice.test")
	require.NoError(t, err)
	require.EqualValues(t, 1, plcRequests.Load())
	require.EqualValues(t, 1, webRequests.Load())
}
