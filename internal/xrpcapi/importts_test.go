package xrpcapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream/internal/importer"
	"github.com/stretchr/testify/require"
)

// fakeImportManager is a scriptable ImportManager for handler tests.
type fakeImportManager struct {
	submitID  string
	submitErr error
	lastPath  string
	statusRec importer.Record
	statusErr error
	current   *importer.Record
}

func (f *fakeImportManager) Submit(_ context.Context, path string) (string, error) {
	f.lastPath = path
	if f.submitErr != nil {
		return "", f.submitErr
	}
	return f.submitID, nil
}

func (f *fakeImportManager) Status(string) (importer.Record, error) {
	if f.statusErr != nil {
		return importer.Record{}, f.statusErr
	}
	return f.statusRec, nil
}

func (f *fakeImportManager) Current() (importer.Record, bool) {
	if f.current == nil {
		return importer.Record{}, false
	}
	return *f.current, true
}

// importTestServer builds an xrpc server with the import endpoints wired to
// mgr under token, and returns an httptest server. Src is left nil: the import
// handlers never touch the segment source.
func importTestServer(t *testing.T, mgr ImportManager, token string) *httptest.Server {
	t.Helper()
	srv := New(Config{
		Import: ImportConfig{Manager: mgr, Token: token, RunCtx: context.Background()},
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func postJSON(t *testing.T, url, token, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func getReq(t *testing.T, url, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

const importNSID = "/xrpc/network.bsky.jetstream.importTimestamps"
const statusNSID = "/xrpc/network.bsky.jetstream.getImportStatus"

func TestImportTimestamps_DisabledWhenNoToken(t *testing.T) {
	t.Parallel()
	mgr := &fakeImportManager{submitID: "job1"}
	ts := importTestServer(t, mgr, "") // no token -> always 401

	resp := postJSON(t, ts.URL+importNSID, "", `{"path":"a.csv"}`)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Empty(t, mgr.lastPath, "handler must not reach the manager when unauthorized")

	// Even presenting a token is rejected when none is configured.
	resp2 := postJSON(t, ts.URL+importNSID, "anything", `{"path":"a.csv"}`)
	defer func() { _ = resp2.Body.Close() }()
	require.Equal(t, http.StatusUnauthorized, resp2.StatusCode)

	// An empty presented token must not match an empty configured token (the
	// hash-compare would otherwise "match"): the empty header is a missing
	// token, and disabled means nothing is accepted.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+importNSID, strings.NewReader(`{"path":"a.csv"}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp3, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp3.Body.Close() }()
	require.Equal(t, http.StatusUnauthorized, resp3.StatusCode)
}

// TestImportTimestamps_DisabledIndistinguishableFromWrongToken enforces the
// withBearer doc contract: a probe must not be able to learn whether import is
// enabled by comparing 401 responses, so the disabled and wrong-token
// rejections must carry identical status codes and bodies.
func TestImportTimestamps_DisabledIndistinguishableFromWrongToken(t *testing.T) {
	t.Parallel()
	readBody := func(url string) (int, string) {
		resp := postJSON(t, url+importNSID, "some-token", `{"path":"a.csv"}`)
		defer func() { _ = resp.Body.Close() }()
		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, string(b)
	}

	disabled := importTestServer(t, &fakeImportManager{}, "")
	enabled := importTestServer(t, &fakeImportManager{}, "s3cret")

	disabledCode, disabledBody := readBody(disabled.URL)
	wrongCode, wrongBody := readBody(enabled.URL)
	require.Equal(t, http.StatusUnauthorized, disabledCode)
	require.Equal(t, wrongCode, disabledCode)
	require.Equal(t, wrongBody, disabledBody,
		"disabled vs wrong-token 401 bodies must be identical")
}

func TestImportTimestamps_WrongTokenRejected(t *testing.T) {
	t.Parallel()
	mgr := &fakeImportManager{submitID: "job1"}
	ts := importTestServer(t, mgr, "s3cret")

	resp := postJSON(t, ts.URL+importNSID, "wrong", `{"path":"a.csv"}`)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Empty(t, mgr.lastPath)

	// Missing header also 401.
	resp2 := postJSON(t, ts.URL+importNSID, "", `{"path":"a.csv"}`)
	defer func() { _ = resp2.Body.Close() }()
	require.Equal(t, http.StatusUnauthorized, resp2.StatusCode)
}

func TestImportTimestamps_CorrectTokenSubmits(t *testing.T) {
	t.Parallel()
	mgr := &fakeImportManager{submitID: "job-42"}
	ts := importTestServer(t, mgr, "s3cret")

	resp := postJSON(t, ts.URL+importNSID, "s3cret", `{"path":"atlantis.csv"}`)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "atlantis.csv", mgr.lastPath)

	var out struct {
		Job string `json:"job"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Equal(t, "job-42", out.Job)
}

func TestImportTimestamps_ConcurrentReturns409(t *testing.T) {
	t.Parallel()
	mgr := &fakeImportManager{submitErr: importer.ErrJobInProgress}
	ts := importTestServer(t, mgr, "s3cret")

	resp := postJSON(t, ts.URL+importNSID, "s3cret", `{"path":"a.csv"}`)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestImportTimestamps_PathEscapeReturns400(t *testing.T) {
	t.Parallel()
	mgr := &fakeImportManager{submitErr: importer.ErrPathEscape}
	ts := importTestServer(t, mgr, "s3cret")

	resp := postJSON(t, ts.URL+importNSID, "s3cret", `{"path":"../../etc/passwd"}`)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestGetImportStatus_ReportsRecord(t *testing.T) {
	t.Parallel()
	mgr := &fakeImportManager{statusRec: importer.Record{
		ID:              "job-42",
		State:           importer.StateComplete,
		Phase:           "apply",
		SegmentsPatched: 3,
		RowsMutated:     9,
	}}
	ts := importTestServer(t, mgr, "s3cret")

	resp := getReq(t, ts.URL+statusNSID+"?job=job-42", "s3cret")
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out struct {
		Job             string `json:"job"`
		State           string `json:"state"`
		SegmentsPatched int64  `json:"segmentsPatched"`
		RowsMutated     int64  `json:"rowsMutated"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Equal(t, "job-42", out.Job)
	require.Equal(t, "complete", out.State)
	require.EqualValues(t, 3, out.SegmentsPatched)
	require.EqualValues(t, 9, out.RowsMutated)
}

func TestGetImportStatus_UnknownJob404(t *testing.T) {
	t.Parallel()
	mgr := &fakeImportManager{statusErr: importer.ErrJobNotFound}
	ts := importTestServer(t, mgr, "s3cret")

	resp := getReq(t, ts.URL+statusNSID+"?job=nope", "s3cret")
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGetImportStatus_RequiresAuth(t *testing.T) {
	t.Parallel()
	mgr := &fakeImportManager{}
	ts := importTestServer(t, mgr, "s3cret")

	resp := getReq(t, ts.URL+statusNSID, "") // no token
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}
