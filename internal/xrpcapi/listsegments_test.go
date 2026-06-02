package xrpcapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/stretchr/testify/require"
)

type listResp struct {
	Cursor   string `json:"cursor"`
	Segments []struct {
		Name         string `json:"name"`
		Index        int64  `json:"index"`
		SizeBytes    int64  `json:"sizeBytes"`
		Checksum     string `json:"checksum"`
		EventCount   int64  `json:"eventCount"`
		MinSeq       int64  `json:"minSeq"`
		MaxSeq       int64  `json:"maxSeq"`
		MinIndexedAt int64  `json:"minIndexedAt"`
		MaxIndexedAt int64  `json:"maxIndexedAt"`
	} `json:"segments"`
}

// getList issues a listSegments request and fully consumes+closes the
// response, returning the status code and the decoded body. Returning the
// closed-and-parsed result (not the live *http.Response) keeps callers from
// leaking bodies.
func getList(t *testing.T, base, query string) (status int, out listResp) {
	t.Helper()
	resp := doGet(t, base+"/xrpc/network.bsky.jetstream.listSegments"+query)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusOK {
		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(b, &out))
	}
	return resp.StatusCode, out
}

func TestListSegments_Empty(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, 0)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	status, out := getList(t, ts.URL, "")
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, out.Segments)
	require.Empty(t, out.Cursor)
}

func TestListSegments_Pagination(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, 5)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	status1, p1 := getList(t, ts.URL, "?limit=2")
	require.Equal(t, http.StatusOK, status1)
	require.Len(t, p1.Segments, 2)
	require.NotEmpty(t, p1.Cursor)
	require.EqualValues(t, 0, p1.Segments[0].Index)
	require.Equal(t, ingest.SegmentFilename(0), p1.Segments[0].Name)

	status2, p2 := getList(t, ts.URL, "?limit=2&cursor="+p1.Cursor)
	require.Equal(t, http.StatusOK, status2)
	require.Len(t, p2.Segments, 2)
	require.EqualValues(t, 2, p2.Segments[0].Index)

	status3, p3 := getList(t, ts.URL, "?limit=2&cursor="+p2.Cursor)
	require.Equal(t, http.StatusOK, status3)
	require.Len(t, p3.Segments, 1)
	require.Empty(t, p3.Cursor, "last page omits the cursor")
}

func TestListSegments_RowMetadata(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, 1)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	_, out := getList(t, ts.URL, "")
	require.Len(t, out.Segments, 1)
	seg := out.Segments[0]
	require.Equal(t, ingest.SegmentFilename(0), seg.Name)
	require.Positive(t, seg.SizeBytes)
	require.Len(t, seg.Checksum, 16, "checksum is 16 hex chars")
	require.Positive(t, seg.EventCount)

	// Row checksum must equal the getSegment ETag (sans quotes).
	g := doGet(t, fmt.Sprintf("%s/xrpc/network.bsky.jetstream.getSegment?name=%s", ts.URL, seg.Name))
	_ = g.Body.Close()
	require.Equal(t, fmt.Sprintf("%q", seg.Checksum), g.Header.Get("ETag"))
}

func TestListSegments_BadParams(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, 2)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	for _, q := range []string{"?limit=0", "?limit=-1", "?limit=abc", "?cursor=notanumber"} {
		status, _ := getList(t, ts.URL, q)
		require.Equal(t, http.StatusBadRequest, status, "query %q", q)
	}
}

func TestListSegments_LimitClamped(t *testing.T) {
	t.Parallel()
	s, _ := newTestServer(t, 3)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	status, out := getList(t, ts.URL, "?limit=99999")
	require.Equal(t, http.StatusOK, status)
	require.Len(t, out.Segments, 3) // clamped, returns all available
}
