package xrpcapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// snapshotResp decodes the §R4 DID-tombstone fields planResp omits.
type snapshotResp struct {
	DidTombstonesIncluded *bool `json:"didTombstonesIncluded"`
	DidTombstones         []struct {
		DID string `json:"did"`
		Seq int64  `json:"seq"`
	} `json:"didTombstones"`
}

func accountDeletePayload(t *testing.T, did string) []byte {
	t.Helper()
	acc := &comatproto.SyncSubscribeRepos_Account{DID: did, Active: false, Status: gt.Some("deleted")}
	payload, err := acc.MarshalCBOR()
	require.NoError(t, err)
	return payload
}

// newSnapshotTestServer builds an archive server over one segment plus a
// tombstone set populated by Observe'ing the same events, mirroring the runtime
// wiring. tombstones=nil models a misconfigured server (fail-closed path).
func newSnapshotTestServer(t *testing.T, withTombstones bool, events ...segment.Event) *httptest.Server {
	t.Helper()
	dir := t.TempDir()
	if len(events) > 0 {
		writePlanSegment(t, dir, 0, events...)
	}
	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)

	var set *tombstone.Set
	if withTombstones {
		set = tombstone.New()
		for i := range events {
			require.NoError(t, set.Observe(&events[i]))
		}
	}
	srv := New(Config{Src: m, Logger: slog.Default(), Tombstones: set, Plan: defaultPlanTestConfig()})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func postSnapshot(t *testing.T, ts *httptest.Server, body any) (int, snapshotResp) {
	t.Helper()
	resp := doPostJSON(t, ts.URL+planURLPath, body)
	defer func() { _ = resp.Body.Close() }()
	var out snapshotResp
	if resp.StatusCode == http.StatusOK {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	}
	return resp.StatusCode, out
}

// TestPlanBackfill_DIDTombstoneSnapshotOnlyWhenRequested: the snapshot fields
// appear ONLY when wantDidTombstones is set. A plain request omits them.
func TestPlanBackfill_DIDTombstoneSnapshotOnlyWhenRequested(t *testing.T) {
	t.Parallel()
	ts := newSnapshotTestServer(t, true,
		planEvent(1, "did:plc:a", "app.bsky.feed.post"),
		segment.Event{Seq: 2, Kind: segment.KindAccount, DID: "did:plc:a", Payload: accountDeletePayload(t, "did:plc:a")},
	)

	// Not requested: no snapshot fields.
	status, out := postSnapshot(t, ts, map[string]any{})
	require.Equal(t, http.StatusOK, status)
	require.Nil(t, out.DidTombstonesIncluded, "didTombstonesIncluded must be absent when not requested")
	require.Empty(t, out.DidTombstones)

	// Requested: snapshot included with the account-delete keyed by DID.
	status, out = postSnapshot(t, ts, map[string]any{"wantDidTombstones": true})
	require.Equal(t, http.StatusOK, status)
	require.NotNil(t, out.DidTombstonesIncluded)
	require.True(t, *out.DidTombstonesIncluded)
	require.Len(t, out.DidTombstones, 1)
	require.Equal(t, "did:plc:a", out.DidTombstones[0].DID)
	require.EqualValues(t, 2, out.DidTombstones[0].Seq)
}

// TestPlanBackfill_DIDTombstoneSnapshotEmptyButIncluded: when nothing was
// deleted in range, the snapshot is INCLUDED but empty — the flag is true, the
// array empty. This is what lets the client distinguish "nothing deleted" from
// "too-old server".
func TestPlanBackfill_DIDTombstoneSnapshotEmptyButIncluded(t *testing.T) {
	t.Parallel()
	ts := newSnapshotTestServer(t, true,
		planEvent(1, "did:plc:a", "app.bsky.feed.post"),
		planEvent(2, "did:plc:b", "app.bsky.feed.post"),
	)
	status, out := postSnapshot(t, ts, map[string]any{"wantDidTombstones": true})
	require.Equal(t, http.StatusOK, status)
	require.NotNil(t, out.DidTombstonesIncluded)
	require.True(t, *out.DidTombstonesIncluded, "an empty snapshot is still INCLUDED")
	require.Empty(t, out.DidTombstones)
}

// TestPlanBackfill_DIDTombstoneSnapshotFilteredByDID: a DID-filtered request
// bounds the snapshot to the requested DIDs server-side.
func TestPlanBackfill_DIDTombstoneSnapshotFilteredByDID(t *testing.T) {
	t.Parallel()
	ts := newSnapshotTestServer(t, true,
		planEvent(1, "did:plc:keep", "app.bsky.feed.post"),
		segment.Event{Seq: 2, Kind: segment.KindAccount, DID: "did:plc:keep", Payload: accountDeletePayload(t, "did:plc:keep")},
		planEvent(3, "did:plc:drop", "app.bsky.feed.post"),
		segment.Event{Seq: 4, Kind: segment.KindAccount, DID: "did:plc:drop", Payload: accountDeletePayload(t, "did:plc:drop")},
	)
	status, out := postSnapshot(t, ts, map[string]any{
		"wantDidTombstones": true,
		"dids":              []string{"did:plc:keep"},
	})
	require.Equal(t, http.StatusOK, status)
	require.True(t, *out.DidTombstonesIncluded)
	require.Len(t, out.DidTombstones, 1, "snapshot must be filtered to the requested DIDs")
	require.Equal(t, "did:plc:keep", out.DidTombstones[0].DID)
}

// TestPlanBackfill_DIDTombstoneSnapshotRespectsSeqWindow: only tombstones in
// (afterSeq, plannedThroughSeq] are returned (SnapshotRange's half-open window).
func TestPlanBackfill_DIDTombstoneSnapshotRespectsSeqWindow(t *testing.T) {
	t.Parallel()
	ts := newSnapshotTestServer(t, true,
		planEvent(1, "did:plc:early", "app.bsky.feed.post"),
		segment.Event{Seq: 2, Kind: segment.KindAccount, DID: "did:plc:early", Payload: accountDeletePayload(t, "did:plc:early")},
		planEvent(3, "did:plc:late", "app.bsky.feed.post"),
		segment.Event{Seq: 4, Kind: segment.KindAccount, DID: "did:plc:late", Payload: accountDeletePayload(t, "did:plc:late")},
	)
	// afterSeq=2 excludes the early@2 tombstone; only late@4 remains.
	status, out := postSnapshot(t, ts, map[string]any{
		"wantDidTombstones": true,
		"afterSeq":          2,
	})
	require.Equal(t, http.StatusOK, status)
	require.True(t, *out.DidTombstonesIncluded)
	require.Len(t, out.DidTombstones, 1)
	require.Equal(t, "did:plc:late", out.DidTombstones[0].DID)
	require.EqualValues(t, 4, out.DidTombstones[0].Seq)
}

// TestPlanBackfill_DIDTombstoneFailsClosedWhenUnwired: a server that requests
// the snapshot but has no tombstone set wired must fail LOUD (500), never
// silently return an empty snapshot the client would trust. Crash over
// corruption.
func TestPlanBackfill_DIDTombstoneFailsClosedWhenUnwired(t *testing.T) {
	t.Parallel()
	ts := newSnapshotTestServer(t, false, // no tombstone set
		planEvent(1, "did:plc:a", "app.bsky.feed.post"),
	)
	resp := doPostJSON(t, ts.URL+planURLPath, map[string]any{"wantDidTombstones": true})
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode,
		"requesting the snapshot from a server with no tombstone set must fail closed")

	// Without the flag the same unwired server answers normally (no snapshot).
	status, out := postSnapshot(t, ts, map[string]any{})
	require.Equal(t, http.StatusOK, status)
	require.Nil(t, out.DidTombstonesIncluded)
}
