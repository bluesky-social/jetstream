package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// planServer spins an httptest server that answers the planBackfill XRPC
// procedure with the given raw JSON body (status 200) or an XRPC error. It
// captures the decoded request input for assertion.
type planServer struct {
	srv      *httptest.Server
	lastBody []byte
}

func newPlanServer(t *testing.T, status int, respBody string) *planServer {
	t.Helper()
	ps := &planServer{}
	ps.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/xrpc/network.bsky.jetstream.planBackfill", r.URL.Path)
		require.Equal(t, http.MethodPost, r.Method)
		body, _ := io.ReadAll(r.Body)
		ps.lastBody = body
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, respBody)
	}))
	t.Cleanup(ps.srv.Close)
	return ps
}

func (ps *planServer) planner() *Planner {
	// Disable retry backoff so the error-path tests don't wait real seconds on
	// 5xx responses; one attempt is enough to assert mapping behavior.
	return NewPlanner(&xrpc.Client{
		Host:  ps.srv.URL,
		Retry: gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
	})
}

func (ps *planServer) decodeInput(t *testing.T) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(ps.lastBody, &m))
	return m
}

func TestPlanWholeSegmentAndBlocks(t *testing.T) {
	t.Parallel()
	resp := `{
		"plannedThroughSeq": 500,
		"segments": [
			{"name":"seg_0000000000.jss","index":0,"checksum":"00112233aabbccdd","minSeq":1,"maxSeq":200,"mode":"segment"},
			{"name":"seg_0000000001.jss","index":1,"checksum":"44556677eeff0011","minSeq":201,"maxSeq":500,"mode":"blocks","blocks":[{"first":2,"last":4},{"first":7,"last":7}]}
		],
		"stats": {"segmentsExamined":2,"segmentsMatched":2,"blocksMatched":3,"entries":3}
	}`
	ps := newPlanServer(t, http.StatusOK, resp)

	plan, err := ps.planner().Plan(context.Background(), PlanRequest{AfterSeq: 0})
	require.NoError(t, err)

	require.EqualValues(t, 500, plan.PlannedThroughSeq)
	require.Len(t, plan.Entries, 2)

	require.Equal(t, "seg_0000000000.jss", plan.Entries[0].SegmentName)
	require.Equal(t, ModeWholeSegment, plan.Entries[0].Mode)
	require.Equal(t, "00112233aabbccdd", plan.Entries[0].Checksum)
	require.EqualValues(t, 1, plan.Entries[0].MinSeq)
	require.EqualValues(t, 200, plan.Entries[0].MaxSeq)
	require.Empty(t, plan.Entries[0].Blocks)

	require.Equal(t, ModeBlocks, plan.Entries[1].Mode)
	require.Equal(t, []BlockRange{{First: 2, Last: 4}, {First: 7, Last: 7}}, plan.Entries[1].Blocks)

	require.Equal(t, PlanStats{SegmentsExamined: 2, SegmentsMatched: 2, BlocksMatched: 3, Entries: 3}, plan.Stats)
}

func TestPlanEmptyArchive(t *testing.T) {
	t.Parallel()
	resp := `{"plannedThroughSeq":0,"segments":[],"stats":{"segmentsExamined":0,"segmentsMatched":0,"blocksMatched":0,"entries":0}}`
	ps := newPlanServer(t, http.StatusOK, resp)

	plan, err := ps.planner().Plan(context.Background(), PlanRequest{AfterSeq: 0})
	require.NoError(t, err)
	require.EqualValues(t, 0, plan.PlannedThroughSeq)
	require.Empty(t, plan.Entries)
}

func TestPlanFilterMatchesNothingButReportsTip(t *testing.T) {
	t.Parallel()
	// A filter that matches no segment in a non-empty archive still reports
	// the sealed tip as the cutover cursor.
	resp := `{"plannedThroughSeq":900,"segments":[],"stats":{"segmentsExamined":3,"segmentsMatched":0,"blocksMatched":0,"entries":0}}`
	ps := newPlanServer(t, http.StatusOK, resp)

	plan, err := ps.planner().Plan(context.Background(), PlanRequest{Collections: []string{"app.example.absent"}})
	require.NoError(t, err)
	require.EqualValues(t, 900, plan.PlannedThroughSeq)
	require.Empty(t, plan.Entries)
}

func TestPlanInputMapping(t *testing.T) {
	t.Parallel()
	resp := `{"plannedThroughSeq":0,"segments":[],"stats":{"segmentsExamined":0,"segmentsMatched":0,"blocksMatched":0,"entries":0}}`
	ps := newPlanServer(t, http.StatusOK, resp)

	_, err := ps.planner().Plan(context.Background(), PlanRequest{
		DIDs:         []string{"did:plc:abc", "did:plc:def"},
		Collections:  []string{"app.bsky.feed.post", "app.bsky.feed.*"},
		AfterSeq:     42,
		HasBeforeSeq: true,
		BeforeSeq:    1000,
	})
	require.NoError(t, err)

	in := ps.decodeInput(t)
	require.ElementsMatch(t, []any{"did:plc:abc", "did:plc:def"}, in["dids"])
	require.ElementsMatch(t, []any{"app.bsky.feed.post", "app.bsky.feed.*"}, in["collections"])
	require.EqualValues(t, 42, in["afterSeq"])
	require.EqualValues(t, 1000, in["beforeSeq"])
}

func TestPlanInputOmitsEmptyFilters(t *testing.T) {
	t.Parallel()
	resp := `{"plannedThroughSeq":0,"segments":[],"stats":{"segmentsExamined":0,"segmentsMatched":0,"blocksMatched":0,"entries":0}}`
	ps := newPlanServer(t, http.StatusOK, resp)

	_, err := ps.planner().Plan(context.Background(), PlanRequest{AfterSeq: 0})
	require.NoError(t, err)

	in := ps.decodeInput(t)
	_, hasDIDs := in["dids"]
	_, hasColls := in["collections"]
	_, hasAfter := in["afterSeq"]
	_, hasBefore := in["beforeSeq"]
	require.False(t, hasDIDs, "empty dids must be omitted")
	require.False(t, hasColls, "empty collections must be omitted")
	require.False(t, hasAfter, "afterSeq=0 must be omitted")
	require.False(t, hasBefore, "unset beforeSeq must be omitted")
}

func TestPlanTooLarge(t *testing.T) {
	t.Parallel()
	resp := `{"error":"PlanTooLarge","message":"too many entries"}`
	ps := newPlanServer(t, http.StatusBadRequest, resp)

	_, err := ps.planner().Plan(context.Background(), PlanRequest{AfterSeq: 0})
	require.ErrorIs(t, err, ErrPlanTooLarge)
}

func TestPlanOtherXRPCError(t *testing.T) {
	t.Parallel()
	resp := `{"error":"InternalError","message":"boom"}`
	ps := newPlanServer(t, http.StatusInternalServerError, resp)

	_, err := ps.planner().Plan(context.Background(), PlanRequest{AfterSeq: 0})
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrPlanTooLarge)
}

func TestPlanRejectsMalformedSegments(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		resp string
	}{
		{name: "missing name", resp: `{"plannedThroughSeq":10,"segments":[{"name":"","index":0,"checksum":"x","minSeq":1,"maxSeq":2,"mode":"segment"}],"stats":{}}`},
		{name: "unknown mode", resp: `{"plannedThroughSeq":10,"segments":[{"name":"s","index":0,"checksum":"x","minSeq":1,"maxSeq":2,"mode":"bogus"}],"stats":{}}`},
		{name: "blocks mode no ranges", resp: `{"plannedThroughSeq":10,"segments":[{"name":"s","index":0,"checksum":"x","minSeq":1,"maxSeq":2,"mode":"blocks"}],"stats":{}}`},
		{name: "inverted block range", resp: `{"plannedThroughSeq":10,"segments":[{"name":"s","index":0,"checksum":"x","minSeq":1,"maxSeq":2,"mode":"blocks","blocks":[{"first":5,"last":2}]}],"stats":{}}`},
		{name: "inverted seq range", resp: `{"plannedThroughSeq":10,"segments":[{"name":"s","index":0,"checksum":"x","minSeq":500,"maxSeq":200,"mode":"segment"}],"stats":{}}`},
		{name: "index exceeds uint32", resp: `{"plannedThroughSeq":10,"segments":[{"name":"s","index":4294967296,"checksum":"x","minSeq":1,"maxSeq":2,"mode":"segment"}],"stats":{}}`},
		{name: "block last exceeds uint32", resp: `{"plannedThroughSeq":10,"segments":[{"name":"s","index":0,"checksum":"x","minSeq":1,"maxSeq":2,"mode":"blocks","blocks":[{"first":0,"last":4294967296}]}],"stats":{}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ps := newPlanServer(t, http.StatusOK, tc.resp)
			_, err := ps.planner().Plan(context.Background(), PlanRequest{AfterSeq: 0})
			require.Error(t, err)
		})
	}
}
