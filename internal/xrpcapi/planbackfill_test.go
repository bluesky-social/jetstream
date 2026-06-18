package xrpcapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/manifest"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/stretchr/testify/require"
)

const planURLPath = "/xrpc/network.bsky.jetstream.planBackfill"

type planResp struct {
	PlannedThroughSeq int64 `json:"plannedThroughSeq"`
	Segments          []struct {
		Name     string `json:"name"`
		Index    int64  `json:"index"`
		Checksum string `json:"checksum"`
		MinSeq   int64  `json:"minSeq"`
		MaxSeq   int64  `json:"maxSeq"`
		Mode     string `json:"mode"`
		Blocks   []struct {
			First int64 `json:"first"`
			Last  int64 `json:"last"`
		} `json:"blocks"`
	} `json:"segments"`
	Stats struct {
		SegmentsExamined int64 `json:"segmentsExamined"`
		SegmentsMatched  int64 `json:"segmentsMatched"`
		BlocksMatched    int64 `json:"blocksMatched"`
		Entries          int64 `json:"entries"`
	} `json:"stats"`
}

func planEvent(seq uint64, did, collection string) segment.Event {
	return segment.Event{
		Seq:        seq,
		IndexedAt:  int64(1_730_000_000_000_000 + seq),
		Kind:       segment.KindCreate,
		DID:        did,
		Collection: collection,
		Rkey:       "rkey",
		Rev:        "rev",
		Payload:    []byte{0xa0},
	}
}

func writePlanSegment(t *testing.T, dir string, idx uint64, events ...segment.Event) {
	t.Helper()
	path := filepath.Join(dir, ingest.SegmentFilename(idx))
	w, err := segment.New(segment.Config{Path: path, MaxEventsPerBlock: 1})
	require.NoError(t, err)
	for i, ev := range events {
		full, err := w.Append(ev)
		require.NoError(t, err)
		if full && i < len(events)-1 {
			require.NoError(t, w.Flush())
		}
	}
	_, err = w.Seal()
	require.NoError(t, err)
}

func newPlanTestServer(t *testing.T, cfg PlanConfig, events ...segment.Event) *httptest.Server {
	t.Helper()
	dir := t.TempDir()
	if len(events) > 0 {
		writePlanSegment(t, dir, 0, events...)
	}
	m, err := manifest.Open(manifest.Options{SegmentsDir: dir, Logger: slog.Default()})
	require.NoError(t, err)
	srv := New(Config{Src: m, Logger: slog.Default(), Plan: cfg})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func defaultPlanTestConfig() PlanConfig {
	return PlanConfig{
		MaxDIDs:               10,
		MaxCollections:        10,
		MaxEntries:            100,
		WholeSegmentThreshold: 1,
	}
}

func postPlan(t *testing.T, ts *httptest.Server, body any) (int, planResp) {
	t.Helper()
	resp := doPostJSON(t, ts.URL+planURLPath, body)
	defer func() { _ = resp.Body.Close() }()
	var out planResp
	if resp.StatusCode == http.StatusOK {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	}
	return resp.StatusCode, out
}

func TestPlanBackfill_ReturnsBlockPlan(t *testing.T) {
	t.Parallel()

	ts := newPlanTestServer(t, defaultPlanTestConfig(),
		planEvent(1, "did:plc:other", "app.bsky.feed.post"),
		planEvent(2, "did:plc:target", "app.bsky.feed.post"),
		planEvent(3, "did:plc:target", "app.bsky.feed.like"),
		planEvent(4, "did:plc:other", "app.bsky.feed.like"),
	)

	status, out := postPlan(t, ts, map[string]any{
		"dids":        []string{"did:plc:target"},
		"collections": []string{"app.bsky.feed.like"},
	})
	require.Equal(t, http.StatusOK, status)
	require.EqualValues(t, 4, out.PlannedThroughSeq)
	require.Len(t, out.Segments, 1)
	seg := out.Segments[0]
	require.Equal(t, ingest.SegmentFilename(0), seg.Name)
	require.EqualValues(t, 0, seg.Index)
	require.Len(t, seg.Checksum, 16)
	require.EqualValues(t, 1, seg.MinSeq)
	require.EqualValues(t, 4, seg.MaxSeq)
	require.Equal(t, "blocks", seg.Mode)
	require.Len(t, seg.Blocks, 1)
	require.EqualValues(t, 2, seg.Blocks[0].First)
	require.EqualValues(t, 2, seg.Blocks[0].Last)
	require.EqualValues(t, 1, out.Stats.SegmentsExamined)
	require.EqualValues(t, 1, out.Stats.SegmentsMatched)
	require.EqualValues(t, 1, out.Stats.BlocksMatched)
	require.EqualValues(t, 1, out.Stats.Entries)
}

func TestPlanBackfill_WholeSegmentWireShape(t *testing.T) {
	t.Parallel()

	// A DID present in 3 of 4 blocks at the default 0.75 threshold yields a
	// whole-segment entry. WholeSegmentThreshold:0 exercises the withDefaults()
	// path that fills in the 0.75 default.
	cfg := PlanConfig{MaxDIDs: 10, MaxCollections: 10, MaxEntries: 100, WholeSegmentThreshold: 0}
	ts := newPlanTestServer(t, cfg,
		planEvent(1, "did:plc:target", "app.bsky.feed.post"),
		planEvent(2, "did:plc:target", "app.bsky.feed.post"),
		planEvent(3, "did:plc:target", "app.bsky.feed.post"),
		planEvent(4, "did:plc:other", "app.bsky.feed.post"),
	)

	resp := doPostJSON(t, ts.URL+planURLPath, map[string]any{"dids": []string{"did:plc:target"}})
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// Decode into the typed shape and assert mode=segment with no block ranges.
	var out planResp
	require.NoError(t, json.Unmarshal(body, &out))
	require.Len(t, out.Segments, 1)
	require.Equal(t, "segment", out.Segments[0].Mode)
	require.Empty(t, out.Segments[0].Blocks, "whole-segment entries carry no block ranges")
	require.EqualValues(t, 1, out.Stats.Entries)

	// And assert the raw wire payload omits the blocks key entirely (the
	// generated binding tags it omitempty), so clients never see a stray empty
	// array for a whole-segment plan.
	var raw struct {
		Segments []map[string]json.RawMessage `json:"segments"`
	}
	require.NoError(t, json.Unmarshal(body, &raw))
	require.Len(t, raw.Segments, 1)
	_, hasBlocks := raw.Segments[0]["blocks"]
	require.False(t, hasBlocks, "mode=segment must omit the blocks field on the wire, got: %s", body)
}

func TestPlanBackfill_IsPOSTProcedure(t *testing.T) {
	t.Parallel()

	ts := newPlanTestServer(t, defaultPlanTestConfig())

	resp := doGet(t, ts.URL+planURLPath)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)

	status, out := postPlan(t, ts, map[string]any{})
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, out.Segments)
}

func TestPlanBackfill_ReadinessGate(t *testing.T) {
	t.Parallel()

	s, _ := newTestServer(t, 1)
	gated := New(Config{
		Src:    s.src,
		Logger: s.logger,
		Plan:   defaultPlanTestConfig(),
		Ready: func(_ context.Context) error {
			return errors.New("bootstrap in progress")
		},
	})
	ts := httptest.NewServer(gated.Handler())
	t.Cleanup(ts.Close)

	resp := doPostJSON(t, ts.URL+planURLPath, map[string]any{})
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Equal(t, "ServiceUnavailable", readXRPCError(t, resp))
}

func TestPlanBackfill_InvalidRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  PlanConfig
		body any
	}{
		{
			name: "invalid DID",
			cfg:  defaultPlanTestConfig(),
			body: map[string]any{"dids": []string{"not-a-did"}},
		},
		{
			name: "too many DIDs",
			cfg:  PlanConfig{MaxDIDs: 1, MaxCollections: 10, MaxEntries: 100, WholeSegmentThreshold: 1},
			body: map[string]any{"dids": []string{"did:plc:a", "did:plc:b"}},
		},
		{
			name: "DID filters disabled",
			cfg:  PlanConfig{MaxDIDs: 0, MaxCollections: 10, MaxEntries: 100, WholeSegmentThreshold: 1},
			body: map[string]any{"dids": []string{"did:plc:a"}},
		},
		{
			name: "invalid collection",
			cfg:  defaultPlanTestConfig(),
			body: map[string]any{"collections": []string{"not a collection"}},
		},
		{
			name: "too many collections",
			cfg:  PlanConfig{MaxDIDs: 10, MaxCollections: 1, MaxEntries: 100, WholeSegmentThreshold: 1},
			body: map[string]any{"collections": []string{"app.bsky.feed.post", "app.bsky.feed.like"}},
		},
		{
			name: "collection filters disabled",
			cfg:  PlanConfig{MaxDIDs: 10, MaxCollections: 0, MaxEntries: 100, WholeSegmentThreshold: 1},
			body: map[string]any{"collections": []string{"app.bsky.feed.post"}},
		},
		{
			name: "negative afterSeq",
			cfg:  defaultPlanTestConfig(),
			body: map[string]any{"afterSeq": -1},
		},
		{
			name: "invalid seq window",
			cfg:  defaultPlanTestConfig(),
			body: map[string]any{"afterSeq": 10, "beforeSeq": 10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts := newPlanTestServer(t, tt.cfg)
			resp := doPostJSON(t, ts.URL+planURLPath, tt.body)
			defer func() { _ = resp.Body.Close() }()
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			require.Equal(t, "InvalidRequest", readXRPCError(t, resp))
		})
	}
}

func TestPlanBackfill_MisconfiguredServerReturnsInternalError(t *testing.T) {
	t.Parallel()

	// A bad operator config is a server fault, not the client's: it must
	// surface as a 500 InternalError, never a 400 InvalidRequest that would
	// blame the caller.
	ts := newPlanTestServer(t, PlanConfig{MaxDIDs: 10, MaxCollections: 10, MaxEntries: -1, WholeSegmentThreshold: 1})
	resp := doPostJSON(t, ts.URL+planURLPath, map[string]any{})
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	require.Equal(t, "InternalServerError", readXRPCError(t, resp))
}

func TestPlanBackfill_InvalidJSON(t *testing.T) {
	t.Parallel()

	ts := newPlanTestServer(t, defaultPlanTestConfig())
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+planURLPath, bytes.NewBufferString(`{`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "InvalidRequest", readXRPCError(t, resp))
}

func TestPlanBackfill_PlanTooLarge(t *testing.T) {
	t.Parallel()

	cfg := defaultPlanTestConfig()
	cfg.MaxEntries = 2
	ts := newPlanTestServer(t, cfg,
		planEvent(1, "did:plc:target", "app.bsky.feed.post"),
		planEvent(2, "did:plc:other", "app.bsky.feed.post"),
		planEvent(3, "did:plc:target", "app.bsky.feed.post"),
		planEvent(4, "did:plc:other", "app.bsky.feed.post"),
		planEvent(5, "did:plc:target", "app.bsky.feed.post"),
	)

	resp := doPostJSON(t, ts.URL+planURLPath, map[string]any{"dids": []string{"did:plc:target"}})
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "PlanTooLarge", readXRPCError(t, resp))
}
