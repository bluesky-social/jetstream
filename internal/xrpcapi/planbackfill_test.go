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
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

const planURLPath = "/xrpc/network.bsky.jetstream.planBackfill"

type planResp struct {
	PlannedThroughSeq int64 `json:"plannedThroughSeq"`
	SealedTipSeq      int64 `json:"sealedTipSeq"`
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

func TestClassifyCollectionPattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		raw        string
		wantExact  string
		wantPrefix string
		wantErr    bool
	}{
		// Exact NSIDs.
		{name: "exact post", raw: "app.bsky.feed.post", wantExact: "app.bsky.feed.post"},
		{name: "exact com", raw: "com.example.foo.bar", wantExact: "com.example.foo.bar"},

		// Accepted wildcards.
		{name: "wildcard feed", raw: "app.bsky.feed.*", wantPrefix: "app.bsky.feed."},
		{name: "wildcard two-segment head", raw: "app.bsky.*", wantPrefix: "app.bsky."},
		{name: "wildcard com.example", raw: "com.example.*", wantPrefix: "com.example."},
		// atmos permits uppercase domain labels; we assert whatever the parser
		// decides rather than inventing a stricter rule. If this ever flips in
		// atmos, this row documents the dependency and will fail loudly.
		{name: "wildcard uppercase labels", raw: "APP.BSKY.*", wantPrefix: "APP.BSKY."},

		// Rejected wildcards (head fails NSID-authority grammar).
		{name: "wildcard single-segment head", raw: "app.*", wantErr: true},
		{name: "wildcard empty head", raw: ".*", wantErr: true},
		{name: "wildcard trailing-dot head", raw: "app.bsky..*", wantErr: true},
		{name: "wildcard double star", raw: "app.bsky.feed.*.*", wantErr: true},
		{name: "wildcard non-letter tld", raw: "1pp.bsky.*", wantErr: true},

		// Not wildcards (no ".*" suffix) -> exact branch -> invalid NSID.
		{name: "bare star", raw: "*", wantErr: true},
		{name: "star no dot", raw: "app.bsky.fo*", wantErr: true},
		{name: "star suffix not dotstar", raw: "app.bsky.feed.*x", wantErr: true},
		{name: "garbage", raw: "not a collection", wantErr: true},
		{name: "empty", raw: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			exact, prefix, err := classifyCollectionPattern(tt.raw)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantExact, exact, "exact mismatch")
			require.Equal(t, tt.wantPrefix, prefix, "prefix mismatch")
			// Exactly one of exact/prefix is set.
			require.True(t, (exact == "") != (prefix == ""), "exactly one of exact/prefix must be set")
			if prefix != "" {
				require.True(t, strings.HasSuffix(prefix, "."), "stored prefix must end in '.'")
				require.False(t, strings.Contains(prefix, "*"), "stored prefix must not retain '*'")
			}
		})
	}
}

func TestValidatePlanCollections(t *testing.T) {
	t.Parallel()

	t.Run("empty input is match-all", func(t *testing.T) {
		t.Parallel()
		exact, prefixes, err := validatePlanCollections(nil, 10)
		require.NoError(t, err)
		require.Nil(t, exact)
		require.Nil(t, prefixes)
	})

	t.Run("splits exact and prefix", func(t *testing.T) {
		t.Parallel()
		exact, prefixes, err := validatePlanCollections(
			[]string{"app.bsky.feed.post", "app.bsky.graph.*", "app.bsky.feed.like"}, 10)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"app.bsky.feed.post", "app.bsky.feed.like"}, exact)
		require.Equal(t, []string{"app.bsky.graph."}, prefixes)
	})

	t.Run("exact and its own wildcard are distinct patterns", func(t *testing.T) {
		t.Parallel()
		exact, prefixes, err := validatePlanCollections(
			[]string{"app.bsky.feed.post", "app.bsky.feed.*"}, 10)
		require.NoError(t, err)
		require.Equal(t, []string{"app.bsky.feed.post"}, exact)
		require.Equal(t, []string{"app.bsky.feed."}, prefixes)
	})

	t.Run("dedup exact", func(t *testing.T) {
		t.Parallel()
		exact, _, err := validatePlanCollections(
			[]string{"app.bsky.feed.post", "app.bsky.feed.post"}, 10)
		require.NoError(t, err)
		require.Equal(t, []string{"app.bsky.feed.post"}, exact)
	})

	t.Run("dedup prefix", func(t *testing.T) {
		t.Parallel()
		_, prefixes, err := validatePlanCollections(
			[]string{"app.bsky.feed.*", "app.bsky.feed.*"}, 10)
		require.NoError(t, err)
		require.Equal(t, []string{"app.bsky.feed."}, prefixes)
	})

	t.Run("order independence", func(t *testing.T) {
		t.Parallel()
		e1, p1, err := validatePlanCollections([]string{"app.bsky.graph.*", "app.bsky.feed.post"}, 10)
		require.NoError(t, err)
		e2, p2, err := validatePlanCollections([]string{"app.bsky.feed.post", "app.bsky.graph.*"}, 10)
		require.NoError(t, err)
		require.ElementsMatch(t, e1, e2)
		require.ElementsMatch(t, p1, p2)
	})

	t.Run("cap counts patterns: at limit ok", func(t *testing.T) {
		t.Parallel()
		// 1 exact + 1 prefix = 2 distinct patterns, limit 2.
		exact, prefixes, err := validatePlanCollections(
			[]string{"app.bsky.feed.post", "app.bsky.graph.*"}, 2)
		require.NoError(t, err)
		require.Len(t, exact, 1)
		require.Len(t, prefixes, 1)
	})

	t.Run("cap counts patterns: over by one rejects", func(t *testing.T) {
		t.Parallel()
		_, _, err := validatePlanCollections(
			[]string{"app.bsky.feed.post", "app.bsky.graph.*", "app.bsky.actor.*"}, 2)
		require.Error(t, err)
	})

	t.Run("wildcard counts as one toward cap", func(t *testing.T) {
		t.Parallel()
		// A single wildcard covering many collections is one pattern.
		_, prefixes, err := validatePlanCollections([]string{"app.bsky.*"}, 1)
		require.NoError(t, err)
		require.Len(t, prefixes, 1)
	})

	t.Run("disabled with any pattern", func(t *testing.T) {
		t.Parallel()
		_, _, err := validatePlanCollections([]string{"app.bsky.feed.*"}, 0)
		require.Error(t, err)
		_, _, err = validatePlanCollections([]string{"app.bsky.feed.post"}, 0)
		require.Error(t, err)
	})

	t.Run("invalid wildcard rejected", func(t *testing.T) {
		t.Parallel()
		_, _, err := validatePlanCollections([]string{"app.*"}, 10)
		require.Error(t, err)
	})
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
	require.EqualValues(t, 4, out.SealedTipSeq, "un-truncated page reports the tip on both cursors")
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

func TestPlanBackfill_WildcardOnly(t *testing.T) {
	t.Parallel()

	ts := newPlanTestServer(t, defaultPlanTestConfig(),
		planEvent(1, "did:plc:a", "app.bsky.feed.post"),
		planEvent(2, "did:plc:b", "app.bsky.graph.follow"),
		planEvent(3, "did:plc:c", "app.bsky.feed.like"),
		planEvent(4, "did:plc:d", "com.example.thing"),
	)

	status, out := postPlan(t, ts, map[string]any{
		"collections": []string{"app.bsky.feed.*"},
	})
	require.Equal(t, http.StatusOK, status)
	require.Len(t, out.Segments, 1)
	require.Equal(t, "blocks", out.Segments[0].Mode)
	// app.bsky.feed.post (block 0) and app.bsky.feed.like (block 2) match.
	require.Equal(t, []int64{0, 0}, []int64{out.Segments[0].Blocks[0].First, out.Segments[0].Blocks[0].Last})
	require.Equal(t, []int64{2, 2}, []int64{out.Segments[0].Blocks[1].First, out.Segments[0].Blocks[1].Last})
	require.EqualValues(t, 2, out.Stats.BlocksMatched)
}

func TestPlanBackfill_WildcardMixedWithExact(t *testing.T) {
	t.Parallel()

	ts := newPlanTestServer(t, defaultPlanTestConfig(),
		planEvent(1, "did:plc:a", "app.bsky.feed.post"),
		planEvent(2, "did:plc:b", "app.bsky.graph.follow"),
		planEvent(3, "did:plc:c", "app.bsky.graph.block"),
		planEvent(4, "did:plc:d", "com.example.thing"),
	)

	status, out := postPlan(t, ts, map[string]any{
		"collections": []string{"app.bsky.feed.post", "app.bsky.graph.*"},
	})
	require.Equal(t, http.StatusOK, status)
	require.Len(t, out.Segments, 1)
	// blocks 0 (post, exact), 1 and 2 (graph.*) match; block 3 (com.example) does not.
	require.EqualValues(t, 3, out.Stats.BlocksMatched)
}

func TestPlanBackfill_WildcardMatchesNothing(t *testing.T) {
	t.Parallel()

	ts := newPlanTestServer(t, defaultPlanTestConfig(),
		planEvent(1, "did:plc:a", "app.bsky.feed.post"),
		planEvent(2, "did:plc:b", "app.bsky.feed.like"),
	)

	status, out := postPlan(t, ts, map[string]any{
		"collections": []string{"com.example.*"},
	})
	require.Equal(t, http.StatusOK, status)
	// Coverage horizon reported even though no segment matched.
	require.EqualValues(t, 2, out.PlannedThroughSeq)
	require.Empty(t, out.Segments)
	require.EqualValues(t, 1, out.Stats.SegmentsExamined)
	require.EqualValues(t, 0, out.Stats.SegmentsMatched)
}

func TestPlanBackfill_WildcardWithDIDFilter(t *testing.T) {
	t.Parallel()

	ts := newPlanTestServer(t, defaultPlanTestConfig(),
		planEvent(1, "did:plc:target", "app.bsky.feed.post"),
		planEvent(2, "did:plc:other", "app.bsky.feed.like"),
		planEvent(3, "did:plc:target", "app.bsky.graph.follow"),
		planEvent(4, "did:plc:target", "com.example.thing"),
	)

	status, out := postPlan(t, ts, map[string]any{
		"dids":        []string{"did:plc:target"},
		"collections": []string{"app.bsky.*"},
	})
	require.Equal(t, http.StatusOK, status)
	require.Len(t, out.Segments, 1)
	// target ∩ app.bsky.*: block 0 (feed.post) and block 2 (graph.follow).
	// block 1 is other's, block 3 is com.example -> excluded.
	require.EqualValues(t, 2, out.Stats.BlocksMatched)
}

func TestPlanBackfill_WildcardWithSeqWindow(t *testing.T) {
	t.Parallel()

	ts := newPlanTestServer(t, defaultPlanTestConfig(),
		planEvent(1, "did:plc:a", "app.bsky.feed.post"),
		planEvent(2, "did:plc:b", "app.bsky.feed.like"),
		planEvent(3, "did:plc:c", "app.bsky.feed.repost"),
		planEvent(4, "did:plc:d", "app.bsky.feed.post"),
	)

	status, out := postPlan(t, ts, map[string]any{
		"collections": []string{"app.bsky.feed.*"},
		"afterSeq":    1,
		"beforeSeq":   3,
	})
	require.Equal(t, http.StatusOK, status)
	require.EqualValues(t, 3, out.PlannedThroughSeq)
	require.Len(t, out.Segments, 1)
	// seq window (1,3]: blocks 1 and 2.
	require.EqualValues(t, 2, out.Stats.BlocksMatched)
}

func TestPlanBackfill_InvalidWildcardReturns400(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{"app.*", ".*", "app.bsky..*", "app.bsky.feed.*.*"} {
		t.Run(bad, func(t *testing.T) {
			t.Parallel()
			ts := newPlanTestServer(t, defaultPlanTestConfig())
			resp := doPostJSON(t, ts.URL+planURLPath, map[string]any{"collections": []string{bad}})
			defer func() { _ = resp.Body.Close() }()
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			require.Equal(t, "InvalidRequest", readXRPCError(t, resp))
		})
	}
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

func TestPlanBackfill_TruncatesAndPaginatesOverWire(t *testing.T) {
	t.Parallel()

	// target on odd seqs → sparse non-adjacent blocks → three coalesced
	// ranges. MaxEntries=2 truncates the first page after two ranges and
	// reports a continuation cursor below the sealed tip; the client pages
	// from there and the union covers everything.
	cfg := defaultPlanTestConfig()
	cfg.MaxEntries = 2
	ts := newPlanTestServer(t, cfg,
		planEvent(1, "did:plc:target", "app.bsky.feed.post"),
		planEvent(2, "did:plc:other", "app.bsky.feed.post"),
		planEvent(3, "did:plc:target", "app.bsky.feed.post"),
		planEvent(4, "did:plc:other", "app.bsky.feed.post"),
		planEvent(5, "did:plc:target", "app.bsky.feed.post"),
	)

	status, page1 := postPlan(t, ts, map[string]any{"dids": []string{"did:plc:target"}})
	require.Equal(t, http.StatusOK, status, "truncation paginates, never 400s")
	require.EqualValues(t, 2, page1.Stats.Entries, "first page capped at MaxEntries")
	require.EqualValues(t, 5, page1.SealedTipSeq, "tip pinned at the true sealed tip")
	require.Less(t, page1.PlannedThroughSeq, page1.SealedTipSeq, "page truncated below the tip")
	require.EqualValues(t, 3, page1.PlannedThroughSeq, "cursor = last included block's MaxSeq")

	status, page2 := postPlan(t, ts, map[string]any{
		"dids":     []string{"did:plc:target"},
		"afterSeq": page1.PlannedThroughSeq,
	})
	require.Equal(t, http.StatusOK, status)
	require.EqualValues(t, 5, page2.PlannedThroughSeq, "second page reaches the tip")
	require.EqualValues(t, 5, page2.SealedTipSeq)
	require.NotEmpty(t, page2.Segments, "the un-included tail block is delivered on page 2")
}

func TestPlanBackfill_ZeroMaxEntriesDisablesPagination(t *testing.T) {
	t.Parallel()

	// MaxEntries == 0 disables the per-page cap: the same workload that
	// paginates under a positive limit returns in one un-truncated page whose
	// continuation cursor already equals the sealed tip.
	cfg := defaultPlanTestConfig()
	cfg.MaxEntries = 0
	ts := newPlanTestServer(t, cfg,
		planEvent(1, "did:plc:target", "app.bsky.feed.post"),
		planEvent(2, "did:plc:other", "app.bsky.feed.post"),
		planEvent(3, "did:plc:target", "app.bsky.feed.post"),
		planEvent(4, "did:plc:other", "app.bsky.feed.post"),
		planEvent(5, "did:plc:target", "app.bsky.feed.post"),
	)

	status, out := postPlan(t, ts, map[string]any{"dids": []string{"did:plc:target"}})
	require.Equal(t, http.StatusOK, status)
	require.NotEmpty(t, out.Segments)
	require.EqualValues(t, 5, out.SealedTipSeq)
	require.EqualValues(t, out.SealedTipSeq, out.PlannedThroughSeq, "un-truncated: cursor == tip")
}
