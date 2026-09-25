package jetstream

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// failoverT0 is an arbitrary witnessed time well inside the timestamp cursor
// namespace (>= seqspace.CursorSeqMaxThreshold).
const failoverT0 = int64(1_779_667_200_000_000)

// fakeSession is one scripted dial outcome: a dial error, or a connection
// that replays steps.
type fakeSession struct {
	err   error
	steps []readStep
}

// fakeFleet routes dials by URL host to per-host session scripts, consumed in
// order (the last repeats), and records each dial as "host cursor" with "-"
// for an omitted cursor.
type fakeFleet struct {
	mu       sync.Mutex
	sessions map[string][]fakeSession
	dials    []string
}

func (f *fakeFleet) dial(_ context.Context, raw string) (wsConn, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	cur := "-"
	if u.Query().Has("cursor") {
		cur = u.Query().Get("cursor")
	}
	f.dials = append(f.dials, u.Host+" "+cur)
	ss := f.sessions[u.Host]
	if len(ss) == 0 {
		return nil, errors.New("no such host")
	}
	s := ss[0]
	if len(ss) > 1 {
		f.sessions[u.Host] = ss[1:]
	}
	if s.err != nil {
		return nil, s.err
	}
	return &scriptedConn{steps: s.steps}, nil
}

func (f *fakeFleet) dialLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.dials...)
}

// witnessedFrame is an #identity frame carrying witnessedAt.
func witnessedFrame(seq uint64, w int64) readStep {
	ts := time.UnixMicro(w).UTC().Format("2006-01-02T15:04:05.000000Z")
	s := strconv.FormatUint(seq, 10)
	return readStep{data: []byte(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#identity"` +
		`,"seq":` + s + `,"did":"did:plc:a","time":"` + testWireTime + `","witnessedAt":"` + ts + `"` +
		`,"identity":{"did":"did:plc:a","seq":1,"time":"2026-05-25T00:00:00Z"}}}`)}
}

func tsCursor(w int64, rewind time.Duration) string {
	return strconv.FormatInt(w-rewind.Microseconds(), 10)
}

func timeModeConfig(f *fakeFleet, failover ...string) liveConfig {
	return liveConfig{
		host:          "https://a",
		failoverHosts: failover,
		timeMode:      true,
		rewind:        5 * time.Second,
		dial:          f.dial,
	}
}

// A reconnect to the same host keeps exact seq resume.
func TestLiveFailoverSameHostResumesBySeq(t *testing.T) {
	t.Parallel()
	f := &fakeFleet{sessions: map[string][]fakeSession{"a": {
		{steps: []readStep{witnessedFrame(100, failoverT0), witnessedFrame(101, failoverT0+1)}},
		{steps: []readStep{witnessedFrame(101, failoverT0+1), witnessedFrame(102, failoverT0+2)}},
		{},
	}}}
	cfg := timeModeConfig(f)
	cfg.cursor, cfg.dedupFloor, cfg.archiveNS = 99, 99, true
	events, _ := runConsumer(t, cfg, 3)
	require.Equal(t, []uint64{100, 101, 102}, seqs(events), "seq dedup still drops the inclusive overlap")
	require.Equal(t, []string{"a 99", "a 101"}, f.dialLog()[:2])
}

func TestLiveFailoverDialFailureRotatesHosts(t *testing.T) {
	t.Parallel()
	down := errors.New("connection refused")

	t.Run("from tip", func(t *testing.T) {
		t.Parallel()
		f := &fakeFleet{sessions: map[string][]fakeSession{
			"a": {{err: down}},
			"b": {{steps: []readStep{witnessedFrame(1, failoverT0)}}, {}},
		}}
		cfg := timeModeConfig(f, "https://b")
		cfg.fromTip = true
		events, _ := runConsumer(t, cfg, 1)
		require.Equal(t, []uint64{1}, seqs(events))
		require.Equal(t, []string{"a -", "b -"}, f.dialLog()[:2], "a from-tip start stays from-tip on the next host")
	})

	t.Run("after delivery", func(t *testing.T) {
		t.Parallel()
		f := &fakeFleet{sessions: map[string][]fakeSession{
			"a": {{steps: []readStep{witnessedFrame(10, failoverT0)}}, {err: down}},
			"b": {{steps: []readStep{witnessedFrame(3, failoverT0)}}, {}},
		}}
		cfg := timeModeConfig(f, "https://b")
		cfg.cursor, cfg.dedupFloor = 9, 9
		events, _ := runConsumer(t, cfg, 2)
		require.Equal(t, []uint64{10, 3}, seqs(events))
		require.Equal(t, []string{"a 9", "a 10", "b " + tsCursor(failoverT0, 5*time.Second)}, f.dialLog()[:3])
	})

	t.Run("failing back resumes by time", func(t *testing.T) {
		t.Parallel()
		f := &fakeFleet{sessions: map[string][]fakeSession{
			"a": {{steps: []readStep{witnessedFrame(10, failoverT0)}}, {err: down}, {steps: []readStep{witnessedFrame(11, failoverT0+2)}}, {}},
			"b": {{steps: []readStep{witnessedFrame(3, failoverT0+1)}}, {err: down}},
		}}
		cfg := timeModeConfig(f, "https://b")
		cfg.cursor, cfg.dedupFloor = 9, 9
		events, _ := runConsumer(t, cfg, 3)
		require.Equal(t, []uint64{10, 3, 11}, seqs(events))
		require.Equal(t, []string{
			"a 9", "a 10", "b " + tsCursor(failoverT0, 5*time.Second),
			"b 3", "a " + tsCursor(failoverT0+1, 5*time.Second),
		}, f.dialLog()[:5], "a seq from b is never sent to a")
	})

	t.Run("cutover uses the backfill's witnessed floor", func(t *testing.T) {
		t.Parallel()
		f := &fakeFleet{sessions: map[string][]fakeSession{
			"a": {{err: down}},
			"b": {{steps: []readStep{witnessedFrame(3, failoverT0)}}, {}},
		}}
		cfg := timeModeConfig(f, "https://b")
		cfg.cursor, cfg.dedupFloor, cfg.archiveNS, cfg.witnessedFloor = 50, 50, true, failoverT0
		events, _ := runConsumer(t, cfg, 1)
		require.Equal(t, []uint64{3}, seqs(events))
		require.Equal(t, []string{"a 50", "b " + tsCursor(failoverT0, 5*time.Second)}, f.dialLog()[:2])
	})
}

// A connection that drops mid-stream retries the same host first.
func TestLiveFailoverReadErrorRetriesSameHost(t *testing.T) {
	t.Parallel()
	f := &fakeFleet{sessions: map[string][]fakeSession{
		"a": {
			{steps: []readStep{witnessedFrame(10, failoverT0), {err: errors.New("reset")}}},
			{steps: []readStep{witnessedFrame(11, failoverT0+1)}},
			{},
		},
		"b": {{}},
	}}
	cfg := timeModeConfig(f, "https://b")
	cfg.cursor, cfg.dedupFloor = 9, 9
	events, _ := runConsumer(t, cfg, 2)
	require.Equal(t, []uint64{10, 11}, seqs(events))
	require.Equal(t, []string{"a 9", "a 10"}, f.dialLog()[:2])
}

// Without witnessedAt (an older server) there is nowhere to resume from on
// another host, so the consumer must stay on its seq namespace.
func TestLiveFailoverWithoutWitnessedStaysPut(t *testing.T) {
	t.Parallel()
	down := errors.New("connection refused")
	f := &fakeFleet{sessions: map[string][]fakeSession{
		"a": {
			{steps: []readStep{{data: liveCommitFrame(t, 10, "did:plc:a", "delete", "c", "r", false)}}},
			{err: down},
			{err: down},
			{steps: []readStep{{data: liveCommitFrame(t, 11, "did:plc:a", "delete", "c", "r", false)}}},
			{},
		},
		"b": {{}},
	}}
	cfg := timeModeConfig(f, "https://b")
	cfg.cursor, cfg.dedupFloor = 9, 9
	events, _ := runConsumer(t, cfg, 2)
	require.Equal(t, []uint64{10, 11}, seqs(events))
	for _, d := range f.dialLog() {
		require.NotContains(t, d, "b ", "must not fail over without a witnessed time")
	}
}

// CursorTooOld may only drive a re-backfill when the rejected seq is in the
// archive's namespace (the primary host, never failed away from); otherwise the consumer resumes by time.
func TestLiveFailoverCursorTooOld(t *testing.T) {
	t.Parallel()

	t.Run("non-archive seq resumes by time", func(t *testing.T) {
		t.Parallel()
		f := &fakeFleet{sessions: map[string][]fakeSession{"a": {
			{steps: []readStep{witnessedFrame(100, failoverT0)}},
			{err: errLiveCursorTooOld},
			{steps: []readStep{witnessedFrame(7, failoverT0)}},
			{},
		}}}
		cfg := timeModeConfig(f)
		cfg.cursor, cfg.dedupFloor = 99, 99
		events, _ := runConsumer(t, cfg, 2)
		require.Equal(t, []uint64{100, 7}, seqs(events))
		require.Equal(t, "a "+tsCursor(failoverT0, 5*time.Second), f.dialLog()[2])
	})

	t.Run("archive seq re-backfills", func(t *testing.T) {
		t.Parallel()
		f := &fakeFleet{sessions: map[string][]fakeSession{"a": {
			{steps: []readStep{witnessedFrame(100, failoverT0)}},
			{err: errLiveCursorTooOld},
		}}}
		cfg := timeModeConfig(f)
		cfg.cursor, cfg.dedupFloor, cfg.archiveNS, cfg.backoffMin = 99, 99, true, time.Millisecond
		c := newLiveConsumer(cfg)
		err := c.Run(context.Background(), func(*Event, error) bool { return true })
		require.ErrorIs(t, err, errLiveCursorTooOld)
		require.Equal(t, uint64(100), c.LastSeq())
	})

	t.Run("foreign namespace never re-backfills", func(t *testing.T) {
		t.Parallel()
		f := &fakeFleet{sessions: map[string][]fakeSession{
			"a": {{err: errors.New("down")}},
			"b": {
				{steps: []readStep{witnessedFrame(7, failoverT0)}},
				{err: errLiveCursorTooOld},
				{steps: []readStep{witnessedFrame(8, failoverT0+1)}},
				{},
			},
		}}
		cfg := timeModeConfig(f, "https://b")
		cfg.cursor, cfg.dedupFloor, cfg.archiveNS, cfg.witnessedFloor = 50, 50, true, failoverT0-1
		events, _ := runConsumer(t, cfg, 2)
		require.Equal(t, []uint64{7, 8}, seqs(events))
		require.Equal(t, "b "+tsCursor(failoverT0, 5*time.Second), f.dialLog()[3])
	})
}

func TestLiveFailoverOutdatedCursorSurfacesClamp(t *testing.T) {
	t.Parallel()
	info := readStep{data: []byte(`{"$type":"message","payload":{"$type":"network.bsky.jetstream.subscribeEvents#info"` +
		`,"name":"OutdatedCursor","message":"starting at seq 200"}}`)}
	for _, timeMode := range []bool{true, false} {
		f := &fakeFleet{sessions: map[string][]fakeSession{"a": {
			{steps: []readStep{info, witnessedFrame(200, failoverT0)}},
			{},
		}}}
		cfg := timeModeConfig(f)
		cfg.timeMode = timeMode
		cfg.cursor = uint64(failoverT0)
		events, errs := runConsumer(t, cfg, 1)
		require.Equal(t, []uint64{200}, seqs(events))
		var clamped bool
		for _, err := range errs {
			clamped = clamped || errors.Is(err, ErrCursorClamped)
		}
		require.Equal(t, timeMode, clamped, "only time mode surfaces the clamp")
	}
}

func TestCursorModeConfig(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig()
	WithFailoverHosts("https://b")(&cfg)
	require.ErrorContains(t, validateConfig(&cfg), "CursorTime")

	cfg = defaultConfig()
	WithCursorMode(CursorTime)(&cfg)
	WithFailoverHosts("b.example")(&cfg)
	require.NoError(t, validateConfig(&cfg))
	require.Equal(t, []string{"https://b.example"}, cfg.failoverHosts)

	cfg = defaultConfig()
	WithCursorMode(CursorMode(9))(&cfg)
	require.Error(t, validateConfig(&cfg))
}

func TestLastCursorFollowsMode(t *testing.T) {
	t.Parallel()
	events := []Event{{Seq: 7, WitnessedAtUS: 300}, {Seq: 9, WitnessedAtUS: 200}}
	require.Equal(t, uint64(9), (&Batch{events: events}).LastCursor())
	require.Equal(t, uint64(300), (&Batch{events: events, mode: CursorTime}).LastCursor())
}

func TestRewindTimeCursor(t *testing.T) {
	t.Parallel()
	require.Equal(t, uint64(failoverT0-5_000_000), rewindTimeCursor(uint64(failoverT0), 5*time.Second))
	floor := rewindTimeCursor(1_000_000_000_000_001, time.Hour)
	require.Equal(t, uint64(1_000_000_000_000_000), floor, "never rewinds into the seq namespace")
}
