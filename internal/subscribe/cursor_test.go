package subscribe_test

import (
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/subscribe"
	"github.com/stretchr/testify/require"
)

func TestResolveCursor_EmptyMeansLive(t *testing.T) {
	t.Parallel()
	p, err := subscribe.ResolveCursor("", subscribe.CursorEnv{})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeLive, p.Mode)
}

func TestResolveCursor_NonNumericRejected(t *testing.T) {
	t.Parallel()
	_, err := subscribe.ResolveCursor("abc", subscribe.CursorEnv{})
	require.ErrorIs(t, err, subscribe.ErrInvalidCursor)
}

func TestResolveCursor_NegativeRejected(t *testing.T) {
	t.Parallel()
	_, err := subscribe.ResolveCursor("-42", subscribe.CursorEnv{})
	require.ErrorIs(t, err, subscribe.ErrInvalidCursor)
}

func TestResolveCursor_FutureSeqDropsToLive(t *testing.T) {
	t.Parallel()
	p, err := subscribe.ResolveCursor("999999", subscribe.CursorEnv{NextSeq: 1000})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeLive, p.Mode)
	require.True(t, p.Clamped, "Clamped is informational here; future-cursor is a special clamp case")
}

func TestResolveCursor_FutureTimestampDropsToLive(t *testing.T) {
	t.Parallel()
	now := time.Now().UnixMicro()
	future := now + int64(24*time.Hour/time.Microsecond)
	p, err := subscribe.ResolveCursor(strconv.FormatInt(future, 10), subscribe.CursorEnv{NextSeq: 100})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeLive, p.Mode)
}

func TestResolveCursor_ThresholdConstantStable(t *testing.T) {
	t.Parallel()
	require.Equal(t, uint64(1_000_000_000_000_000), subscribe.CursorSeqMaxThreshold)
}

func TestResolveCursor_SeqBelowFloorClamped(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 100, maxSeq: 199,
		minIndexedAt: now - int64(50*time.Hour/time.Microsecond),
		maxIndexedAt: now - int64(40*time.Hour/time.Microsecond),
		eventCount:   10,
	})
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000001.jss"), sealedFixture{
		minSeq: 200, maxSeq: 299,
		minIndexedAt: now - int64(10*time.Hour/time.Microsecond),
		maxIndexedAt: now - int64(1*time.Hour/time.Microsecond),
		eventCount:   10,
	})
	m := mustOpenManifest(t, dir)

	p, err := subscribe.ResolveCursor("50", subscribe.CursorEnv{
		Manifest: m,
		NextSeq:  300,
		Lookback: 36 * time.Hour,
	})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeReplaySeq, p.Mode)
	require.Equal(t, uint64(200), p.StartSeq)
	require.True(t, p.Clamped)
}

func TestResolveCursor_SeqAboveFloorPreserved(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000001.jss"), sealedFixture{
		minSeq: 200, maxSeq: 299,
		minIndexedAt: now - int64(10*time.Hour/time.Microsecond),
		maxIndexedAt: now - int64(1*time.Hour/time.Microsecond),
		eventCount:   10,
	})
	m := mustOpenManifest(t, dir)

	p, err := subscribe.ResolveCursor("250", subscribe.CursorEnv{
		Manifest: m,
		NextSeq:  300,
		Lookback: 36 * time.Hour,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(250), p.StartSeq)
	require.False(t, p.Clamped)
}

func TestResolveCursor_ZeroSeqClampsToFloor(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000001.jss"), sealedFixture{
		minSeq: 200, maxSeq: 299,
		minIndexedAt: now - int64(10*time.Hour/time.Microsecond),
		maxIndexedAt: now - int64(1*time.Hour/time.Microsecond),
		eventCount:   10,
	})
	m := mustOpenManifest(t, dir)

	p, err := subscribe.ResolveCursor("0", subscribe.CursorEnv{
		Manifest: m,
		NextSeq:  300,
		Lookback: 36 * time.Hour,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(200), p.StartSeq)
	require.True(t, p.Clamped)
}

func TestResolveCursor_ZeroLookbackDisablesClamp(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000001.jss"), sealedFixture{
		minSeq: 200, maxSeq: 299,
		minIndexedAt: now - int64(10*time.Hour/time.Microsecond),
		maxIndexedAt: now - int64(1*time.Hour/time.Microsecond),
		eventCount:   10,
	})
	m := mustOpenManifest(t, dir)

	p, err := subscribe.ResolveCursor("50", subscribe.CursorEnv{
		Manifest: m,
		NextSeq:  300,
		Lookback: 0,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(50), p.StartSeq, "no clamp when lookback==0")
	require.False(t, p.Clamped)
}

func TestResolveCursor_TimeUSTranslatesToSeq(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 9,
		minIndexedAt: now - int64(10*time.Hour/time.Microsecond),
		maxIndexedAt: now - int64(1*time.Hour/time.Microsecond),
		eventCount:   10,
	})
	m := mustOpenManifest(t, dir)

	cursor := now - int64(5*time.Hour/time.Microsecond)
	p, err := subscribe.ResolveCursor(strconv.FormatInt(cursor, 10), subscribe.CursorEnv{
		Manifest: m,
		NextSeq:  10,
		Lookback: 36 * time.Hour,
	})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeReplayTimeUS, p.Mode)
	require.GreaterOrEqual(t, p.StartSeq, uint64(0))
	require.LessOrEqual(t, p.StartSeq, uint64(9))
}

func TestResolveCursor_TimeUSNewerThanAllSegments(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 9,
		minIndexedAt: now - int64(10*time.Hour/time.Microsecond),
		maxIndexedAt: now - int64(5*time.Hour/time.Microsecond),
		eventCount:   10,
	})
	m := mustOpenManifest(t, dir)

	cursor := now - int64(1*time.Hour/time.Microsecond)
	p, err := subscribe.ResolveCursor(strconv.FormatInt(cursor, 10), subscribe.CursorEnv{
		Manifest: m,
		NextSeq:  20,
		Lookback: 36 * time.Hour,
	})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeReplayTimeUS, p.Mode)
	require.Equal(t, uint64(10), p.StartSeq, "starts at first non-sealed seq")
}

func TestResolveCursor_TimeUSOlderThanAllSegmentsClampsToFloor(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 100, maxSeq: 199,
		minIndexedAt: now - int64(10*time.Hour/time.Microsecond),
		maxIndexedAt: now - int64(5*time.Hour/time.Microsecond),
		eventCount:   10,
	})
	m := mustOpenManifest(t, dir)

	cursor := now - int64(72*time.Hour/time.Microsecond)
	p, err := subscribe.ResolveCursor(strconv.FormatInt(cursor, 10), subscribe.CursorEnv{
		Manifest: m,
		NextSeq:  200,
		Lookback: 36 * time.Hour,
	})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeReplayTimeUS, p.Mode)
	require.True(t, p.Clamped)
	require.Equal(t, uint64(100), p.StartSeq, "clamped to oldest sealed segment's MinSeq")
}

func TestResolveCursor_TimeUSAtThresholdExactly(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 0, maxSeq: 9,
		minIndexedAt: now - int64(10*time.Hour/time.Microsecond),
		maxIndexedAt: now - int64(5*time.Hour/time.Microsecond),
		eventCount:   10,
	})
	m := mustOpenManifest(t, dir)

	p, err := subscribe.ResolveCursor("1000000000000000", subscribe.CursorEnv{
		Manifest: m, NextSeq: 10, Lookback: 36 * time.Hour,
	})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeReplayTimeUS, p.Mode)
	require.True(t, p.Clamped)
	require.Equal(t, uint64(0), p.StartSeq)
}
