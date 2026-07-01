package subscribe_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/stretchr/testify/require"
)

// TestResolveCursor_TranslateIOFaultIsResolveFailed verifies that a server-side
// segment read failure during timestamp-to-seq translation is classified as
// ErrCursorResolveFailed (5xx-class) rather than ErrInvalidCursor/ErrCursorTooOld
// (client-error, 400) — so the handler returns a retryable 503 and does not echo
// the internal segment path. The cursor is in-window and well-formed: the only
// fault is the missing segment file the manifest still references.
func TestResolveCursor_TranslateIOFaultIsResolveFailed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	segPath := filepath.Join(dir, "seg_0000000000.jss")
	mustWriteSealedSegment(t, segPath, sealedFixture{
		minSeq: 1, maxSeq: 9,
		minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(1*time.Hour/time.Microsecond),
		eventCount:     9,
	})
	m := mustOpenManifest(t, dir)

	// Remove the segment file AFTER the manifest cached its bounds, so the
	// translation's block-scan segment.Open fails on a well-formed in-window
	// cursor (a stand-in for a corrupt/transiently-unreadable sealed file).
	require.NoError(t, os.Remove(segPath))

	// A cursor strictly inside the segment's witnessed-at range routes past the
	// older/newer-than-all short-circuits into the block scan that opens the file.
	cursor := now - int64(5*time.Hour/time.Microsecond)
	_, err := subscribe.ResolveCursor(strconv.FormatInt(cursor, 10), subscribe.CursorEnv{
		Manifest: m,
		NextSeq:  10,
		Lookback: 36 * time.Hour,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, subscribe.ErrCursorResolveFailed,
		"a segment-read fault during translation must be a server resolve failure, not a client error")
	require.NotErrorIs(t, err, subscribe.ErrInvalidCursor)
	require.NotErrorIs(t, err, subscribe.ErrCursorTooOld)
}

func TestResolveCursor_EmptyMeansLive(t *testing.T) {
	t.Parallel()
	p, err := subscribe.ResolveCursor("", subscribe.CursorEnv{})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeLive, p.Mode)
}

// TestResolveCursor_ZeroSeqFloorsToOne guards the bufferless cutover's
// empty-archive path: the live consumer sends cursor=0 to mean "replay from the
// first event". Seq 0 is the pure "nothing yet" sentinel (design §R8) and is
// never allocated, so the lowest real event is seq 1. Once the writer has
// started (NextSeq>=1), cursor=0 must resolve to a seq replay floored to
// StartSeq=1 — NOT StartSeq=0, which would dive the cold reader into an empty
// (0, ...] range and return a non-advancing next==0 that disconnects the
// subscriber and spins the reconnect loop.
func TestResolveCursor_ZeroSeqFloorsToOne(t *testing.T) {
	t.Parallel()
	p, err := subscribe.ResolveCursor("0", subscribe.CursorEnv{NextSeq: 1})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeReplaySeq, p.Mode,
		"cursor=0 with a started writer is a replay from the start, not live")
	require.Equal(t, uint64(1), p.StartSeq,
		"seq 0 is a sentinel; replay must floor to the first real event (seq 1)")
	require.True(t, p.Clamped, "flooring 0 up to 1 is a clamp")
}

// TestResolveCursor_TimestampEmptyArchiveFloorsToOne is the timestamp-path
// sibling of the seq-cursor floor: a v1 unix-micros cursor on an archive with no
// sealed segments translates to StartSeq=0 (translateTimeUSToSeq's no-segments
// branch). Seq 0 is the "nothing yet" sentinel and is never a valid replay
// start — left at 0 the cold reader returns a non-advancing next==0 and
// disconnects the subscriber. The resolver must floor it to 1.
func TestResolveCursor_TimestampEmptyArchiveFloorsToOne(t *testing.T) {
	t.Parallel()
	// A past timestamp in the v1 namespace (>= CursorSeqMaxThreshold), with no
	// manifest: translation hits the no-sealed-segments branch and returns 0.
	pastMicros := time.Now().Add(-time.Hour).UnixMicro()
	p, err := subscribe.ResolveCursor(strconv.FormatInt(pastMicros, 10), subscribe.CursorEnv{
		NextSeq: 100,
	})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeReplayTimeUS, p.Mode)
	require.Equal(t, uint64(1), p.StartSeq,
		"a timestamp cursor on an empty archive must floor to seq 1, not the seq-0 sentinel")
	require.True(t, p.Clamped)
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

// TestResolveCursor_ZeroNextSeqDropsToLive pins the CursorEnv.NextSeq contract:
// NextSeq==0 means the writer has not started, so any finite seq cursor is "in
// the future" and resolves to live — even with a manifest floor and
// RejectBelowFloor set, which would otherwise return ErrCursorTooOld. Without
// the NextSeq==0 short-circuit the cursor falls through to the floor logic and
// a below-floor cursor 400s instead of dropping to live, contradicting the doc.
func TestResolveCursor_ZeroNextSeqDropsToLive(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000001.jss"), sealedFixture{
		minSeq: 200, maxSeq: 299,
		minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(1*time.Hour/time.Microsecond),
		eventCount:     10,
	})
	m := mustOpenManifest(t, dir)

	// Cursor 50 is below the floor (200); with NextSeq>0 and RejectBelowFloor
	// this would return ErrCursorTooOld. NextSeq==0 must short-circuit to live.
	p, err := subscribe.ResolveCursor("50", subscribe.CursorEnv{
		Manifest:         m,
		NextSeq:          0,
		Lookback:         36 * time.Hour,
		RejectBelowFloor: true,
	})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeLive, p.Mode)
	require.True(t, p.Clamped, "future-cursor drop-to-live is reported as a clamp")
}

func TestResolveCursor_FutureTimestampDropsToLive(t *testing.T) {
	t.Parallel()
	now := time.Now().UnixMicro()
	future := now + int64(24*time.Hour/time.Microsecond)
	p, err := subscribe.ResolveCursor(strconv.FormatInt(future, 10), subscribe.CursorEnv{NextSeq: 100})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeLive, p.Mode)
}

func TestResolveCursor_SeqBelowFloorClamped(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 100, maxSeq: 199,
		minWitnessedAt: now - int64(50*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(40*time.Hour/time.Microsecond),
		eventCount:     10,
	})
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000001.jss"), sealedFixture{
		minSeq: 200, maxSeq: 299,
		minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(1*time.Hour/time.Microsecond),
		eventCount:     10,
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

// TestResolveCursor_SeqBelowFloorRejectedWhenRejectBelowFloor is the §14/D5
// v2 contract: with RejectBelowFloor set, a v2 seq cursor that resolves below
// the lookback floor returns a typed ErrCursorTooOld carrying both the
// requested seq and the floor seq — instead of the v1 silent clamp — so the
// client can re-backfill from its last seq rather than silently skipping
// (requestedSeq, floor].
func TestResolveCursor_SeqBelowFloorRejectedWhenRejectBelowFloor(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 100, maxSeq: 199,
		minWitnessedAt: now - int64(50*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(40*time.Hour/time.Microsecond),
		eventCount:     10,
	})
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000001.jss"), sealedFixture{
		minSeq: 200, maxSeq: 299,
		minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(1*time.Hour/time.Microsecond),
		eventCount:     10,
	})
	m := mustOpenManifest(t, dir)

	_, err := subscribe.ResolveCursor("50", subscribe.CursorEnv{
		Manifest:         m,
		NextSeq:          300,
		Lookback:         36 * time.Hour,
		RejectBelowFloor: true,
	})
	require.ErrorIs(t, err, subscribe.ErrCursorTooOld)
	// Both the requested seq and the floor seq must be in the message so the
	// client can log how far behind it was and re-backfill from its last seq.
	require.Contains(t, err.Error(), "50")
	require.Contains(t, err.Error(), "200")
}

// TestResolveCursor_SeqBelowFloorClampsWhenV1 pins the v1 parity guarantee:
// with RejectBelowFloor unset (the v1 default), a below-floor seq cursor is
// still silently clamped to the floor with no error — the legacy jetstream-v1
// wire contract is unchanged.
func TestResolveCursor_SeqBelowFloorClampsWhenV1(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000001.jss"), sealedFixture{
		minSeq: 200, maxSeq: 299,
		minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(1*time.Hour/time.Microsecond),
		eventCount:     10,
	})
	m := mustOpenManifest(t, dir)

	p, err := subscribe.ResolveCursor("50", subscribe.CursorEnv{
		Manifest: m,
		NextSeq:  300,
		Lookback: 36 * time.Hour,
		// RejectBelowFloor defaults false (v1).
	})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeReplaySeq, p.Mode)
	require.Equal(t, uint64(200), p.StartSeq)
	require.True(t, p.Clamped)
}

// TestResolveCursor_TimeUSBelowFloorClampsEvenWhenRejectBelowFloor pins the
// intentional asymmetry: RejectBelowFloor governs only the v2 SEQ path. A
// timestamp cursor (v1-style legacy translation) keeps clamping under both
// endpoints, because rejecting a legacy timestamp would break the v1 contract
// that a too-old timestamp simply starts at the oldest retained event.
func TestResolveCursor_TimeUSBelowFloorClampsEvenWhenRejectBelowFloor(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 100, maxSeq: 199,
		minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(5*time.Hour/time.Microsecond),
		eventCount:     10,
	})
	m := mustOpenManifest(t, dir)

	cursor := now - int64(72*time.Hour/time.Microsecond)
	p, err := subscribe.ResolveCursor(strconv.FormatInt(cursor, 10), subscribe.CursorEnv{
		Manifest:         m,
		NextSeq:          200,
		Lookback:         36 * time.Hour,
		RejectBelowFloor: true,
	})
	require.NoError(t, err, "timestamp path never rejects, even under RejectBelowFloor")
	require.Equal(t, subscribe.ModeReplayTimeUS, p.Mode)
	require.True(t, p.Clamped)
	require.Equal(t, uint64(100), p.StartSeq)
}

func TestResolveCursor_SeqAboveFloorPreserved(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000001.jss"), sealedFixture{
		minSeq: 200, maxSeq: 299,
		minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(1*time.Hour/time.Microsecond),
		eventCount:     10,
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
		minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(1*time.Hour/time.Microsecond),
		eventCount:     10,
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
		minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(1*time.Hour/time.Microsecond),
		eventCount:     10,
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
		minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(1*time.Hour/time.Microsecond),
		eventCount:     10,
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
		minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(5*time.Hour/time.Microsecond),
		eventCount:     10,
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
		minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(5*time.Hour/time.Microsecond),
		eventCount:     10,
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
	// Seqs are 1-based (design §R8): the first real event is seq 1, so a real
	// segment's MinSeq is never 0. The threshold timestamp is older than this
	// segment, so translation clamps to the first segment's MinSeq (1).
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 1, maxSeq: 9,
		minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(5*time.Hour/time.Microsecond),
		eventCount:     9,
	})
	m := mustOpenManifest(t, dir)

	p, err := subscribe.ResolveCursor("1000000000000000", subscribe.CursorEnv{
		Manifest: m, NextSeq: 10, Lookback: 36 * time.Hour,
	})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeReplayTimeUS, p.Mode)
	require.True(t, p.Clamped)
	require.Equal(t, uint64(1), p.StartSeq)
}
