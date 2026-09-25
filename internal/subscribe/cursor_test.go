package subscribe_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/seqspace"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func mustSeqGaps(t *testing.T, ranges ...seqspace.Gap) *seqspace.Gaps {
	t.Helper()
	gaps, err := seqspace.NewGaps(ranges)
	require.NoError(t, err)
	return gaps
}

func TestResolveCursor_SeqGapClampMatrix(t *testing.T) {
	t.Parallel()
	gaps := mustSeqGaps(t, seqspace.Gap{Start: 100, End: 200})

	for _, tc := range []struct {
		name      string
		cursor    string
		wantStart uint64
		clamped   bool
	}{
		{name: "before", cursor: "99", wantStart: 99},
		{name: "at start", cursor: "100", wantStart: 200, clamped: true},
		{name: "inside", cursor: "150", wantStart: 200, clamped: true},
		{name: "at end", cursor: "200", wantStart: 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := subscribe.ResolveCursor(tc.cursor, subscribe.CursorEnv{
				NextSeq: 300,
				Gaps:    gaps,
			})
			require.NoError(t, err)
			require.Equal(t, subscribe.ModeReplaySeq, p.Mode)
			require.Equal(t, tc.wantStart, p.StartSeq)
			require.Equal(t, tc.clamped, p.Clamped)
			if tc.clamped {
				require.Equal(t, "gap", p.ClampReason)
			} else {
				require.Empty(t, p.ClampReason)
			}
		})
	}
}

// TestResolveCursor_TranslateIOFaultIsResolveFailed verifies that a server-side
// segment read failure during timestamp-to-seq translation is classified as
// ErrCursorResolveFailed (5xx-class) rather than ErrInvalidCursor/ErrCursorTooOld
// (client-error, 400) — so the handler returns a retryable 503 and does not echo
// the internal segment path. The cursor is in-window and well-formed: the only
// fault is the corrupt block frame the catalog still references.
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
	cat := mustCatalog(t, dir, nil)

	// Clobber the block's zstd frame in place, leaving the header (and so
	// the generation) intact: a stand-in for a corrupt sealed file.
	f, err := os.OpenFile(segPath, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt(make([]byte, 16), int64(segment.ReservedHeaderBytes)+8)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// A cursor strictly inside the segment's witnessed-at range routes past the
	// older/newer-than-all short-circuits into the block decode.
	cursor := now - int64(5*time.Hour/time.Microsecond)
	_, err = subscribe.ResolveCursor(strconv.FormatInt(cursor, 10), subscribe.CursorEnv{
		Manifest: m,
		Catalog:  cat,
		Fetcher:  cat.Fetcher(),
		NextSeq:  10,
		Lookback: 36 * time.Hour,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, subscribe.ErrCursorResolveFailed,
		"a segment-read fault during translation must be a server resolve failure, not a client error")
	require.NotErrorIs(t, err, subscribe.ErrInvalidCursor)
	require.NotErrorIs(t, err, subscribe.ErrCursorTooOld)
}

// TestResolveCursor_TimeUSResolvesInsideCandidateBlock pins exact
// translation through the catalog: the manifest picks the segment, the
// catalog's block index picks the block, and the decoded block yields the
// first seq witnessed at or after the cursor. Without a catalog the resolver
// falls back to the segment's MinSeq.
func TestResolveCursor_TimeUSResolvesInsideCandidateBlock(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	base := time.Now().Add(-10 * time.Hour).UnixMicro()
	sw, err := segment.New(segment.Config{Path: filepath.Join(dir, "seg_0000000000.jss"), MaxEventsPerBlock: 3})
	require.NoError(t, err)
	for seq := uint64(1); seq <= 12; seq++ {
		full, err := sw.Append(segment.Event{
			Seq: seq, WitnessedAt: base + int64(seq)*1_000, Kind: segment.KindCreate,
			DID: "did:plc:fixture", Collection: "app.bsky.feed.post", Rkey: "abc", Rev: "rev", Payload: []byte{0xa0},
		})
		require.NoError(t, err)
		if full {
			require.NoError(t, sw.Flush())
		}
	}
	_, err = sw.Seal()
	require.NoError(t, err)
	require.NoError(t, sw.Close())
	m := mustOpenManifest(t, dir)
	cat := mustCatalog(t, dir, nil)

	for _, tc := range []struct {
		offset int64
		want   uint64
	}{
		{offset: 2_000, want: 2}, // an exact hit in the first block
		{offset: 7_500, want: 8}, // between rows of the third block
		{offset: 9_001, want: 10},
		{offset: 12_000, want: 12},
	} {
		p, err := subscribe.ResolveCursor(strconv.FormatInt(base+tc.offset, 10), subscribe.CursorEnv{
			Manifest: m, Catalog: cat, Fetcher: cat.Fetcher(), NextSeq: 13,
		})
		require.NoError(t, err)
		require.Equal(t, tc.want, p.StartSeq, "offset %d", tc.offset)
	}

	p, err := subscribe.ResolveCursor(strconv.FormatInt(base+7_500, 10), subscribe.CursorEnv{Manifest: m, NextSeq: 13})
	require.NoError(t, err)
	require.Equal(t, uint64(1), p.StartSeq, "no catalog: the candidate segment's MinSeq")
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

func TestResolveCursor_TimestampEmptyArchiveClampsAcrossInitialGap(t *testing.T) {
	t.Parallel()
	pastMicros := time.Now().Add(-time.Hour).UnixMicro()
	p, err := subscribe.ResolveCursor(strconv.FormatInt(pastMicros, 10), subscribe.CursorEnv{
		NextSeq: 5,
		Gaps:    mustSeqGaps(t, seqspace.Gap{Start: 1, End: 5}),
	})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeReplayTimeUS, p.Mode)
	require.Equal(t, uint64(5), p.StartSeq)
	require.True(t, p.Clamped)
	require.Equal(t, "gap", p.ClampReason)
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

func TestResolveCursor_SeqGapEndingAtFloorIsNotTooOld(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000001.jss"), sealedFixture{
		minSeq: 200, maxSeq: 299,
		minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(time.Hour/time.Microsecond),
		eventCount:     10,
	})
	m := mustOpenManifest(t, dir)

	p, err := subscribe.ResolveCursor("150", subscribe.CursorEnv{
		Manifest:         m,
		NextSeq:          300,
		Lookback:         36 * time.Hour,
		RejectBelowFloor: true,
		Gaps:             mustSeqGaps(t, seqspace.Gap{Start: 100, End: 200}),
	})
	require.NoError(t, err, "a cursor whose entire path to the floor is a registered vacancy is not stale")
	require.Equal(t, uint64(200), p.StartSeq)
	require.True(t, p.Clamped)
	require.Equal(t, "gap", p.ClampReason)
}

func TestResolveCursor_SeqGapEndingBelowFloorIsTooOld(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000001.jss"), sealedFixture{
		minSeq: 200, maxSeq: 299,
		minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(time.Hour/time.Microsecond),
		eventCount:     10,
	})
	m := mustOpenManifest(t, dir)

	_, err := subscribe.ResolveCursor("120", subscribe.CursorEnv{
		Manifest:         m,
		NextSeq:          300,
		Lookback:         36 * time.Hour,
		RejectBelowFloor: true,
		Gaps:             mustSeqGaps(t, seqspace.Gap{Start: 100, End: 150}),
	})
	require.ErrorIs(t, err, subscribe.ErrCursorTooOld)
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
		ActiveTimeFloor: func(got int64) uint64 {
			require.Equal(t, cursor, got)
			return 15
		},
	})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeReplayTimeUS, p.Mode)
	require.Equal(t, uint64(15), p.StartSeq, "starts at the active candidate block, not the active segment floor")
}

func TestResolveCursor_TimeUSActiveOnly(t *testing.T) {
	t.Parallel()
	cursor := time.Now().Add(-time.Hour).UnixMicro()
	p, err := subscribe.ResolveCursor(strconv.FormatInt(cursor, 10), subscribe.CursorEnv{
		NextSeq: 50,
		ActiveTimeFloor: func(got int64) uint64 {
			require.Equal(t, cursor, got)
			return 42
		},
	})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeReplayTimeUS, p.Mode)
	require.Equal(t, uint64(42), p.StartSeq)
	require.False(t, p.Clamped)
}

func TestResolveCursor_TimeUSAfterActiveTipParksAtLiveEdge(t *testing.T) {
	t.Parallel()
	cursor := time.Now().Add(-time.Hour).UnixMicro()
	p, err := subscribe.ResolveCursor(strconv.FormatInt(cursor, 10), subscribe.CursorEnv{
		NextSeq: 20,
		ActiveTimeFloor: func(int64) uint64 {
			return 20
		},
	})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeReplayTimeUS, p.Mode)
	require.Equal(t, uint64(20), p.StartSeq)
}

func TestResolveCursor_TimeUSRotationRaceRechecksManifest(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	m := mustOpenManifest(t, dir)
	now := time.Now().UnixMicro()
	cursor := now - int64(5*time.Hour/time.Microsecond)
	called := false

	p, err := subscribe.ResolveCursor(strconv.FormatInt(cursor, 10), subscribe.CursorEnv{
		Manifest: m,
		NextSeq:  10,
		Lookback: 36 * time.Hour,
		ActiveTimeFloor: func(int64) uint64 {
			called = true
			path := filepath.Join(dir, "seg_0000000000.jss")
			mustWriteSealedSegment(t, path, sealedFixture{
				minSeq: 1, maxSeq: 9,
				minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
				maxWitnessedAt: now - int64(time.Hour/time.Microsecond),
				eventCount:     9,
			})
			require.NoError(t, manifest.ApplySegmentFile(m, nil, 0, path))
			// The new active generation also has a candidate, but the
			// just-sealed generation is earlier and must win.
			return 10
		},
	})
	require.NoError(t, err)
	require.True(t, called)
	require.GreaterOrEqual(t, p.StartSeq, uint64(1))
	require.LessOrEqual(t, p.StartSeq, uint64(9), "the just-sealed generation must be searched instead of skipped")
}

func TestResolveCursor_TimeUSTranslationLandingInGapClampsToEnd(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now().UnixMicro()
	mustWriteSealedSegment(t, filepath.Join(dir, "seg_0000000000.jss"), sealedFixture{
		minSeq: 1, maxSeq: 99,
		minWitnessedAt: now - int64(10*time.Hour/time.Microsecond),
		maxWitnessedAt: now - int64(5*time.Hour/time.Microsecond),
		eventCount:     10,
	})
	m := mustOpenManifest(t, dir)

	cursor := now - int64(time.Hour/time.Microsecond)
	p, err := subscribe.ResolveCursor(strconv.FormatInt(cursor, 10), subscribe.CursorEnv{
		Manifest: m,
		NextSeq:  300,
		Lookback: 36 * time.Hour,
		Gaps:     mustSeqGaps(t, seqspace.Gap{Start: 100, End: 200}),
	})
	require.NoError(t, err)
	require.Equal(t, subscribe.ModeReplayTimeUS, p.Mode)
	require.Equal(t, uint64(200), p.StartSeq)
	require.True(t, p.Clamped)
	require.Equal(t, "gap", p.ClampReason)
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
