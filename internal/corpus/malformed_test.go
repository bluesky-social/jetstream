package corpus

import (
	"sync/atomic"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// frameMeta extracts the upstream seq for each captured frame from the
// manifest's contiguous range: frame i carries seq SeqFirst+i. The
// capture tool verified contiguity, and TestCorpusFirehoseReplay
// re-proves it end to end, so indexing is safe here.
func frameSeq(m manifest, i int) int64 { return m.SeqFirst + int64(i) }

// buildSeqEventCounts replays the clean window once and returns, per
// upstream seq, how many v1-visible events that frame produced and
// which DID it belongs to. expected_v1.jsonl cannot provide this (v1
// commit events carry no upstream seq), so the baseline comes from the
// clean replay's live stream, whose events retain UpstreamRelayCursor.
func buildSeqEventCounts(t *testing.T, m manifest, frames [][]byte, docs map[string][]byte) (map[int64]int, map[int64]string) {
	t.Helper()
	_, live, _, _ := runCorpusConsumer(t, frames, docs, m.V1Events, nil)
	counts := make(map[int64]int, len(live))
	dids := make(map[int64]string, len(live))
	for _, ev := range live {
		require.Positive(t, ev.UpstreamRelayCursor, "live event missing upstream cursor attribution")
		counts[ev.UpstreamRelayCursor]++
		dids[ev.UpstreamRelayCursor] = ev.DID
	}
	return counts, dids
}

// TestCorpusMalformedFrames corrupts single frames of the real
// captured window and requires the production drop-and-continue
// contract: the poisoned frame's events are lost (with the decode
// error or verification failure counted), and every event from every
// OTHER frame is still archived. One malformed upstream event must
// never take down or stall the firehose consumer (AGENTS.md:
// never-crash on external input; the m009-adjacent blind spot for
// frame handling).
//
// The corruptions are derived mechanically from real bytes rather than
// synthesized, so the shapes entering the decoder are exactly
// production-shaped garbage: a production frame truncated mid-CBOR and
// a production frame with payload bytes flipped.
func TestCorpusMalformedFrames(t *testing.T) {
	t.Parallel()

	m := loadManifest(t)
	frames := loadFrames(t)
	require.Len(t, frames, m.Frames)
	docs := loadDIDDocs(t)

	seqEvents, seqDID := buildSeqEventCounts(t, m, frames, docs)

	// Count how many frames each DID appears on so the poison pick can
	// avoid multi-commit DIDs: dropping an earlier commit of a DID
	// that commits again later in the window would chain-break the
	// later commit and cascade extra drops, making wantEvents
	// fixture-fragile across re-captures.
	didFrames := map[string]int{}
	for _, did := range seqDID {
		didFrames[did]++
	}

	// Pick the event-richest frame whose DID appears exactly once.
	// Skip index 0 so the poisoned frame has predecessors whose events
	// must survive.
	poisonIdx, poisonEvents := -1, 0
	for i := 1; i < len(frames)-1; i++ {
		seq := frameSeq(m, i)
		if didFrames[seqDID[seq]] != 1 {
			continue
		}
		if n := seqEvents[seq]; n > poisonEvents {
			poisonIdx, poisonEvents = i, n
		}
	}
	require.Positive(t, poisonEvents, "no single-DID frame with attributable events found")

	corruptions := map[string]func([]byte) []byte{
		// Cut mid-body: the CBOR header parses, the body reader hits EOF.
		"truncated": func(raw []byte) []byte {
			return append([]byte(nil), raw[:len(raw)*2/3]...)
		},
		// Flip bytes deep in the CAR payload: frame decodes or fails,
		// but whatever survives must not verify as authentic.
		"bitflip": func(raw []byte) []byte {
			mutated := append([]byte(nil), raw...)
			for off := len(mutated) * 3 / 4; off < len(mutated)*3/4+8 && off < len(mutated); off++ {
				mutated[off] ^= 0xff
			}
			return mutated
		},
	}

	for name, corrupt := range corruptions {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mutated := make([][]byte, len(frames))
			copy(mutated, frames)
			mutated[poisonIdx] = corrupt(frames[poisonIdx])

			wantEvents := m.V1Events - poisonEvents
			var verifyFailures atomic.Int64
			events, live, metrics, _ := runCorpusConsumer(t, mutated, docs, wantEvents,
				func(atmos.DID, error) { verifyFailures.Add(1) })

			require.Len(t, events, wantEvents,
				"all events from non-poisoned frames must be archived")

			// The failure must be observable, not silent: either the
			// frame failed to decode or its content failed
			// verification.
			decodeErrs := testutil.ToFloat64(metrics.DecodeErrors)
			require.Positive(t, decodeErrs+float64(verifyFailures.Load()),
				"poisoned frame was absorbed with no decode error and no verification failure")

			// And nothing from the poisoned frame may have slipped
			// through. UpstreamRelayCursor is memory-only, so the check
			// runs on the live-delivered stream (which is what fed the
			// archive: len(events) == len(live) == wantEvents).
			for _, ev := range live {
				require.NotEqual(t, frameSeq(m, poisonIdx), ev.UpstreamRelayCursor,
					"event from the poisoned frame reached the archive")
			}
		})
	}
}
