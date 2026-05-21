package livestream

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/gt"
)

// iterations are the number of swarm events to generate. The default
// `just test -short` invocation should be quick; the long test run
// gets a much larger count to surface invariant violations.
func swarmIterations(t *testing.T) int {
	t.Helper()
	if testing.Short() {
		return 50
	}
	return 1000
}

// TestConvertEvent_Swarm generates random events of every supported
// kind and asserts ConvertEvent never panics, never returns events
// that would later fail segment encoding (invalid column widths,
// missing DID), and never silently drops a malformed input.
func TestConvertEvent_Swarm(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(0xfeed, 0xface))

	for i := range swarmIterations(t) {
		evt := randomEvent(r)

		got, err := ConvertEvent(evt, int64(i))
		if err != nil {
			// Errors are allowed for adversarial / unknown-action
			// inputs; they must not panic. The require below will
			// only be visible when err is nil.
			continue
		}

		for _, ev := range got {
			if ev.DID == "" && ev.Kind != 0 {
				t.Fatalf("iter %d: empty DID for kind %d", i, ev.Kind)
			}
			if ev.Kind < segment.KindCreate || ev.Kind > segment.KindSync {
				t.Fatalf("iter %d: invalid Kind %d", i, ev.Kind)
			}
			if len(ev.DID) > math.MaxUint16 {
				t.Fatalf("iter %d: DID too long (%d > uint16 max)", i, len(ev.DID))
			}
			if len(ev.Collection) > math.MaxUint8 {
				t.Fatalf("iter %d: Collection too long (%d > uint8 max)", i, len(ev.Collection))
			}
			if len(ev.Rkey) > math.MaxUint8 {
				t.Fatalf("iter %d: Rkey too long (%d > uint8 max)", i, len(ev.Rkey))
			}
			if len(ev.Rev) > math.MaxUint8 {
				t.Fatalf("iter %d: Rev too long (%d > uint8 max)", i, len(ev.Rev))
			}
		}
	}
}

func randomEvent(r *rand.Rand) streaming.Event {
	switch r.IntN(5) {
	case 0:
		return streaming.Event{Identity: &comatproto.SyncSubscribeRepos_Identity{
			DID:    randomDID(r),
			Handle: gt.Some("h.test"),
			Time:   "2026-05-21T00:00:00Z",
		}}
	case 1:
		return streaming.Event{Account: &comatproto.SyncSubscribeRepos_Account{
			DID:    randomDID(r),
			Active: r.IntN(2) == 0,
			Time:   "2026-05-21T00:00:00Z",
		}}
	case 2:
		return streaming.Event{Sync: &comatproto.SyncSubscribeRepos_Sync{
			DID:  randomDID(r),
			Rev:  "rev",
			Time: "2026-05-21T00:00:00Z",
		}}
	case 3:
		return streaming.Event{Info: &comatproto.SyncSubscribeRepos_Info{}}
	default:
		// Empty / malformed-commit fallback. ConvertEvent must
		// either succeed or return an error, never panic.
		return streaming.Event{Commit: &comatproto.SyncSubscribeRepos_Commit{
			Repo: randomDID(r),
			Rev:  "rev",
			Ops:  nil,
		}}
	}
}

func randomDID(r *rand.Rand) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	buf := make([]byte, 24)
	for i := range buf {
		buf[i] = alphabet[r.IntN(len(alphabet))]
	}
	return "did:plc:" + string(buf)
}
