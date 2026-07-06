package oracle

import (
	"context"
	"testing"
	"time"

	simhttp "github.com/bluesky-social/jetstream/internal/simulator/http"
	"github.com/stretchr/testify/require"
)

func TestOracle_LiveVerifierPLCFaultDoesNotWedgeStream(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	h := newLiveTailHarness(t, ctx)

	faulted, err := h.World.LoadAccount(0)
	require.NoError(t, err)
	sibling, err := h.World.LoadAccount(1)
	require.NoError(t, err)
	h.Faults.AddPLCFault(string(faulted.DID), simhttp.PLCFaultResolutionFailure, 1)

	_, _, err = h.World.GenerateRecordOpForTest(ctx, 0, "create", "app.bsky.feed.post", "plc-faulted")
	require.NoError(t, err)
	_, siblingOp, err := h.World.GenerateRecordOpForTest(ctx, 1, "create", "app.bsky.feed.post", "plc-sibling")
	require.NoError(t, err)

	h.StartConsumer(t, ctx)
	require.Eventually(t, func() bool {
		if h.Faults.PLCFaultsFired(string(faulted.DID)) != 1 {
			return false
		}
		for _, ev := range h.Recorder.snapshotEvents() {
			if ev.DID == string(sibling.DID) && ev.Collection == siblingOp.Collection && ev.Rkey == siblingOp.Rkey {
				return true
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond, "PLC fault fired but sibling DID did not continue archiving")
	h.StopConsumer(t)

	require.Equal(t, 1, h.Faults.PLCFaultsFired(string(faulted.DID)))
}
