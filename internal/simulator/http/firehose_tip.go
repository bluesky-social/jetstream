package http

import (
	"encoding/json"
	"net/http"

	"github.com/bluesky-social/jetstream/internal/simulator/world"
)

// oracleFirehoseTipURLPath is the request path of the read-only endpoint
// that reports the world's current firehose tip seq. It is namespaced off
// the atproto xrpc surface ("/_oracle/…") so it can never collide with a
// real lexicon method, and is mounted only when HandlerOptions.
// EnableFirehoseTip is set (the restart oracle harness; never production).
const oracleFirehoseTipURLPath = "/_oracle/firehose-tip"

type firehoseTipOutput struct {
	Seq int64 `json:"seq"`
}

// newFirehoseTipHandler serves the current firehose tip seq as JSON.
// CurrentSeq reflects every live op the world has broadcast (it advances
// on the same atomic the commit/account/sync publishers bump), so a tip
// sampled after backfill drains spans the full generated chain. The
// restart oracle's pre-cutover gate queries this so a child can hold the
// cutover barrier until its bootstrap-live consumer has durably archived
// every frame up to the tip (see TestOracleRestartChild's
// BarrierBeforeCutover).
func newFirehoseTipHandler(w *world.World) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(rw).Encode(firehoseTipOutput{Seq: w.CurrentSeq()})
	})
}
