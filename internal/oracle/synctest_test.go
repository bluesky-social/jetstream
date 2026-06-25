package oracle

import (
	"sync/atomic"
	"time"
)

// synctestBubbleUsed guards against a second synctest bubble in the same
// process. The production zstd encoders (overlay/segment/subscribe) are package
// globals whose worker goroutines + channels bind to whichever synctest bubble
// first uses them; a second bubble in the same process (go test -count=N>1)
// then receives on the first bubble's channels and the runtime aborts with
// "receive on synctest channel from outside bubble". Re-runs must be separate
// `go test` invocations; the lifecycle test's guard turns the confusing fatal
// into a clear skip.
var synctestBubbleUsed atomic.Bool

// simulatorEpochMicros mirrors the simulator's logical-clock base
// (internal/simulator/world/logical_clock.go). Commit revs are stamped at or
// after this instant.
const simulatorEpochMicros int64 = 1_700_000_000_000_000

// advanceClockToSimulatorEpoch sleeps the synctest bubble clock from its
// 2000-01-01 start to just past the simulator's rev epoch (~2023), so atmos's
// verifier future-rev check (5m tolerance) passes. Sleeping is the
// synctest-sanctioned way to move the fake clock.
func advanceClockToSimulatorEpoch() {
	target := time.UnixMicro(simulatorEpochMicros).Add(time.Hour)
	if d := time.Until(target); d > 0 {
		time.Sleep(d)
	}
}
