package oracle

import (
	"testing"

	"github.com/bluesky-social/jetstream/internal/crashpoint"
	"github.com/stretchr/testify/require"
)

// TestOracle_RestartMultiSourceMergeCursorNoReprocess is the m003 kill tier:
// it forces bootstrap-live to rotate after every archived event, then kills the
// first child after a later source segment has been flushed but before that
// source cursor commit. Correct recovery resumes at the first uncommitted
// source. The m003 mutant records "last done" as the next-source cursor, so it
// reprocesses the already-committed source immediately before the crash.
//
// nolint:paralleltest
func TestOracle_RestartMultiSourceMergeCursorNoReprocess(t *testing.T) {
	run := runChainThroughCrashWithOptions(t, restartChainCrashOptions{
		label:                          "multi-source-merge-cursor",
		seedIdx:                        0,
		point:                          crashpoint.AfterMergeDstFlushBeforeSourceCommit,
		ordinal:                        9,
		accounts:                       1,
		minInitialRecords:              1,
		maxInitialRecords:              4,
		liveEventsBootstrap:            4,
		liveEventsSteady:               4,
		bootstrapLiveMaxSegmentBytes:   1,
		bootstrapLiveMaxEventsPerBlock: 1,
		minMergeSourceSegments:         9,
		captureCommittedSourceRows:     true,
	})

	assertOracleMatchesAfterReplay(t, run.dataDir, run.w, run.cfg, "multi-source-merge-cursor")
	assertChainDurable(t, run.dataDir, run.coord, "multi-source-merge-cursor")
	assertCommittedSourceRowsNotReprocessed(t, run, "multi-source-merge-cursor")
}

func assertCommittedSourceRowsNotReprocessed(t *testing.T, run recoveredChainRun, phase string) {
	t.Helper()
	require.NotEmpty(t, run.committedSourceRows, "%s: committed source rows were not captured", phase)

	events, err := ObserveSegments(run.dataDir)
	require.NoError(t, err, "%s: observe recovered destination segments", phase)
	got := zeroRowSeqs(NormalizeEventLog(events))

	wantCounts := countRows(run.committedSourceRows)
	gotCounts := countRows(got)

	survivors := 0
	for row, want := range wantCounts {
		got := gotCounts[row]
		if got == 0 {
			continue
		}
		survivors++
		require.Equalf(t, want, got,
			"%s: committed merge source row was reprocessed during recovery: %s", phase, row.describe())
	}
	require.Positivef(t, survivors,
		"%s: none of the captured committed-source rows survived merge-tail compaction; no-reprocess assertion is vacuous", phase)
}

func countRows(rows []EventLogRow) map[EventLogRow]int {
	out := make(map[EventLogRow]int, len(rows))
	for _, row := range rows {
		out[row]++
	}
	return out
}
