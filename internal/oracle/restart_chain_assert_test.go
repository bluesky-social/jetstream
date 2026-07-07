package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// assertChainDurable is the post-restart assertion bundle for the durable
// intermediates a chainCoordinator injected. It runs three independent
// checks over the recovered on-disk segments (plan §4):
//
//  1. at-least-once event-log coverage (§4.2): every model-derived chain
//     row that should be durable appears on disk at least once, after
//     subtracting the creates compaction legitimately removed at W;
//  2. compaction contract (§4.3): no superseded create/update survives
//     at or below W (CheckCompacted);
//  3. record-level no-permanent-tombstone (§4.3): for the delete→recreate
//     shape, the recreated record reconstructs as present.
//
// The expected side is model-derived (the ops the coordinator issued), so
// it is independent of the system under test; seqs come from the disk rows
// only as a join coordinate for the watermark filter.
// chainCoverageView is the computed coverage comparison for a recovered
// run: the model-derived expected durable rows (filtered at W) and the
// observed on-disk rows, both seq-zeroed for key-based comparison. It is
// shared by the positive assertion (assertChainDurable) and the per-shape
// red-first power tests, so both compare against the identical sets.
type chainCoverageView struct {
	want      []EventLogRow
	got       []EventLogRow
	events    []ObservedEvent
	watermark uint64
}

func chainCoverage(t *testing.T, dataDir string, coord *chainCoordinator) chainCoverageView {
	t.Helper()

	ops := coord.recordedOps()
	events, err := ObserveSegments(dataDir)
	require.NoError(t, err, "observe segments")
	events = EventsSortedBySeq(events)
	diskRows := NormalizeEventLog(events)
	watermark := readCompactionWatermark(t, dataDir)

	// Model-derived expected chain rows (seq-agnostic, key form).
	wantPre := expectedChainRows(coord.hostDID, ops)

	// Join model rows to on-disk seqs by key so the watermark-based
	// compaction filter can run. A model row with no on-disk match keeps
	// seq 0 (treated as <= W); if it was genuinely lost it surfaces as a
	// coverage gap, which is the intended failure.
	seqByKey := firstSeqByRowKey(diskRows)
	wantSeqed := make([]EventLogRow, len(wantPre))
	for i, r := range wantPre {
		r.Seq = seqByKey[rowIdentity(r)]
		wantSeqed[i] = r
	}

	return chainCoverageView{
		want:      zeroRowSeqs(filterCompactedExpectedRowsWithObservedTombstones(wantSeqed, diskRows, watermark)),
		got:       zeroRowSeqs(diskRows),
		events:    events,
		watermark: watermark,
	}
}

func filterCompactedExpectedRowsWithObservedTombstones(want, observed []EventLogRow, watermark uint64) []EventLogRow {
	recordTombstones := make(map[RecordKey]uint64)
	didTombstones := make(map[string]uint64)
	observeTombstone := func(row EventLogRow) {
		if row.Seq > watermark {
			return
		}
		switch row.Kind {
		case "delete", "update":
			key := RecordKey{DID: row.DID, Collection: row.Collection, Rkey: row.Rkey}
			if row.Seq > recordTombstones[key] {
				recordTombstones[key] = row.Seq
			}
		case "sync":
			if row.Seq > didTombstones[row.DID] {
				didTombstones[row.DID] = row.Seq
			}
		case "account":
			if !row.AccountDeleted {
				return
			}
			if row.Seq > didTombstones[row.DID] {
				didTombstones[row.DID] = row.Seq
			}
		}
	}
	for _, row := range want {
		observeTombstone(row)
	}
	for _, row := range observed {
		observeTombstone(row)
	}

	out := make([]EventLogRow, 0, len(want))
	for _, row := range want {
		if row.Seq <= watermark && (row.Kind == "create" || row.Kind == "update" || row.Kind == "create_resync") {
			key := RecordKey{DID: row.DID, Collection: row.Collection, Rkey: row.Rkey}
			if recordTombstones[key] > row.Seq || didTombstones[row.DID] > row.Seq {
				continue
			}
		}
		out = append(out, row)
	}
	return out
}

func assertChainDurable(t *testing.T, dataDir string, coord *chainCoordinator, phase string) {
	t.Helper()

	cov := chainCoverage(t, dataDir, coord)

	// At-least-once coverage: every expected durable row that survives
	// compaction is present on disk at least once.
	require.NoErrorf(t, CompareEventLogCoverage(cov.want, cov.got),
		"%s: at-least-once event-log coverage (W=%d)", phase, cov.watermark)

	// Compaction contract over the recovered segments.
	require.NoErrorf(t, CheckCompacted(cov.events, cov.watermark),
		"%s: compaction contract (W=%d)", phase, cov.watermark)

	// No-permanent-tombstone: every delete→recreate record reconstructs as
	// present at head.
	assertRecreatedRecordsVisible(t, cov.events, coord.spec, coord.hostDID, phase)
}

// assertRecreatedRecordsVisible reconstructs the durable stream and checks
// that each shapeLiveDeleteRecreate record is present in the host repo's
// final state — proving the recreate above the delete tombstone is not
// masked (no permanent tombstone, docs/README.md:358).
func assertRecreatedRecordsVisible(t *testing.T, events []ObservedEvent, spec chainSpec, hostDID, phase string) {
	t.Helper()

	model, err := Reconstruct(events)
	require.NoErrorf(t, err, "%s: reconstruct for visibility", phase)

	for _, rc := range spec.records {
		if rc.shape != shapeLiveDeleteRecreate {
			continue
		}
		key := RecordKey{DID: hostDID, Collection: rc.collection, Rkey: rc.rkey}
		snap, ok := model.Accounts[hostDID]
		require.Truef(t, ok, "%s: host DID %s absent from reconstructed model", phase, hostDID)
		_, present := snap.Records[key]
		require.Truef(t, present,
			"%s: recreated record %s/%s must be visible (no permanent tombstone)", phase, rc.collection, rc.rkey)
	}
}

// rowIdentity is the seq-agnostic key of a row, used to join model rows to
// on-disk rows.
type rowIdent struct {
	kind, did, coll, rkey, rev, payloadHash string
	payloadLen                              int
	accountDeleted                          bool
}

func rowIdentity(r EventLogRow) rowIdent {
	return rowIdent{
		kind:           r.Kind,
		did:            r.DID,
		coll:           r.Collection,
		rkey:           r.Rkey,
		rev:            r.Rev,
		payloadHash:    r.PayloadSHA256_64,
		payloadLen:     r.PayloadLen,
		accountDeleted: r.AccountDeleted,
	}
}

// firstSeqByRowKey maps each disk row's seq-agnostic identity to the
// lowest seq it appears at (the create lands before its tombstone, so the
// lowest seq is the right join coordinate for supersession at W).
func firstSeqByRowKey(rows []EventLogRow) map[rowIdent]uint64 {
	out := make(map[rowIdent]uint64, len(rows))
	for _, r := range rows {
		id := rowIdentity(r)
		if cur, ok := out[id]; !ok || r.Seq < cur {
			out[id] = r.Seq
		}
	}
	return out
}
