package oracle

import (
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/bluesky-social/jetstream/segment"
)

// chainActionKind maps a GeneratedChainOp action to its durable segment
// kind. A create/update materializes a record; a delete is a tombstone.
func chainActionKind(action string) segment.Kind {
	switch action {
	case "create":
		return segment.KindCreate
	case "update":
		return segment.KindUpdate
	case "delete":
		return segment.KindDelete
	default:
		panic("oracle: unknown chain action " + action)
	}
}

// expectedChainRows converts the ops a test injected on hostDID into the
// normalized, seq-agnostic event-log rows they must produce on disk
// (pre-compaction). This is the model-derived side (plan §5a): the rows'
// existence and content come purely from what the test issued —
// independent of the system under test — so a jetstream rev-filter or
// merge bug cannot corrupt both sides into agreement. Payload bytes are
// the exact record block the simulator committed (GeneratedChainOp.Payload),
// which equals the block jetstream records on disk, so the SHA-256 prefix
// matches NormalizeEventLog's hashing of the observed row. Seq is left
// zero; coverage matching is key-based (see CompareEventLogCoverage).
func expectedChainRows(hostDID string, ops []world.GeneratedChainOp) []EventLogRow {
	observed := make([]ObservedEvent, 0, len(ops))
	for _, op := range ops {
		ev := ObservedEvent{
			Kind:       chainActionKind(op.Action),
			DID:        hostDID,
			Collection: op.Collection,
			Rkey:       op.Rkey,
			Rev:        op.Rev,
		}
		if op.Action != "delete" {
			ev.Payload = append([]byte(nil), op.Payload...)
		}
		observed = append(observed, ev)
	}
	return NormalizeEventLog(observed)
}

// zeroRowSeqs returns a copy of rows with Seq cleared, for key-based
// (seq-agnostic) coverage comparison between model-derived expectations
// (no seq) and on-disk rows (jetstream-seq, not comparable to the model).
func zeroRowSeqs(rows []EventLogRow) []EventLogRow {
	out := make([]EventLogRow, len(rows))
	for i, r := range rows {
		r.Seq = 0
		out[i] = r
	}
	return out
}
