package oracle

import "fmt"

// CheckInvariants requires unique increasing seqs, non-empty commit revs, and
// non-regressing per-DID revs. Use it for clean streams.
//
// Crash recovery can re-merge rows at new seqs with their original lower
// revs. Use CheckStructuralInvariants for those streams, plus final-state and
// at-least-once coverage checks. Keeping the stricter clean-stream check
// detects rev regressions without rejecting permitted replay.
func CheckInvariants(events []ObservedEvent) error {
	if err := CheckStructuralInvariants(events); err != nil {
		return err
	}
	return checkPerDIDRevMonotonic(events)
}

// CheckStructuralInvariants validates the guarantees that hold for ANY observed
// stream, replayed or not: seqs are unique and strictly increasing and every
// commit event carries a non-empty rev. It deliberately omits the per-DID
// rev-monotonicity check, which only holds for non-replayed streams.
func CheckStructuralInvariants(events []ObservedEvent) error {
	seenSeqs := make(map[uint64]struct{}, len(events))

	var lastSeq uint64
	for i, ev := range events {
		if _, ok := seenSeqs[ev.Seq]; ok {
			return fmt.Errorf("oracle: duplicate seq %d at event %d", ev.Seq, i)
		}
		seenSeqs[ev.Seq] = struct{}{}

		if i > 0 && ev.Seq <= lastSeq {
			return fmt.Errorf("oracle: non-increasing seq at event %d: %d after %d", i, ev.Seq, lastSeq)
		}
		lastSeq = ev.Seq

		if ev.Rev == "" && isCommitKind(ev.Kind) {
			return fmt.Errorf("oracle: empty rev for commit event at event %d: seq=%d kind=%d did=%s collection=%s rkey=%s",
				i, ev.Seq, ev.Kind, ev.DID, ev.Collection, ev.Rkey)
		}
	}
	return nil
}

// checkPerDIDRevMonotonic asserts that, within each DID, rev never regresses as
// seq increases. Valid only for a non-replayed stream (see CheckInvariants).
func checkPerDIDRevMonotonic(events []ObservedEvent) error {
	lastRevByDID := make(map[string]ObservedEvent)
	for i, ev := range events {
		if ev.Rev == "" {
			continue
		}
		if last, ok := lastRevByDID[ev.DID]; ok && ev.Rev < last.Rev {
			return fmt.Errorf("oracle: rev regression for DID %s at event %d: seq=%d %s/%s rev=%q after seq=%d %s/%s rev=%q",
				ev.DID, i,
				ev.Seq, ev.Collection, ev.Rkey, ev.Rev,
				last.Seq, last.Collection, last.Rkey, last.Rev)
		}
		lastRevByDID[ev.DID] = ev
	}
	return nil
}
