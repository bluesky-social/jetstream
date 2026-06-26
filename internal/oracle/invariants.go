package oracle

import "fmt"

// CheckInvariants validates the full structural guarantees of an observed
// event stream: seqs are unique and strictly increasing, commit events carry a
// rev, and per-DID revs never regress. It assumes a NON-replayed stream — one
// where seq order tracks rev order per DID. Use it for clean (no-crash)
// observations.
//
// For a stream recovered across a crash boundary, the per-DID
// rev-monotonicity-by-seq guarantee does NOT hold: an idempotent at-least-once
// replay re-emits already-merged survivors at fresh higher seqs carrying their
// original (lower) revs, so a later seq can legitimately carry an earlier rev
// (the AfterMergeDstFlushBeforeSourceCommit contract: "recovery may replay
// duplicates, but must not lose survivors"). Use CheckStructuralInvariants
// there — final-state Compare + at-least-once coverage own correctness once
// replay is in play. Splitting the check keeps the strong per-DID rev-monotonic
// signal (which kills m005 / backstops m018) intact for every clean-stream
// caller while not flagging benign replay as corruption.
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
