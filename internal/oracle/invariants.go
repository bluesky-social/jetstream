package oracle

import "fmt"

// CheckInvariants validates structural guarantees of an observed event stream:
// seqs are unique and strictly increasing, commit events carry a rev, and per-DID
// revs never regress.
func CheckInvariants(events []ObservedEvent) error {
	seenSeqs := make(map[uint64]struct{}, len(events))
	lastRevByDID := make(map[string]ObservedEvent)

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
