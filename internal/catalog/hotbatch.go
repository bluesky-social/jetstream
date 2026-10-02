package catalog

import "github.com/bluesky-social/jetstream/segment"

// DecodeHotBatch decodes row's frame, fetched by the caller for a pointer
// row, and checks it holds exactly the row's seqs. Both the row and the frame
// come from storage, so every mismatch is a CorruptionError, never a panic.
func DecodeHotBatch(row HotBatchRow, frame []byte) ([]segment.Event, error) {
	// Checked before the decode so the count comparison below cannot wrap.
	if row.FirstSeq == 0 || row.LastSeq < row.FirstSeq || row.LastSeq-row.FirstSeq >= maxBatchEvents ||
		row.LastSeq-row.FirstSeq+1 != uint64(row.EventCount) {
		return nil, Corruptf(SourceHotBatch, "hot batch [%d,%d] has a bad descriptor (%d events)",
			row.FirstSeq, row.LastSeq, row.EventCount)
	}
	evs, err := segment.DecodeBlockFrame(frame)
	if err != nil {
		return nil, Corruptf(SourceHotBatch, "hot batch [%d,%d]: %v", row.FirstSeq, row.LastSeq, err)
	}
	if len(evs) != int(row.EventCount) {
		return nil, Corruptf(SourceHotBatch, "hot batch [%d,%d] (%d events) decodes to %d events",
			row.FirstSeq, row.LastSeq, row.EventCount, len(evs))
	}
	for j := range evs {
		if want := row.FirstSeq + uint64(j); evs[j].Seq != want {
			return nil, Corruptf(SourceHotBatch, "hot batch [%d,%d] holds seq %d where %d belongs",
				row.FirstSeq, row.LastSeq, evs[j].Seq, want)
		}
	}
	return evs, nil
}
