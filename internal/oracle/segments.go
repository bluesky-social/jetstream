package oracle

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/segment"
)

func ObserveSegments(dataDir string) ([]ObservedEvent, error) {
	return observeSegmentDir(filepath.Join(dataDir, "segments"))
}

func ObserveBootstrapSegments(dataDir string) ([]ObservedEvent, error) {
	primary, err := observeSegmentDir(filepath.Join(dataDir, "segments"))
	if err != nil {
		return nil, err
	}
	if err := CheckInvariants(primary); err != nil {
		return nil, fmt.Errorf("primary segments: %w", err)
	}

	live, err := observeSegmentDir(filepath.Join(dataDir, "backfill", "live_segments"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return EventsSortedBySeq(primary), nil
		}
		return nil, err
	}

	if err := CheckInvariants(live); err != nil {
		return nil, fmt.Errorf("bootstrap live segments: %w", err)
	}

	out := EventsSortedBySeq(primary)
	out = append(out, EventsSortedBySeq(live)...)
	return out, nil
}

func observeSegmentDir(dir string) ([]ObservedEvent, error) {
	files, err := ingest.SegmentFiles(dir)
	if err != nil {
		return nil, err
	}

	var out []ObservedEvent
	for _, file := range files {
		events, err := observeSealedSegment(file.Path)
		if err == nil {
			out = append(out, events...)
			continue
		}
		if !errors.Is(err, segment.ErrActiveSegment) {
			return nil, err
		}

		events, err = observeActiveSegment(file.Path)
		if err != nil {
			return nil, err
		}
		out = append(out, events...)
	}

	return out, nil
}

// EventsSortedBySeq returns a copy of events ordered by Seq. Use this only
// after running CheckInvariants on the observed physical stream; sorting before
// invariant checks hides source-order regressions.
func EventsSortedBySeq(events []ObservedEvent) []ObservedEvent {
	out := append([]ObservedEvent(nil), events...)
	sortObservedEventsBySeq(out)
	return out
}

func sortObservedEventsBySeq(events []ObservedEvent) {
	// Stable sort so that, if two events ever share a Seq, their relative
	// physical order is preserved rather than scrambled. Reconstruct
	// applies create/update/delete in slice order, so a non-stable sort
	// could reorder same-seq ops (e.g. a delete ahead of its create) and
	// produce run-to-run mismatches. CheckInvariants already rejects
	// duplicate seqs upstream; this keeps the helper robust on its own.
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].Seq < events[j].Seq
	})
}

func observeSealedSegment(path string) ([]ObservedEvent, error) {
	rd, err := segment.Open(segment.ReaderConfig{Path: path})
	if err != nil {
		return nil, err
	}
	defer func() { _ = rd.Close() }()

	header := rd.Header()
	blocks := rd.Blocks()
	if err := checkSegmentStructure(path, header, blocks); err != nil {
		return nil, err
	}

	var out []ObservedEvent
	for i := range int(header.BlockCount) {
		block, err := rd.DecodeBlock(i)
		if err != nil {
			return nil, fmt.Errorf("oracle: decode sealed segment %s block %d: %w", path, i, err)
		}
		for _, ev := range block {
			out = append(out, observedEventFromSegment(ev))
		}
	}
	return out, nil
}

func checkSegmentStructure(path string, header segment.Header, blocks []segment.BlockInfo) error {
	if len(blocks) != int(header.BlockCount) {
		return fmt.Errorf("oracle: sealed segment %s block count mismatch: header=%d index=%d",
			path, header.BlockCount, len(blocks))
	}

	for i := 1; i < len(blocks); i++ {
		prev := blocks[i-1]
		cur := blocks[i]
		if cur.Offset <= prev.Offset {
			return fmt.Errorf("oracle: sealed segment %s block %d non-increasing block offset: prev=%d current=%d",
				path, i, prev.Offset, cur.Offset)
		}
	}

	var prevMaxSeq uint64
	havePrevSeq := false
	for i, block := range blocks {
		if block.EventCount == 0 {
			continue
		}
		if block.MinSeq > block.MaxSeq {
			return fmt.Errorf("oracle: sealed segment %s block %d invalid seq range: min_seq=%d max_seq=%d",
				path, i, block.MinSeq, block.MaxSeq)
		}
		if havePrevSeq && block.MinSeq <= prevMaxSeq {
			return fmt.Errorf("oracle: sealed segment %s block %d block seq overlap: prev_max_seq=%d min_seq=%d",
				path, i, prevMaxSeq, block.MinSeq)
		}
		prevMaxSeq = block.MaxSeq
		havePrevSeq = true
	}

	for i, block := range blocks {
		if block.Offset < segment.ReservedHeaderBytes {
			return fmt.Errorf("oracle: sealed segment %s block %d offset before segment header: offset=%d header_bytes=%d",
				path, i, block.Offset, segment.ReservedHeaderBytes)
		}
	}

	return nil
}

func observeActiveSegment(path string) ([]ObservedEvent, error) {
	var out []ObservedEvent
	err := segment.WalkActive(path, func(block []segment.Event) error {
		for _, ev := range block {
			out = append(out, observedEventFromSegment(ev))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("oracle: walk active segment %s: %w", path, err)
	}
	return out, nil
}

func observedEventFromSegment(ev segment.Event) ObservedEvent {
	return ObservedEvent{
		Seq:        ev.Seq,
		IndexedAt:  ev.IndexedAt,
		Kind:       ev.Kind,
		DID:        ev.DID,
		Collection: ev.Collection,
		Rkey:       ev.Rkey,
		Rev:        ev.Rev,
		Payload:    append([]byte(nil), ev.Payload...),
	}
}
