package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/vfs"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/segment"
)

// witnessedFloor keeps witnessed_at monotonic with seq (docs/README.md
// §3.1). Sources stamp events before they wait for admission or the writer
// lock, so concurrent backfill workers, a merge, or a new leader whose clock
// runs behind the old one's would otherwise store a later seq with an
// earlier time. The timestamp cursor lookup binary-searches block witnessed
// envelopes, so a regression silently skips events.
//
// The floor is seeded from the namespace's durable tip when the writer
// opens, and raised under the writer lock as seqs are assigned.
type witnessedFloor struct{ us int64 }

// clamp returns the witnessed_at to store for the next seq.
func (f *witnessedFloor) clamp(m *Metrics, us int64) int64 {
	if us < f.us {
		m.incWitnessedClamped()
		return f.us
	}
	f.us = us
	return us
}

func maxBlockWitnessed(blocks []segment.BlockInfo) int64 {
	var us int64
	for _, b := range blocks {
		us = max(us, b.MaxWitnessedAt)
	}
	return us
}

// sealedTailWitnessed returns MaxWitnessedAt of the highest sealed segment
// below belowIdx that ever held an event, or 0 if there is none. It is the
// local writer's floor when the active segment has no flushed blocks.
func sealedTailWitnessed(fs vfs.FS, dir string, belowIdx uint64) (int64, error) {
	files, err := SegmentFilesFS(fs, dir)
	if err != nil {
		return 0, err
	}
	for i := len(files) - 1; i >= 0; i-- {
		if files[i].Idx >= belowIdx {
			continue
		}
		r, err := segment.Open(segment.ReaderConfig{Path: files[i].Path, FS: fs})
		if err != nil {
			if errors.Is(err, segment.ErrActiveSegment) {
				continue
			}
			return 0, fmt.Errorf("ingest: open sealed tail %s: %w", files[i].Path, err)
		}
		hdr := r.Header()
		if err := r.Close(); err != nil {
			return 0, fmt.Errorf("ingest: close sealed tail %s: %w", files[i].Path, err)
		}
		if hdr.MaxSeq > 0 {
			return hdr.MaxWitnessedAt, nil
		}
	}
	return 0, nil
}

// readWitnessedFloor returns the largest witnessed_at durable in ns at
// session start: hot batches (Main only, passed in by the caller), then the
// active segment's blocks, then the last sealed segment's header. It reads
// through tx, not the follower, because the mirror may not have caught up
// with the previous leader's last commits.
func readWitnessedFloor(ctx context.Context, tx catalog.ReadTx, ns catalog.Namespace, hot []catalog.HotBatchRow) (int64, error) {
	var us int64
	for _, r := range hot {
		us = max(us, r.MaxWitnessedUS)
	}
	active, err := tx.ActiveBlocksSince(ctx, 0)
	if err != nil {
		return 0, fmt.Errorf("ingest: read active blocks: %w", err)
	}
	for _, r := range active {
		if r.Namespace == ns {
			us = max(us, r.MaxWitnessedUS)
		}
	}
	if us > 0 {
		return us, nil
	}
	segs, err := tx.SegmentsSince(ctx, 0)
	if err != nil {
		return 0, fmt.Errorf("ingest: read segments: %w", err)
	}
	var gen uint64
	for _, s := range segs {
		if s.Namespace == ns && s.State == catalog.Sealed {
			gen = s.GenerationID
		}
	}
	if gen == 0 {
		return 0, nil
	}
	rows, err := tx.Generations(ctx, []uint64{gen})
	if err != nil {
		return 0, fmt.Errorf("ingest: read generation %d: %w", gen, err)
	}
	if len(rows) != 1 {
		return 0, catalog.Corruptf(catalog.SourceGeneration, "sealed segment names generation %d, which has %d rows", gen, len(rows))
	}
	hdr, err := segment.ReadSealedHeader(bytes.NewReader(rows[0].Header))
	if err != nil {
		return 0, catalog.Corruptf(catalog.SourceGeneration, "generation %d header: %v", gen, err)
	}
	return hdr.MaxWitnessedAt, nil
}
