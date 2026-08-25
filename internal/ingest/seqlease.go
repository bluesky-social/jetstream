package ingest

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/seqspace"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
)

const (
	seqReservedKey = "seq/max_reserved"
	seqGapPrefix   = "seq/gap/"
	gapVersion     = byte(1)
	gapReasonCrash = byte(1)
)

type initializedSeqLease struct {
	nextSeq     uint64
	reservedEnd uint64
	gaps        *seqspace.Gaps
}

func initializeSeqLease(cfg Config, w *Writer, durableNext uint64, seqFound, hadSegments bool) (initializedSeqLease, error) {
	gaps, err := loadSeqGaps(cfg.Store)
	if err != nil {
		return initializedSeqLease{}, err
	}
	priorReserved, reservationFound, err := loadNextSeqFound(cfg.Store, seqReservedKey)
	if err != nil {
		return initializedSeqLease{}, err
	}
	for _, gap := range gaps.Ranges() {
		if gap.End > durableNext {
			return initializedSeqLease{}, fmt.Errorf("ingest: seq gap [%d,%d) exceeds durable coverage frontier %d", gap.Start, gap.End, durableNext)
		}
	}
	nextSeq := durableNext
	createdGap := false
	var createdGapRange seqspace.Gap
	if reservationFound {
		if priorReserved > seqspace.CursorSeqMaxThreshold {
			return initializedSeqLease{}, fmt.Errorf("ingest: reserved seq %d exceeds cursor ceiling", priorReserved)
		}
		if priorReserved < nextSeq {
			return initializedSeqLease{}, fmt.Errorf("ingest: reserved seq %d trails durable coverage frontier %d", priorReserved, nextSeq)
		}
		if priorReserved > nextSeq {
			createdGapRange = seqspace.Gap{Start: nextSeq, End: priorReserved}
			gaps, err = gaps.Add(createdGapRange)
			if err != nil {
				return initializedSeqLease{}, err
			}
			nextSeq = priorReserved
			createdGap = true
		}
	} else if (hadSegments || seqFound) && !cfg.UnreservedSeqsUnobservable {
		// An old binary had no reservation key. Its final sub-block could have
		// been client-visible while absent from both durable recovery sources.
		nextSeq, err = seqspace.ReserveEnd(durableNext, cfg.MaxEventsPerBlock)
		if err != nil {
			return initializedSeqLease{}, err
		}
		createdGapRange = seqspace.Gap{Start: durableNext, End: nextSeq}
		gaps, err = gaps.Add(createdGapRange)
		if err != nil {
			return initializedSeqLease{}, err
		}
		createdGap = true
	}
	reservedEnd, err := seqspace.ReserveEnd(nextSeq, cfg.MaxEventsPerBlock)
	if err != nil {
		return initializedSeqLease{}, err
	}
	if err := validateSeqGapsAgainstSegments(cfg, w, gaps); err != nil {
		return initializedSeqLease{}, err
	}

	b := cfg.Store.NewBatch()
	defer func() { _ = b.Close() }()
	// Every value below nextSeq is now durably covered by either a segment
	// event or the gap registry staged in this same batch. Advance the durable
	// coverage frontier atomically with that registry.
	if err := stageNextSeq(b, cfg.SeqKey, nextSeq); err != nil {
		return initializedSeqLease{}, err
	}
	if err := stageNextSeq(b, seqReservedKey, reservedEnd); err != nil {
		return initializedSeqLease{}, err
	}
	// Canonicalize the registry in the same startup transaction even when no
	// new gap was created. Older/manual records may be valid but adjacent; one
	// normalized representation bounds startup reads and status cardinality.
	if err := stageSeqGaps(b, gaps); err != nil {
		return initializedSeqLease{}, err
	}
	if err := cfg.Store.Commit(b, store.SyncWrites); err != nil {
		return initializedSeqLease{}, cfg.wrapSegmentPersistenceError("initializing sequence lease", err)
	}
	if createdGap {
		cfg.Metrics.incSeqGapRegistered(createdGapRange.End - createdGapRange.Start)
		cfg.Logger.Warn("registered abandoned sequence lease", "gap_start", createdGapRange.Start, "gap_end", createdGapRange.End)
	}
	return initializedSeqLease{nextSeq: nextSeq, reservedEnd: reservedEnd, gaps: gaps}, nil
}

func loadSeqGaps(st *store.Store) (*seqspace.Gaps, error) {
	prefix := []byte(seqGapPrefix)
	it, err := st.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: store.PrefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("ingest: iterate seq gaps: %w", err)
	}
	defer func() { _ = it.Close() }()
	var gaps []seqspace.Gap
	for it.First(); it.Valid(); it.Next() {
		key, val := it.Key(), it.Value()
		if len(key) != len(prefix)+8 || len(val) != 10 || val[0] != gapVersion || val[1] != gapReasonCrash {
			return nil, fmt.Errorf("ingest: malformed seq gap record key_len=%d value_len=%d", len(key), len(val))
		}
		gaps = append(gaps, seqspace.Gap{
			Start: binary.BigEndian.Uint64(key[len(prefix):]),
			End:   binary.BigEndian.Uint64(val[2:]),
		})
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("ingest: iterate seq gaps: %w", err)
	}
	return seqspace.NewGaps(gaps)
}

func stageSeqGaps(b *pebble.Batch, gaps *seqspace.Gaps) error {
	prefix := []byte(seqGapPrefix)
	if err := b.DeleteRange(prefix, store.PrefixUpperBound(prefix), nil); err != nil {
		return fmt.Errorf("ingest: clear seq gaps: %w", err)
	}
	for _, gap := range gaps.Ranges() {
		key := make([]byte, len(prefix)+8)
		copy(key, prefix)
		binary.BigEndian.PutUint64(key[len(prefix):], gap.Start)
		val := make([]byte, 10)
		val[0], val[1] = gapVersion, gapReasonCrash
		binary.BigEndian.PutUint64(val[2:], gap.End)
		if err := b.Set(key, val, nil); err != nil {
			return fmt.Errorf("ingest: stage seq gap: %w", err)
		}
	}
	return nil
}

func validateSeqGapsAgainstSegments(cfg Config, w *Writer, gaps *seqspace.Gaps) error {
	files, err := SegmentFilesFS(cfg.FS, cfg.SegmentsDir)
	if err != nil {
		return err
	}
	var ranges []seqspace.BlockRange
	for _, sf := range files {
		if sf.Idx == w.activeIdx && w.active != nil {
			for _, block := range w.active.Blocks() {
				if block.EventCount > 0 {
					ranges = append(ranges, seqspace.BlockRange{Min: block.MinSeq, Max: block.MaxSeq})
				}
			}
			continue
		}
		r, openErr := segment.Open(segment.ReaderConfig{Path: sf.Path, FS: cfg.FS})
		if errors.Is(openErr, segment.ErrActiveSegment) {
			return fmt.Errorf("ingest: validate seq gaps: unexpected non-current active segment %s", sf.Path)
		}
		if openErr != nil {
			return fmt.Errorf("ingest: validate seq gaps: open %s: %w", sf.Path, openErr)
		}
		for _, block := range r.Blocks() {
			if block.EventCount > 0 {
				ranges = append(ranges, seqspace.BlockRange{Min: block.MinSeq, Max: block.MaxSeq})
			}
		}
		if err := r.Close(); err != nil {
			return fmt.Errorf("ingest: validate seq gaps: close %s: %w", sf.Path, err)
		}
	}
	return gaps.ValidateVacant(ranges)
}
