package storagefake

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"sync"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/zeebo/xxh3"
)

// FrameGetter reads a pointer hot batch's frame: protocol.Reader.
type FrameGetter interface {
	Get(ctx context.Context, objectID uint64) ([]byte, error)
}

// RelayWatch checks design §9.3 invariant 7: relay/cursor never names an
// upstream seq whose rows are not all committed. The catalog cannot check it
// alone, because the segment format does not keep an event's upstream seq;
// the test's model says which rows each upstream seq produces (Expect), and
// the watch finds them by content in the committed hot batches.
//
// Set Config.RelayWatch and the DB runs it with every post-commit invariant
// check. Only main's hot batches are read, so it covers a hot-mode live
// consumer, not a direct-mode one.
type RelayWatch struct {
	frames FrameGetter

	mu sync.Mutex
	// want is each upstream seq's rows, from the model.
	want map[int64][]rowKey
	// batches caches every hot batch the watch has decoded, by first seq.
	// A committed batch's rows never change and its seqs are never reused,
	// so a batch stays committed in every later snapshot whose seq/next is
	// past it, even after a fold removes its row.
	batches map[uint64]watchedBatch
	// framesFrom is one past the highest first seq decoded: the snapshot
	// only needs frames from there.
	framesFrom uint64
}

type watchedBatch struct {
	last uint64
	rows []rowKey
}

// rowKey identifies a row by content. The segment format does not keep the
// upstream seq, but one upstream commit's rows differ from every other's by
// rev or payload.
type rowKey struct {
	kind                      segment.Kind
	did, collection, rkey, rv string
	payload                   uint64
}

func keyOf(ev *segment.Event) rowKey {
	return rowKey{ev.Kind, ev.DID, ev.Collection, ev.Rkey, ev.Rev, xxh3.Hash(ev.Payload)}
}

func (k rowKey) String() string {
	return fmt.Sprintf("kind=%d %s %s/%s rev=%s", k.kind, k.did, k.collection, k.rkey, k.rv)
}

// NewRelayWatch returns a watch. frames reads pointer batches; nil means the
// test commits only inline batches, and a pointer batch is a violation.
func NewRelayWatch(frames FrameGetter) *RelayWatch {
	return &RelayWatch{frames: frames, want: map[int64][]rowKey{}, batches: map[uint64]watchedBatch{}, framesFrom: 1}
}

// Expect records the rows upstream seq produces. Call it before the consumer
// can see that seq. An upstream seq with no rows (a dropped event) needs no
// call.
func (w *RelayWatch) Expect(upstream int64, rows ...segment.Event) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := range rows {
		w.want[upstream] = append(w.want[upstream], keyOf(&rows[i]))
	}
}

func (w *RelayWatch) from() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.framesFrom
}

// Check is catalog.InvariantOptions.RelayCursor. The DB installs it; a test
// can also pass it to maintainer.RebuildConfig.
func (w *RelayWatch) Check(s *catalog.Snapshot, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, h := range s.HotBatches {
		if _, ok := w.batches[h.FirstSeq]; ok {
			continue
		}
		evs, err := w.decode(h)
		if err != nil {
			return err
		}
		b := watchedBatch{last: h.LastSeq}
		for i := range evs {
			b.rows = append(b.rows, keyOf(&evs[i]))
		}
		w.batches[h.FirstSeq] = b
		w.framesFrom = max(w.framesFrom, h.FirstSeq+1)
	}
	if value == nil {
		return nil
	}
	// live's cursor encoding: [version 1][8B LE].
	if len(value) != 9 || value[0] != 1 {
		return fmt.Errorf("relay/cursor %x is not a v1 cursor", value)
	}
	cursor := int64(binary.LittleEndian.Uint64(value[1:]))
	next, err := catalog.DecodeSeq(catalog.MainSeqKey, s.Meta[catalog.MainSeqKey], s.Meta[catalog.MainSeqKey] != nil)
	if err != nil {
		return err
	}
	committed := map[rowKey]bool{}
	for _, b := range w.batches {
		if b.last < next {
			for _, k := range b.rows {
				committed[k] = true
			}
		}
	}
	upstreams := make([]int64, 0, len(w.want))
	for u := range w.want {
		if u <= cursor {
			upstreams = append(upstreams, u)
		}
	}
	slices.Sort(upstreams)
	for _, u := range upstreams {
		for _, k := range w.want[u] {
			if !committed[k] {
				return fmt.Errorf("relay/cursor %d covers upstream seq %d, but its row %s is not committed", cursor, u, k)
			}
		}
	}
	return nil
}

func (w *RelayWatch) decode(h catalog.HotBatchRow) ([]segment.Event, error) {
	frame := h.Frame
	if !h.Inline {
		if w.frames == nil {
			return nil, fmt.Errorf("hot batch [%d,%d] is a pointer and the watch reads only inline batches", h.FirstSeq, h.LastSeq)
		}
		var err error
		if frame, err = w.frames.Get(context.Background(), h.ObjectID); err != nil {
			return nil, fmt.Errorf("hot batch [%d,%d]: %w", h.FirstSeq, h.LastSeq, err)
		}
	}
	evs, err := segment.DecodeBlockFrame(frame)
	if err != nil {
		return nil, fmt.Errorf("hot batch [%d,%d]: %w", h.FirstSeq, h.LastSeq, err)
	}
	return evs, nil
}
