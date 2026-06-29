package jetstream

import (
	"context"
	"iter"
	"sort"
	"sync"

	"github.com/jcalabro/gt"
)

// memLiveBuffer is the default in-memory LiveBuffer: a simple seq-ordered
// slice. Suitable for short cutovers and tests; a full-network backfill should
// use NewFileLiveBuffer to avoid holding the entire live backlog in RAM.
type memLiveBuffer struct {
	mu     sync.Mutex
	frames []LiveFrame
}

// NewMemLiveBuffer returns an in-memory LiveBuffer. This is the client default
// when WithLiveBuffer is not supplied.
func NewMemLiveBuffer() LiveBuffer {
	return &memLiveBuffer{}
}

func (b *memLiveBuffer) Append(frames []LiveFrame) error {
	if len(frames) == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, f := range frames {
		// Copy the frame bytes: the caller's buffer may be reused after Append.
		b.frames = append(b.frames, LiveFrame{Seq: f.Seq, Data: append([]byte(nil), f.Data...)})
	}
	return nil
}

func (b *memLiveBuffer) Replay(ctx context.Context, after gt.Option[uint64]) iter.Seq2[LiveFrame, error] {
	return func(yield func(LiveFrame, error) bool) {
		b.mu.Lock()
		snapshot := make([]LiveFrame, len(b.frames))
		copy(snapshot, b.frames)
		b.mu.Unlock()

		sort.Slice(snapshot, func(i, j int) bool { return snapshot[i].Seq < snapshot[j].Seq })
		for _, f := range snapshot {
			// None replays everything (seqs start at 1); Some(n) skips Seq <= n.
			if after.HasVal() && f.Seq <= after.Val() {
				continue
			}
			if ctx.Err() != nil {
				yield(LiveFrame{}, ctx.Err())
				return
			}
			if !yield(f, nil) {
				return
			}
		}
	}
}

func (b *memLiveBuffer) Truncate(throughSeq uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	kept := b.frames[:0]
	for _, f := range b.frames {
		if f.Seq > throughSeq {
			kept = append(kept, f)
		}
	}
	// Compact so dropped frames are eligible for GC.
	b.frames = append([]LiveFrame(nil), kept...)
	return nil
}

func (b *memLiveBuffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.frames = nil
	return nil
}
