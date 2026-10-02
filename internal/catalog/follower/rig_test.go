package follower_test

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/catalog/follower"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/bluesky-social/jetstream/segment"
)

// TestMain runs the zstd encoder's lazy initialization outside any synctest
// bubble, so its goroutines never bind to one bubble and break the next.
func TestMain(m *testing.M) {
	b, err := segment.NewBlockBuilder(1)
	if err != nil {
		panic(err)
	}
	if _, err := b.Append(segment.Event{Seq: 1, Kind: segment.KindCreate, DID: "did:plc:w", Collection: "c", Rkey: "r", Rev: "r"}); err != nil {
		panic(err)
	}
	frame, _ := b.Encode()
	if _, _, _, err := segment.BuildSealed(segment.SliceFrameSource([][]byte{frame})); err != nil {
		panic(err)
	}
	if _, err := segment.DecodeBlockFrame(frame); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

var testArchiveID = [16]byte{0xf0, 0x11, 0x0e, 0x12}

// rig is a leader session writing Main through the real catalog scripts and
// object protocol on storagefake and memblob, tracking the committed event
// stream so tests can compare a follower's output against it.
type rig struct {
	t    *testing.T
	db   *storagefake.DB
	s    *catalog.Session
	up   *protocol.Uploader
	blob *memblob.Blob

	// events[i] has seq i+1.
	events []segment.Event
	// hot is the unfolded hot batches, oldest first.
	hot [][2]uint64
	// active is Main's active blocks since the last seal.
	active []activeBlock
	seg    uint64
	// sealed is each sealed segment's current generation, by index.
	sealed []sealedGen
	// dropped is the seqs compaction removed.
	dropped map[uint64]bool
}

type sealedGen struct {
	gen    uint64
	header []byte
	footer []byte
	frames [][]byte
	blocks []catalog.SealBlock
}

type activeBlock struct {
	frame []byte
	info  segment.BlockInfo
	id    uint64
}

func newRig(t *testing.T) *rig {
	t.Helper()
	db := storagefake.New(storagefake.Config{ArchiveID: testArchiveID})
	lease := db.NewLease()
	require.NoError(t, lease.Acquire(t.Context(), time.Hour))
	r := &rig{t: t, db: db, blob: memblob.New()}
	r.s = catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: lease.Epoch()})
	_, err := r.s.InitNamespace(t.Context(), catalog.Main, nil)
	require.NoError(t, err)
	r.up, err = protocol.NewUploader(protocol.UploaderConfig{
		Blob: r.blob, ArchiveID: testArchiveID, GCDelay: 6 * time.Hour, OrphanAge: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if !t.Failed() {
			require.NoError(t, db.Violation(), "catalog invariants")
		}
	})
	return r
}

func (r *rig) setPhase(p lifecycle.Phase) {
	r.t.Helper()
	_, err := r.s.CommitMeta(r.t.Context(), []metastore.Op{{Kind: metastore.OpSet, Key: []byte(lifecycle.PhaseKey), Value: []byte(p)}})
	require.NoError(r.t, err)
}

func (r *rig) frame(lo, hi uint64) ([]byte, segment.BlockInfo) {
	r.t.Helper()
	b, err := segment.NewBlockBuilder(int(hi - lo + 1))
	require.NoError(r.t, err)
	for seq := lo; seq <= hi; seq++ {
		_, err := b.Append(r.events[seq-1])
		require.NoError(r.t, err)
	}
	frame, info := b.Encode()
	return frame, info
}

// commitHot commits the next n events as one hot batch, inline or through
// an object.
func (r *rig) commitHot(n int, pointer bool) {
	r.t.Helper()
	lo := uint64(len(r.events)) + 1
	now := time.Now().UnixMicro()
	for i := range n {
		seq := lo + uint64(i)
		r.events = append(r.events, segment.Event{
			Seq: seq, WitnessedAt: now, Kind: segment.KindCreate,
			DID: fmt.Sprintf("did:plc:%d", seq%7), Collection: "app.bsky.feed.post",
			Rkey: fmt.Sprint(seq), Rev: "r", Payload: []byte{0xa1, 0x61, 0x61, byte(seq % 24)},
		})
	}
	hi := uint64(len(r.events))
	frame, _ := r.frame(lo, hi)
	b := catalog.HotBatch{FirstSeq: lo, LastSeq: hi, MinWitnessedUS: now, MaxWitnessedUS: now}
	if pointer {
		ref, err := r.up.Put(r.t.Context(), r.s, frame)
		require.NoError(r.t, err)
		b.Object = ref
	} else {
		b.Frame = frame
	}
	_, err := r.s.CommitHotBatch(r.t.Context(), b)
	require.NoError(r.t, err)
	r.hot = append(r.hot, [2]uint64{lo, hi})
}

// fold folds the oldest k hot batches into one active block.
func (r *rig) fold(k int) {
	r.t.Helper()
	lo, hi := r.hot[0][0], r.hot[k-1][1]
	frame, info := r.frame(lo, hi)
	ref, err := r.up.Put(r.t.Context(), r.s, frame)
	require.NoError(r.t, err)
	c, err := r.s.Fold(r.t.Context(), catalog.Block{Namespace: catalog.Main, Info: info, Object: ref})
	require.NoError(r.t, err)
	r.hot = r.hot[k:]
	r.active = append(r.active, activeBlock{frame: frame, info: info, id: c.ObjectID})
}

// seal seals Main's active segment.
func (r *rig) seal() {
	r.t.Helper()
	frames := make([][]byte, len(r.active))
	sl := catalog.Seal{Namespace: catalog.Main, Segment: r.seg}
	for i, b := range r.active {
		frames[i] = b.frame
		sl.Blocks = append(sl.Blocks, catalog.SealBlock{ObjectID: b.id, CompressedLength: int64(b.info.CompressedSize)})
	}
	hdr, footer, _, err := segment.BuildSealed(segment.SliceFrameSource(frames))
	require.NoError(r.t, err)
	sl.Header = hdr
	sl.Footer, err = r.up.Put(r.t.Context(), r.s, footer)
	require.NoError(r.t, err)
	c, err := r.s.Seal(r.t.Context(), sl)
	require.NoError(r.t, err)
	r.sealed = append(r.sealed, sealedGen{gen: c.GenerationID, header: hdr, footer: footer, frames: frames, blocks: sl.Blocks})
	r.active = nil
	r.seg++
}

// compact publishes a compacted generation of sealed segment seg without
// the rows at seqs, the way the leader's compaction pass does (design §12.2).
func (r *rig) compact(seg uint64, seqs ...uint64) {
	r.t.Helper()
	ctx := r.t.Context()
	src := r.sealed[seg]
	recs := map[segment.RecordKey]uint64{}
	for _, seq := range seqs {
		ev := r.events[seq-1]
		recs[segment.RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}] = seq + 1
	}
	res, err := segment.SparseRewrite(src.header, src.footer, func(i int) ([]byte, error) { return src.frames[i], nil },
		segment.NewTombstones(nil, recs, 0), segment.SparseOptions{})
	require.NoError(r.t, err)
	require.True(r.t, res.Rewritten)
	footer, err := r.up.Put(ctx, r.s, res.Footer)
	require.NoError(r.t, err)
	p := catalog.Publish{Segment: seg, Source: src.gen, Header: res.HeaderBytes, Footer: footer, Blocks: make([]catalog.PublishBlock, len(src.frames))}
	next := sealedGen{header: res.HeaderBytes, footer: res.Footer, frames: append([][]byte(nil), src.frames...), blocks: make([]catalog.SealBlock, len(src.frames))}
	for _, i := range res.Reused {
		p.Blocks[i] = catalog.PublishBlock{Object: catalog.ObjectRef{ID: src.blocks[i].ObjectID}, Reused: true, CompressedLength: src.blocks[i].CompressedLength}
	}
	for _, f := range res.Frames {
		ref, err := r.up.Put(ctx, r.s, f.Frame)
		require.NoError(r.t, err)
		next.frames[f.Block] = f.Frame
		p.Blocks[f.Block] = catalog.PublishBlock{Object: ref, CompressedLength: int64(len(f.Frame))}
	}
	c, err := r.s.PublishGeneration(ctx, p)
	require.NoError(r.t, err)
	next.gen = c.GenerationID
	for i, id := range c.ObjectIDs {
		next.blocks[i] = catalog.SealBlock{ObjectID: id, CompressedLength: p.Blocks[i].CompressedLength}
	}
	r.sealed[seg] = next
	if r.dropped == nil {
		r.dropped = map[uint64]bool{}
	}
	for _, seq := range seqs {
		r.dropped[seq] = true
	}
}

type followerOpts struct {
	listen   bool
	manifest bool
}

func (r *rig) follower(o followerOpts) (*follower.Follower, *follower.Metrics, *manifest.Manifest) {
	r.t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := follower.NewMetrics(prometheus.NewRegistry())
	cfg := follower.Config{
		DB: r.db, Blob: r.blob, ArchiveID: testArchiveID,
		Logger: logger, Metrics: metrics, ReadConcurrency: 4,
	}
	if o.listen {
		cfg.Listener = r.db
	}
	var m *manifest.Manifest
	if o.manifest {
		var err error
		m, err = manifest.NewRemote(manifest.Options{Logger: logger})
		require.NoError(r.t, err)
		cfg.Manifest = m
	}
	f, err := follower.New(cfg)
	require.NoError(r.t, err)
	return f, metrics, m
}

// logEvents reads f's readable log from seq from to its tip.
func logEvents(t *testing.T, f *follower.Follower, from uint64) []segment.Event {
	t.Helper()
	l := f.Log()
	require.NotNil(t, l, "readable log")
	var out []segment.Event
	for cur := from; cur < l.TipSeq(); {
		entries, _, ok, atTip := l.ReadFrom(cur, 1024)
		if atTip {
			break // only compacted-away seqs remain
		}
		require.True(t, ok, "log read at %d (floor %d, tip %d)", cur, l.FloorSeq(), l.TipSeq())
		for _, e := range entries {
			out = append(out, *e.Event())
		}
		cur = entries[len(entries)-1].Event().Seq + 1
	}
	return out
}

// coldEvents decodes Main from seq from through f's view and fetcher.
func coldEvents(t *testing.T, f *follower.Follower, from uint64) []segment.Event {
	t.Helper()
	var out []segment.Event
	for ref := range f.Snapshot().RefsFrom(catalog.Main, from) {
		evs, err := catalog.DecodeRef(t.Context(), f, ref)
		require.NoError(t, err)
		for _, ev := range evs {
			if ev.Seq >= from {
				out = append(out, ev)
			}
		}
	}
	return out
}

// requireStream checks got is exactly the committed events from seq from on,
// less the ones compaction dropped.
func (r *rig) requireStream(got []segment.Event, from uint64) {
	r.t.Helper()
	var want []segment.Event
	for _, ev := range r.events[from-1:] {
		if !r.dropped[ev.Seq] {
			want = append(want, ev)
		}
	}
	require.Len(r.t, got, len(want))
	for i := range want {
		require.Equal(r.t, want[i].Seq, got[i].Seq, "seq at %d", i)
		require.Equal(r.t, want[i].Rkey, got[i].Rkey, "rkey of seq %d", want[i].Seq)
		require.Equal(r.t, want[i].Payload, got[i].Payload, "payload of seq %d", want[i].Seq)
	}
}
