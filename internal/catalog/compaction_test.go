package catalog_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// sealedSegment is main segment 0, sealed from blocks [1,3] and [4,6].
type sealedSegment struct {
	commit catalog.SealCommit
	seal   catalog.Seal
	frames [][]byte
	footer []byte
}

func (h *harness) sealed() sealedSegment {
	h.t.Helper()
	h.hot(1, 3)
	h.hot(4, 6)
	bs := []builtBlock{h.fold(1, 3), h.fold(4, 6)}
	out := sealedSegment{seal: catalog.Seal{Namespace: catalog.Main}}
	for _, b := range bs {
		out.frames = append(out.frames, b.frame)
		out.seal.Blocks = append(out.seal.Blocks, catalog.SealBlock{ObjectID: b.commit.ObjectID, CompressedLength: int64(b.info.CompressedSize)})
	}
	hdr, footer, _, err := segment.BuildSealed(segment.SliceFrameSource(out.frames))
	require.NoError(h.t, err)
	out.seal.Header, out.footer = hdr, footer
	out.seal.Footer = h.object(footer)
	out.commit, err = h.s.Seal(h.t.Context(), out.seal)
	require.NoError(h.t, err)
	return out
}

// rewrite sparse-rewrites the generation src (built from frames and
// footer, whose blocks are srcBlocks) dropping the rows at seqs, uploads
// the changed blocks and footer, and returns the Publish, the new frames,
// and the new footer.
func (h *harness) rewrite(src uint64, hdr, footer []byte, frames [][]byte, srcBlocks []catalog.SealBlock, seqs ...uint64) (catalog.Publish, [][]byte, []byte) {
	h.t.Helper()
	recs := map[segment.RecordKey]uint64{}
	for _, seq := range seqs {
		recs[segment.RecordKey{DID: "did:plc:test", Collection: "app.bsky.feed.post", Rkey: fmt.Sprint(seq)}] = 1000
	}
	res, err := segment.SparseRewrite(hdr, footer, func(i int) ([]byte, error) { return frames[i], nil },
		segment.NewTombstones(nil, recs, 0), segment.SparseOptions{})
	require.NoError(h.t, err)
	require.True(h.t, res.Rewritten)
	p := catalog.Publish{Source: src, Header: res.HeaderBytes, Footer: h.object(res.Footer)}
	out := append([][]byte(nil), frames...)
	p.Blocks = make([]catalog.PublishBlock, len(frames))
	for _, i := range res.Reused {
		p.Blocks[i] = catalog.PublishBlock{Object: catalog.ObjectRef{ID: srcBlocks[i].ObjectID}, Reused: true, CompressedLength: srcBlocks[i].CompressedLength}
	}
	for _, f := range res.Frames {
		out[f.Block] = f.Frame
		p.Blocks[f.Block] = catalog.PublishBlock{Object: h.object(f.Frame), CompressedLength: int64(len(f.Frame))}
	}
	return p, out, res.Footer
}

func TestScripts_PublishGeneration(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		ctx := t.Context()
		sg := h.sealed()
		p, frames, footer := h.rewrite(sg.commit.GenerationID, sg.seal.Header, sg.footer, sg.frames, sg.seal.Blocks, 5)
		require.True(t, p.Blocks[0].Reused)
		require.False(t, p.Blocks[1].Reused)
		c, err := h.s.PublishGeneration(ctx, p)
		require.NoError(t, err)
		require.Equal(t, sg.seal.Blocks[0].ObjectID, c.ObjectIDs[0])

		snap, err := h.snapshot()
		require.NoError(t, err)
		require.Equal(t, c.GenerationID, snap.Segments[0].GenerationID)
		require.Equal(t, c.Revision, snap.Segments[0].Revision)
		require.NotContains(t, snap.Generations, sg.commit.GenerationID, "the source generation is gone")
		gbs := snap.GenerationBlocks[c.GenerationID]
		require.Len(t, gbs, 2)
		require.Equal(t, int64(len(frames[1])), gbs[1].CompressedLength)
		require.Equal(t, c.FooterObjectID, snap.Generations[c.GenerationID].FooterObjectID)

		// A second rewrite of the new generation drops everything.
		p2, _, _ := h.rewrite(c.GenerationID, p.Header, footer, frames,
			[]catalog.SealBlock{{ObjectID: c.ObjectIDs[0], CompressedLength: gbs[0].CompressedLength}, {ObjectID: c.ObjectIDs[1], CompressedLength: gbs[1].CompressedLength}},
			1, 2, 3, 4, 6)
		c2, err := h.s.PublishGeneration(ctx, p2)
		require.NoError(t, err)
		snap, err = h.snapshot()
		require.NoError(t, err)
		hdr, err := segment.ReadSealedHeader(bytes.NewReader(snap.Generations[c2.GenerationID].Header))
		require.NoError(t, err)
		require.Zero(t, hdr.EventCount)
		require.Equal(t, uint64(1), hdr.MinSeq, "an empty rewrite keeps the envelope")
		require.Equal(t, uint64(6), hdr.MaxSeq)

		// The segment after it still continues at seq 7.
		h.hot(7, 7)
		require.NoError(t, h.s.Err())
	})
}

func TestScripts_PublishGenerationRejects(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		setup := func(t *testing.T) (*harness, sealedSegment, catalog.Publish) {
			h := newHarness(t, be)
			sg := h.sealed()
			p, _, _ := h.rewrite(sg.commit.GenerationID, sg.seal.Header, sg.footer, sg.frames, sg.seal.Blocks, 2, 5)
			return h, sg, p
		}
		corrupt := map[string]func(h *harness, sg sealedSegment, p *catalog.Publish){
			"stale source": func(h *harness, sg sealedSegment, p *catalog.Publish) {
				_, err := h.s.PublishGeneration(h.t.Context(), *p)
				require.NoError(h.t, err)
				// The same publish again names a source that is no longer
				// current.
				p.Footer = h.object([]byte("another footer"))
			},
			"active segment": func(h *harness, sg sealedSegment, p *catalog.Publish) {
				p.Segment = 1
			},
			"reused block is not the source's": func(h *harness, sg sealedSegment, p *catalog.Publish) {
				p.Blocks[0] = catalog.PublishBlock{Object: catalog.ObjectRef{ID: sg.seal.Footer.ID}, Reused: true, CompressedLength: p.Blocks[0].CompressedLength}
			},
			"reused block at another ordinal": func(h *harness, sg sealedSegment, p *catalog.Publish) {
				p.Blocks[0] = catalog.PublishBlock{Object: catalog.ObjectRef{ID: sg.seal.Blocks[1].ObjectID}, Reused: true, CompressedLength: sg.seal.Blocks[1].CompressedLength}
			},
			"events do not decrease": func(h *harness, sg sealedSegment, p *catalog.Publish) {
				p.Header = sg.seal.Header
			},
			"envelope changes": func(h *harness, sg sealedSegment, p *catalog.Publish) {
				// Two blocks and five events, but covering [1,5].
				f1, _ := block(h.t, 1, 2)
				f2, _ := block(h.t, 3, 5)
				hdr, _, _, err := segment.BuildSealed(segment.SliceFrameSource([][]byte{f1, f2}))
				require.NoError(h.t, err)
				p.Header = hdr
			},
		}
		for name, f := range corrupt {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				h, sg, p := setup(t)
				f(h, sg, &p)
				before, err := h.snapshot()
				require.NoError(t, err)
				_, err = h.s.PublishGeneration(t.Context(), p)
				requireCorruption(t, err, catalog.SourceCompaction)
				after, err := h.snapshot()
				require.NoError(t, err)
				require.Equal(t, before.Segments, after.Segments, "a rejected publish changes nothing")
			})
		}
		t.Run("block count", func(t *testing.T) {
			t.Parallel()
			h, _, p := setup(t)
			p.Blocks = p.Blocks[:1]
			_, err := h.s.PublishGeneration(t.Context(), p)
			require.Error(t, err)
			_, ok := catalog.IsCorruption(err)
			require.False(t, ok, "a malformed publish is a caller bug, not corruption")
			require.Error(t, h.s.Err(), "and still ends the session")
		})
	})
}

func TestScripts_CompareAndSetMeta(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		ctx := t.Context()
		_, err := h.s.CompareAndSetMeta(ctx, "compaction/seq", nil, []byte("a"))
		require.NoError(t, err)
		_, err = h.s.CompareAndSetMeta(ctx, "compaction/seq", []byte("a"), []byte("b"))
		require.NoError(t, err)
		v, _ := h.meta("compaction/seq")
		require.Equal(t, "b", string(v))
		_, err = h.s.CompareAndSetMeta(ctx, "compaction/seq", []byte("a"), []byte("c"))
		requireCorruption(t, err, catalog.SourceCompaction)

		h.newSession()
		_, err = h.s.CompareAndSetMeta(ctx, "compaction/seq", nil, []byte("c"))
		requireCorruption(t, err, catalog.SourceCompaction)
		v, _ = h.meta("compaction/seq")
		require.Equal(t, "b", string(v))
	})
}

// objectStates reads the object-state counts.
func (h *harness) objectStates() map[catalog.ObjectState]int64 {
	h.t.Helper()
	r, err := h.db.BeginRead(h.t.Context())
	require.NoError(h.t, err)
	defer func() { _ = r.Close(context.Background()) }()
	got, err := r.ObjectStates(h.t.Context())
	require.NoError(h.t, err)
	return got
}

// gcMark runs the mark to completion in pages of limit.
func (h *harness) gcMark(limit int) int {
	h.t.Helper()
	var after uint64
	marked := 0
	for {
		page, err := h.s.GCMark(h.t.Context(), after, limit)
		require.NoError(h.t, err)
		marked += page.Marked
		if page.Scanned < limit {
			return marked
		}
		after = page.Last
	}
}

// Every delay below is negative so the claim does not wait on the clock.
const claimNow = -time.Hour

func TestScripts_GC(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		ctx := t.Context()
		ptr := h.object([]byte("pointer batch"))
		_, err := h.s.CommitHotBatch(ctx, catalog.HotBatch{FirstSeq: 1, LastSeq: 1, Object: ptr})
		require.NoError(t, err)
		orphan := h.object([]byte("never committed"))
		require.Zero(t, h.gcMark(1000), "the pointer batch is referenced; the orphan is not available")

		// Folding deletes the pointer batch, leaving its object
		// unreferenced.
		h.fold(1, 1)
		require.Equal(t, 1, h.gcMark(1), "in pages of one")
		require.Zero(t, h.gcMark(1000), "a marked object is not marked again")

		claimed, err := h.s.GCClaim(ctx, time.Hour, time.Hour, 1000)
		require.NoError(t, err)
		require.Empty(t, claimed, "nothing has aged past the delays")

		claimed, err = h.s.GCClaim(ctx, claimNow, claimNow, 1000)
		require.NoError(t, err)
		ids := []uint64{}
		for _, o := range claimed {
			ids = append(ids, o.ID)
		}
		require.Equal(t, []uint64{ptr.ID, orphan.ID}, ids)
		require.Equal(t, catalog.ObjectAvailable, claimed[0].State, "claims report their pre-claim state")
		require.Equal(t, catalog.ObjectUploading, claimed[1].State)
		require.Equal(t, int64(2), h.objectStates()[catalog.ObjectDeleting])

		// A run that dies before forgetting resumes with the same rows.
		h.newSession()
		again, err := h.s.GCClaim(ctx, claimNow, claimNow, 1000)
		require.NoError(t, err)
		require.Len(t, again, 2)
		for i := range again {
			require.Equal(t, catalog.ObjectDeleting, again[i].State)
			require.Equal(t, claimed[i].Key, again[i].Key)
		}

		n, err := h.s.GCForget(ctx, ids)
		require.NoError(t, err)
		require.Equal(t, 2, n)
		n, err = h.s.GCForget(ctx, ids)
		require.NoError(t, err)
		require.Zero(t, n, "forget is idempotent")
		require.Zero(t, h.objectStates()[catalog.ObjectDeleting])

		claimed, err = h.s.GCClaim(ctx, claimNow, claimNow, 1000)
		require.NoError(t, err)
		require.Empty(t, claimed, "everything left is referenced")

		// Dedup never finds a claimed row, so the same bytes upload afresh.
		ref := h.object([]byte("pointer batch"))
		require.True(t, ref.Pending)
		require.NotEqual(t, ptr.ID, ref.ID)
	})
}

func TestScripts_GCRewrittenGeneration(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		ctx := t.Context()
		sg := h.sealed()
		p, _, _ := h.rewrite(sg.commit.GenerationID, sg.seal.Header, sg.footer, sg.frames, sg.seal.Blocks, 5)
		_, err := h.s.PublishGeneration(ctx, p)
		require.NoError(t, err)
		require.Equal(t, 2, h.gcMark(1000), "the source footer and its rewritten block")
		claimed, err := h.s.GCClaim(ctx, claimNow, time.Hour, 1000)
		require.NoError(t, err)
		ids := []uint64{}
		for _, o := range claimed {
			ids = append(ids, o.ID)
		}
		require.ElementsMatch(t, []uint64{sg.seal.Footer.ID, sg.seal.Blocks[1].ObjectID}, ids)
		_, err = h.s.GCForget(ctx, ids)
		require.NoError(t, err)
	})
}

// A claimed object that is still referenced is corruption. §7.4 clears
// unreferenced_at on every reference, so only a reference that skipped it
// reaches the claim; this inserts one with a raw transaction.
func TestScripts_GCClaimReferenced(t *testing.T) {
	t.Parallel()
	eachBackend(t, func(t *testing.T, be backend) {
		h := newHarness(t, be)
		ctx := t.Context()
		ptr := h.object([]byte("pointer batch"))
		_, err := h.s.CommitHotBatch(ctx, catalog.HotBatch{FirstSeq: 1, LastSeq: 1, Object: ptr})
		require.NoError(t, err)
		h.fold(1, 1)
		require.Equal(t, 1, h.gcMark(1000))

		tx, err := h.db.Begin(ctx, catalog.TxHotBatch)
		require.NoError(t, err)
		rev, ok, err := tx.FenceBump(ctx, h.lock.Epoch())
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, tx.InsertHotBatch(ctx, catalog.HotBatchRow{
			FirstSeq: 2, LastSeq: 2, EventCount: 1, Epoch: h.lock.Epoch(), Revision: rev, ObjectID: ptr.ID,
		}))
		require.NoError(t, tx.ApplyMeta(ctx, []metastore.Op{{Kind: metastore.OpSet, Key: []byte(catalog.MainSeqKey), Value: catalog.EncodeSeq(3)}}))
		require.NoError(t, tx.Commit(ctx))

		h.newSession()
		_, err = h.s.GCClaim(ctx, claimNow, claimNow, 1000)
		requireCorruption(t, err, catalog.SourceGC)
		require.Zero(t, h.objectStates()[catalog.ObjectDeleting], "the failed claim rolled back")
	})
}
