package segment

import (
	"bytes"
	"testing"
)

// fuzzSegmentFrames turns fuzz bytes into block frames. data[0] picks
// the block size; each later 4-byte group is one event (kind, DID,
// collection, payload length) whose payload is taken from the bytes
// that follow. If data[0]'s high bit is set, whatever is left becomes
// one more raw frame, usually garbage, so BuildSealed also sees frames
// no BlockBuilder made.
func fuzzSegmentFrames(t *testing.T, data []byte) (frames [][]byte, dids []string, raw bool) {
	if len(data) == 0 {
		return nil, nil, false
	}
	raw = data[0]&0x80 != 0
	b, err := NewBlockBuilder(1 + int(data[0]%8))
	if err != nil {
		t.Fatal(err)
	}
	didNames := []string{"did:plc:a", "did:plc:b", "did:plc:c", "did:web:d", ""}
	colls := []string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.graph.follow", ""}
	rest := data[1:]
	seq := uint64(0)
	for len(rest) >= 4 && seq < 256 {
		g := rest[:4]
		rest = rest[4:]
		n := min(int(g[3]%16), len(rest))
		seq++
		ev := Event{
			Seq:         seq,
			WitnessedAt: int64(g[0]) - 100,
			Kind:        Kind(1 + g[0]%7),
			DID:         didNames[int(g[1])%len(didNames)],
			Collection:  colls[int(g[2])%len(colls)],
			Rkey:        "k",
			Payload:     rest[:n],
		}
		rest = rest[n:]
		if ev.DID != "" {
			dids = append(dids, ev.DID)
		}
		full, err := b.Append(ev)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		if full {
			fr, _ := b.Encode()
			frames = append(frames, fr)
		}
	}
	if fr, _ := b.Encode(); fr != nil {
		frames = append(frames, fr)
	}
	if raw {
		frames = append(frames, rest)
	}
	return frames, dids, raw
}

// FuzzBuildSealed builds sealed metadata from fuzz-derived frames and
// reads it back through the byte-source Readers: valid frames must seal,
// every Reader must agree and pass VerifySealedMetadata, and a flipped
// byte anywhere in the checksummed header or footer must be rejected.
// Corrupt input of any kind must error, never panic.
func FuzzBuildSealed(f *testing.F) {
	f.Add([]byte{}, uint32(0), byte(0))
	f.Add([]byte{3, 1, 0, 0, 2, 'h', 'i', 2, 1, 1, 0, 3, 2, 2, 5, 'p', 'a', 'y', 'l', 'd'}, uint32(40), byte(1))
	f.Add([]byte{0x81, 1, 1, 1, 0, 2, 2, 2, 0, 'z', 's', 't', 'd'}, uint32(7), byte(0x80))
	f.Add(bytes.Repeat([]byte{1, 2, 3, 4}, 64), uint32(300), byte(0xff))

	f.Fuzz(func(t *testing.T, data []byte, flipAt uint32, flip byte) {
		frames, dids, raw := fuzzSegmentFrames(t, data)
		header, footer, h, err := BuildSealed(SliceFrameSource(frames))
		if err != nil {
			if !raw {
				t.Fatalf("BuildSealed rejected BlockBuilder frames: %v", err)
			}
			return
		}
		seg := assembleSegment(header, frames, footer)
		if h.FooterOffset+uint64(len(footer)) != uint64(len(seg)) {
			t.Fatalf("footer offset %d + footer %d != segment %d", h.FooterOffset, len(footer), len(seg))
		}

		at, err := OpenReaderAt(bytes.NewReader(seg), int64(len(seg)), ReaderOptions{})
		if err != nil {
			t.Fatalf("OpenReaderAt of a BuildSealed segment: %v", err)
		}
		parts, err := OpenReaderParts(header, footer, frameFetcher(frames[:h.BlockCount]), ReaderOptions{})
		if err != nil {
			t.Fatalf("OpenReaderParts of a BuildSealed segment: %v", err)
		}
		requireReadersAgree(t, map[string]*Reader{"file": at, "parts": parts}, dids)

		if len(seg) == 0 {
			return
		}
		mut := append([]byte(nil), seg...)
		pos := int(flipAt % uint32(len(mut)))
		if flip == 0 {
			flip = 1
		}
		mut[pos] ^= flip
		checksummed := pos < ReservedHeaderBytes || uint64(pos) >= h.FooterOffset
		if r, err := OpenReaderAt(bytes.NewReader(mut), int64(len(mut)), ReaderOptions{}); err == nil {
			if checksummed {
				t.Fatalf("flip at %d (footer at %d) passed the checksum", pos, h.FooterOffset)
			}
			exerciseReader(r, dids)
		}
		if r, err := OpenReaderAt(bytes.NewReader(mut), int64(len(mut)), ReaderOptions{SkipChecksum: true}); err == nil {
			exerciseReader(r, dids)
		}
		mutHeader := mut[:ReservedHeaderBytes]
		mutFooter := mut[h.FooterOffset:]
		if r, err := OpenReaderParts(mutHeader, mutFooter, frameFetcher(frames), ReaderOptions{SkipChecksum: true}); err == nil {
			exerciseReader(r, dids)
		}
	})
}

// exerciseReader calls every query on r, ignoring errors: on corrupt
// input the only requirement is that none of them panic.
func exerciseReader(r *Reader, dids []string) {
	_ = VerifySealedMetadata(r)
	for i := range r.Blocks() {
		_, _ = r.DecodeBlock(i)
		_, _ = r.BlockBloom(i)
		_, _ = r.BlockCollections(i)
	}
	for _, did := range dids {
		_, _ = r.BlocksContainingDID(did)
	}
	_ = r.Close()
}
