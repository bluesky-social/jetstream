package segment

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// sealBuilderEvents is a seeded event stream that exercises every footer
// section: DIDs repeating within and across blocks, DID-less events, marker
// kinds (sentinel collections), and collections first seen at different
// points of a block.
func sealBuilderEvents(rng *rand.Rand, n int) []Event {
	colls := []string{"app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.graph.follow", "com.example.rare", ""}
	events := make([]Event, n)
	for i := range events {
		did := ""
		if rng.IntN(10) != 0 {
			did = fmt.Sprintf("did:plc:%04d", rng.IntN(1+n/8))
		}
		events[i] = Event{
			Seq:         uint64(i + 1),
			WitnessedAt: int64(1000 + i - rng.IntN(50)),
			Kind:        Kind(1 + rng.IntN(7)),
			DID:         did,
			Collection:  colls[rng.IntN(len(colls))],
			Rkey:        fmt.Sprintf("k%d", i),
			Rev:         "rev",
			Payload:     []byte{byte(i), byte(rng.IntN(256))},
		}
	}
	return events
}

// indexedSeal builds the sealed metadata from frames indexed concurrently
// ahead of the builder and added in order, as a direct-mode segment does.
func indexedSeal(t testing.TB, frames [][]byte) (header, footer []byte, h Header, err error) {
	t.Helper()
	ixs := make([]*BlockIndex, len(frames))
	errs := make([]error, len(frames))
	var wg sync.WaitGroup
	for i, frame := range frames {
		wg.Go(func() { ixs[i], errs[i] = IndexBlock(frame) })
	}
	wg.Wait()
	b := NewSealBuilder()
	for i := range frames {
		if errs[i] != nil {
			return nil, nil, Header{}, errs[i]
		}
		if err := b.Add(ixs[i]); err != nil {
			return nil, nil, Header{}, err
		}
	}
	return b.Finish()
}

// sealBuilderDigests are sha256(header || footer) of each seed's segment,
// as the seal built them before SealBuilder: the indexed path must not
// change a byte of what seals produce.
var sealBuilderDigests = []string{
	"09b7a1cafb4617b38317df846ce02d791b74066300830ab337c9abba9f6cc85f",
	"2f4dad12fdcfa63ea50221e8f2130aa8fb1d8e5caca0108c1c0431113bf0641a",
	"211a32859ed2bcdf4738d87a44db120e26bc383dc2411700cdccbf9907278f26",
	"dce4d088606f0ab895b3e288b35feff25e0e09cd5f9276fbeebe6f69623b9da8",
	"800938cf7ec06048a776753d31fd39ad04dd0275e1f59a870177a5f7c6dae562",
	"486613d668f25d25fc554e222a813bc431fd970106ffde10bd74606d44a0a846",
	"8eec40c925f0fa1c6da73669f8ae71513440f5bf5b81bb31a91f27ca4750c12f",
	"6577365006a92a94b6f24064a2410deb2c8c74a4f1be8e2e70bc7d18be53c54a",
	"c6eadc6418ab55263d531bee9f10488b98ca9185a9b69474d8bf5f800aa97e3f",
	"78df2ff2d9545ad8dffd49732665ccedf70b02ec38cf571b6d590ca60d5f270a",
	"e72370ddee89511860c066633d95ec31fda4b8522fd402b3a833825b0e7efd2e",
	"e4ba6720315267d432880f959f5eabf1b8cd1da46bc11060f35687f6eb984290",
	"e18e415ec4e11091f4050fdfa221c1b6d6cd5295491442e0f07699eaceebf12a",
	"d3bc5c71a6d052b80a6a4fe88f4517b7b187a887073b74fd6c4f0939c38a0f77",
	"1ae41f4bf9dd552a0ee790d27bd9c3b0cc7f3f2fc7c6a257597424963533dcc0",
	"deded430e2170698acd22951e4fa8e20338aef075d54ea67f0f01798f563d264",
}

// TestSealBuilderIndexedMatchesBuildSealed: blocks indexed ahead, out of
// order, and added in order seal to exactly BuildSealed's bytes, which
// TestBuildSealedMatchesFileSeal ties to the file seal, and both match the
// bytes seals produced before SealBuilder.
func TestSealBuilderIndexedMatchesBuildSealed(t *testing.T) {
	t.Parallel()
	for seed := range uint64(len(sealBuilderDigests)) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewPCG(seed, 0x5ea1))
			frames := encodeFrames(t, sealBuilderEvents(rng, 1+rng.IntN(600)), 1+rng.IntN(64))
			wantHeader, wantFooter, wantH, err := BuildSealed(SliceFrameSource(frames))
			require.NoError(t, err)
			sum := sha256.Sum256(append(append([]byte(nil), wantHeader...), wantFooter...))
			require.Equal(t, sealBuilderDigests[seed], hex.EncodeToString(sum[:]))
			header, footer, h, err := indexedSeal(t, frames)
			require.NoError(t, err)
			require.Equal(t, wantH, h)
			require.Equal(t, wantHeader, header)
			require.Equal(t, wantFooter, footer)

			r, err := OpenReaderParts(header, footer, frameFetcher(frames), ReaderOptions{})
			require.NoError(t, err)
			require.NoError(t, VerifySealedMetadata(r))
		})
	}
}

// An empty block ends the index through Add as it does through
// BuildSealed, while later blocks still count toward the footer offset.
func TestSealBuilderIndexedEmptyBlockEndsIndex(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(3, 3))
	frames := encodeFrames(t, sealBuilderEvents(rng, 40), 8)
	frames = append(frames[:2:2], append([][]byte{encodeEmptyBlockCompressed()}, frames[2:]...)...)
	wantHeader, wantFooter, wantH, err := BuildSealed(SliceFrameSource(frames))
	require.NoError(t, err)
	require.EqualValues(t, 2, wantH.BlockCount)
	header, footer, h, err := indexedSeal(t, frames)
	require.NoError(t, err)
	require.Equal(t, wantH, h)
	require.Equal(t, wantHeader, header)
	require.Equal(t, wantFooter, footer)
}

func TestIndexBlockRejectsCorruptFrame(t *testing.T) {
	t.Parallel()
	_, err := IndexBlock([]byte("not zstd"))
	require.Error(t, err)
}

func BenchmarkBuildSealedManyBlocks(b *testing.B) {
	rng := rand.New(rand.NewPCG(1, 1))
	frames := encodeFrames(b, sealBuilderEvents(rng, 256*256), 256)
	b.ResetTimer()
	for b.Loop() {
		if _, _, _, err := BuildSealed(SliceFrameSource(frames)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSealBuilderIndexedManyBlocks is the seal-time work left once
// every block was indexed as it was written.
func BenchmarkSealBuilderIndexedManyBlocks(b *testing.B) {
	rng := rand.New(rand.NewPCG(1, 1))
	frames := encodeFrames(b, sealBuilderEvents(rng, 256*256), 256)
	ixs := make([]*BlockIndex, len(frames))
	for i, frame := range frames {
		var err error
		if ixs[i], err = IndexBlock(frame); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for b.Loop() {
		sb := NewSealBuilder()
		for _, ix := range ixs {
			if err := sb.Add(ix); err != nil {
				b.Fatal(err)
			}
		}
		if _, _, _, err := sb.Finish(); err != nil {
			b.Fatal(err)
		}
	}
}
