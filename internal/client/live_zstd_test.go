package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/coder/websocket"
	kpdict "github.com/klauspost/compress/dict"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// TestLiveConsumerZstd_DecodesBinaryFrames pins the client half of the v2
// dict-zstd contract: with zstdDict set, the consumer sends the parsed
// dictionary ID on the wire and decompresses BINARY frames before decode.
// The dictionary is generated with klauspost's dict builder (any valid
// structured dictionary works — the test only needs encoder/decoder
// symmetry, not ratio).
func TestLiveConsumerZstd_DecodesBinaryFrames(t *testing.T) {
	t.Parallel()

	dict := buildKPDict(t, 424242)

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(dict), zstd.WithEncoderConcurrency(1))
	require.NoError(t, err)

	f1 := enc.EncodeAll(liveCommitFrame(t, 1, "did:plc:a", "create", "c", "r1", true), nil)
	f2 := enc.EncodeAll(liveCommitFrame(t, 2, "did:plc:a", "create", "c", "r2", true), nil)

	conn := &scriptedConn{steps: []readStep{
		{data: f1, msgType: websocket.MessageBinary},
		{data: f2, msgType: websocket.MessageBinary},
	}}
	var (
		mu   sync.Mutex
		urls []string
	)
	dial := capturingDialer(&urls, &mu, conn)

	events, errs := runConsumer(t, liveConfig{host: "https://h", dial: dial, fromTip: true, zstdDict: dict}, 2)
	require.Empty(t, errs)
	require.Equal(t, []uint64{1, 2}, seqs(events))

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, urls)
	require.Contains(t, urls[0], "zstdDictionary=424242",
		"the wire opt-in must carry the ID parsed from the dictionary header")
}

// TestLiveConsumerZstd_MalformedFrameSurfacesAndContinues pins never-crash:
// a corrupt binary frame emits an error and the tail keeps going.
func TestLiveConsumerZstd_MalformedFrameSurfacesAndContinues(t *testing.T) {
	t.Parallel()

	dict := buildKPDict(t, 424243)
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(dict), zstd.WithEncoderConcurrency(1))
	require.NoError(t, err)
	good := enc.EncodeAll(liveCommitFrame(t, 1, "did:plc:a", "create", "c", "r1", true), nil)

	conn := &scriptedConn{steps: []readStep{
		{data: []byte("not zstd at all"), msgType: websocket.MessageBinary},
		{data: good, msgType: websocket.MessageBinary},
	}}
	dial, _ := scriptedDialer(conn)

	events, errs := runConsumer(t, liveConfig{host: "https://h", dial: dial, fromTip: true, zstdDict: dict}, 1)
	require.Equal(t, []uint64{1}, seqs(events), "the good frame after the bad one must still deliver")
	require.NotEmpty(t, errs, "the malformed frame must surface as an error")
	require.Contains(t, errs[0].Error(), "zstd frame decode")
}

// TestLiveConsumerZstd_InvalidDictFallsBackUncompressed pins the documented
// degradation: an unparseable dictionary logs and falls back to plain text
// frames (no zstdDictionary param on the wire).
func TestLiveConsumerZstd_InvalidDictFallsBackUncompressed(t *testing.T) {
	t.Parallel()

	conn := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 1, "did:plc:a", "create", "c", "r1", true)},
	}}
	var (
		mu   sync.Mutex
		urls []string
	)
	dial := capturingDialer(&urls, &mu, conn)

	events, _ := runConsumer(t, liveConfig{host: "https://h", dial: dial, fromTip: true, zstdDict: []byte("junk")}, 1)
	require.Equal(t, []uint64{1}, seqs(events))

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, urls)
	require.NotContains(t, urls[0], "zstdDictionary=",
		"an invalid dictionary must not put an opt-in on the wire")
}

// TestLiveConsumerZstd_DictRotationRefetches pins the recovery path for a
// server-side dictionary rotation: the first dial is refused with the
// dict-rejected 400 marker, the consumer refetches the CURRENT dictionary,
// and the reconnect negotiates the new ID and decodes frames compressed
// with it.
func TestLiveConsumerZstd_DictRotationRefetches(t *testing.T) {
	t.Parallel()

	oldDict := buildKPDict(t, 424244)
	newDict := buildKPDict(t, 424245)

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(newDict), zstd.WithEncoderConcurrency(1))
	require.NoError(t, err)
	frame := enc.EncodeAll(liveCommitFrame(t, 1, "did:plc:a", "create", "c", "r1", true), nil)

	conn := &scriptedConn{steps: []readStep{{data: frame, msgType: websocket.MessageBinary}}}
	var (
		mu   sync.Mutex
		urls []string
	)
	dial := func(ctx context.Context, rawURL string) (wsConn, error) {
		mu.Lock()
		defer mu.Unlock()
		urls = append(urls, rawURL)
		if len(urls) == 1 {
			// Mirror the server's rotation refusal, as mapped by dialWebsocket.
			return nil, fmt.Errorf("%w: %s 424244; current dictionary id is 424245", errLiveDictRejected, zstdDictRejectedMarker)
		}
		return conn, nil
	}

	events, errs := runConsumer(t, liveConfig{
		host: "https://h", dial: dial, fromTip: true, zstdDict: oldDict,
		refetchDict: func(context.Context) []byte { return newDict },
	}, 1)
	require.Equal(t, []uint64{1}, seqs(events), "the rotated-dictionary frame must decode after refetch")
	require.NotEmpty(t, errs, "the rejected dial must surface as a reconnect error")

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, urls, 2)
	require.Contains(t, urls[0], "zstdDictionary=424244")
	require.Contains(t, urls[1], "zstdDictionary=424245",
		"the reconnect must negotiate the refetched dictionary's ID")
}

// TestLiveConsumerZstd_DictRejectedFallsBackUncompressed pins the degradation
// when no refetch hook is wired (or it keeps failing): after a dict-rejected
// dial the consumer sheds the opt-in and tails uncompressed rather than
// 400-looping forever.
func TestLiveConsumerZstd_DictRejectedFallsBackUncompressed(t *testing.T) {
	t.Parallel()

	dict := buildKPDict(t, 424246)
	conn := &scriptedConn{steps: []readStep{
		{data: liveCommitFrame(t, 1, "did:plc:a", "create", "c", "r1", true)},
	}}
	var (
		mu   sync.Mutex
		urls []string
	)
	dial := func(ctx context.Context, rawURL string) (wsConn, error) {
		mu.Lock()
		defer mu.Unlock()
		urls = append(urls, rawURL)
		if len(urls) == 1 {
			return nil, fmt.Errorf("%w: %s 424246", errLiveDictRejected, zstdDictRejectedMarker)
		}
		return conn, nil
	}

	events, _ := runConsumer(t, liveConfig{host: "https://h", dial: dial, fromTip: true, zstdDict: dict}, 1)
	require.Equal(t, []uint64{1}, seqs(events), "the tail must keep flowing uncompressed")

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, urls, 2)
	require.Contains(t, urls[0], "zstdDictionary=424246")
	require.NotContains(t, urls[1], "zstdDictionary=",
		"after a rejected dial with no refetch, the opt-in must come off the wire")
}

// TestLiveConsumerZstd_DecodeBoundedByReadLimit pins the decompression-bomb
// guard: a frame whose decoded size exceeds the connection read limit is
// rejected by the decoder (surfaced as a frame error), not allocated — the
// read limit bounds logical frame size on both compressed and uncompressed
// connections.
func TestLiveConsumerZstd_DecodeBoundedByReadLimit(t *testing.T) {
	t.Parallel()

	dict := buildKPDict(t, 424247)
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderDict(dict), zstd.WithEncoderConcurrency(1))
	require.NoError(t, err)

	// Highly compressible 1 MiB payload; tiny on the wire, huge decoded.
	bomb := enc.EncodeAll(bytes.Repeat([]byte("a"), 1<<20), nil)
	require.Less(t, len(bomb), 64<<10, "the bomb must fit the compressed read limit")
	good := enc.EncodeAll(liveCommitFrame(t, 1, "did:plc:a", "create", "c", "r1", true), nil)

	conn := &scriptedConn{steps: []readStep{
		{data: bomb, msgType: websocket.MessageBinary},
		{data: good, msgType: websocket.MessageBinary},
	}}
	dial, _ := scriptedDialer(conn)

	events, errs := runConsumer(t, liveConfig{
		host: "https://h", dial: dial, fromTip: true, zstdDict: dict,
		readLimit: 64 << 10, // decoded frames must fit 64 KiB
	}, 1)
	require.Equal(t, []uint64{1}, seqs(events), "the good frame after the bomb must still deliver")
	require.NotEmpty(t, errs, "the oversize decode must surface as an error")
	require.Contains(t, errs[0].Error(), "zstd frame decode")
}

// buildKPDict makes a small but valid structured zstd dictionary with the
// given ID using klauspost's builder over synthetic JSON-ish samples.
func buildKPDict(t *testing.T, id uint32) []byte {
	t.Helper()
	samples := make([][]byte, 0, 128)
	for i := range 128 {
		frame := liveCommitFrame(t, uint64(i+1), "did:plc:traindata", "create", "app.bsky.feed.post", strings.Repeat("r", i%7+1), true)
		samples = append(samples, frame)
	}
	d, err := kpdict.BuildZstdDict(samples, kpdict.Options{
		MaxDictSize: 8 << 10,
		HashBytes:   6,
		ZstdDictID:  id,
	})
	require.NoError(t, err)
	gotID := binary.LittleEndian.Uint32(d[4:8])
	require.Equal(t, id, gotID)
	return d
}
