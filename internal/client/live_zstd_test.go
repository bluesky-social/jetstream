package client

import (
	"encoding/binary"
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
