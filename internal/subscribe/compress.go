package subscribe

import (
	"encoding/binary"
	"fmt"

	_ "embed"

	"github.com/klauspost/compress/zstd"
)

// zstdDictionary is the Jetstream v1 custom zstd dictionary (dict ID
// 1612007021), copied verbatim from jetstream v1
// (pkg/models/zstd_dictionary). It is trained on the atproto firehose
// JSON and gives a better ratio than generic deflate on this
// small-message, highly repetitive stream.
//
// NOT PREFERRED. This custom-dictionary scheme exists only for
// backwards compatibility with v1 clients (?compress=true /
// Socket-Encoding: zstd). New consumers should use standard RFC 7692
// permessage-deflate, which v2 negotiates automatically and which needs
// no out-of-band dictionary.
//
//go:embed zstd_dictionary
var zstdDictionary []byte

// zstdEncoder is the process-wide encoder for the v1 compatibility
// scheme. klauspost/compress's EncodeAll is safe for concurrent use on a
// shared *zstd.Encoder, so one instance serves every subscriber. The
// configuration mirrors v1 exactly (WithEncoderDict + 128 KiB window +
// single-goroutine concurrency) so frames are byte-compatible with v1
// decoders.
var zstdEncoder = mustNewZstdEncoder()

func mustNewZstdEncoder() *zstd.Encoder {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderDict(zstdDictionary),
		zstd.WithWindowSize(1<<17),
		zstd.WithEncoderConcurrency(1))
	if err != nil {
		// The dictionary is embedded at build time; a failure here is a
		// build/programmer error, not runtime input. Fail loud.
		panic(fmt.Sprintf("subscribe: build zstd encoder: %v", err))
	}
	return enc
}

// compressFrame returns src compressed as a single zstd frame using the
// v1 custom dictionary. The result is a fresh slice (EncodeAll appends to
// a nil dst), safe to hand to a websocket write without aliasing src.
func compressFrame(src []byte) []byte {
	return zstdEncoder.EncodeAll(src, nil)
}

// zstdDictionaryV2 is the /subscribe-v2 dictionary, trained on live
// firehose traffic in the v2 wire shape (which the v1 dictionary predates:
// it has never seen record_cbor, seq, or current lexicons and manages only
// ~1.67x on v2 frames vs this dictionary's ~2.5x). Retrain with
// `just train-subscribe-dict`; the embedded dictionary ID (the trainer
// defaults it to the training date, YYYYMMDD) versions the artifact, and
// clients fetch the bytes by that ID via the getZstdDictionary XRPC
// endpoint rather than compiling them in. See
// specs/notes/2026-07-09-subscribe-compression-cpu-analysis.md.
//
//go:embed zstd_dictionary_v2
var zstdDictionaryV2 []byte

// DictionaryV2 exposes the current /subscribe-v2 dictionary bytes for the
// download endpoint. Treat as read-only.
func DictionaryV2() []byte { return zstdDictionaryV2 }

// DictionaryV2ID is the dictionary ID embedded in the v2 dictionary,
// parsed from its header at init (RFC 8878 §5: magic 0xEC30A437 then a
// little-endian uint32 ID). The negotiation query param and the download
// endpoint both key on this value.
var DictionaryV2ID = mustParseDictID(zstdDictionaryV2)

func mustParseDictID(d []byte) uint32 {
	const dictMagic = 0xEC30A437
	if len(d) < 8 || binary.LittleEndian.Uint32(d[:4]) != dictMagic {
		panic("subscribe: zstd_dictionary_v2 is not a structured zstd dictionary")
	}
	id := binary.LittleEndian.Uint32(d[4:8])
	if id == 0 {
		panic("subscribe: zstd_dictionary_v2 has dictionary ID 0")
	}
	return id
}

// zstdEncoderV2 is the process-wide encoder for /subscribe-v2 zstd frames.
// Unlike the v1 encoder it uses SpeedFastest: measured on live traffic the
// level costs ~1% ratio for ~3x less CPU per message (the per-message cost
// of dictionary encoding is dominated by match-table Reset, not the
// compression itself). Same concurrency contract as zstdEncoder.
var zstdEncoderV2 = mustNewZstdEncoderV2()

func mustNewZstdEncoderV2() *zstd.Encoder {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderDict(zstdDictionaryV2),
		zstd.WithWindowSize(1<<17),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		panic(fmt.Sprintf("subscribe: build v2 zstd encoder: %v", err))
	}
	return enc
}

// compressFrameV2 is compressFrame for the /subscribe-v2 dictionary.
func compressFrameV2(src []byte) []byte {
	return zstdEncoderV2.EncodeAll(src, nil)
}

// WarmEncoder forces the package-global v1 zstd encoder to create its internal
// worker-pool channel now, outside any testing/synctest bubble. See
// segment.WarmEncoder for the full rationale: klauspost/compress builds that
// channel lazily on the first EncodeAll, so the first in-bubble compressFrame
// would otherwise bind the global channel to the bubble and a later
// out-of-bubble EncodeAll would fatal "receive on synctest channel from
// outside bubble". Test-support only; cheap and idempotent.
func WarmEncoder() {
	_ = compressFrame(nil)
	_ = compressFrameV2(nil)
}
