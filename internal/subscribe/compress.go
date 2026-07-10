package subscribe

import (
	"fmt"
	"runtime"

	_ "embed"

	"github.com/bluesky-social/jetstream/internal/zstddict"
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

// encoderPoolLimit bounds each endpoint's encoder free list. One lane per
// core covers the worst realistic demand (every subscriber goroutine
// compressing simultaneously); the cap keeps worst-case encoder-state
// memory bounded (~2 MB per encoder) on very-many-core machines.
var encoderPoolLimit = min(runtime.GOMAXPROCS(0), 32)

// zstdEncoders is the encoder free list for the v1 compatibility scheme.
// Each pooled encoder is configured EXACTLY as jetstream-legacy's
// (pkg/consumer/consumer.go: WithEncoderDict + 128 KiB window +
// single-goroutine concurrency, default level) so frames are
// byte-compatible with v1 decoders — pooling changes how many encoder
// instances exist, never the bytes any one of them produces (pinned by
// TestCompressFrame_PooledOutputMatchesReferenceEncoder). Pooling replaced
// a process-wide WithEncoderConcurrency(1) singleton that serialized every
// subscriber's compression behind one encoder state (#295).
var zstdEncoders = newEncoderPool(encoderPoolLimit, mustNewZstdEncoder)

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
	return zstdEncoders.encodeAll(src)
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

// ZstdDictRejectedMarker is the stable substring every /subscribe-v2
// dictionary-ID rejection carries in its pre-upgrade HTTP 400 body. The
// thick client substring-matches it (it cannot import this package; see
// CursorTooOldMarker for the precedent and the contract test that pins the
// duplicate) to distinguish "the server rotated its dictionary — refetch
// and reconnect" from an unrelated 400. Do not reword without updating the
// client's duplicated literal.
const ZstdDictRejectedMarker = "unknown zstd dictionary id"

// DictionaryV2ID is the dictionary ID embedded in the v2 dictionary,
// parsed from its header at init. The negotiation query param and the
// download endpoint both key on this value.
var DictionaryV2ID = mustParseDictID(zstdDictionaryV2)

func mustParseDictID(d []byte) uint32 {
	id, err := zstddict.ParseID(d)
	if err != nil {
		// The dictionary is embedded at build time; failure is a
		// build/programmer error, not runtime input. Fail loud.
		panic(fmt.Sprintf("subscribe: zstd_dictionary_v2: %v", err))
	}
	return id
}

// zstdEncodersV2 is the encoder free list for /subscribe-v2 zstd frames.
// Unlike the v1 encoders these use SpeedFastest: measured on live traffic
// the level costs ~1% ratio for ~3x less CPU per message (the per-message
// cost of dictionary encoding is dominated by match-table Reset, not the
// compression itself). Same pooling rationale as zstdEncoders.
var zstdEncodersV2 = newEncoderPool(encoderPoolLimit, mustNewZstdEncoderV2)

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
	return zstdEncodersV2.encodeAll(src)
}

// WarmEncoder pre-creates (and first-uses) every pooled zstd encoder now,
// outside any testing/synctest bubble. See segment.WarmEncoder for the
// full rationale: klauspost/compress builds an encoder-internal channel
// lazily on the first EncodeAll, so an encoder first used inside a bubble
// binds that channel to the bubble and a later out-of-bubble EncodeAll
// fatals "receive on synctest channel from outside bubble". Filling the
// free lists to capacity here means in-bubble compressions only ever draw
// bubble-safe encoders. Test-support only; idempotent.
func WarmEncoder() {
	zstdEncoders.warm()
	zstdEncodersV2.warm()
}
