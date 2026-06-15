package subscribe

import (
	_ "embed"
	"fmt"

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
