package segment

import (
	"fmt"
	"runtime"

	"github.com/klauspost/compress/zstd"
)

var (
	blockEncoder *zstd.Encoder
	blockDecoder *zstd.Decoder
)

// maxDecodedBlockBytes caps the uncompressed size of any single
// block frame handed to the zstd decoder. Without this, the
// klauspost/compress default of 64 GiB lets a hostile or corrupt
// frame decompress to gigabytes before any of decodeBlock's careful
// length-validation runs (a classic "zstd bomb"). The cap is
// generous: a legitimate block is bounded above by the segment
// file's ~256 MB target (DESIGN.md §3.1.1), so 1 GiB leaves headroom
// for the segment-size knob without giving an attacker a runway.
const maxDecodedBlockBytes uint64 = 1 << 30 // 1 GiB

func init() {
	var err error

	blockEncoder, err = zstd.NewWriter(
		nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(true),
	)
	if err != nil {
		panic(fmt.Sprintf("segment: zstd encoder init failed: %v", err))
	}

	// WithDecoderConcurrency raises the pool of block decoders DecodeAll draws
	// from. klauspost defaults this to min(GOMAXPROCS, 4): a hard cap of 4 that
	// silently serializes concurrent DecodeAll callers beyond 4, throttling the
	// client's parallel backfill decode to ~4 cores regardless of its configured
	// concurrency. Sizing the pool to GOMAXPROCS lets the parallel-decode
	// pipeline actually scale; each pooled blockDec is cheap and idle ones cost
	// nothing, and the server's seal/scan callers only benefit from the extra
	// capacity. (DecodeAll is already safe for concurrent use across goroutines.)
	blockDecoder, err = zstd.NewReader(nil,
		zstd.WithDecoderMaxMemory(maxDecodedBlockBytes),
		zstd.WithDecoderConcurrency(runtime.GOMAXPROCS(0)),
	)
	if err != nil {
		panic(fmt.Sprintf("segment: zstd decoder init failed: %v", err))
	}
}

// encodeBlockCompressed encodes events with encodeBlock, then wraps
// the result in a single zstd frame with content checksums enabled.
func encodeBlockCompressed(events []Event) ([]byte, error) {
	frame, _, err := encodeBlockCompressedSized(events)
	return frame, err
}

// encodeBlockCompressedSized is encodeBlockCompressed returning the
// uncompressed body length alongside the frame, so callers that need
// both (the rewrite path's block-index entries) encode exactly once.
func encodeBlockCompressedSized(events []Event) ([]byte, int, error) {
	body, err := encodeBlock(events)
	if err != nil {
		return nil, 0, err
	}

	return blockEncoder.EncodeAll(body, nil), len(body), nil
}

func encodeEmptyBlockCompressed() []byte {
	return blockEncoder.EncodeAll(encodeEmptyBlock(), nil)
}

// decodeBlockCompressed is the inverse: decompress, then decodeBlock.
//
// We pass a fresh `nil` dst to DecodeAll so body is a private
// allocation; that satisfies decodeBlock's buffer-aliasing contract
// (the returned events alias body for their string columns and we
// never mutate body afterwards).
func decodeBlockCompressed(frame []byte) ([]Event, error) {
	body, err := blockDecoder.DecodeAll(frame, nil)
	if err != nil {
		return nil, fmt.Errorf("segment: zstd decompress: %w", err)
	}

	return decodeBlock(body)
}

// decodeBlockCompressedSized is like decodeBlockCompressed but also
// returns the decompressed body length. Seal needs the size to
// populate BlockInfo.UncompressedSize for the block index without a
// second decompress.
//
// The buffer-aliasing contract from decodeBlock applies: the returned
// events alias the (private) decompressed body for their string and
// payload columns. Callers that need to retain string fields beyond
// the events' lifetime must clone.
func decodeBlockCompressedSized(frame []byte) ([]Event, int, error) {
	body, err := blockDecoder.DecodeAll(frame, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("segment: zstd decompress: %w", err)
	}
	events, err := decodeBlock(body)
	if err != nil {
		return nil, 0, err
	}
	return events, len(body), nil
}
