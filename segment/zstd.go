package segment

import (
	"fmt"

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

	blockDecoder, err = zstd.NewReader(nil,
		zstd.WithDecoderMaxMemory(maxDecodedBlockBytes),
	)
	if err != nil {
		panic(fmt.Sprintf("segment: zstd decoder init failed: %v", err))
	}
}

// encodeBlockCompressed encodes events with encodeBlock, then wraps
// the result in a single zstd frame with content checksums enabled.
func encodeBlockCompressed(events []Event) ([]byte, error) {
	body, err := encodeBlock(events)
	if err != nil {
		return nil, err
	}

	return blockEncoder.EncodeAll(body, nil), nil
}

// decodeBlockCompressed is the inverse: decompress, then decodeBlock.
func decodeBlockCompressed(frame []byte) ([]Event, error) {
	body, err := blockDecoder.DecodeAll(frame, nil)
	if err != nil {
		return nil, fmt.Errorf("segment: zstd decompress: %w", err)
	}

	return decodeBlock(body)
}
