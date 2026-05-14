package segment

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
)

var (
	blockEncoder *zstd.Encoder
	blockDecoder *zstd.Decoder
)

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

	blockDecoder, err = zstd.NewReader(nil)
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
