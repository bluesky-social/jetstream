package segment

// Stats is the cheap aggregate-size view of a segment file.
// Used by the status page to sum compressed and uncompressed bytes
// across an entire segment tree without decompressing blocks.
type Stats struct {
	Path              string
	FileSize          int64
	Sealed            bool
	CompressedBytes   int64
	UncompressedBytes int64
}

// QuickStats reads enough of the file at path to populate a
// Stats: the 256-byte header (to decide sealed/active and find
// the block index), and the block index (sealed) or framed-block
// walk (active). No decompression.
//
// Implementation note: this is a thin wrapper over Inspect. Inspect
// already does exactly the right work — sealed-file path is just a
// header parse + block-index decode, no decompression — and it's
// well-tested. If profiling later shows this is hot, replace with a
// minimal direct reader that skips per-block-collections decoding.
func QuickStats(path string) (Stats, error) {
	ins, err := Inspect(path)
	if err != nil {
		return Stats{}, err
	}
	var comp, uncomp int64
	for _, b := range ins.Blocks {
		comp += int64(b.CompressedSize)
		uncomp += int64(b.UncompressedSize)
	}
	return Stats{
		Path:              path,
		FileSize:          ins.FileSize,
		Sealed:            ins.Sealed,
		CompressedBytes:   comp,
		UncompressedBytes: uncomp,
	}, nil
}
